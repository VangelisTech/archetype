# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""SQLite reference control plane for durable activities.

This catalog is intentionally separate from ``ControlCatalog``.  It proves
single-host restart and concurrency semantics without claiming parity from the
remote control plane before that backend implements the same crash contracts.
"""

from __future__ import annotations

import asyncio
import math
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

from archetype.core.config import StorageConfig
from archetype.storage.activity_catalog.records import (
    ActivityAdmissionRecord,
    ActivityCatalogClaimError,
    ActivityCatalogConflictError,
    ActivityCatalogNotFoundError,
    ActivityClaimRecord,
    ActivityRecord,
)
from archetype.storage.catalog.sqlite import catalog_path_for
from archetype.storage.config import ControlCatalogConfig

_SCHEMA_VERSION = 1

_DDL = f"""
CREATE TABLE IF NOT EXISTS activity_catalog_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS activities (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    source_world_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_tick INTEGER NOT NULL,
    source_visibility_token TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    result_ref TEXT,
    result_digest TEXT,
    result_media_type TEXT,
    result_size_bytes INTEGER,
    result_attempt INTEGER,
    result_fence INTEGER,
    result_recorded_at TEXT,
    observed_world_id TEXT,
    observed_run_id TEXT,
    observed_tick INTEGER,
    observed_visibility_token TEXT,
    observed_result_digest TEXT,
    observed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (source_world_id, kind, activity_id)
);
CREATE INDEX IF NOT EXISTS activities_unobserved_result_idx
    ON activities (source_world_id, kind, observed_at, sequence)
    WHERE result_ref IS NOT NULL;
CREATE TABLE IF NOT EXISTS activity_provider_operations (
    provider TEXT NOT NULL,
    provider_operation_id TEXT NOT NULL,
    source_world_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    bound_at TEXT NOT NULL,
    PRIMARY KEY (provider, provider_operation_id),
    FOREIGN KEY (source_world_id, kind, activity_id)
        REFERENCES activities (source_world_id, kind, activity_id)
);
CREATE TABLE IF NOT EXISTS activity_attempts (
    source_world_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    fence INTEGER NOT NULL,
    owner TEXT NOT NULL,
    lease_expires_at REAL,
    provider TEXT,
    provider_operation_id TEXT,
    provider_bound_at TEXT,
    reconciles_attempt INTEGER,
    provider_absence_confirmed_at TEXT,
    retry_guard_ref TEXT,
    retry_guard_digest TEXT,
    authorized_by_reconciliation_attempt INTEGER,
    released_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source_world_id, kind, activity_id, attempt),
    UNIQUE (source_world_id, kind, activity_id, fence),
    FOREIGN KEY (source_world_id, kind, activity_id)
        REFERENCES activities (source_world_id, kind, activity_id)
);
INSERT OR IGNORE INTO activity_catalog_meta (key, value)
    VALUES ('schema_version', '{_SCHEMA_VERSION}');
"""


def activity_catalog_path_for(
    config: StorageConfig,
    catalog_config: ControlCatalogConfig | None = None,
) -> Path:
    """Derive a local activity-catalog path from the same storage identity."""

    control_path = catalog_path_for(config, catalog_config)
    return control_path.with_name(f"{control_path.stem}-activities{control_path.suffix}")


class SqliteActivityCatalog:
    """Single-host durable authority for activity claims and settlement."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 5000) -> None:
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self.path = Path(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    def _connect_sync(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self._busy_timeout_ms / 1000
        delay = 0.005
        while True:
            conn = sqlite3.connect(
                self.path,
                timeout=self._busy_timeout_ms / 1000,
                check_same_thread=False,
            )
            try:
                conn.row_factory = sqlite3.Row
                conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
                conn.execute("PRAGMA foreign_keys=ON")
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_DDL)
                row = conn.execute(
                    "SELECT value FROM activity_catalog_meta WHERE key='schema_version'"
                ).fetchone()
                assert row is not None
                version = int(row["value"])
                if version != _SCHEMA_VERSION:
                    raise RuntimeError(
                        f"activity catalog {self.path} has schema_version={version}, "
                        f"this build expects {_SCHEMA_VERSION}"
                    )
                self._conn = conn
                return conn
            except sqlite3.OperationalError as exc:
                conn.close()
                busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                remaining = deadline - time.monotonic()
                if not busy or remaining <= 0:
                    raise
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.1)
            except BaseException:
                conn.close()
                raise

    async def _run(self, fn, *args):
        async with self._lock:
            worker = asyncio.create_task(asyncio.to_thread(fn, *args))
            cancellation: asyncio.CancelledError | None = None
            while True:
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError as exc:
                    cancellation = cancellation or exc
                    if worker.done():
                        break
                except BaseException:
                    if cancellation is None:
                        raise
                    break
                else:
                    break
            if cancellation is not None:
                try:
                    worker.result()
                except BaseException:
                    # Caller cancellation remains the public outcome, but
                    # retrieving the worker outcome prevents an orphaned task.
                    pass
                raise cancellation
            return worker.result()

    async def close(self) -> None:
        def _close() -> None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

        await self._run(_close)

    async def admit_activity(self, admission: ActivityAdmissionRecord) -> ActivityRecord:
        """Idempotently admit immutable activity content."""

        _require_bounded_text(
            admission.source_visibility_token,
            "activity source visibility token",
            512,
        )

        def _admit() -> ActivityRecord:
            conn = self._connect_sync()
            now = _utcnow()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = _select_activity(
                    conn,
                    admission.source_world_id,
                    admission.kind,
                    admission.activity_id,
                )
                if row is not None:
                    existing = _activity_from_row(row)
                    immutable = (
                        existing.kind,
                        existing.source_run_id,
                        existing.source_tick,
                        existing.source_visibility_token,
                        existing.input_ref,
                        existing.input_digest,
                    )
                    requested = (
                        admission.kind,
                        admission.source_run_id,
                        admission.source_tick,
                        admission.source_visibility_token,
                        admission.input_ref,
                        admission.input_digest,
                    )
                    if immutable != requested:
                        raise ActivityCatalogConflictError(
                            f"activity ({admission.source_world_id}, {admission.kind}, "
                            f"{admission.activity_id}) already has different immutable content"
                        )
                    return existing
                conn.execute(
                    "INSERT INTO activities "
                    "(source_world_id, activity_id, kind, source_run_id, source_tick, "
                    "source_visibility_token, input_ref, input_digest, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        admission.source_world_id,
                        admission.activity_id,
                        admission.kind,
                        admission.source_run_id,
                        admission.source_tick,
                        admission.source_visibility_token,
                        admission.input_ref,
                        admission.input_digest,
                        now,
                        now,
                    ),
                )
                inserted = _select_activity(
                    conn,
                    admission.source_world_id,
                    admission.kind,
                    admission.activity_id,
                )
                assert inserted is not None
                return _activity_from_row(inserted)

        return await self._run(_admit)

    async def get_activity(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
    ) -> ActivityRecord | None:
        def _get() -> ActivityRecord | None:
            row = _select_activity(self._connect_sync(), world_id, kind, activity_id)
            return _activity_from_row(row) if row is not None else None

        return await self._run(_get)

    async def claim_activity(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaimRecord:
        """Claim safe execution or provider-specific reconciliation.

        Expiry of a provider-bound attempt never produces an ordinary
        execution claim.  The new fenced claim points at the unresolved
        provider operation and authorizes reconciliation only.
        """

        _require_bounded_text(owner, "activity claim owner", 512)
        _require_lease_seconds(lease_seconds)

        def _claim() -> ActivityClaimRecord:
            conn = self._connect_sync()
            now_seconds = time.time()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                activity_row = _select_activity(conn, world_id, kind, activity_id)
                if activity_row is None:
                    raise ActivityCatalogNotFoundError((world_id, kind, activity_id))
                activity = _activity_from_row(activity_row)
                latest = _latest_attempt(conn, world_id, kind, activity_id)

                if activity.result_ref is not None:
                    return _claim_from_row(
                        conn,
                        activity,
                        latest,
                        acquired=False,
                    )

                if latest is not None:
                    live = (
                        latest["released_at"] is None
                        and latest["lease_expires_at"] is not None
                        and float(latest["lease_expires_at"]) > now_seconds
                    )
                    if live:
                        return _claim_from_row(
                            conn,
                            activity,
                            latest,
                            acquired=False,
                        )

                unresolved = conn.execute(
                    "SELECT provider_attempt.* FROM activity_attempts AS provider_attempt "
                    "WHERE provider_attempt.source_world_id=? "
                    "AND provider_attempt.kind=? "
                    "AND provider_attempt.activity_id=? "
                    "AND provider_attempt.provider_operation_id IS NOT NULL "
                    "AND NOT EXISTS ("
                    "SELECT 1 FROM activity_attempts AS reconciliation "
                    "WHERE reconciliation.source_world_id=provider_attempt.source_world_id "
                    "AND reconciliation.kind=provider_attempt.kind "
                    "AND reconciliation.activity_id=provider_attempt.activity_id "
                    "AND reconciliation.reconciles_attempt=provider_attempt.attempt "
                    "AND reconciliation.provider_absence_confirmed_at IS NOT NULL"
                    ") ORDER BY provider_attempt.attempt DESC LIMIT 1",
                    (world_id, kind, activity_id),
                ).fetchone()
                attempt = int(latest["attempt"]) + 1 if latest is not None else 1
                fence_row = conn.execute(
                    "SELECT MAX(fence) AS fence FROM activity_attempts "
                    "WHERE source_world_id=? AND kind=? AND activity_id=?",
                    (world_id, kind, activity_id),
                ).fetchone()
                fence = int(fence_row["fence"] or 0) + 1
                now = _utcnow()
                conn.execute(
                    "INSERT INTO activity_attempts "
                    "(source_world_id, kind, activity_id, attempt, fence, owner, "
                    "lease_expires_at, reconciles_attempt, retry_guard_ref, "
                    "retry_guard_digest, authorized_by_reconciliation_attempt, "
                    "created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        world_id,
                        kind,
                        activity_id,
                        attempt,
                        fence,
                        owner,
                        now_seconds + lease_seconds,
                        int(unresolved["attempt"]) if unresolved is not None else None,
                        (
                            str(latest["retry_guard_ref"])
                            if latest is not None and latest["retry_guard_ref"] is not None
                            else None
                        ),
                        (
                            str(latest["retry_guard_digest"])
                            if latest is not None and latest["retry_guard_digest"] is not None
                            else None
                        ),
                        (
                            int(latest["authorized_by_reconciliation_attempt"])
                            if latest is not None
                            and latest["authorized_by_reconciliation_attempt"] is not None
                            else None
                        ),
                        now,
                        now,
                    ),
                )
                inserted = _attempt(conn, world_id, kind, activity_id, attempt)
                assert inserted is not None
                return _claim_from_row(
                    conn,
                    activity,
                    inserted,
                    acquired=True,
                )

        return await self._run(_claim)

    async def bind_provider_operation(
        self,
        claim: ActivityClaimRecord,
        provider: str,
        operation_id: str,
    ) -> ActivityClaimRecord:
        """Durably bind provider identity before the external invocation."""

        _require_bounded_text(provider, "activity provider", 255)
        _require_bounded_text(operation_id, "provider operation_id", 1024)

        def _bind() -> ActivityClaimRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                activity = _require_activity(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                )
                row = _require_live_claim(conn, claim)
                if row["reconciles_attempt"] is not None:
                    raise ActivityCatalogClaimError(
                        "a reconciliation claim cannot invoke a new provider operation"
                    )
                authorization_attempt = row["authorized_by_reconciliation_attempt"]
                if authorization_attempt is not None:
                    reconciliation = _attempt(
                        conn,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        int(authorization_attempt),
                    )
                    if reconciliation is None or reconciliation["reconciles_attempt"] is None:
                        raise ActivityCatalogConflictError(
                            "activity retry authorization provenance is incomplete"
                        )
                    provider_attempt = _attempt(
                        conn,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        int(reconciliation["reconciles_attempt"]),
                    )
                    expected = (
                        provider_attempt["provider"] if provider_attempt is not None else None,
                        (
                            provider_attempt["provider_operation_id"]
                            if provider_attempt is not None
                            else None
                        ),
                    )
                    if None in expected:
                        raise ActivityCatalogConflictError(
                            "activity retry authorization has no provider identity"
                        )
                    if (provider, operation_id) != expected:
                        raise ActivityCatalogConflictError(
                            "retry-authorized activity attempt must bind the reconciled "
                            "provider operation identity"
                        )
                existing = (row["provider"], row["provider_operation_id"])
                requested = (provider, operation_id)
                if existing[1] is not None:
                    if existing != requested:
                        raise ActivityCatalogConflictError(
                            "activity attempt already has a different provider operation"
                        )
                    _bind_provider_identity(
                        conn,
                        claim,
                        provider=provider,
                        operation_id=operation_id,
                    )
                    return _claim_from_row(conn, activity, row, acquired=True)
                now = _utcnow()
                _bind_provider_identity(
                    conn,
                    claim,
                    provider=provider,
                    operation_id=operation_id,
                    bound_at=now,
                )
                conn.execute(
                    "UPDATE activity_attempts SET provider=?, provider_operation_id=?, "
                    "provider_bound_at=?, updated_at=? WHERE source_world_id=? "
                    "AND kind=? AND activity_id=? AND attempt=?",
                    (
                        provider,
                        operation_id,
                        now,
                        now,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        claim.attempt,
                    ),
                )
                updated = _attempt(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                    _required_int(claim.attempt),
                )
                assert updated is not None
                return _claim_from_row(conn, activity, updated, acquired=True)

        return await self._run(_bind)

    async def confirm_provider_operation_absent(
        self,
        claim: ActivityClaimRecord,
        retry_guard_ref: str,
        retry_guard_digest: str,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaimRecord:
        """Record provider-confirmed absence and mint a fresh execution claim.

        The provider adapter owns the evidence and decision behind this call.
        The catalog only preserves the reconciliation fact before authorizing
        another fenced attempt.
        """

        _require_bounded_text(retry_guard_ref, "activity retry guard ref", 4096)
        _require_bounded_text(retry_guard_digest, "activity retry guard digest", 255)
        _require_lease_seconds(lease_seconds)

        def _confirm() -> ActivityClaimRecord:
            conn = self._connect_sync()
            now_seconds = time.time()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                activity = _require_activity(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                )
                if activity.result_ref is not None:
                    raise ActivityCatalogClaimError(
                        "a completed activity cannot confirm provider absence"
                    )
                claim_attempt = _required_int(claim.attempt)
                existing = _attempt(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                    claim_attempt,
                )
                if existing is not None and existing["provider_absence_confirmed_at"] is not None:
                    existing_guard = (
                        existing["retry_guard_ref"],
                        existing["retry_guard_digest"],
                    )
                    requested_guard = (retry_guard_ref, retry_guard_digest)
                    if existing_guard != requested_guard:
                        raise ActivityCatalogConflictError(
                            "provider absence was confirmed with a different retry guard"
                        )
                    authorized = conn.execute(
                        "SELECT * FROM activity_attempts WHERE source_world_id=? "
                        "AND kind=? AND activity_id=? "
                        "AND authorized_by_reconciliation_attempt=? "
                        "ORDER BY attempt DESC LIMIT 1",
                        (
                            claim.activity.source_world_id,
                            claim.activity.kind,
                            claim.activity.activity_id,
                            claim_attempt,
                        ),
                    ).fetchone()
                    latest = _latest_attempt(
                        conn,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                    )
                    exact_retry = (
                        claim.acquired
                        and authorized is not None
                        and latest is not None
                        and int(latest["attempt"]) == int(authorized["attempt"])
                        and existing["owner"] == claim.owner
                        and int(existing["fence"]) == claim.fence
                        and authorized["owner"] == claim.owner
                        and authorized["released_at"] is None
                        and authorized["lease_expires_at"] is not None
                        and float(authorized["lease_expires_at"]) > now_seconds
                    )
                    if exact_retry:
                        conn.execute(
                            "UPDATE activity_attempts SET lease_expires_at=?, updated_at=? "
                            "WHERE source_world_id=? AND kind=? AND activity_id=? "
                            "AND attempt=?",
                            (
                                now_seconds + lease_seconds,
                                _utcnow(),
                                claim.activity.source_world_id,
                                claim.activity.kind,
                                claim.activity.activity_id,
                                int(authorized["attempt"]),
                            ),
                        )
                        renewed = _attempt(
                            conn,
                            claim.activity.source_world_id,
                            claim.activity.kind,
                            claim.activity.activity_id,
                            int(authorized["attempt"]),
                        )
                        assert renewed is not None
                        return _claim_from_row(
                            conn,
                            activity,
                            renewed,
                            acquired=True,
                        )
                    raise ActivityCatalogClaimError("provider-absence confirmation claim is stale")

                reconciliation = _require_live_claim(conn, claim)
                reconciles_attempt = reconciliation["reconciles_attempt"]
                if reconciles_attempt is None:
                    raise ActivityCatalogClaimError(
                        "provider absence requires a reconciliation claim"
                    )
                provider_attempt = _attempt(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                    int(reconciles_attempt),
                )
                if provider_attempt is None or provider_attempt["provider_operation_id"] is None:
                    raise ActivityCatalogClaimError(
                        "reconciliation claim has no provider operation identity"
                    )

                now = _utcnow()
                conn.execute(
                    "UPDATE activity_attempts SET provider_absence_confirmed_at=?, "
                    "retry_guard_ref=?, retry_guard_digest=?, lease_expires_at=NULL, "
                    "released_at=?, updated_at=? WHERE source_world_id=? AND kind=? "
                    "AND activity_id=? AND attempt=?",
                    (
                        now,
                        retry_guard_ref,
                        retry_guard_digest,
                        now,
                        now,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        claim_attempt,
                    ),
                )
                latest = _latest_attempt(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                )
                assert latest is not None
                next_attempt = int(latest["attempt"]) + 1
                next_fence = int(latest["fence"]) + 1
                conn.execute(
                    "INSERT INTO activity_attempts "
                    "(source_world_id, kind, activity_id, attempt, fence, owner, "
                    "lease_expires_at, retry_guard_ref, retry_guard_digest, "
                    "authorized_by_reconciliation_attempt, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        next_attempt,
                        next_fence,
                        claim.owner,
                        now_seconds + lease_seconds,
                        retry_guard_ref,
                        retry_guard_digest,
                        claim_attempt,
                        now,
                        now,
                    ),
                )
                authorized = _attempt(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                    next_attempt,
                )
                assert authorized is not None
                return _claim_from_row(
                    conn,
                    activity,
                    authorized,
                    acquired=True,
                )

        return await self._run(_confirm)

    async def record_activity_result(
        self,
        claim: ActivityClaimRecord,
        *,
        result_ref: str,
        result_digest: str,
        result_media_type: str,
        result_size_bytes: int,
    ) -> ActivityRecord:
        """Record a bounded result reference under the current fence."""

        requested = (
            result_ref,
            result_digest,
            result_media_type,
            result_size_bytes,
        )

        def _record() -> ActivityRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                activity = _require_activity(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                )
                if activity.result_ref is not None:
                    existing = (
                        activity.result_ref,
                        activity.result_digest,
                        activity.result_media_type,
                        activity.result_size_bytes,
                    )
                    if existing != requested:
                        raise ActivityCatalogConflictError(
                            "activity already has a different durable result"
                        )
                    return activity

                attempt_row = _require_live_claim(conn, claim)
                if (
                    attempt_row["provider_operation_id"] is None
                    and attempt_row["reconciles_attempt"] is None
                ):
                    raise ActivityCatalogClaimError(
                        "recording a result requires a pre-bound provider operation "
                        "or a reconciliation claim"
                    )
                now = _utcnow()
                conn.execute(
                    "UPDATE activities SET result_ref=?, result_digest=?, "
                    "result_media_type=?, result_size_bytes=?, result_attempt=?, "
                    "result_fence=?, result_recorded_at=?, updated_at=? "
                    "WHERE source_world_id=? AND kind=? AND activity_id=?",
                    (
                        result_ref,
                        result_digest,
                        result_media_type,
                        result_size_bytes,
                        claim.attempt,
                        claim.fence,
                        now,
                        now,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                    ),
                )
                conn.execute(
                    "UPDATE activity_attempts SET lease_expires_at=NULL, updated_at=? "
                    "WHERE source_world_id=? AND kind=? AND activity_id=? AND attempt=?",
                    (
                        now,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        claim.attempt,
                    ),
                )
                updated = _select_activity(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                )
                assert updated is not None
                return _activity_from_row(updated)

        return await self._run(_record)

    async def release_activity(self, claim: ActivityClaimRecord) -> None:
        """Release an unbound attempt that performed no external operation."""

        def _release() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = _attempt(
                    conn,
                    claim.activity.source_world_id,
                    claim.activity.kind,
                    claim.activity.activity_id,
                    _required_int(claim.attempt),
                )
                if row is not None and row["released_at"] is not None:
                    if int(row["fence"]) == claim.fence and row["owner"] == claim.owner:
                        return
                row = _require_live_claim(conn, claim)
                if (
                    row["provider_operation_id"] is not None
                    or row["reconciles_attempt"] is not None
                ):
                    raise ActivityCatalogClaimError(
                        "provider-bound or reconciliation work cannot be released for replay"
                    )
                now = _utcnow()
                conn.execute(
                    "UPDATE activity_attempts SET lease_expires_at=NULL, released_at=?, "
                    "updated_at=? WHERE source_world_id=? AND kind=? AND activity_id=? "
                    "AND attempt=?",
                    (
                        now,
                        now,
                        claim.activity.source_world_id,
                        claim.activity.kind,
                        claim.activity.activity_id,
                        claim.attempt,
                    ),
                )

        await self._run(_release)

    async def list_incomplete_activities(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
    ) -> list[ActivityRecord]:
        """Discover admitted work without relying on a process-local queue."""

        return await self._list_by_completion(
            completed=False,
            kind=kind,
            world_id=world_id,
            limit=limit,
        )

    async def list_unobserved_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
    ) -> list[ActivityRecord]:
        """Discover durable results that a later world tick has not observed."""

        return await self._list_by_completion(
            completed=True,
            kind=kind,
            world_id=world_id,
            limit=limit,
        )

    async def has_unsettled_activities(self, world_id: str) -> bool:
        """Whether conservative fork/destroy admission must refuse this world."""

        def _has_unsettled() -> bool:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT 1 FROM activities WHERE source_world_id=? "
                    "AND observed_at IS NULL LIMIT 1",
                    (world_id,),
                )
                .fetchone()
            )
            return row is not None

        return await self._run(_has_unsettled)

    async def _list_by_completion(
        self,
        *,
        completed: bool,
        kind: str | None,
        world_id: str | None,
        limit: int,
    ) -> list[ActivityRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")

        def _list() -> list[ActivityRecord]:
            conditions = (
                ["result_ref IS NOT NULL", "observed_at IS NULL"]
                if completed
                else ["result_ref IS NULL"]
            )
            parameters: list[object] = []
            if kind is not None:
                conditions.append("kind=?")
                parameters.append(kind)
            if world_id is not None:
                conditions.append("source_world_id=?")
                parameters.append(world_id)
            parameters.append(limit)
            rows = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM activities WHERE "
                    + " AND ".join(conditions)
                    + " ORDER BY sequence LIMIT ?",
                    parameters,
                )
                .fetchall()
            )
            return [_activity_from_row(row) for row in rows]

        return await self._run(_list)

    async def settle_activity_observation(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        *,
        observed_world_id: str,
        observed_run_id: str,
        observed_tick: int,
        observed_visibility_token: str | None,
        expected_result_digest: str,
    ) -> ActivityRecord:
        """Bind a result to the exact later commit that observed it."""

        _require_bounded_text(
            observed_visibility_token,
            "activity observation visibility token",
            512,
        )
        _require_bounded_text(
            expected_result_digest,
            "activity observation result digest",
            255,
        )
        requested = (
            observed_world_id,
            observed_run_id,
            observed_tick,
            observed_visibility_token,
            expected_result_digest,
        )

        def _settle() -> ActivityRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                activity = _require_activity(conn, world_id, kind, activity_id)
                if activity.result_ref is None:
                    raise ActivityCatalogConflictError(
                        "an activity cannot be observed before its result is durable"
                    )
                if activity.result_digest != expected_result_digest:
                    raise ActivityCatalogConflictError(
                        "activity observation does not bind the durable result digest"
                    )
                if (
                    observed_world_id != activity.source_world_id
                    or observed_run_id != activity.source_run_id
                ):
                    raise ActivityCatalogConflictError(
                        "activity observation must commit in its source world and run"
                    )
                if observed_tick <= activity.source_tick:
                    raise ActivityCatalogConflictError(
                        "activity observation must be committed by a later tick"
                    )
                if activity.observed_world_id is not None:
                    existing = (
                        activity.observed_world_id,
                        activity.observed_run_id,
                        activity.observed_tick,
                        activity.observed_visibility_token,
                        activity.observed_result_digest,
                    )
                    if existing != requested:
                        raise ActivityCatalogConflictError(
                            "activity already has a different observation settlement"
                        )
                    return activity
                now = _utcnow()
                conn.execute(
                    "UPDATE activities SET observed_world_id=?, observed_run_id=?, "
                    "observed_tick=?, observed_visibility_token=?, "
                    "observed_result_digest=?, observed_at=?, updated_at=? "
                    "WHERE source_world_id=? AND kind=? AND activity_id=?",
                    (
                        observed_world_id,
                        observed_run_id,
                        observed_tick,
                        observed_visibility_token,
                        expected_result_digest,
                        now,
                        now,
                        world_id,
                        kind,
                        activity_id,
                    ),
                )
                updated = _select_activity(conn, world_id, kind, activity_id)
                assert updated is not None
                return _activity_from_row(updated)

        return await self._run(_settle)


def _select_activity(
    conn: sqlite3.Connection,
    world_id: str,
    kind: str,
    activity_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM activities WHERE source_world_id=? AND kind=? AND activity_id=?",
        (world_id, kind, activity_id),
    ).fetchone()


def _require_activity(
    conn: sqlite3.Connection,
    world_id: str,
    kind: str,
    activity_id: str,
) -> ActivityRecord:
    row = _select_activity(conn, world_id, kind, activity_id)
    if row is None:
        raise ActivityCatalogNotFoundError((world_id, kind, activity_id))
    return _activity_from_row(row)


def _latest_attempt(
    conn: sqlite3.Connection,
    world_id: str,
    kind: str,
    activity_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM activity_attempts WHERE source_world_id=? AND kind=? AND activity_id=? "
        "ORDER BY attempt DESC LIMIT 1",
        (world_id, kind, activity_id),
    ).fetchone()


def _attempt(
    conn: sqlite3.Connection,
    world_id: str,
    kind: str,
    activity_id: str,
    attempt: int,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM activity_attempts WHERE source_world_id=? AND kind=? "
        "AND activity_id=? AND attempt=?",
        (world_id, kind, activity_id, attempt),
    ).fetchone()


def _require_live_claim(
    conn: sqlite3.Connection,
    claim: ActivityClaimRecord,
) -> sqlite3.Row:
    if not claim.acquired:
        raise ActivityCatalogClaimError("activity claim was not acquired")
    attempt = _required_int(claim.attempt)
    row = _attempt(
        conn,
        claim.activity.source_world_id,
        claim.activity.kind,
        claim.activity.activity_id,
        attempt,
    )
    if row is None:
        raise ActivityCatalogClaimError("activity claim attempt does not exist")
    latest = _latest_attempt(
        conn,
        claim.activity.source_world_id,
        claim.activity.kind,
        claim.activity.activity_id,
    )
    exact = (
        latest is not None
        and int(latest["attempt"]) == attempt
        and int(row["fence"]) == claim.fence
        and row["owner"] == claim.owner
    )
    if not exact:
        raise ActivityCatalogClaimError("activity claim is stale")
    if row["released_at"] is not None:
        raise ActivityCatalogClaimError("activity claim was released")
    expires = row["lease_expires_at"]
    if expires is None or float(expires) <= time.time():
        raise ActivityCatalogClaimError("activity claim lease expired")
    return row


def _claim_from_row(
    conn: sqlite3.Connection,
    activity: ActivityRecord,
    attempt_row: sqlite3.Row | None,
    *,
    acquired: bool,
) -> ActivityClaimRecord:
    if attempt_row is None:
        return ActivityClaimRecord(
            activity=activity,
            acquired=False,
            attempt=None,
            fence=None,
            owner=None,
            lease_expires_at=None,
            provider=None,
            provider_operation_id=None,
            retry_guard_ref=None,
            retry_guard_digest=None,
            reconciles_attempt=None,
            reconciles_provider=None,
            reconciles_provider_operation_id=None,
        )
    reconciles_attempt = attempt_row["reconciles_attempt"]
    reconciles = (
        _attempt(
            conn,
            activity.source_world_id,
            activity.kind,
            activity.activity_id,
            int(reconciles_attempt),
        )
        if reconciles_attempt is not None
        else None
    )
    return ActivityClaimRecord(
        activity=activity,
        acquired=acquired,
        attempt=int(attempt_row["attempt"]),
        fence=int(attempt_row["fence"]),
        owner=str(attempt_row["owner"]),
        lease_expires_at=(
            float(attempt_row["lease_expires_at"])
            if attempt_row["lease_expires_at"] is not None
            else None
        ),
        provider=(str(attempt_row["provider"]) if attempt_row["provider"] is not None else None),
        provider_operation_id=(
            str(attempt_row["provider_operation_id"])
            if attempt_row["provider_operation_id"] is not None
            else None
        ),
        retry_guard_ref=(
            str(attempt_row["retry_guard_ref"])
            if attempt_row["retry_guard_ref"] is not None
            else None
        ),
        retry_guard_digest=(
            str(attempt_row["retry_guard_digest"])
            if attempt_row["retry_guard_digest"] is not None
            else None
        ),
        reconciles_attempt=(int(reconciles_attempt) if reconciles_attempt is not None else None),
        reconciles_provider=(
            str(reconciles["provider"])
            if reconciles is not None and reconciles["provider"] is not None
            else None
        ),
        reconciles_provider_operation_id=(
            str(reconciles["provider_operation_id"])
            if reconciles is not None and reconciles["provider_operation_id"] is not None
            else None
        ),
    )


def _activity_from_row(row: sqlite3.Row) -> ActivityRecord:
    return ActivityRecord(
        activity_id=str(row["activity_id"]),
        kind=str(row["kind"]),
        source_world_id=str(row["source_world_id"]),
        source_run_id=str(row["source_run_id"]),
        source_tick=int(row["source_tick"]),
        source_visibility_token=(
            str(row["source_visibility_token"])
            if row["source_visibility_token"] is not None
            else None
        ),
        input_ref=str(row["input_ref"]),
        input_digest=str(row["input_digest"]),
        result_ref=str(row["result_ref"]) if row["result_ref"] is not None else None,
        result_digest=(str(row["result_digest"]) if row["result_digest"] is not None else None),
        result_media_type=(
            str(row["result_media_type"]) if row["result_media_type"] is not None else None
        ),
        result_size_bytes=(
            int(row["result_size_bytes"]) if row["result_size_bytes"] is not None else None
        ),
        result_attempt=(int(row["result_attempt"]) if row["result_attempt"] is not None else None),
        result_fence=(int(row["result_fence"]) if row["result_fence"] is not None else None),
        result_recorded_at=(
            str(row["result_recorded_at"]) if row["result_recorded_at"] is not None else None
        ),
        observed_world_id=(
            str(row["observed_world_id"]) if row["observed_world_id"] is not None else None
        ),
        observed_run_id=(
            str(row["observed_run_id"]) if row["observed_run_id"] is not None else None
        ),
        observed_tick=(int(row["observed_tick"]) if row["observed_tick"] is not None else None),
        observed_visibility_token=(
            str(row["observed_visibility_token"])
            if row["observed_visibility_token"] is not None
            else None
        ),
        observed_result_digest=(
            str(row["observed_result_digest"])
            if row["observed_result_digest"] is not None
            else None
        ),
        observed_at=str(row["observed_at"]) if row["observed_at"] is not None else None,
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _bind_provider_identity(
    conn: sqlite3.Connection,
    claim: ActivityClaimRecord,
    *,
    provider: str,
    operation_id: str,
    bound_at: str | None = None,
) -> None:
    """Reserve one provider operation for exactly one logical Activity."""

    identity = (
        claim.activity.source_world_id,
        claim.activity.kind,
        claim.activity.activity_id,
    )
    conn.execute(
        "INSERT OR IGNORE INTO activity_provider_operations "
        "(provider, provider_operation_id, source_world_id, kind, activity_id, bound_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            provider,
            operation_id,
            *identity,
            bound_at or _utcnow(),
        ),
    )
    row = conn.execute(
        "SELECT source_world_id, kind, activity_id "
        "FROM activity_provider_operations "
        "WHERE provider=? AND provider_operation_id=?",
        (provider, operation_id),
    ).fetchone()
    if row is None:
        raise AssertionError("provider operation reservation disappeared")
    bound_identity = (
        str(row["source_world_id"]),
        str(row["kind"]),
        str(row["activity_id"]),
    )
    if bound_identity != identity:
        raise ActivityCatalogConflictError(
            "provider operation is already bound to another activity"
        )


def _required_int(value: int | None) -> int:
    if value is None:
        raise ActivityCatalogClaimError("activity claim lacks attempt identity")
    return value


def _require_bounded_text(value: str | None, field_name: str, max_chars: int) -> None:
    if value is None:
        raise ValueError(f"{field_name} must be non-empty")
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    if len(value) > max_chars:
        raise ValueError(f"{field_name} must be at most {max_chars} characters")


def _require_lease_seconds(value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("lease_seconds must be a number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("lease_seconds must be finite and positive")


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


__all__ = ["SqliteActivityCatalog", "activity_catalog_path_for"]
