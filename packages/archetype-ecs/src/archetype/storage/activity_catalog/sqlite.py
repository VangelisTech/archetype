# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""SQLite index for ECS activity admission and settlement evidence."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from archetype.core.config import StorageConfig
from archetype.storage.activity_catalog.migration import (
    ActivityCatalogInspectionError,
    ActivityCatalogInventory,
)
from archetype.storage.activity_catalog.records import (
    ActivityAdmissionRecord,
    ActivityCatalogConflictError,
    ActivityCatalogNotFoundError,
    ActivityRecord,
)
from archetype.storage.catalog.sqlite import catalog_path_for
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.hardened_sqlite import HardenedSqliteCatalog

_SCHEMA_VERSION = 2

_MIGRATION_TABLE_COLUMNS = {
    "activity_catalog_meta": ("key", "value"),
    "activities": (
        "sequence",
        "source_world_id",
        "activity_id",
        "kind",
        "source_run_id",
        "source_tick",
        "source_visibility_token",
        "input_ref",
        "input_digest",
        "result_ref",
        "result_digest",
        "result_media_type",
        "result_size_bytes",
        "result_recorded_at",
        "observed_world_id",
        "observed_run_id",
        "observed_tick",
        "observed_visibility_token",
        "observed_result_digest",
        "observed_at",
        "created_at",
        "updated_at",
    ),
    "activity_executions": (
        "source_world_id",
        "kind",
        "activity_id",
        "provider",
        "operation_id",
        "bound_at",
    ),
}

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
CREATE TABLE IF NOT EXISTS activity_executions (
    source_world_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    bound_at TEXT NOT NULL,
    PRIMARY KEY (source_world_id, kind, activity_id),
    UNIQUE (provider, operation_id),
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


def inspect_sqlite_activity_catalog(path: Path) -> ActivityCatalogInventory:
    """Inventory all local Activity history without creating or changing a DB.

    Absence is the one valid uninitialized state.  Once a file exists, an
    unknown schema, missing table, corrupt database, or broken relationship
    fails closed because migration cannot prove the catalog is empty.
    """

    candidate = Path(path)
    if not candidate.exists():
        return ActivityCatalogInventory(
            catalog_present=False,
            schema_version=None,
            activity_count=0,
            attempt_count=0,
            provider_operation_count=0,
        )
    expected_tables = set(_MIGRATION_TABLE_COLUMNS)
    try:
        connection = sqlite3.connect(
            candidate.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=0,
            check_same_thread=False,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA foreign_keys=ON")
            quick_check = connection.execute("PRAGMA quick_check").fetchall()
            if [str(row[0]) for row in quick_check] != ["ok"]:
                raise ActivityCatalogInspectionError(
                    "existing Activity catalog failed SQLite integrity validation"
                )
            observed_tables = {
                str(row["name"])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }
            if observed_tables != expected_tables:
                raise ActivityCatalogInspectionError(
                    "existing Activity catalog has an unsupported table inventory"
                )
            for table_name, expected_columns in _MIGRATION_TABLE_COLUMNS.items():
                observed_columns = tuple(
                    str(row["name"])
                    for row in connection.execute(
                        "SELECT name FROM pragma_table_info(?) ORDER BY cid",
                        (table_name,),
                    ).fetchall()
                )
                if observed_columns != expected_columns:
                    raise ActivityCatalogInspectionError(
                        "existing Activity catalog has an unsupported table schema"
                    )
            version_row = connection.execute(
                "SELECT value FROM activity_catalog_meta WHERE key='schema_version'"
            ).fetchone()
            if version_row is None:
                raise ActivityCatalogInspectionError(
                    "existing Activity catalog has no schema version"
                )
            try:
                schema_version = int(version_row["value"])
            except (TypeError, ValueError):
                raise ActivityCatalogInspectionError(
                    "existing Activity catalog has an invalid schema version"
                ) from None
            if schema_version != _SCHEMA_VERSION:
                raise ActivityCatalogInspectionError(
                    "existing Activity catalog has an unsupported schema version"
                )
            violations = connection.execute("PRAGMA foreign_key_check").fetchone()
            if violations is not None:
                raise ActivityCatalogInspectionError(
                    "existing Activity catalog has invalid record relationships"
                )

            activity_row = connection.execute("SELECT COUNT(*) AS count FROM activities").fetchone()
            assert activity_row is not None

            return ActivityCatalogInventory(
                catalog_present=True,
                schema_version=schema_version,
                activity_count=int(activity_row["count"]),
                attempt_count=0,
                provider_operation_count=0,
            )
        finally:
            connection.close()
    except ActivityCatalogInspectionError:
        raise
    except (OSError, sqlite3.Error):
        raise ActivityCatalogInspectionError(
            "existing Activity catalog could not be inspected read-only"
        ) from None


class SqliteActivityCatalogMigrationInspector:
    """Async endpoint wrapper around the read-only SQLite inventory."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    async def inspect_activity_catalog(self) -> ActivityCatalogInventory:
        return await asyncio.to_thread(inspect_sqlite_activity_catalog, self.path)


class SqliteActivityCatalog(HardenedSqliteCatalog):
    """Strongly consistent ECS admission and settlement index."""

    _DDL = _DDL
    _META_TABLE = "activity_catalog_meta"
    _SCHEMA_VERSION = _SCHEMA_VERSION
    _CATALOG_LABEL = "activity catalog"

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int = 5000,
    ) -> None:
        super().__init__(path, busy_timeout_ms=busy_timeout_ms)

    async def admit_activity(
        self,
        admission: ActivityAdmissionRecord,
        *,
        execution_provider: str | None = None,
        execution_operation_id: str | None = None,
    ) -> ActivityRecord:
        """Idempotently admit immutable activity content."""

        _require_bounded_text(
            admission.source_visibility_token,
            "activity source visibility token",
            512,
        )
        if (execution_provider is None) != (execution_operation_id is None):
            raise ValueError("activity execution identity must be complete")
        if execution_provider is not None:
            _require_bounded_text(execution_provider, "activity execution provider", 255)
            assert execution_operation_id is not None
            _require_bounded_text(execution_operation_id, "activity execution operation_id", 1024)

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
                    _reserve_execution_identity(
                        conn,
                        (admission.source_world_id, admission.kind, admission.activity_id),
                        provider=execution_provider,
                        operation_id=execution_operation_id,
                        allow_unbound=execution_provider is None,
                    )
                    refreshed = _select_activity(
                        conn, admission.source_world_id, admission.kind, admission.activity_id
                    )
                    assert refreshed is not None
                    return _activity_from_row(refreshed)
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
                _reserve_execution_identity(
                    conn,
                    (admission.source_world_id, admission.kind, admission.activity_id),
                    provider=execution_provider,
                    operation_id=execution_operation_id,
                    allow_unbound=execution_provider is None,
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

    async def record_orchestrated_activity_result(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        *,
        provider: str,
        provider_operation_id: str,
        result_ref: str,
        result_digest: str,
        result_media_type: str,
        result_size_bytes: int,
    ) -> ActivityRecord:
        """Record a result for the exact prebound Temporal execution."""

        _require_bounded_text(provider, "activity provider", 255)
        _require_bounded_text(provider_operation_id, "provider operation_id", 1024)
        _require_bounded_text(result_ref, "activity result ref", 4096)
        _require_bounded_text(result_digest, "activity result digest", 255)
        _require_bounded_text(result_media_type, "activity result media_type", 255)
        if isinstance(result_size_bytes, bool) or not isinstance(result_size_bytes, int):
            raise TypeError("activity result size_bytes must be an integer")
        if result_size_bytes < 0:
            raise ValueError("activity result size_bytes must be non-negative")
        requested = (result_ref, result_digest, result_media_type, result_size_bytes)
        identity = (world_id, kind, activity_id)

        def _record() -> ActivityRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                stored = _require_activity(conn, *identity)
                bound = conn.execute(
                    "SELECT provider, operation_id FROM activity_executions "
                    "WHERE source_world_id=? AND kind=? AND activity_id=?",
                    identity,
                ).fetchall()
                requested_operation = (provider, provider_operation_id)
                if not bound:
                    raise ActivityCatalogConflictError(
                        "activity has no prebound orchestration execution identity"
                    )
                if bound and (
                    len(bound) != 1
                    or (str(bound[0]["provider"]), str(bound[0]["operation_id"]))
                    != requested_operation
                ):
                    raise ActivityCatalogConflictError(
                        "activity already has a different orchestration operation identity"
                    )
                if stored.result_ref is not None:
                    existing = (
                        stored.result_ref,
                        stored.result_digest,
                        stored.result_media_type,
                        stored.result_size_bytes,
                    )
                    if existing != requested:
                        raise ActivityCatalogConflictError(
                            "activity already has a different durable result"
                        )
                    return stored

                now = _utcnow()
                conn.execute(
                    "UPDATE activities SET result_ref=?, result_digest=?, "
                    "result_media_type=?, result_size_bytes=?, "
                    "result_recorded_at=?, updated_at=? "
                    "WHERE source_world_id=? AND kind=? AND activity_id=?",
                    (
                        result_ref,
                        result_digest,
                        result_media_type,
                        result_size_bytes,
                        now,
                        now,
                        *identity,
                    ),
                )
                updated = _select_activity(conn, *identity)
                assert updated is not None
                return _activity_from_row(updated)

        return await self._run(_record)

    async def list_unobserved_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> list[ActivityRecord]:
        """Discover durable results that a later world tick has not observed."""

        return await self._list_by_completion(
            completed=True,
            kind=kind,
            world_id=world_id,
            limit=limit,
            after_sequence=after_sequence,
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
        after_sequence: int = 0,
    ) -> list[ActivityRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")

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
            conditions.append("sequence>?")
            parameters.append(after_sequence)
            parameters.append(limit)
            rows = (
                self._connect_sync()
                .execute(
                    "SELECT activities.*, activity_executions.provider AS execution_provider, "
                    "activity_executions.operation_id AS execution_operation_id "
                    "FROM activities LEFT JOIN activity_executions USING "
                    "(source_world_id, kind, activity_id) WHERE "
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
        "SELECT activities.*, activity_executions.provider AS execution_provider, "
        "activity_executions.operation_id AS execution_operation_id "
        "FROM activities LEFT JOIN activity_executions USING "
        "(source_world_id, kind, activity_id) "
        "WHERE source_world_id=? AND kind=? AND activity_id=?",
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


def _activity_from_row(row: sqlite3.Row) -> ActivityRecord:
    return ActivityRecord(
        sequence=int(row["sequence"]),
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
        execution_provider=(
            str(row["execution_provider"]) if row["execution_provider"] is not None else None
        ),
        execution_operation_id=(
            str(row["execution_operation_id"])
            if row["execution_operation_id"] is not None
            else None
        ),
        result_ref=str(row["result_ref"]) if row["result_ref"] is not None else None,
        result_digest=(str(row["result_digest"]) if row["result_digest"] is not None else None),
        result_media_type=(
            str(row["result_media_type"]) if row["result_media_type"] is not None else None
        ),
        result_size_bytes=(
            int(row["result_size_bytes"]) if row["result_size_bytes"] is not None else None
        ),
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


def _reserve_execution_identity(
    conn: sqlite3.Connection,
    identity: tuple[str, str, str],
    *,
    provider: str | None,
    operation_id: str | None,
    allow_unbound: bool,
) -> None:
    """Atomically bind the durable orchestrator route at admission."""

    row = conn.execute(
        "SELECT provider, operation_id FROM activity_executions "
        "WHERE source_world_id=? AND kind=? AND activity_id=?",
        identity,
    ).fetchone()
    if provider is None or operation_id is None:
        if row is not None and allow_unbound:
            raise ActivityCatalogConflictError(
                "activity already has a durable orchestration execution identity"
            )
        return
    conn.execute(
        "INSERT OR IGNORE INTO activity_executions "
        "(source_world_id, kind, activity_id, provider, operation_id, bound_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (*identity, provider, operation_id, _utcnow()),
    )
    row = conn.execute(
        "SELECT source_world_id, kind, activity_id, provider, operation_id "
        "FROM activity_executions WHERE provider=? AND operation_id=?",
        (provider, operation_id),
    ).fetchone()
    if row is None:
        raise ActivityCatalogConflictError(
            "orchestration execution identity is already bound to another activity"
        )
    stored = (
        str(row["source_world_id"]),
        str(row["kind"]),
        str(row["activity_id"]),
        str(row["provider"]),
        str(row["operation_id"]),
    )
    if stored != (*identity, provider, operation_id):
        raise ActivityCatalogConflictError(
            "orchestration execution identity is already bound to another activity"
        )


def _require_bounded_text(value: str | None, field_name: str, max_chars: int) -> None:
    if value is None:
        raise ValueError(f"{field_name} must be non-empty")
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    if len(value) > max_chars:
        raise ValueError(f"{field_name} must be at most {max_chars} characters")


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "SqliteActivityCatalog",
    "SqliteActivityCatalogMigrationInspector",
    "activity_catalog_path_for",
    "inspect_sqlite_activity_catalog",
]
