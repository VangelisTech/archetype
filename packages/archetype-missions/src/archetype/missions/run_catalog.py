# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""SQLite persistence for missions-owned MissionRun control records.

Physical durability follows the Activity-catalog pattern: the path is a pure
function of storage identity. Semantic transitions stay in ``run_lifecycle``.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path

from archetype.core.config import StorageConfig
from archetype.storage.catalog.sqlite import catalog_path_for
from archetype.storage.config import ControlCatalogConfig

_SCHEMA_VERSION = 1

_DDL = f"""
CREATE TABLE IF NOT EXISTS mission_run_catalog_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mission_runs (
    run_id TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    profile_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    submission_json TEXT NOT NULL,
    world_id TEXT NOT NULL,
    mission_id INTEGER,
    episode_id TEXT NOT NULL DEFAULT '',
    task_ids_json TEXT NOT NULL DEFAULT '[]',
    active_operation TEXT NOT NULL DEFAULT '',
    active_activity_kind TEXT NOT NULL DEFAULT '',
    active_activity_id TEXT NOT NULL DEFAULT '',
    cancellation_intent INTEGER NOT NULL DEFAULT 0,
    cancellation_reason TEXT NOT NULL DEFAULT '',
    result_json TEXT,
    cleanup_state TEXT NOT NULL DEFAULT 'none',
    cleanup_error TEXT NOT NULL DEFAULT '',
    accepted_at_ms INTEGER NOT NULL,
    running_at_ms INTEGER,
    terminal_at_ms INTEGER,
    interrupted_reason TEXT NOT NULL DEFAULT '',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (principal, idempotency_key)
);
CREATE INDEX IF NOT EXISTS mission_runs_open_idx
    ON mission_runs (status, accepted_at_ms)
    WHERE terminal_at_ms IS NULL;
CREATE TABLE IF NOT EXISTS mission_run_events (
    run_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    phase TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, cursor)
);
INSERT OR IGNORE INTO mission_run_catalog_meta (key, value)
    VALUES ('schema_version', '{_SCHEMA_VERSION}');
"""


def mission_run_catalog_path_for(
    config: StorageConfig,
    catalog_config: ControlCatalogConfig | None = None,
) -> Path:
    """Derive a local MissionRun catalog path from the same storage identity."""

    control_path = catalog_path_for(config, catalog_config)
    return control_path.with_name(f"{control_path.stem}-mission-runs{control_path.suffix}")


class SqliteMissionRunCatalog:
    """Single-host SQLite control plane for MissionRun records."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000) -> None:
        self.path = Path(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    def _connect_sync(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + (self._busy_timeout_ms / 1000)
        delay = 0.001
        while True:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            try:
                conn.row_factory = sqlite3.Row
                conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
                conn.execute("PRAGMA foreign_keys=ON")
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_DDL)
                row = conn.execute(
                    "SELECT value FROM mission_run_catalog_meta WHERE key='schema_version'"
                ).fetchone()
                assert row is not None
                version = int(row["value"])
                if version != _SCHEMA_VERSION:
                    raise RuntimeError(
                        f"mission-run catalog {self.path} has schema_version={version}, "
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
                    pass
                raise cancellation
            return worker.result()

    async def close(self) -> None:
        def _close() -> None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

        await self._run(_close)

    async def insert_accepted(
        self,
        record: dict[str, object],
        *,
        events: tuple[dict[str, object], ...] = (),
    ) -> dict[str, object]:
        """Insert one accepted run, or return the existing idempotent row.

        ``events`` are appended in the same transaction only when the run row
        is actually inserted, so idempotent replay never duplicates a logical
        event.
        """

        def _insert() -> dict[str, object]:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                existing = conn.execute(
                    "SELECT * FROM mission_runs WHERE principal=? AND idempotency_key=?",
                    (record["principal"], record["idempotency_key"]),
                ).fetchone()
                if existing is not None:
                    return dict(existing)
                conn.execute(
                    "INSERT INTO mission_runs ("
                    "run_id, principal, idempotency_key, request_digest, profile_id, "
                    "profile_version, profile_digest, status, submission_json, world_id, "
                    "accepted_at_ms, created_at_ms, updated_at_ms"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        record["run_id"],
                        record["principal"],
                        record["idempotency_key"],
                        record["request_digest"],
                        record["profile_id"],
                        record["profile_version"],
                        record["profile_digest"],
                        record["status"],
                        record["submission_json"],
                        record["world_id"],
                        record["accepted_at_ms"],
                        record["created_at_ms"],
                        record["updated_at_ms"],
                    ),
                )
                _append_events_sync(conn, str(record["run_id"]), events)
                inserted = conn.execute(
                    "SELECT * FROM mission_runs WHERE run_id=?",
                    (record["run_id"],),
                ).fetchone()
                assert inserted is not None
                return dict(inserted)

        return await self._run(_insert)

    async def get(self, run_id: str) -> dict[str, object] | None:
        def _get() -> dict[str, object] | None:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM mission_runs WHERE run_id=?",
                    (run_id,),
                )
                .fetchone()
            )
            return dict(row) if row is not None else None

        return await self._run(_get)

    async def get_by_idempotency(
        self,
        principal: str,
        idempotency_key: str,
    ) -> dict[str, object] | None:
        def _get() -> dict[str, object] | None:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM mission_runs WHERE principal=? AND idempotency_key=?",
                    (principal, idempotency_key),
                )
                .fetchone()
            )
            return dict(row) if row is not None else None

        return await self._run(_get)

    async def list_open(self) -> tuple[dict[str, object], ...]:
        def _list() -> tuple[dict[str, object], ...]:
            rows = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM mission_runs WHERE terminal_at_ms IS NULL "
                    "ORDER BY accepted_at_ms ASC, run_id ASC"
                )
                .fetchall()
            )
            return tuple(dict(row) for row in rows)

        return await self._run(_list)

    async def compare_and_set(
        self,
        run_id: str,
        *,
        expected_status: str,
        expected_updated_at_ms: int,
        values: dict[str, object],
        events: tuple[dict[str, object], ...] = (),
    ) -> dict[str, object] | None:
        """Apply one CAS update. Return the new row, or None if the predicate missed.

        ``events`` are appended atomically with the update, so an event exists
        exactly when its durable transition committed.
        """

        assignments = [f"{column}=?" for column in values]
        parameters = [*values.values(), run_id, expected_status, expected_updated_at_ms]

        def _update() -> dict[str, object] | None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE mission_runs SET "
                    + ", ".join(assignments)
                    + " WHERE run_id=? AND status=? AND updated_at_ms=?",
                    parameters,
                )
                if conn.execute("SELECT changes()").fetchone()[0] != 1:
                    return None
                _append_events_sync(conn, run_id, events)
                row = conn.execute(
                    "SELECT * FROM mission_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
                assert row is not None
                return dict(row)

        return await self._run(_update)

    async def list_events(
        self,
        run_id: str,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> tuple[dict[str, object], ...]:
        """Return one ordered event page strictly after ``after``."""

        def _list() -> tuple[dict[str, object], ...]:
            rows = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM mission_run_events WHERE run_id=? AND cursor>? "
                    "ORDER BY cursor ASC LIMIT ?",
                    (run_id, after, limit),
                )
                .fetchall()
            )
            return tuple(dict(row) for row in rows)

        return await self._run(_list)


def _append_events_sync(
    conn: sqlite3.Connection,
    run_id: str,
    events: tuple[dict[str, object], ...],
) -> None:
    """Append events with a contiguous run-local cursor inside a transaction."""

    if not events:
        return
    row = conn.execute(
        "SELECT COALESCE(MAX(cursor), 0) FROM mission_run_events WHERE run_id=?",
        (run_id,),
    ).fetchone()
    cursor = int(row[0])
    for event in events:
        cursor += 1
        conn.execute(
            "INSERT INTO mission_run_events ("
            "run_id, cursor, schema_version, event_type, phase, payload_json, created_at_ms"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                cursor,
                event["schema_version"],
                event["event_type"],
                event["phase"],
                event["payload_json"],
                event["created_at_ms"],
            ),
        )


def encode_json(value: object) -> str:
    """Encode one catalog payload as canonical JSON."""

    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def decode_json(value: str) -> object:
    """Decode one catalog JSON payload."""

    return json.loads(value)


__all__ = [
    "SqliteMissionRunCatalog",
    "decode_json",
    "encode_json",
    "mission_run_catalog_path_for",
]
