# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local SQLite implementation of the durable control-record contract.

LanceDB remains the immutable data plane.  This module provides the small,
linearizable control plane needed for uniqueness and manifest-head compare and
swap on a local filesystem.  A connection is opened for every operation so no
process-local connection or lock is part of the correctness boundary.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import daft
from daft import DataFrame

from archetype.ledger.errors import (
    DurableRecordCASMismatchError,
    DurableRecordConflictError,
)
from archetype.ledger.models import InternalDigest
from archetype.ledger.records import AtomicPutResult, DurableRecord

_TABLE = "durable_records"
_COLUMNS = (
    "kind",
    "scope",
    "key",
    "revision",
    "content_digest",
    "previous_digest",
    "payload_json",
    "committed_at_ms",
)
_SELECT_COLUMNS = ", ".join(_COLUMNS)
_SUPPORTED_JOURNAL_MODES = frozenset({"DELETE", "WAL"})


class SQLiteAtomicRecordStore:
    """Linearizable durable records in a colocated SQLite catalog.

    The default ``WAL`` profile is intended only for processes on one host and
    a local filesystem.  Callers must initialize the catalog before use.  The
    database path should normally be beneath the normalized LanceDB root, for
    example ``<root>/<namespace>/.archetype/catalog-v1.sqlite3``.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
        journal_mode: str = "WAL",
    ) -> None:
        if busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be positive")
        normalized_mode = journal_mode.upper()
        if normalized_mode not in _SUPPORTED_JOURNAL_MODES:
            supported = ", ".join(sorted(_SUPPORTED_JOURNAL_MODES))
            raise ValueError(f"journal_mode must be one of: {supported}")

        self.database_path = Path(database_path).expanduser().resolve()
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_mode = normalized_mode

    async def initialize(self) -> None:
        """Create and validate the local control catalog."""

        await asyncio.to_thread(self._initialize_sync)

    async def shutdown(self) -> None:
        """Complete the service lifecycle; per-operation connections need no teardown."""

    async def put_if_absent(self, record: DurableRecord) -> AtomicPutResult:
        """Insert one immutable revision or replay its original record."""

        return await asyncio.to_thread(self._put_if_absent_sync, record)

    async def get(
        self,
        *,
        kind: str,
        scope: str,
        key: str,
        revision: int = 0,
    ) -> DurableRecord | None:
        """Return one exact immutable revision."""

        return await asyncio.to_thread(self._get_sync, kind, scope, key, revision)

    async def get_latest(
        self,
        *,
        kind: str,
        scope: str,
        key: str,
    ) -> DurableRecord | None:
        """Return the highest committed revision for one logical key."""

        return await asyncio.to_thread(self._get_latest_sync, kind, scope, key)

    async def compare_and_swap(
        self,
        record: DurableRecord,
        *,
        expected_revision: int | None,
        expected_digest: InternalDigest | None,
    ) -> AtomicPutResult:
        """Insert the next revision only when its immutable head still matches.

        An exact retry is checked before the expected head.  This lets a caller
        recover a successful CAS after losing its response, even if another
        writer has since appended a later revision.
        """

        return await asyncio.to_thread(
            self._compare_and_swap_sync,
            record,
            expected_revision,
            expected_digest,
        )

    async def scan(self, *, kind: str, scope: str | None = None) -> DataFrame:
        """Return deterministic durable-record rows as a lazy Daft frame."""

        rows = await asyncio.to_thread(self._scan_sync, kind, scope)
        return daft.from_pydict({column: [row[column] for row in rows] for column in _COLUMNS})

    async def scan_latest(self, *, kind: str, scope: str | None = None) -> DataFrame:
        """Return only the latest immutable revision of every matching key."""

        rows = await asyncio.to_thread(self._scan_latest_sync, kind, scope)
        return daft.from_pydict({column: [row[column] for row in rows] for column in _COLUMNS})

    def _initialize_sync(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + (self.busy_timeout_ms / 1_000)
        delay = 0.01
        while True:
            try:
                self._initialize_once_sync()
                return
            except sqlite3.OperationalError as exc:
                retryable = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                if not retryable or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.1)

    def _initialize_once_sync(self) -> None:
        with closing(self._connect(verify_journal=False)) as connection:
            actual_mode = connection.execute(f"PRAGMA journal_mode={self.journal_mode}").fetchone()[
                0
            ]
            if str(actual_mode).upper() != self.journal_mode:
                raise RuntimeError(
                    f"SQLite journal mode {self.journal_mode} is unavailable; "
                    f"database selected {actual_mode!r}"
                )
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_TABLE} (
                    kind TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    key TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision >= 0),
                    content_digest TEXT NOT NULL,
                    previous_digest TEXT,
                    payload_json TEXT NOT NULL,
                    committed_at_ms INTEGER NOT NULL CHECK (committed_at_ms >= 0),
                    PRIMARY KEY (kind, scope, key, revision)
                ) WITHOUT ROWID
                """
            )
            self._validate_schema(connection)

    def _connect(self, *, verify_journal: bool = True) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        if verify_journal:
            actual_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).upper()
            if actual_mode != self.journal_mode:
                connection.close()
                raise RuntimeError(
                    f"SQLite journal mode changed: expected {self.journal_mode}, "
                    f"found {actual_mode}"
                )
        return connection

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        rows = connection.execute(f"PRAGMA table_info({_TABLE})").fetchall()
        names = tuple(row["name"] for row in rows)
        primary_key = tuple(
            row["name"] for row in sorted(rows, key=lambda item: item["pk"]) if row["pk"]
        )
        if names != _COLUMNS or primary_key != ("kind", "scope", "key", "revision"):
            raise RuntimeError("existing SQLite durable-record schema is incompatible")

    @staticmethod
    def _record_from_row(row: sqlite3.Row | None) -> DurableRecord | None:
        if row is None:
            return None
        return DurableRecord.model_validate(dict(row))

    @staticmethod
    def _same_content(left: DurableRecord, right: DurableRecord) -> bool:
        return (
            left.kind == right.kind
            and left.scope == right.scope
            and left.key == right.key
            and left.revision == right.revision
            and left.content_digest == right.content_digest
            and left.previous_digest == right.previous_digest
            and left.payload_json == right.payload_json
        )

    @staticmethod
    def _values(record: DurableRecord) -> tuple[str | int | None, ...]:
        return (
            record.kind,
            record.scope,
            record.key,
            record.revision,
            record.content_digest,
            record.previous_digest,
            record.payload_json,
            record.committed_at_ms,
        )

    @staticmethod
    def _exact_row(
        connection: sqlite3.Connection,
        *,
        kind: str,
        scope: str,
        key: str,
        revision: int,
    ) -> sqlite3.Row | None:
        return connection.execute(
            f"SELECT {_SELECT_COLUMNS} FROM {_TABLE} "
            "WHERE kind = ? AND scope = ? AND key = ? AND revision = ?",
            (kind, scope, key, revision),
        ).fetchone()

    @staticmethod
    def _latest_row(
        connection: sqlite3.Connection,
        *,
        kind: str,
        scope: str,
        key: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            f"SELECT {_SELECT_COLUMNS} FROM {_TABLE} "
            "WHERE kind = ? AND scope = ? AND key = ? "
            "ORDER BY revision DESC LIMIT 1",
            (kind, scope, key),
        ).fetchone()

    @staticmethod
    def _insert(connection: sqlite3.Connection, record: DurableRecord) -> None:
        placeholders = ", ".join("?" for _ in _COLUMNS)
        connection.execute(
            f"INSERT INTO {_TABLE} ({_SELECT_COLUMNS}) VALUES ({placeholders})",
            SQLiteAtomicRecordStore._values(record),
        )

    @staticmethod
    def _raise_conflict(
        record: DurableRecord,
        actual: DurableRecord,
        *,
        latest: DurableRecord | None,
    ) -> None:
        raise DurableRecordConflictError(
            kind=record.kind,
            scope=record.scope,
            key=record.key,
            revision=record.revision,
            expected_digest=record.content_digest,
            actual_digest=actual.content_digest,
            latest_record=latest,
        )

    def _put_if_absent_sync(self, record: DurableRecord) -> AtomicPutResult:
        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self._record_from_row(
                    self._exact_row(
                        connection,
                        kind=record.kind,
                        scope=record.scope,
                        key=record.key,
                        revision=record.revision,
                    )
                )
                if existing is not None:
                    latest = self._record_from_row(
                        self._latest_row(
                            connection,
                            kind=record.kind,
                            scope=record.scope,
                            key=record.key,
                        )
                    )
                    if not self._same_content(existing, record):
                        self._raise_conflict(record, existing, latest=latest)
                    connection.commit()
                    return AtomicPutResult(record=existing, replayed=True)

                self._insert(connection, record)
                connection.commit()
                return AtomicPutResult(record=record, replayed=False)
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def _get_sync(
        self,
        kind: str,
        scope: str,
        key: str,
        revision: int,
    ) -> DurableRecord | None:
        with closing(self._connect()) as connection:
            return self._record_from_row(
                self._exact_row(
                    connection,
                    kind=kind,
                    scope=scope,
                    key=key,
                    revision=revision,
                )
            )

    def _get_latest_sync(self, kind: str, scope: str, key: str) -> DurableRecord | None:
        with closing(self._connect()) as connection:
            return self._record_from_row(
                self._latest_row(connection, kind=kind, scope=scope, key=key)
            )

    def _compare_and_swap_sync(
        self,
        record: DurableRecord,
        expected_revision: int | None,
        expected_digest: InternalDigest | None,
    ) -> AtomicPutResult:
        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self._record_from_row(
                    self._exact_row(
                        connection,
                        kind=record.kind,
                        scope=record.scope,
                        key=record.key,
                        revision=record.revision,
                    )
                )
                latest = self._record_from_row(
                    self._latest_row(
                        connection,
                        kind=record.kind,
                        scope=record.scope,
                        key=record.key,
                    )
                )
                if existing is not None:
                    if not self._same_content(existing, record):
                        self._raise_conflict(record, existing, latest=latest)
                    connection.commit()
                    return AtomicPutResult(record=existing, replayed=True)

                if (expected_revision is None) != (expected_digest is None):
                    raise ValueError(
                        "expected_revision and expected_digest must either both be set or both be None"
                    )
                target_revision = 0 if expected_revision is None else expected_revision + 1
                if record.revision != target_revision:
                    raise ValueError(
                        f"CAS target revision must be {target_revision}, got {record.revision}"
                    )
                if record.previous_digest != expected_digest:
                    raise ValueError("record previous_digest must match expected_digest")

                head_matches = (
                    latest is None and expected_revision is None and expected_digest is None
                ) or (
                    latest is not None
                    and latest.revision == expected_revision
                    and latest.content_digest == expected_digest
                )
                if not head_matches:
                    raise DurableRecordCASMismatchError(
                        kind=record.kind,
                        scope=record.scope,
                        key=record.key,
                        expected_revision=expected_revision,
                        expected_digest=expected_digest,
                        latest_record=latest,
                    )

                self._insert(connection, record)
                connection.commit()
                return AtomicPutResult(record=record, replayed=False)
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def _scan_sync(self, kind: str, scope: str | None) -> list[dict[str, str | int | None]]:
        where = "kind = ?"
        parameters: tuple[str, ...] = (kind,)
        if scope is not None:
            where += " AND scope = ?"
            parameters += (scope,)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {_SELECT_COLUMNS} FROM {_TABLE} WHERE {where} "
                "ORDER BY scope, key, revision",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]

    def _scan_latest_sync(
        self,
        kind: str,
        scope: str | None,
    ) -> list[dict[str, str | int | None]]:
        where = "record.kind = ?"
        parameters: tuple[str, ...] = (kind,)
        if scope is not None:
            where += " AND record.scope = ?"
            parameters += (scope,)
        selected = ", ".join(f"record.{column}" for column in _COLUMNS)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {selected} FROM {_TABLE} AS record WHERE {where} "
                "AND record.revision = ("
                f"SELECT MAX(head.revision) FROM {_TABLE} AS head "
                "WHERE head.kind = record.kind AND head.scope = record.scope "
                "AND head.key = record.key) "
                "ORDER BY record.scope, record.key",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]
