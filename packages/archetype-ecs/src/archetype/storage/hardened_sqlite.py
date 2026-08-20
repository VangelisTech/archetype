# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared single-host hardened SQLite control-plane lifecycle.

Every local control catalog (Activity claims, MissionRun records) opens the
same way: WAL journaling with ``synchronous=FULL`` for crash durability,
foreign keys on, a bounded busy-retry loop around first connection, one
``asyncio.Lock``-serialized worker thread, cancellation-shielded IO so a
cancelled caller can never observe a torn transaction, and an exact
schema-version check against a single-row meta table. Subclasses own their
DDL, schema, and queries; this base owns only that physical lifecycle.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path


class HardenedSqliteCatalog:
    """Own one hardened SQLite connection for a single-host control catalog."""

    # Subclasses declare their physical schema exactly once.
    _DDL: str = ""
    _META_TABLE: str = ""
    _SCHEMA_VERSION: int = 1
    _CATALOG_LABEL: str = "control catalog"

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000) -> None:
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
                conn.executescript(self._DDL)
                row = conn.execute(
                    f"SELECT value FROM {self._META_TABLE} WHERE key='schema_version'"
                ).fetchone()
                assert row is not None
                version = int(row["value"])
                if version != self._SCHEMA_VERSION:
                    raise RuntimeError(
                        f"{self._CATALOG_LABEL} {self.path} has schema_version={version}, "
                        f"this build expects {self._SCHEMA_VERSION}"
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


__all__ = ["HardenedSqliteCatalog"]
