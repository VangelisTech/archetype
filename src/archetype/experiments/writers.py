# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Experiment Writers
==================

Write experiment state to external stores, starting with the
``requests.db`` file that Archetype Runner polls for new work.

Follows the loader/writer separation: loaders are read-only and
side-effect-free, writers are the only functions that modify
external state. Both use SQLite with WAL mode for safe concurrent
access.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from archetype.experiments.components import RunRequest


_CREATE_RUN_REQUESTS_TABLE = """
CREATE TABLE IF NOT EXISTS run_requests (
    request_id      TEXT PRIMARY KEY,
    experiment_name TEXT NOT NULL,
    repo_url        TEXT NOT NULL,
    branch          TEXT NOT NULL,
    base_commit     TEXT NOT NULL DEFAULT '',
    task            TEXT NOT NULL,
    harness         TEXT NOT NULL DEFAULT 'claude-code',
    timeout_sec     INTEGER NOT NULL DEFAULT 1800,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL,
    claimed_at      TEXT,
    claimed_by      TEXT
)
"""

_INSERT_RUN_REQUEST = """
INSERT INTO run_requests (
    request_id, experiment_name, repo_url, branch, base_commit,
    task, harness, timeout_sec, metadata_json, status, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _default_requests_db_path() -> Path:
    """Conventional path where Archetype writes run requests for Runner."""
    return Path.home() / ".archetype-runner" / "requests.db"


def _ms_to_iso(ms: int) -> str:
    """Convert Unix milliseconds to ISO-8601 UTC string.

    Returns empty string for 0, matching the loader convention where
    0 means 'not set'.
    """
    if ms == 0:
        return datetime.now(tz=UTC).isoformat()
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def write_run_request(
    request: RunRequest,
    path: str | Path | None = None,
) -> Path:
    """Write a RunRequest to the ``requests.db`` file for Runner to pick up.

    Creates the database file and ``run_requests`` table if they do not
    exist. Uses WAL mode for safe concurrent reads by Runner.

    Args:
        request: The RunRequest component to persist.
        path: Path to the requests database. If None, defaults to
            ``~/.archetype-runner/requests.db``.

    Returns:
        The path to the database file.

    Raises:
        sqlite3.IntegrityError: If a request with the same request_id
            already exists (idempotency guard — caller should generate
            unique IDs).
    """
    db_path = Path(path) if path is not None else _default_requests_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(_CREATE_RUN_REQUESTS_TABLE)
        conn.execute(
            _INSERT_RUN_REQUEST,
            (
                request.request_id,
                request.experiment_name,
                request.repo_url,
                request.branch,
                request.base_commit,
                request.task,
                request.harness,
                request.timeout_sec,
                request.metadata_json,
                request.status,
                _ms_to_iso(request.created_at_ms),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return db_path
