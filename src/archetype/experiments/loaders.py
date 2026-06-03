# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Experiment Loaders
==================

Ingest external state into experiment Components, starting with
archetype-runner's SQLite registry.

Direct ingestion path (no CommandBroker / RBAC) following the
``scripts/ingest_claude_sessions.py`` pattern: SQLite → list of Run
Components → ``world.create_entity(...)``. The runner's SQLite uses
WAL mode, so these reads are safe even while the runner is actively
writing.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from archetype.experiments.components import Run, RunReport

if TYPE_CHECKING:
    from archetype.core.aio import AsyncWorld


def _iso_to_ms(iso_str: str | None) -> int:
    """Parse a runner-written ISO-8601 timestamp into Unix milliseconds.

    Returns 0 for None or empty strings, so that the Arrow ``int``
    column can represent 'not set' without nullable complications.
    """
    if not iso_str:
        return 0
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)


def _default_runner_state_path() -> Path:
    """Conventional path where archetype-runner stores its registry."""
    return Path.home() / ".archetype-runner" / "state.db"


def load_runner_state_db(path: str | Path | None = None) -> list[Run]:
    """Read archetype-runner's ``state.db`` and return one Run per row.

    Args:
        path: Path to the runner's SQLite registry. If None, defaults
            to ``~/.archetype-runner/state.db``.

    Returns:
        One ``Run`` Component instance per row in the ``agents`` table,
        ordered by ``started_at`` ascending (oldest first).

    Raises:
        FileNotFoundError: If the SQLite file does not exist.

    Notes:
        Opens the database read-only via SQLite URI mode. Safe to call
        while the runner is writing: the runner enables WAL mode so
        concurrent readers don't block writers.
    """
    db_path = Path(path) if path is not None else _default_runner_state_path()
    if not db_path.exists():
        raise FileNotFoundError(f"archetype-runner state.db not found at {db_path}")

    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        cursor = conn.execute(
            """
            SELECT agent_id, vm_name, status, harness, repo_url, branch,
                   task, started_at, finished_at, agent_name, workspace_path
            FROM agents
            ORDER BY started_at ASC
            """
        )
        runs: list[Run] = []
        for row in cursor:
            runs.append(
                Run(
                    run_id=row[0] or "",
                    vm_name=row[1] or "",
                    status=row[2] or "booting",
                    harness=row[3] or "",
                    repo_url=row[4] or "",
                    branch=row[5] or "main",
                    task=row[6] or "",
                    started_at_ms=_iso_to_ms(row[7]),
                    finished_at_ms=_iso_to_ms(row[8]),
                    agent_name=row[9] or "",
                    workspace_path=row[10] or "",
                )
            )
        return runs
    finally:
        conn.close()


async def ingest_runner_state(
    world: AsyncWorld,
    path: str | Path | None = None,
) -> int:
    """Read archetype-runner's ``state.db`` and spawn one entity per run.

    Thin wrapper over ``load_runner_state_db`` that calls
    ``world.create_entity`` for each loaded Run. Entities are
    in-memory after this call; caller should run one
    ``simulation_service.step`` to materialize them to storage
    if durable persistence is required.

    Args:
        world: The archetype world to load runs into.
        path: Path to the runner's SQLite registry. If None, defaults
            to ``~/.archetype-runner/state.db``.

    Returns:
        The number of Run entities spawned.

    Example:
        >>> container = ServiceContainer()
        >>> world = await container.world_service.create_world(
        ...     WorldConfig(name="runner-fleet"),
        ...     StorageConfig(namespace="experiments"),
        ... )
        >>> count = await ingest_runner_state(world)
        >>> await container.simulation_service.step(
        ...     world.world_id, RunConfig(num_steps=1)
        ... )
    """
    runs = load_runner_state_db(path)
    for run in runs:
        await world.create_entity([run])
    return len(runs)


def _default_requests_db_path() -> Path:
    """Conventional path where Archetype writes run requests for Runner."""
    return Path.home() / ".archetype-runner" / "requests.db"


def load_runner_reports(path: str | Path | None = None) -> list[RunReport]:
    """Read Runner's ``run_reports`` table and return one RunReport per row.

    Args:
        path: Path to the runner's SQLite registry. If None, defaults
            to ``~/.archetype-runner/state.db``.

    Returns:
        One ``RunReport`` Component instance per row, ordered by
        ``created_at`` ascending (oldest first).

    Raises:
        FileNotFoundError: If the SQLite file does not exist.

    Notes:
        Opens the database read-only via SQLite URI mode. Returns an
        empty list if the ``run_reports`` table does not yet exist
        (Runner hasn't written any reports yet).
    """
    db_path = Path(path) if path is not None else _default_runner_state_path()
    if not db_path.exists():
        raise FileNotFoundError(f"archetype-runner state.db not found at {db_path}")

    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        # Check if the run_reports table exists — Runner may not have
        # created it yet if no runs have completed.
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='run_reports'"
        )
        if cursor.fetchone() is None:
            return []

        cursor = conn.execute(
            """
            SELECT report_id, agent_id, request_id, exit_status,
                   commit_hash, diff_stat, stdout_tail, stderr_tail,
                   metadata_json, created_at
            FROM run_reports
            ORDER BY created_at ASC
            """
        )
        reports: list[RunReport] = []
        for row in cursor:
            reports.append(
                RunReport(
                    report_id=row[0] or "",
                    run_id=row[1] or "",
                    request_id=row[2] or "",
                    exit_status=row[3] or "",
                    commit_hash=row[4] or "",
                    diff_stat=row[5] or "",
                    stdout_tail=row[6] or "",
                    stderr_tail=row[7] or "",
                    metadata_json=row[8] or "{}",
                    created_at_ms=_iso_to_ms(row[9]),
                )
            )
        return reports
    finally:
        conn.close()


def load_request_status(path: str | Path | None = None) -> list[dict]:
    """Read ``run_requests`` from ``requests.db`` to check claim/reject status.

    Returns lightweight dicts (not full Components) since Archetype
    already has the original RunRequest entities — this is a status
    poll, not a full load.

    Args:
        path: Path to the requests database. If None, defaults to
            ``~/.archetype-runner/requests.db``.

    Returns:
        List of dicts with keys: ``request_id``, ``status``,
        ``claimed_at``, ``claimed_by``. Empty list if the file or
        table does not exist.
    """
    db_path = Path(path) if path is not None else _default_requests_db_path()
    if not db_path.exists():
        return []

    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='run_requests'"
        )
        if cursor.fetchone() is None:
            return []

        cursor = conn.execute("SELECT request_id, status, claimed_at, claimed_by FROM run_requests")
        return [
            {
                "request_id": row[0],
                "status": row[1],
                "claimed_at": row[2] or "",
                "claimed_by": row[3] or "",
            }
            for row in cursor
        ]
    finally:
        conn.close()
