# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Read coding-agent sessions from the legacy archetype-runner registry."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RunnerSession:
    """One coding-agent session decoded from runner-owned SQLite state."""

    session_id: str
    vm_name: str
    status: str
    harness: str
    repository: str
    branch: str
    task: str
    started_at_ms: int
    finished_at_ms: int
    agent_name: str
    workspace_path: str

    @property
    def is_active(self) -> bool:
        """Whether the legacy runner still considers this session live."""

        return self.status in {"booting", "running", "stopping"}

    @property
    def is_terminal(self) -> bool:
        """Whether the legacy runner recorded a terminal session state."""

        return self.status in {"stopped", "crashed"}


def _iso_to_ms(value: str | None) -> int:
    if not value:
        return 0
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _default_runner_state_path() -> Path:
    return Path.home() / ".archetype-runner" / "state.db"


def load_runner_sessions(path: str | Path | None = None) -> list[RunnerSession]:
    """Decode runner sessions without granting SQLite write authority."""

    db_path = Path(path) if path is not None else _default_runner_state_path()
    if not db_path.exists():
        raise FileNotFoundError(f"archetype-runner state.db not found at {db_path}")

    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            """
            SELECT agent_id, vm_name, status, harness, repo_url, branch,
                   task, started_at, finished_at, agent_name, workspace_path
            FROM agents
            ORDER BY started_at ASC
            """
        )
        return [
            RunnerSession(
                session_id=row[0] or "",
                vm_name=row[1] or "",
                status=row[2] or "booting",
                harness=row[3] or "",
                repository=row[4] or "",
                branch=row[5] or "main",
                task=row[6] or "",
                started_at_ms=_iso_to_ms(row[7]),
                finished_at_ms=_iso_to_ms(row[8]),
                agent_name=row[9] or "",
                workspace_path=row[10] or "",
            )
            for row in rows
        ]
    finally:
        connection.close()


__all__ = ["RunnerSession", "load_runner_sessions"]
