# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for Missions-owned coding-agent session ingestion."""

import sqlite3
from pathlib import Path

import pytest

from archetype.missions.sessions.runner_state import (
    RunnerSession,
    _iso_to_ms,
    load_runner_sessions,
)
from archetype.research import Run


def _make_runner_state_db(
    tmp_path: Path,
    rows: list[tuple] | None = None,
) -> Path:
    """Build a SQLite file with the exact schema archetype-runner uses.

    Mirrors the legacy archetype-runner ``agents`` table so ingestion round
    trips through the same schema the runner actually writes.
    """
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE agents (
            agent_id TEXT PRIMARY KEY,
            vm_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'booting',
            harness TEXT NOT NULL,
            repo_url TEXT NOT NULL,
            branch TEXT NOT NULL,
            task TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            agent_name TEXT NOT NULL DEFAULT '',
            workspace_path TEXT NOT NULL DEFAULT ''
        )
        """
    )
    if rows:
        conn.executemany(
            "INSERT INTO agents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    conn.commit()
    conn.close()
    return db_path


class TestIsoToMs:
    def test_parses_utc_iso_string(self):
        ms = _iso_to_ms("2026-04-10T12:00:00+00:00")
        # 2026-04-10T12:00:00 UTC = 1_775_822_400_000 ms since epoch
        assert ms == 1_775_822_400_000

    def test_none_becomes_zero(self):
        assert _iso_to_ms(None) == 0

    def test_empty_string_becomes_zero(self):
        assert _iso_to_ms("") == 0


class TestLoadRunnerStateDb:
    def test_session_decoder_is_missions_owned_and_not_research_state(self):
        assert RunnerSession.__module__ == "archetype.missions.sessions.runner_state"
        assert not set(RunnerSession.__dataclass_fields__) <= set(Run.model_fields)

    def test_raises_when_file_missing(self, tmp_path):
        missing = tmp_path / "does-not-exist.db"
        with pytest.raises(FileNotFoundError, match="state.db not found"):
            load_runner_sessions(missing)

    def test_empty_db_returns_empty_list(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        runs = load_runner_sessions(db_path)
        assert runs == []

    def test_single_running_agent(self, tmp_path):
        db_path = _make_runner_state_db(
            tmp_path,
            rows=[
                (
                    "claude-001",
                    "mb-vm-1",
                    "running",
                    "claude-code",
                    "https://github.com/foo/bar",
                    "main",
                    "Make all tests pass",
                    "2026-04-10T12:00:00+00:00",
                    None,  # still running → finished_at is NULL
                    "adhoc-claude-code",
                    "/home/vangelis/.archetype-runner/workspaces/claude-001",
                ),
            ],
        )
        runs = load_runner_sessions(db_path)
        assert len(runs) == 1
        run = runs[0]
        assert isinstance(run, RunnerSession)
        assert run.session_id == "claude-001"
        assert run.vm_name == "mb-vm-1"
        assert run.status == "running"
        assert run.is_active is True
        assert run.harness == "claude-code"
        assert run.repository == "https://github.com/foo/bar"
        assert run.branch == "main"
        assert run.task == "Make all tests pass"
        assert run.started_at_ms == 1_775_822_400_000
        assert run.finished_at_ms == 0  # NULL → 0
        assert run.agent_name == "adhoc-claude-code"
        assert run.workspace_path.endswith("claude-001")

    def test_completed_agent_has_finished_timestamp(self, tmp_path):
        db_path = _make_runner_state_db(
            tmp_path,
            rows=[
                (
                    "claude-002",
                    "mb-vm-2",
                    "stopped",
                    "claude-code",
                    "https://github.com/foo/bar",
                    "main",
                    "task",
                    "2026-04-10T12:00:00+00:00",
                    "2026-04-10T12:30:00+00:00",
                    "adhoc-claude-code",
                    "/ws/claude-002",
                ),
            ],
        )
        runs = load_runner_sessions(db_path)
        assert len(runs) == 1
        run = runs[0]
        assert run.status == "stopped"
        assert run.is_active is False
        assert run.is_terminal is True
        assert run.started_at_ms == 1_775_822_400_000
        assert run.finished_at_ms == 1_775_822_400_000 + 30 * 60 * 1000

    def test_multiple_agents_sorted_by_started_at(self, tmp_path):
        db_path = _make_runner_state_db(
            tmp_path,
            rows=[
                # Insert in wrong order on purpose — loader must sort by started_at ASC
                (
                    "third",
                    "vm-3",
                    "running",
                    "hermes",
                    "r",
                    "main",
                    "t3",
                    "2026-04-10T14:00:00+00:00",
                    None,
                    "",
                    "",
                ),
                (
                    "first",
                    "vm-1",
                    "stopped",
                    "claude-code",
                    "r",
                    "main",
                    "t1",
                    "2026-04-10T12:00:00+00:00",
                    "2026-04-10T12:15:00+00:00",
                    "",
                    "",
                ),
                (
                    "second",
                    "vm-2",
                    "crashed",
                    "opencode",
                    "r",
                    "main",
                    "t2",
                    "2026-04-10T13:00:00+00:00",
                    "2026-04-10T13:05:00+00:00",
                    "",
                    "",
                ),
            ],
        )
        runs = load_runner_sessions(db_path)
        assert [r.session_id for r in runs] == ["first", "second", "third"]
        assert [r.harness for r in runs] == ["claude-code", "opencode", "hermes"]

    def test_loader_opens_readonly_and_coexists_with_writer(self, tmp_path):
        # Sanity: the read-only URI mode should not block a concurrent
        # writer and should not crash on an active connection.
        db_path = _make_runner_state_db(
            tmp_path,
            rows=[
                (
                    "x",
                    "v",
                    "running",
                    "claude-code",
                    "r",
                    "main",
                    "t",
                    "2026-04-10T12:00:00+00:00",
                    None,
                    "",
                    "",
                ),
            ],
        )
        # Open a writer connection with WAL mode (what runner does).
        writer = sqlite3.connect(str(db_path))
        writer.execute("PRAGMA journal_mode=WAL")
        try:
            runs = load_runner_sessions(db_path)
            assert len(runs) == 1
            assert runs[0].session_id == "x"
        finally:
            writer.close()

    def test_all_five_run_statuses_round_trip(self, tmp_path):
        # Asserts that every runner status string deserializes into a
        # Run without translation and that is_active / is_terminal
        # classifies them correctly.
        rows = [
            (
                sid,
                "vm",
                status,
                "h",
                "r",
                "main",
                "t",
                "2026-04-10T12:00:00+00:00",
                None
                if status in ("booting", "running", "stopping")
                else "2026-04-10T13:00:00+00:00",
                "",
                "",
            )
            for sid, status in [
                ("a", "booting"),
                ("b", "running"),
                ("c", "stopping"),
                ("d", "stopped"),
                ("e", "crashed"),
            ]
        ]
        db_path = _make_runner_state_db(tmp_path, rows=rows)
        runs = load_runner_sessions(db_path)
        by_status = {r.status: r for r in runs}
        assert by_status["booting"].is_active is True
        assert by_status["running"].is_active is True
        assert by_status["stopping"].is_active is True
        assert by_status["stopped"].is_terminal is True
        assert by_status["crashed"].is_terminal is True
