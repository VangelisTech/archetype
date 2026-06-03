# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for archetype-runner state.db ingestion."""

import sqlite3
from pathlib import Path

import pytest

from archetype.experiments import Run, RunReport
from archetype.experiments.loaders import (
    _iso_to_ms,
    load_request_status,
    load_runner_reports,
    load_runner_state_db,
)


def _make_runner_state_db(
    tmp_path: Path,
    rows: list[tuple] | None = None,
) -> Path:
    """Build a SQLite file with the exact schema archetype-runner uses.

    Mirrors src/runner/state.py's CREATE TABLE so that ingestion round
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
    def test_raises_when_file_missing(self, tmp_path):
        missing = tmp_path / "does-not-exist.db"
        with pytest.raises(FileNotFoundError, match="state.db not found"):
            load_runner_state_db(missing)

    def test_empty_db_returns_empty_list(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        runs = load_runner_state_db(db_path)
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
        runs = load_runner_state_db(db_path)
        assert len(runs) == 1
        run = runs[0]
        assert isinstance(run, Run)
        assert run.run_id == "claude-001"
        assert run.vm_name == "mb-vm-1"
        assert run.status == "running"
        assert run.is_active is True
        assert run.harness == "claude-code"
        assert run.repo_url == "https://github.com/foo/bar"
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
        runs = load_runner_state_db(db_path)
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
        runs = load_runner_state_db(db_path)
        assert [r.run_id for r in runs] == ["first", "second", "third"]
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
            runs = load_runner_state_db(db_path)
            assert len(runs) == 1
            assert runs[0].run_id == "x"
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
        runs = load_runner_state_db(db_path)
        by_status = {r.status: r for r in runs}
        assert by_status["booting"].is_active is True
        assert by_status["running"].is_active is True
        assert by_status["stopping"].is_active is True
        assert by_status["stopped"].is_terminal is True
        assert by_status["crashed"].is_terminal is True


# ============================================================================
# Helpers for run_reports and run_requests tables
# ============================================================================


def _add_run_reports_table(
    db_path: Path,
    rows: list[tuple] | None = None,
) -> None:
    """Add a run_reports table to an existing state.db."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_reports (
            report_id     TEXT PRIMARY KEY,
            agent_id      TEXT NOT NULL,
            request_id    TEXT NOT NULL DEFAULT '',
            exit_status   TEXT NOT NULL,
            commit_hash   TEXT NOT NULL DEFAULT '',
            diff_stat     TEXT NOT NULL DEFAULT '',
            stdout_tail   TEXT NOT NULL DEFAULT '',
            stderr_tail   TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at    TEXT NOT NULL
        )
        """
    )
    if rows:
        conn.executemany(
            "INSERT INTO run_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    conn.commit()
    conn.close()


def _make_requests_db(
    tmp_path: Path,
    rows: list[tuple] | None = None,
) -> Path:
    """Build a requests.db with the run_requests table."""
    db_path = tmp_path / "requests.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE run_requests (
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
    )
    if rows:
        conn.executemany(
            "INSERT INTO run_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    conn.commit()
    conn.close()
    return db_path


# ============================================================================
# Tests: load_runner_reports
# ============================================================================


class TestLoadRunnerReports:
    def test_raises_when_file_missing(self, tmp_path):
        missing = tmp_path / "does-not-exist.db"
        with pytest.raises(FileNotFoundError, match="state.db not found"):
            load_runner_reports(missing)

    def test_returns_empty_when_table_missing(self, tmp_path):
        # state.db exists but has no run_reports table yet.
        db_path = _make_runner_state_db(tmp_path)
        reports = load_runner_reports(db_path)
        assert reports == []

    def test_returns_empty_when_table_exists_but_empty(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        _add_run_reports_table(db_path)
        reports = load_runner_reports(db_path)
        assert reports == []

    def test_single_completed_report(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        _add_run_reports_table(
            db_path,
            rows=[
                (
                    "rpt-001",
                    "claude-001",
                    "req-001",
                    "completed",
                    "abc123def",
                    "3 files changed",
                    "All tests passed",
                    "",
                    '{"wall_time_sec": 312}',
                    "2026-04-10T12:30:00+00:00",
                ),
            ],
        )
        reports = load_runner_reports(db_path)
        assert len(reports) == 1
        r = reports[0]
        assert isinstance(r, RunReport)
        assert r.report_id == "rpt-001"
        assert r.run_id == "claude-001"
        assert r.request_id == "req-001"
        assert r.exit_status == "completed"
        assert r.commit_hash == "abc123def"
        assert r.diff_stat == "3 files changed"
        assert r.stdout_tail == "All tests passed"
        assert r.stderr_tail == ""
        assert r.created_at_ms == _iso_to_ms("2026-04-10T12:30:00+00:00")
        meta = r.get_metadata()
        assert meta["wall_time_sec"] == 312

    def test_crashed_report_has_no_commit(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        _add_run_reports_table(
            db_path,
            rows=[
                (
                    "rpt-002",
                    "claude-002",
                    "",
                    "crashed",
                    "",
                    "",
                    "",
                    "RuntimeError: OOM",
                    "{}",
                    "2026-04-10T13:00:00+00:00",
                ),
            ],
        )
        reports = load_runner_reports(db_path)
        assert len(reports) == 1
        assert reports[0].exit_status == "crashed"
        assert reports[0].commit_hash == ""
        assert "OOM" in reports[0].stderr_tail

    def test_multiple_reports_sorted_by_created_at(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        _add_run_reports_table(
            db_path,
            rows=[
                # Insert in wrong order — loader must sort by created_at ASC
                (
                    "rpt-b",
                    "agent-b",
                    "",
                    "completed",
                    "bb",
                    "",
                    "",
                    "",
                    "{}",
                    "2026-04-10T14:00:00+00:00",
                ),
                (
                    "rpt-a",
                    "agent-a",
                    "",
                    "crashed",
                    "",
                    "",
                    "",
                    "",
                    "{}",
                    "2026-04-10T12:00:00+00:00",
                ),
                (
                    "rpt-c",
                    "agent-c",
                    "",
                    "timed_out",
                    "cc",
                    "",
                    "",
                    "",
                    "{}",
                    "2026-04-10T16:00:00+00:00",
                ),
            ],
        )
        reports = load_runner_reports(db_path)
        assert [r.report_id for r in reports] == ["rpt-a", "rpt-b", "rpt-c"]

    def test_all_three_exit_statuses_round_trip(self, tmp_path):
        db_path = _make_runner_state_db(tmp_path)
        _add_run_reports_table(
            db_path,
            rows=[
                (
                    "rpt-1",
                    "a-1",
                    "",
                    "completed",
                    "sha1",
                    "",
                    "",
                    "",
                    "{}",
                    "2026-04-10T12:00:00+00:00",
                ),
                ("rpt-2", "a-2", "", "crashed", "", "", "", "", "{}", "2026-04-10T13:00:00+00:00"),
                (
                    "rpt-3",
                    "a-3",
                    "",
                    "timed_out",
                    "sha3",
                    "",
                    "",
                    "",
                    "{}",
                    "2026-04-10T14:00:00+00:00",
                ),
            ],
        )
        reports = load_runner_reports(db_path)
        statuses = {r.exit_status for r in reports}
        assert statuses == {"completed", "crashed", "timed_out"}


# ============================================================================
# Tests: load_request_status
# ============================================================================


class TestLoadRequestStatus:
    def test_returns_empty_when_file_missing(self, tmp_path):
        missing = tmp_path / "no-such-file.db"
        result = load_request_status(missing)
        assert result == []

    def test_returns_empty_when_table_missing(self, tmp_path):
        # Create an empty SQLite file with no tables.
        db_path = tmp_path / "requests.db"
        conn = sqlite3.connect(str(db_path))
        conn.close()
        result = load_request_status(db_path)
        assert result == []

    def test_single_pending_request(self, tmp_path):
        db_path = _make_requests_db(
            tmp_path,
            rows=[
                (
                    "req-001",
                    "exp-1",
                    "https://github.com/foo/bar",
                    "main",
                    "",
                    "Fix the bug",
                    "claude-code",
                    1800,
                    "{}",
                    "pending",
                    "2026-04-10T12:00:00+00:00",
                    None,
                    None,
                ),
            ],
        )
        result = load_request_status(db_path)
        assert len(result) == 1
        assert result[0]["request_id"] == "req-001"
        assert result[0]["status"] == "pending"
        assert result[0]["claimed_at"] == ""
        assert result[0]["claimed_by"] == ""

    def test_claimed_request_has_claim_fields(self, tmp_path):
        db_path = _make_requests_db(
            tmp_path,
            rows=[
                (
                    "req-002",
                    "exp-1",
                    "https://github.com/foo/bar",
                    "main",
                    "abc123",
                    "Optimize",
                    "claude-code",
                    600,
                    "{}",
                    "claimed",
                    "2026-04-10T12:00:00+00:00",
                    "2026-04-10T12:01:00+00:00",
                    "runner-instance-1",
                ),
            ],
        )
        result = load_request_status(db_path)
        assert len(result) == 1
        assert result[0]["status"] == "claimed"
        assert result[0]["claimed_at"] == "2026-04-10T12:01:00+00:00"
        assert result[0]["claimed_by"] == "runner-instance-1"

    def test_multiple_requests_mixed_statuses(self, tmp_path):
        db_path = _make_requests_db(
            tmp_path,
            rows=[
                (
                    "req-a",
                    "e",
                    "r",
                    "main",
                    "",
                    "t",
                    "h",
                    1800,
                    "{}",
                    "pending",
                    "2026-04-10T12:00:00+00:00",
                    None,
                    None,
                ),
                (
                    "req-b",
                    "e",
                    "r",
                    "main",
                    "",
                    "t",
                    "h",
                    1800,
                    "{}",
                    "claimed",
                    "2026-04-10T12:00:00+00:00",
                    "2026-04-10T12:01:00+00:00",
                    "runner-1",
                ),
                (
                    "req-c",
                    "e",
                    "r",
                    "main",
                    "",
                    "t",
                    "h",
                    1800,
                    "{}",
                    "rejected",
                    "2026-04-10T12:00:00+00:00",
                    None,
                    None,
                ),
            ],
        )
        result = load_request_status(db_path)
        by_id = {r["request_id"]: r for r in result}
        assert by_id["req-a"]["status"] == "pending"
        assert by_id["req-b"]["status"] == "claimed"
        assert by_id["req-c"]["status"] == "rejected"
