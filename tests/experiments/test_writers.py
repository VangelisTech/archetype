# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for experiment writers — run request persistence."""

import sqlite3

import pytest

from archetype.experiments import RunRequest, write_run_request


class TestWriteRunRequest:
    def test_creates_db_and_table_if_missing(self, tmp_path):
        db_path = tmp_path / "subdir" / "requests.db"
        req = RunRequest.make(
            request_id="req-001",
            experiment_name="exp-1",
            repo_url="https://github.com/foo/bar",
            task="Make tests pass",
        )
        result_path = write_run_request(req, db_path)
        assert result_path == db_path
        assert db_path.exists()

        # Verify the table was created with the right schema.
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT * FROM run_requests")
        cols = [desc[0] for desc in cursor.description]
        assert "request_id" in cols
        assert "experiment_name" in cols
        assert "status" in cols
        conn.close()

    def test_round_trip_fields(self, tmp_path):
        db_path = tmp_path / "requests.db"
        req = RunRequest.make(
            request_id="req-002",
            experiment_name="perf-test",
            repo_url="https://github.com/foo/bar",
            task="Optimize the hot loop",
            branch="autoresearch/perf",
            base_commit="abc123",
            harness="opencode",
            timeout_sec=600,
            metadata={"priority": "high", "gpu": "H100"},
        )
        write_run_request(req, db_path)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT * FROM run_requests WHERE request_id = ?",
            ("req-002",),
        ).fetchone()
        conn.close()

        assert row is not None
        # Unpack by column order matching the schema.
        assert row[0] == "req-002"  # request_id
        assert row[1] == "perf-test"  # experiment_name
        assert row[2] == "https://github.com/foo/bar"  # repo_url
        assert row[3] == "autoresearch/perf"  # branch
        assert row[4] == "abc123"  # base_commit
        assert row[5] == "Optimize the hot loop"  # task
        assert row[6] == "opencode"  # harness
        assert row[7] == 600  # timeout_sec
        assert '"priority": "high"' in row[8]  # metadata_json
        assert row[9] == "pending"  # status
        assert row[10] != ""  # created_at (ISO-8601)

    def test_duplicate_request_id_raises(self, tmp_path):
        db_path = tmp_path / "requests.db"
        req = RunRequest.make(
            request_id="req-dup",
            experiment_name="e",
            repo_url="r",
            task="t",
        )
        write_run_request(req, db_path)

        with pytest.raises(sqlite3.IntegrityError):
            write_run_request(req, db_path)

    def test_multiple_requests_coexist(self, tmp_path):
        db_path = tmp_path / "requests.db"
        for i in range(5):
            req = RunRequest.make(
                request_id=f"req-{i:03d}",
                experiment_name="exp",
                repo_url="r",
                task=f"task-{i}",
            )
            write_run_request(req, db_path)

        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM run_requests").fetchone()[0]
        conn.close()
        assert count == 5

    def test_wal_mode_is_enabled(self, tmp_path):
        db_path = tmp_path / "requests.db"
        req = RunRequest.make(
            request_id="req-wal",
            experiment_name="e",
            repo_url="r",
            task="t",
        )
        write_run_request(req, db_path)

        conn = sqlite3.connect(str(db_path))
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode == "wal"

    def test_default_values_for_optional_fields(self, tmp_path):
        db_path = tmp_path / "requests.db"
        req = RunRequest.make(
            request_id="req-defaults",
            experiment_name="e",
            repo_url="r",
            task="t",
        )
        write_run_request(req, db_path)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT branch, base_commit, harness, timeout_sec FROM run_requests WHERE request_id = ?",
            ("req-defaults",),
        ).fetchone()
        conn.close()

        assert row[0] == "main"  # branch default
        assert row[1] == ""  # base_commit default
        assert row[2] == "claude-code"  # harness default
        assert row[3] == 1800  # timeout_sec default

    def test_reader_can_open_readonly_while_writer_active(self, tmp_path):
        db_path = tmp_path / "requests.db"
        req = RunRequest.make(
            request_id="req-concurrent",
            experiment_name="e",
            repo_url="r",
            task="t",
        )
        write_run_request(req, db_path)

        # Simulate Runner opening the file read-only via URI mode.
        uri = f"file:{db_path}?mode=ro"
        reader = sqlite3.connect(uri, uri=True)
        try:
            rows = reader.execute("SELECT request_id FROM run_requests").fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "req-concurrent"
        finally:
            reader.close()
