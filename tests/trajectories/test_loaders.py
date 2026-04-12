# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``archetype.trajectories.loaders``.

Two test tiers:

1. **Unit tier**: hand-built JSONL fixtures that exercise every branch of the
   parser (including error paths and corrupt-line handling). Always runs.
2. **Real-data tier**: a ``skipif``-guarded smoke test that points at the
   current machine's ``~/.claude/projects/`` directory. Skipped on machines
   without real data (CI, fresh clones). Runs automatically during local
   development, catching format drift the instant it happens. Only asserts
   structural invariants that should hold for any real input — never
   specific values, since those vary per machine.

The real-data tier encodes the ``feedback_freeze_dogfood_as_skipif_tests``
memory lesson: hand-built fixtures test your *understanding* of a format,
not the format itself. Reality drifts; your fixtures don't.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archetype.trajectories.loaders import (
    discover_sessions,
    discover_sessions_for_cwd,
    from_claude_session,
)

# ── Fixtures ──────────────────────────────────────────────────────────────


def _write_session(path: Path, records: list[dict]) -> None:
    with path.open("w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


@pytest.fixture
def minimal_session(tmp_path: Path) -> Path:
    """Two-turn session: user prompt + assistant text reply."""
    path = tmp_path / "session-min.jsonl"
    _write_session(
        path,
        [
            {
                "type": "user",
                "timestamp": "2026-04-10T10:00:00.000Z",
                "sessionId": "test-min",
                "cwd": "/tmp/test",
                "gitBranch": "main",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-04-10T10:00:30.000Z",
                "message": {
                    "role": "assistant",
                    "model": "claude-opus-4-6",
                    "content": [{"type": "text", "text": "Hi there"}],
                    "usage": {"output_tokens": 5},
                },
            },
        ],
    )
    return path


@pytest.fixture
def rich_session(tmp_path: Path) -> Path:
    """Session with thinking, tool_use, and tool_result content parts."""
    path = tmp_path / "session-rich.jsonl"
    _write_session(
        path,
        [
            # Snapshot record — should be ignored
            {"type": "file-history-snapshot", "messageId": "abc"},
            # User prompt
            {
                "type": "user",
                "timestamp": "2026-04-10T10:00:00.000Z",
                "sessionId": "test-rich",
                "cwd": "/tmp/test",
                "gitBranch": "feature/x",
                "message": {"role": "user", "content": "List files"},
            },
            # Assistant: thinking + tool_use
            {
                "type": "assistant",
                "timestamp": "2026-04-10T10:00:15.000Z",
                "message": {
                    "role": "assistant",
                    "model": "claude-opus-4-6",
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "I should list files",
                            "signature": "sig",
                        },
                        {
                            "type": "tool_use",
                            "name": "Bash",
                            "input": {"command": "ls"},
                            "id": "tool_1",
                        },
                    ],
                    "usage": {"output_tokens": 42},
                },
            },
            # Tool result comes back as user record
            {
                "type": "user",
                "timestamp": "2026-04-10T10:00:17.000Z",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "tool_1",
                            "content": "file1.txt\nfile2.txt",
                        }
                    ],
                },
            },
            # System message — should be ignored
            {"type": "system", "content": "bookkeeping"},
        ],
    )
    return path


# ── Unit tier: from_claude_session ────────────────────────────────────────


class TestFromClaudeSession:
    def test_minimal_session(self, minimal_session):
        traj = from_claude_session(minimal_session)
        assert traj.source == "claude-code"
        assert traj.trajectory_id == "test-min"
        assert traj.total_turns == 2
        assert traj.total_tokens == 5
        assert traj.duration_seconds == 30.0  # 10:00:00 -> 10:00:30

    def test_minimal_session_turn_content(self, minimal_session):
        traj = from_claude_session(minimal_session)
        turns = traj.get_turns()
        assert turns[0].role == "user"
        assert turns[0].content == "Hello"
        assert turns[1].role == "assistant"
        assert turns[1].content == "Hi there"
        assert turns[1].tokens == 5

    def test_rich_session_flattens_multi_part(self, rich_session):
        traj = from_claude_session(rich_session)
        turns = traj.get_turns()
        # 1 user prompt + 1 thinking + 1 tool_use + 1 tool_result
        assert traj.total_turns == 4
        roles = [t.role for t in turns]
        assert roles == ["user", "assistant", "tool_call", "tool_result"]

    def test_rich_session_skips_non_conversational(self, rich_session):
        # file-history-snapshot and system records must NOT produce turns
        traj = from_claude_session(rich_session)
        assert traj.total_turns == 4  # not 6

    def test_rich_session_thinking_metadata(self, rich_session):
        traj = from_claude_session(rich_session)
        turns = traj.get_turns()
        thinking_turn = turns[1]
        assert thinking_turn.role == "assistant"
        assert "list files" in thinking_turn.content.lower()
        assert thinking_turn.metadata.get("kind") == "thinking"

    def test_rich_session_tool_call(self, rich_session):
        traj = from_claude_session(rich_session)
        turns = traj.get_turns()
        tool_turn = turns[2]
        assert tool_turn.role == "tool_call"
        assert tool_turn.tool_name == "Bash"
        assert tool_turn.tool_input is not None
        assert "ls" in tool_turn.tool_input

    def test_rich_session_tool_result(self, rich_session):
        traj = from_claude_session(rich_session)
        turns = traj.get_turns()
        result_turn = turns[3]
        assert result_turn.role == "tool_result"
        assert "file1.txt" in (result_turn.tool_output or "")

    def test_rich_session_tags_include_tool_and_branch(self, rich_session):
        traj = from_claude_session(rich_session)
        tags = json.loads(traj.tags_json)
        assert "tool:Bash" in tags
        assert "branch:feature/x" in tags

    def test_rich_session_metadata_captures_model_cwd(self, rich_session):
        traj = from_claude_session(rich_session)
        metadata = json.loads(traj.metadata_json)
        assert metadata["model"] == "claude-opus-4-6"
        assert metadata["cwd"] == "/tmp/test"
        assert metadata["git_branch"] == "feature/x"
        assert "session_file" in metadata

    def test_tool_result_is_error_flag(self, tmp_path):
        """is_error=True in a tool_result must set Turn.error."""
        path = tmp_path / "err.jsonl"
        _write_session(
            path,
            [
                {
                    "type": "user",
                    "timestamp": "2026-04-10T10:00:00.000Z",
                    "message": {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "t1",
                                "content": "boom",
                                "is_error": True,
                            }
                        ],
                    },
                },
            ],
        )
        traj = from_claude_session(path)
        turns = traj.get_turns()
        assert turns[0].role == "tool_result"
        assert turns[0].error == "tool reported error"

    def test_tool_result_no_error_flag(self, tmp_path):
        """is_error absent or False must leave Turn.error as None."""
        path = tmp_path / "ok.jsonl"
        _write_session(
            path,
            [
                {
                    "type": "user",
                    "timestamp": "2026-04-10T10:00:00.000Z",
                    "message": {
                        "role": "user",
                        "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}],
                    },
                }
            ],
        )
        traj = from_claude_session(path)
        turns = traj.get_turns()
        assert turns[0].error is None

    def test_corrupt_line_is_skipped(self, tmp_path):
        """A malformed JSONL line must not crash the loader."""
        path = tmp_path / "corrupt.jsonl"
        path.write_text(
            '{"type":"user","timestamp":"2026-04-10T10:00:00.000Z",'
            '"sessionId":"c","message":{"role":"user","content":"ok"}}\n'
            "not valid json\n"
            '{"type":"assistant","timestamp":"2026-04-10T10:00:01.000Z",'
            '"message":{"role":"assistant","content":[{"type":"text","text":"ok2"}],'
            '"usage":{"output_tokens":3}}}\n'
        )
        traj = from_claude_session(path)
        assert traj.total_turns == 2  # corrupt line skipped, not fatal
        assert traj.total_tokens == 3

    def test_empty_session_file(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        traj = from_claude_session(path)
        assert traj.total_turns == 0
        assert traj.total_tokens == 0
        assert traj.duration_seconds == 0.0


# ── Unit tier: discover_sessions ──────────────────────────────────────────


class TestDiscoverSessions:
    def test_discover_sessions_explicit_dir(self, tmp_path):
        (tmp_path / "a.jsonl").write_text("")
        (tmp_path / "b.jsonl").write_text("")
        (tmp_path / "ignore.txt").write_text("")
        # subdirs should not be recursed when top-level jsonl exist
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.jsonl").write_text("")

        found = discover_sessions(tmp_path)
        names = sorted(p.name for p in found)
        assert names == ["a.jsonl", "b.jsonl"]

    def test_discover_sessions_missing_dir_returns_empty(self, tmp_path):
        assert discover_sessions(tmp_path / "nope") == []

    def test_discover_sessions_file_arg(self, tmp_path):
        f = tmp_path / "one.jsonl"
        f.write_text("")
        assert discover_sessions(f) == [f]

    def test_discover_sessions_for_cwd_slugifies(self, tmp_path, monkeypatch):
        """cwd /a/b/c should resolve to ~/.claude/projects/-a-b-c"""
        fake_home = tmp_path / "home"
        projects_root = fake_home / ".claude" / "projects"
        # Use a simple known-absolute path for the cwd
        target_cwd = tmp_path / "some" / "project"
        target_cwd.mkdir(parents=True)

        slug = str(target_cwd.resolve()).replace("/", "-")
        project_dir = projects_root / slug
        project_dir.mkdir(parents=True)
        (project_dir / "session.jsonl").write_text("")

        monkeypatch.setattr(Path, "home", lambda: fake_home)

        found = discover_sessions_for_cwd(target_cwd)
        assert len(found) == 1
        assert found[0].name == "session.jsonl"


# ── Real-data tier: skipif-guarded smoke tests ────────────────────────────
# These tests point at the current machine's ~/.claude/projects/ directory.
# They are skipped on machines without real data (CI, fresh clones) and run
# automatically during local development to catch upstream format drift.
#
# Invariants only — never assert specific values, since the data is
# per-machine. The goal is "did this parse without exception and produce a
# structurally-valid Trajectory", not "did it produce exactly these fields".


_CLAUDE_PROJECTS_ROOT = Path.home() / ".claude" / "projects"
_HAS_REAL_DATA = _CLAUDE_PROJECTS_ROOT.exists() and any(_CLAUDE_PROJECTS_ROOT.glob("*/*.jsonl"))


@pytest.mark.skipif(
    not _HAS_REAL_DATA,
    reason="No real Claude Code sessions found under ~/.claude/projects/",
)
class TestRealClaudeSessions:
    """Structural invariant checks against live on-disk data.

    These tests do not run in CI — they are a local-development canary for
    upstream format drift. If Claude Code changes its JSONL schema, the
    unit tier stays green (it tests hand-built fixtures matching our
    understanding of the format) while this tier turns red (it tests
    reality). That's the whole point.
    """

    def test_discover_finds_some_sessions(self):
        paths = discover_sessions()
        assert len(paths) > 0, "Expected at least one real session on this machine"
        assert all(p.suffix == ".jsonl" for p in paths)

    def test_sample_sessions_parse_without_error(self):
        paths = discover_sessions()
        # Limit to 5 to keep the test fast even on machines with lots of history
        sample = paths[:5]
        for p in sample:
            # Must not raise — that's the primary invariant
            traj = from_claude_session(p)
            # Structural shape must be correct
            assert traj.source == "claude-code"
            assert isinstance(traj.trajectory_id, str) and traj.trajectory_id
            assert traj.total_turns >= 0
            assert traj.total_tokens >= 0
            assert traj.duration_seconds >= 0.0

    def test_sample_session_metadata_roundtrips(self):
        paths = discover_sessions()
        sample = paths[:5]
        for p in sample:
            traj = from_claude_session(p)
            # Metadata must always round-trip through JSON cleanly
            metadata = json.loads(traj.metadata_json)
            assert "session_file" in metadata
            tags = json.loads(traj.tags_json)
            assert isinstance(tags, list)

    def test_sample_session_turns_roundtrip(self):
        paths = discover_sessions()
        sample = paths[:3]
        for p in sample:
            traj = from_claude_session(p)
            # get_turns() must reconstruct Turn objects from the JSON blob
            turns = traj.get_turns()
            assert len(turns) == traj.total_turns
            valid_roles = {"user", "assistant", "tool_call", "tool_result", "system"}
            for turn in turns:
                assert turn.role in valid_roles

    def test_token_sum_reconciles_across_turns(self):
        """sum(turn.tokens) must equal traj.total_tokens for every session.

        This catches the sketchiest part of the flatten logic — token
        attribution to the first text/first turn — if it ever drifts.
        """
        paths = discover_sessions()
        sample = paths[:5]
        for p in sample:
            traj = from_claude_session(p)
            turn_sum = sum(t.tokens for t in traj.get_turns())
            assert turn_sum == traj.total_tokens, (
                f"Token mismatch in {p.name}: per-turn sum {turn_sum} != total {traj.total_tokens}"
            )
