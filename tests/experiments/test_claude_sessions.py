# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claude session transcript loader contracts."""

import json

from archetype.experiments.claude_sessions import (
    load_claude_session,
    load_claude_sessions,
)
from archetype.experiments.trajectories import Trajectory, TrajectoryTurn


def _line(line_type: str, content, *, ts: str, **extra) -> str:
    base = {
        "type": line_type,
        "timestamp": ts,
        "sessionId": "s-1",
        "cwd": "/repo",
        "gitBranch": "main",
        "version": "3.0.0",
        "message": {"role": line_type, "content": content},
    }
    base.update(extra)
    return json.dumps(base)


def _write_session(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def test_session_transcribes_to_trajectory(tmp_path):
    session = tmp_path / "proj-a" / "abc123.jsonl"
    _write_session(
        session,
        [
            json.dumps({"type": "queue-operation", "operation": "enqueue"}),
            _line("user", "Fix the login bug", ts="2026-06-01T10:00:00.000Z"),
            json.dumps(
                {
                    "type": "assistant",
                    "timestamp": "2026-06-01T10:00:05.000Z",
                    "gitBranch": "main",
                    "message": {
                        "role": "assistant",
                        "model": "claude-fable-5",
                        "usage": {"output_tokens": 42},
                        "content": [
                            {"type": "thinking", "thinking": "private"},
                            {"type": "text", "text": "Reading auth.py"},
                            {
                                "type": "tool_use",
                                "name": "Read",
                                "input": {"file_path": "auth.py"},
                            },
                        ],
                    },
                }
            ),
            _line(
                "user",
                [{"type": "tool_result", "is_error": False, "content": "def login(): ..."}],
                ts="2026-06-01T10:00:06.000Z",
            ),
            _line(
                "user",
                [{"type": "tool_result", "is_error": True, "content": "boom"}],
                ts="2026-06-01T10:00:07.000Z",
            ),
        ],
    )

    loaded = load_claude_session(session)
    assert loaded is not None
    assert loaded.trajectory.trajectory_id == "abc123"
    assert loaded.trajectory.source == "claude-code"
    assert loaded.trajectory.model == "claude-fable-5"
    assert loaded.trajectory.task_id == "proj-a"
    assert loaded.project == "proj-a"
    assert loaded.git_branch == "main"
    assert loaded.started_at.startswith("2026-06-01T10:00:00")

    roles = [t.role for t in loaded.turns]
    # thinking skipped; assistant text + tool_use each become turns
    assert roles == ["user", "assistant", "tool_call", "tool_result", "tool_result"]
    assert loaded.turns[1].tokens == 42
    assert loaded.turns[2].tokens == 0, "usage attributed once per assistant message"
    assert loaded.turns[2].tool_name == "Read"
    assert loaded.turns[3].error == ""
    assert loaded.turns[4].error == "tool_error"
    # durations derive from timestamps (5s between user and assistant)
    assert loaded.turns[1].duration_ms == 5000.0

    # header totals reflect the turns
    assert loaded.trajectory.total_turns == 5
    assert loaded.trajectory.total_tokens == 42


def test_components_yield_header_and_turn_rows(tmp_path):
    session = tmp_path / "proj-a" / "abc.jsonl"
    _write_session(session, [_line("user", "hello", ts="2026-06-01T10:00:00.000Z")])

    loaded = load_claude_session(session)
    rows = loaded.components()
    assert isinstance(rows[0][0], Trajectory)
    assert all(isinstance(r[0], TrajectoryTurn) for r in rows[1:])
    assert rows[1][0].trajectory_id == "abc"
    assert rows[1][0].seq == 0


def test_sidechain_and_meta_lines_skipped(tmp_path):
    session = tmp_path / "proj-b" / "side.jsonl"
    _write_session(
        session,
        [
            _line("user", "main thread", ts="2026-06-01T10:00:00.000Z"),
            _line("user", "subagent traffic", ts="2026-06-01T10:00:01.000Z", isSidechain=True),
            _line("user", "meta", ts="2026-06-01T10:00:02.000Z", isMeta=True),
        ],
    )

    loaded = load_claude_session(session)
    assert [t.content for t in loaded.turns] == ["main thread"]
    assert loaded.sidechain_turns_skipped == 1

    with_side = load_claude_session(session, include_sidechains=True)
    assert len(with_side.turns) == 2


def test_content_truncation(tmp_path):
    session = tmp_path / "proj-c" / "big.jsonl"
    _write_session(session, [_line("user", "x" * 500, ts="2026-06-01T10:00:00.000Z")])

    loaded = load_claude_session(session, max_content_chars=100)
    content = loaded.turns[0].content
    assert len(content) < 200
    assert content.endswith("…[truncated]")


def test_empty_and_noise_sessions_return_none(tmp_path):
    empty = tmp_path / "proj-d" / "empty.jsonl"
    _write_session(empty, [json.dumps({"type": "queue-operation"}), "not json {{{"])
    assert load_claude_session(empty) is None


def test_load_claude_sessions_walks_projects(tmp_path):
    for i, proj in enumerate(["p1", "p2"]):
        session = tmp_path / proj / f"s{i}.jsonl"
        _write_session(session, [_line("user", f"hello {i}", ts="2026-06-01T10:00:00.000Z")])

    sessions = load_claude_sessions(tmp_path)
    assert len(sessions) == 2
    assert sorted(s.project for s in sessions) == ["p1", "p2"]

    limited = load_claude_sessions(tmp_path, limit=1)
    assert len(limited) == 1

    assert load_claude_sessions(tmp_path / "missing") == []
