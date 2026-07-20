# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure Claude transcript parsing contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archetype.missions.trajectories import (
    ClaudeTranscriptSource,
    TrajectoryTurn,
    parse_claude_transcript,
)


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


def _source(
    *,
    project: str = "proj-a",
    session_id: str = "abc123",
    max_content_chars: int = 4000,
    include_sidechains: bool = False,
) -> ClaudeTranscriptSource:
    return ClaudeTranscriptSource(
        path=Path("/source") / project / f"{session_id}.jsonl",
        project=project,
        session_id=session_id,
        max_content_chars=max_content_chars,
        include_sidechains=include_sidechains,
    )


def _parse(lines: list[str], source: ClaudeTranscriptSource | None = None):
    return parse_claude_transcript("\n".join(lines), source or _source())


def test_session_parses_to_in_memory_trajectory_and_turns() -> None:
    loaded = _parse(
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
                                "id": "tool-1",
                                "name": "Read",
                                "input": {"file_path": "auth.py"},
                            },
                        ],
                    },
                }
            ),
            _line(
                "user",
                [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "is_error": False,
                        "content": "def login(): ...",
                    }
                ],
                ts="2026-06-01T10:00:06.000Z",
            ),
            _line(
                "user",
                [{"type": "tool_result", "is_error": True, "content": "boom"}],
                ts="2026-06-01T10:00:07.000Z",
            ),
        ]
    )

    assert loaded is not None
    assert loaded.source_uri == "claude-session://proj-a/abc123"
    assert loaded.trajectory.trajectory_id == loaded.source_uri
    assert loaded.trajectory.source == "claude-code"
    assert loaded.trajectory.model == "claude-fable-5"
    assert loaded.trajectory.task_id == "proj-a"
    assert loaded.project == "proj-a"
    assert loaded.session_id == "abc123"
    assert loaded.git_branch == "main"
    assert loaded.started_at.startswith("2026-06-01T10:00:00")

    assert [turn.role for turn in loaded.turns] == [
        "user",
        "assistant",
        "tool_call",
        "tool_result",
        "tool_result",
    ]
    assert loaded.turns[1].tokens == 42
    assert loaded.turns[2].tokens == 0
    assert loaded.turns[2].tool_name == "Read"
    assert loaded.turns[3].tool_name == "Read"
    assert loaded.turns[3].tool_output == "def login(): ..."
    assert loaded.turns[3].error == ""
    assert loaded.turns[4].error == "tool_error"
    assert loaded.turns[1].duration_ms == 5000.0
    assert loaded.trajectory.total_turns == 5
    assert loaded.trajectory.total_tokens == 42


def test_parser_cannot_materialize_new_component_rows() -> None:
    loaded = _parse([_line("user", "hello", ts="2026-06-01T10:00:00.000Z")])

    assert loaded is not None
    assert not hasattr(loaded, "components")

    # Historical TrajectoryTurn rows remain a readable authoring contract;
    # transcript ingestion simply stops writing new ones.
    historical = loaded.turns[0].to_component(loaded.trajectory.trajectory_id, 0)
    assert isinstance(historical, TrajectoryTurn)
    assert historical.content == "hello"


def test_sidechain_and_meta_lines_are_skipped() -> None:
    lines = [
        _line("user", "main thread", ts="2026-06-01T10:00:00.000Z"),
        _line(
            "user",
            "subagent traffic",
            ts="2026-06-01T10:00:01.000Z",
            isSidechain=True,
        ),
        _line("user", "meta", ts="2026-06-01T10:00:02.000Z", isMeta=True),
    ]

    loaded = _parse(lines)
    assert loaded is not None
    assert [turn.content for turn in loaded.turns] == ["main thread"]
    assert loaded.sidechain_turns_skipped == 1

    with_side = _parse(lines, _source(include_sidechains=True))
    assert with_side is not None
    assert len(with_side.turns) == 2


def test_content_is_bounded_after_parsing() -> None:
    loaded = _parse(
        [_line("user", "x" * 500, ts="2026-06-01T10:00:00.000Z")],
        _source(max_content_chars=100),
    )

    assert loaded is not None
    assert len(loaded.turns[0].content) < 200
    assert loaded.turns[0].content.endswith("…[truncated]")


def test_noise_only_input_returns_none_without_touching_the_source_path() -> None:
    source = ClaudeTranscriptSource(
        path=Path("/does/not/exist/proj-d/empty.jsonl"),
        project="proj-d",
        session_id="empty",
    )

    assert _parse([json.dumps({"type": "queue-operation"}), "not json {{{"], source) is None


def test_source_identity_is_canonical_and_validated() -> None:
    source = _source(project="repo/name", session_id="session one")
    assert source.source_uri == "claude-session://repo%2Fname/session%20one"
    assert source.trajectory_id == source.source_uri

    with pytest.raises(ValueError, match=".jsonl suffix"):
        ClaudeTranscriptSource(path=Path("proj/session.txt"))
    with pytest.raises(ValueError, match="at least 1"):
        _source(max_content_chars=0)
