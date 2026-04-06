# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for trajectory components."""

import json

from archetype.trajectories.components import Label, Trajectory, Turn


def test_turn_round_trip():
    turn = Turn(role="assistant", content="hello", tokens=10)
    d = turn.to_dict()
    assert d["role"] == "assistant"
    assert d["content"] == "hello"
    assert d["tokens"] == 10
    restored = Turn.from_dict(d)
    assert restored.role == "assistant"
    assert restored.content == "hello"


def test_trajectory_from_turns():
    turns = [
        Turn(role="user", content="fix the bug", tokens=5),
        Turn(role="assistant", content="on it", tokens=3, duration_ms=100),
    ]
    traj = Trajectory.from_turns(
        trajectory_id="t-1",
        turns=turns,
        source="test",
        outcome="success",
        tags=["python", "bugfix"],
        metadata={"repo": "acme"},
    )
    assert traj.trajectory_id == "t-1"
    assert traj.total_turns == 2
    assert traj.total_tokens == 8
    assert traj.source == "test"
    assert "python" in json.loads(traj.tags_json)
    assert json.loads(traj.metadata_json)["repo"] == "acme"


def test_trajectory_get_turns():
    turns = [Turn(role="user", content="hi")]
    traj = Trajectory.from_turns("t-2", turns)
    restored = traj.get_turns()
    assert len(restored) == 1
    assert restored[0].role == "user"


def test_trajectory_json_suffix_fields():
    """Verify JSON-encoded fields follow the _json suffix convention."""
    traj = Trajectory()
    assert hasattr(traj, "turns_json")
    assert hasattr(traj, "tags_json")
    assert hasattr(traj, "metadata_json")
    # Should not have old names
    assert not hasattr(traj, "turns") or traj.__class__.__name__ != "Trajectory"
    assert not hasattr(traj, "tags") or traj.__class__.__name__ != "Trajectory"


def test_label_defaults():
    label = Label()
    assert label.sampled is True
    assert label.score == 0.0
    assert label.technique == ""
