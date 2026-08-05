# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for typed episode-evidence schemas, transforms, and derived views."""

from types import SimpleNamespace

import daft
import pyarrow as pa
import pytest
from uuid_utils import uuid7

from archetype.core.component import Component
from archetype.missions.trajectories import (
    TrajectoryAction,
    TrajectoryCommandEvent,
    TrajectoryObservation,
    TrajectoryReward,
    TrajectorySelection,
    TrajectoryTurn,
    Turn,
    actions_from_observations,
    audit_rows_to_events,
    commands_to_events,
    filter_trajectory_rows,
    observations_from_post_tick_events,
    reward_row,
    trajectory,
    turns_to_components,
)


def test_turn_authoring_helper_round_trips_optional_fields() -> None:
    turn = Turn(
        role="tool_call",
        content="reading file",
        tool_name="Read",
        tool_input='{"path": "a.py"}',
        tokens=5,
        duration_ms=100,
        metadata={"phase": "inspect"},
    )

    restored = Turn.from_dict(turn.to_dict())

    assert restored.role == "tool_call"
    assert restored.tool_name == "Read"
    assert restored.metadata == {"phase": "inspect"}


def test_turns_materialize_as_typed_rows() -> None:
    rows = turns_to_components(
        "episode-1",
        [
            Turn(role="user", content="go", tokens=2),
            Turn(role="assistant", content="done", tokens=3, duration_ms=25),
        ],
    )

    assert rows == [
        TrajectoryTurn(episode_id="episode-1", seq=0, role="user", content="go", tokens=2),
        TrajectoryTurn(
            episode_id="episode-1",
            seq=1,
            role="assistant",
            content="done",
            tokens=3,
            duration_ms=25,
        ),
    ]


def test_typed_evidence_schemas_are_arrow_materializable() -> None:
    for component in (
        TrajectoryTurn,
        TrajectoryCommandEvent,
        TrajectoryObservation,
        TrajectoryAction,
        TrajectoryReward,
    ):
        schema = component.to_pyarrow_schema()
        assert isinstance(schema, pa.Schema)
        assert "episode_id" in schema.names
        assert "trajectory_id" not in schema.names
        assert not any(name.endswith("_json") for name in schema.names)


def test_commands_and_audit_rows_materialize_as_typed_events() -> None:
    command = SimpleNamespace(
        id=uuid7(),
        type="spawn",
        tick=2,
        priority=5,
        version=3,
    )
    command_events = commands_to_events([command], episode_id="episode-1")
    audit_events = audit_rows_to_events(
        [
            {
                "audit_id": "audit-1",
                "command_id": "cmd-1",
                "world_id": "world-1",
                "actor_id": "actor-1",
                "command_type": "spawn",
                "status": "applied",
            }
        ],
        episode_id="episode-1",
    )

    assert command_events == [
        TrajectoryCommandEvent(
            episode_id="episode-1",
            seq=0,
            command_id=str(command.id),
            command_type="spawn",
            tick=2,
            priority=5,
            version=3,
        )
    ]
    assert audit_events == [
        TrajectoryCommandEvent(
            episode_id="episode-1",
            seq=0,
            audit_id="audit-1",
            command_id="cmd-1",
            world_id="world-1",
            actor_id="actor-1",
            command_type="spawn",
            status="applied",
        )
    ]


def test_observations_actions_and_rewards_are_typed_rows() -> None:
    observations = observations_from_post_tick_events(
        [
            {
                "event": "post_tick",
                "world_id": "world-1",
                "tick": 1,
                "archetype_count": 2,
                "entity_count": 5,
            }
        ],
        episode_id="episode-1",
    )
    actions = actions_from_observations(observations)
    reward = reward_row(episode_id="episode-1", reward=1.0)

    assert observations == [
        TrajectoryObservation(
            episode_id="episode-1",
            seq=0,
            world_id="world-1",
            tick=1,
            event_type="post_tick",
            archetype_count=2,
            entity_count=5,
        )
    ]
    assert actions == [TrajectoryAction(episode_id="episode-1", seq=0, tick=1, action_type="step")]
    assert reward == TrajectoryReward(episode_id="episode-1", seq=0, reward=1.0)


def test_selection_filters_by_stored_fields_only() -> None:
    frame = daft.from_pylist(
        [
            {"trajectoryreward__episode_id": "episode-1", "trajectoryreward__reward": 1.0},
            {"trajectoryreward__episode_id": "episode-2", "trajectoryreward__reward": -1.0},
        ]
    )

    selected = filter_trajectory_rows(
        frame,
        TrajectoryReward,
        TrajectorySelection(episode_ids=("episode-2",)),
    )
    assert [row["trajectoryreward__reward"] for row in selected.collect().to_pylist()] == [-1.0]

    class _NoEpisode(Component):
        value: int = 0

    with pytest.raises(
        ValueError,
        match=r"_NoEpisode does not store requested trajectory filter field\(s\): episode_id",
    ):
        filter_trajectory_rows(
            daft.from_pylist([{"_noepisode__value": 1}]),
            _NoEpisode,
            TrajectorySelection(episode_ids=("episode-1",)),
        )


def test_derived_trajectory_view_reconstructs_ordered_evidence() -> None:
    rows = turns_to_components(
        "episode-1",
        [
            Turn(role="user", content="go"),
            Turn(role="assistant", content="working"),
            Turn(role="assistant", content="done"),
        ],
    ) + turns_to_components("episode-2", [Turn(role="user", content="other")])
    shuffled = [rows[3], rows[2], rows[0], rows[1]]
    prefix = TrajectoryTurn.get_prefix()
    frame = daft.from_pylist(
        [{f"{prefix}{name}": value for name, value in row.model_dump().items()} for row in shuffled]
    )

    view = trajectory(frame, TrajectoryTurn, episode_id="episode-1")
    ordered = view.collect().to_pylist()

    assert [row[f"{prefix}seq"] for row in ordered] == [0, 1, 2]
    assert [row[f"{prefix}content"] for row in ordered] == ["go", "working", "done"]
    assert {row[f"{prefix}episode_id"] for row in ordered} == {"episode-1"}


def test_derived_trajectory_view_rejects_unordered_components() -> None:
    class _Unordered(Component):
        episode_id: str = ""

    with pytest.raises(ValueError, match="missing field\\(s\\): seq"):
        trajectory(daft.from_pylist([{"_unordered__episode_id": "e"}]), _Unordered, episode_id="e")
