# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for typed mission trajectory schemas and pure transforms."""

from types import SimpleNamespace

import pyarrow as pa
from uuid_utils import uuid7

from archetype.core.component import Component
from archetype.missions.trajectories import (
    Trajectory,
    TrajectoryAction,
    TrajectoryCommandEvent,
    TrajectoryObservation,
    TrajectoryReward,
    TrajectoryTurn,
    Turn,
    actions_from_observations,
    audit_rows_to_events,
    commands_to_events,
    observations_from_post_tick_events,
    reward_row,
    trajectory_from_episode_result,
    turns_to_components,
)
from archetype.world.models import EpisodeResult


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


def test_trajectory_from_turns_is_header_only() -> None:
    trajectory = Trajectory.from_turns(
        "traj-1",
        [
            Turn(role="user", content="go", tokens=2, duration_ms=10),
            Turn(role="assistant", content="done", tokens=3, duration_ms=20),
        ],
        source="unit",
        outcome="success",
    )

    assert issubclass(Trajectory, Component)
    assert trajectory.trajectory_id == "traj-1"
    assert trajectory.total_turns == 2
    assert trajectory.total_tokens == 5
    assert trajectory.duration_seconds == 0.03
    assert trajectory.outcome == "success"


def test_turns_materialize_as_typed_rows() -> None:
    rows = turns_to_components(
        "traj-1",
        [
            Turn(role="user", content="go", tokens=2),
            Turn(role="assistant", content="done", tokens=3, duration_ms=25),
        ],
    )

    assert rows == [
        TrajectoryTurn(trajectory_id="traj-1", seq=0, role="user", content="go", tokens=2),
        TrajectoryTurn(
            trajectory_id="traj-1",
            seq=1,
            role="assistant",
            content="done",
            tokens=3,
            duration_ms=25,
        ),
    ]


def test_typed_trajectory_schemas_are_arrow_materializable() -> None:
    for component in (
        Trajectory,
        TrajectoryTurn,
        TrajectoryCommandEvent,
        TrajectoryObservation,
        TrajectoryAction,
        TrajectoryReward,
    ):
        schema = component.to_pyarrow_schema()
        assert isinstance(schema, pa.Schema)
        assert "trajectory_id" in schema.names
        assert not any(name.endswith("_json") for name in schema.names)


def test_trajectory_from_episode_result_records_header_fields() -> None:
    episode = EpisodeResult(
        episode_id="episode-1",
        world_id="world-1",
        run_id="run-1",
        final_tick=3,
        terminated=True,
        duration_steps=3,
    )

    trajectory = trajectory_from_episode_result(
        episode,
        rollout_id="rollout-1",
        run_id="run-1",
        task_id="task-1",
        trial_idx=7,
    )

    assert trajectory.trajectory_id == "rollout-1:trial-7:episode-1"
    assert trajectory.episode_id == "episode-1"
    assert trajectory.rollout_id == "rollout-1"
    assert trajectory.task_id == "task-1"
    assert trajectory.trial_idx == 7
    assert trajectory.terminal is True
    assert trajectory.total_steps == 3


def test_commands_and_audit_rows_materialize_as_typed_events() -> None:
    command = SimpleNamespace(
        id=uuid7(),
        type="spawn",
        tick=2,
        priority=5,
        version=3,
    )
    command_events = commands_to_events([command], trajectory_id="traj-1")
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
        trajectory_id="traj-1",
    )

    assert command_events == [
        TrajectoryCommandEvent(
            trajectory_id="traj-1",
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
            trajectory_id="traj-1",
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
        trajectory_id="traj-1",
    )
    actions = actions_from_observations(observations)
    reward = reward_row(trajectory_id="traj-1", reward=1.0)

    assert observations == [
        TrajectoryObservation(
            trajectory_id="traj-1",
            seq=0,
            world_id="world-1",
            tick=1,
            event_type="post_tick",
            archetype_count=2,
            entity_count=5,
        )
    ]
    assert actions == [TrajectoryAction(trajectory_id="traj-1", seq=0, tick=1, action_type="step")]
    assert reward == TrajectoryReward(trajectory_id="traj-1", seq=0, reward=1.0)
