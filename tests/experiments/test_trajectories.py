# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for typed trajectory/event-log experiment components."""

import importlib
from pathlib import Path

import pyarrow as pa

from archetype.app.models import Command, CommandType, EpisodeResult
from archetype.core.component import Component
from archetype.experiments import (
    EGO_OBSERVATION_JSON_SCHEMA,
    EGO_OBSERVATION_OUTPUT_GRAMMAR,
    EGO_OBSERVATION_PROMPT,
    EgoLabel,
    EgoObservation,
    EgoObservationSource,
    EgoTrajectoryPattern,
    Trajectory,
    TrajectoryAction,
    TrajectoryCommandEvent,
    TrajectoryObservation,
    TrajectoryReward,
    TrajectoryTurn,
    Turn,
    derive_ego_label,
    derive_ego_labels,
    derive_ego_trajectory_pattern,
    ego_observations_from_structured_output,
    turns_to_components,
)
from archetype.experiments.recorders import (
    actions_from_observations,
    audit_rows_to_events,
    commands_to_events,
    observations_from_post_tick_events,
    reward_row,
    trajectory_from_episode_result,
)
from archetype.experiments.trajectories import Trajectory as GenericTrajectory

_EGO_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "10_ego_trajectory.py"


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


def test_ego_components_are_decoupled_from_generic_trajectory_module() -> None:
    trajectory_module = importlib.import_module("archetype.experiments.trajectories")
    ego_module = importlib.import_module("archetype.experiments.ego")

    assert GenericTrajectory is Trajectory
    assert not hasattr(trajectory_module, "EgoObservation")
    assert ego_module.EgoObservation is EgoObservation
    assert EgoObservationSource.__name__ == "EgoObservationSource"


def test_ego_prompt_contract_parses_into_derived_pattern() -> None:
    output = {
        "trajectory_id": "ego-1",
        "subject_id": "self",
        "source": {
            "modality": "screen",
            "artifact_uri": "capture/session-1",
            "description": "watching a speech under pressure",
        },
        "observations": [
            {
                "seq": 0,
                "frame_uri": "frames/000.png",
                "focus": "watching",
                "context": "the subject witnesses someone else's dream",
                "captured_at_ms": 0,
                "salience": 0.7,
                "valence": 0.0,
                "arousal": 0.4,
                "effort": 0.1,
                "agency": 0.2,
                "external_pressure": 0.2,
            },
            {
                "seq": 1,
                "frame_uri": "frames/001.png",
                "focus": "metric",
                "context": "the dream demands performance without agency",
                "captured_at_ms": 1000,
                "salience": 0.9,
                "valence": -0.5,
                "arousal": 0.8,
                "effort": 0.9,
                "agency": 0.2,
                "external_pressure": 0.9,
            },
        ],
    }

    observations = ego_observations_from_structured_output(output)
    pattern = derive_ego_trajectory_pattern(observations)

    assert "ego_trajectory_output" in EGO_OBSERVATION_OUTPUT_GRAMMAR
    assert "Do not emit labels" in EGO_OBSERVATION_PROMPT
    assert EGO_OBSERVATION_JSON_SCHEMA["required"] == [
        "trajectory_id",
        "subject_id",
        "source",
        "observations",
    ]
    assert observations[0].modality == "screen"
    assert observations[1].frame_uri == "frames/001.png"
    assert pattern.pattern == "captured_dream"


def test_ego_trajectory_example_smoke(capsys) -> None:
    spec = importlib.util.spec_from_file_location("ego_example", _EGO_EXAMPLE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    module.main()
    out = capsys.readouterr().out

    assert "grammar_root=True" in out
    assert "pattern=reclaimed_dream" in out
    assert "path=witness>strain>question>commit" in out


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
        EgoObservation,
        EgoLabel,
        EgoTrajectoryPattern,
    ):
        schema = component.to_pyarrow_schema()
        assert isinstance(schema, pa.Schema)
        assert "trajectory_id" in schema.names
        assert not any(name.endswith("_json") for name in schema.names)


def test_trajectory_from_episode_result_records_header_fields() -> None:
    episode = EpisodeResult(
        episode_id="episode-1",
        world_id="world-1",
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
    command = Command(type=CommandType.SPAWN, tick=2, priority=5, version=3)
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


def test_ego_observation_from_screen_frame_normalizes_scores() -> None:
    observation = EgoObservation.from_screen_frame(
        "ego-1",
        0,
        "frames/000.png",
        subject_id="self",
        focus="speech",
        salience=1.5,
        valence=-2.0,
        agency=-1.0,
        external_pressure=2.0,
    )

    assert observation.modality == "screen"
    assert observation.frame_uri == "frames/000.png"
    assert observation.salience == 1.0
    assert observation.valence == -1.0
    assert observation.agency == 0.0
    assert observation.external_pressure == 1.0


def test_ego_label_marks_gamed_intelligence_as_captured() -> None:
    observation = EgoObservation(
        trajectory_id="ego-1",
        seq=2,
        focus="metric",
        salience=0.7,
        effort=0.9,
        agency=0.2,
        external_pressure=0.95,
    )

    label = derive_ego_label(observation)

    assert label.phase == "strain"
    assert label.pattern == "instrumentalized_intelligence"
    assert label.value == "captured"
    assert label.confidence == 0.75


def test_ego_trajectory_pattern_detects_captured_dream() -> None:
    observations = [
        EgoObservation(trajectory_id="ego-1", seq=0, focus="scene", salience=0.5, agency=0.3),
        EgoObservation(
            trajectory_id="ego-1",
            seq=1,
            focus="metric",
            salience=0.9,
            effort=0.95,
            agency=0.2,
            external_pressure=0.95,
        ),
        EgoObservation(
            trajectory_id="ego-1",
            seq=2,
            focus="optimize",
            salience=0.8,
            effort=0.9,
            agency=0.25,
            external_pressure=0.9,
        ),
    ]

    labels = derive_ego_labels(observations)
    pattern = derive_ego_trajectory_pattern(observations, labels)

    assert [label.phase for label in labels] == ["witness", "strain", "strain"]
    assert [label.value for label in labels] == ["witness", "captured", "captured"]
    assert pattern.pattern == "captured_dream"
    assert pattern.canonical_path == "witness>strain>strain"


def test_ego_trajectory_pattern_detects_reclaimed_dream() -> None:
    observations = [
        EgoObservation(trajectory_id="ego-1", seq=0, focus="scene", salience=0.6, agency=0.2),
        EgoObservation(
            trajectory_id="ego-1",
            seq=1,
            focus="performance",
            salience=0.8,
            effort=0.9,
            agency=0.2,
            external_pressure=0.9,
        ),
        EgoObservation(
            trajectory_id="ego-1",
            seq=2,
            focus="question",
            salience=0.8,
            effort=0.7,
            agency=0.55,
            external_pressure=0.7,
        ),
        EgoObservation(
            trajectory_id="ego-1",
            seq=3,
            focus="departure",
            salience=0.9,
            effort=0.6,
            agency=0.8,
            external_pressure=0.2,
        ),
    ]

    labels = derive_ego_labels(list(reversed(observations)))
    pattern = derive_ego_trajectory_pattern(observations, labels)

    assert [label.phase for label in labels] == ["witness", "strain", "question", "commit"]
    assert pattern.pattern == "reclaimed_dream"
    assert pattern.canonical_path == "witness>strain>question>commit"
    assert pattern.agency_delta == 0.6000000000000001
    assert pattern.terminal_phase == "commit"


def test_ego_trajectory_pattern_detects_self_authored_dream() -> None:
    observations = [
        EgoObservation(
            trajectory_id="ego-1",
            seq=0,
            focus="field",
            salience=0.5,
            agency=0.65,
            external_pressure=0.2,
        ),
        EgoObservation(
            trajectory_id="ego-1",
            seq=1,
            focus="practice",
            salience=0.7,
            effort=0.6,
            agency=0.75,
            external_pressure=0.25,
        ),
        EgoObservation(
            trajectory_id="ego-1",
            seq=2,
            focus="choice",
            salience=0.8,
            effort=0.5,
            agency=0.82,
            external_pressure=0.2,
        ),
    ]

    labels = derive_ego_labels(observations)
    pattern = derive_ego_trajectory_pattern(observations, labels)

    assert [label.phase for label in labels] == ["witness", "commit", "commit"]
    assert [label.value for label in labels] == ["witness", "self-authored", "self-authored"]
    assert pattern.pattern == "self_authored_dream"
    assert pattern.agency_delta == 0.16999999999999993
