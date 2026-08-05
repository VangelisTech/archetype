# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Terminal projection contracts for Agent Missions."""

from __future__ import annotations

import daft

from archetype.missions import (
    Commit,
    Mission,
    MissionState,
    SubmittedMission,
    Task,
    TaskDispatch,
    TaskState,
)
from archetype.missions.contracts import mission_episode_id
from archetype.missions.projections import project_mission_result


def test_terminal_result_deduplicates_a_commit_observed_across_retries() -> None:
    mission = Mission.get_prefix()
    mission_state = MissionState.get_prefix()
    task = Task.get_prefix()
    task_state = TaskState.get_prefix()
    dispatch = TaskDispatch.get_prefix()
    commit = Commit.get_prefix()

    frames = {
        (Mission, MissionState): daft.from_pydict(
            {
                "entity_id": [1],
                f"{mission}episode_id": ["mission-episode-test"],
                f"{mission}repository": ["VangelisTech/archetype"],
                f"{mission}branch": ["agent/retry"],
                f"{mission_state}status": ["succeeded"],
                f"{mission_state}reason": [""],
            }
        ),
        (Task, TaskState, TaskDispatch): daft.from_pydict(
            {
                "entity_id": [2],
                f"{task}name": ["implementation"],
                f"{task_state}status": ["accepted"],
                f"{task_state}reason": [""],
                f"{dispatch}sequence": [2],
            }
        ),
        (Commit,): daft.from_pydict(
            {
                f"{commit}task_id": [2, 2, 2],
                f"{commit}sha": ["fix-sha", "fix-sha", "repair-sha"],
            }
        ),
    }

    class View:
        def frame(self, *components):
            return frames.get(components)

    result = project_mission_result(
        View(),  # type: ignore[arg-type]
        SubmittedMission(
            mission_id=1,
            task_ids=(("implementation", 2),),
            episode_id="mission-episode-test",
        ),
        ticks_completed=11,
    )

    assert result.tasks[0].commit_shas == ("fix-sha", "repair-sha")
    assert result.episode_id == "mission-episode-test"


def test_mission_episode_identity_is_stable_and_world_scoped() -> None:
    first = mission_episode_id("world-a", 17)

    assert first == mission_episode_id("world-a", 17)
    assert first != mission_episode_id("world-b", 17)
    assert first != mission_episode_id("world-a", 18)
    assert first.startswith("mission-episode-")
