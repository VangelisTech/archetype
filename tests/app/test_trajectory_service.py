# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""TrajectoryService and runtime composition contracts."""

from __future__ import annotations

import pytest
from daft import DataFrame

from archetype import ArchetypeRuntime
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.episodes.models import GradeTrajectory, QueryTrajectory
from archetype.missions.trajectories import Trajectory, TrajectoryReward, TrajectorySelection
from archetype.world.models import CreateWorld, Run, Spawn
from evals.graders import exact_match, state_check
from evals.types import GraderResult
from tests._runtime import build_test_runtime


def _rows(frame: DataFrame) -> list[dict]:
    return frame.collect().to_pylist()


@pytest.mark.asyncio
async def test_service_filters_one_persisted_trajectory_table(tmp_path) -> None:
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="trajectory")
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="trajectory"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[
                    Trajectory(
                        trajectory_id="traj-a",
                        run_id="run-a",
                        episode_id="episode-a",
                        task_id="reach",
                        trial_idx=0,
                        terminal=True,
                        outcome="success",
                    ),
                ],
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[
                    Trajectory(
                        trajectory_id="traj-b",
                        run_id="run-a",
                        episode_id="episode-b",
                        task_id="reach",
                        trial_idx=1,
                        terminal=True,
                        outcome="failure",
                    ),
                ],
            )
        )

        run = await dispatcher.apply(
            Run(world_id=world.world_id, run_config=RunConfig(num_steps=1))
        )
        frame = await dispatcher.apply(
            QueryTrajectory(
                component=Trajectory,
                world_id=world.world_id,
                run_id=run.run_id,
                selection=TrajectorySelection(task_ids=("reach",), trial_idxs=(1,)),
                storage_config=storage,
            )
        )

        assert [row["trajectory__trajectory_id"] for row in _rows(frame)] == ["traj-b"]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_service_composes_query_with_evaluation_graders(tmp_path) -> None:
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="rewards")
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="rewards"),
                storage_config=storage,
            )
        )
        for reward in (
            TrajectoryReward(trajectory_id="traj-a", seq=0, reward=0.25),
            TrajectoryReward(trajectory_id="traj-a", seq=1, reward=1.0),
            TrajectoryReward(trajectory_id="traj-b", seq=0, reward=-1.0),
        ):
            await dispatcher.apply(
                Spawn.from_components(
                    world_id=world.world_id,
                    components=[reward],
                )
            )
        run = await dispatcher.apply(
            Run(world_id=world.world_id, run_config=RunConfig(num_steps=1))
        )

        def grade_total_reward(frame: DataFrame) -> list[GraderResult]:
            rows = _rows(frame)
            total_reward = sum(row["trajectoryreward__reward"] for row in rows)
            return [
                exact_match(len(rows), 2, name="sample_count"),
                exact_match(total_reward, 1.25, name="total_reward"),
            ]

        results = await dispatcher.apply(
            GradeTrajectory(
                component=TrajectoryReward,
                world_id=world.world_id,
                run_id=run.run_id,
                selection=TrajectorySelection(trajectory_ids=("traj-a",)),
                graders=(grade_total_reward,),
                storage_config=storage,
            )
        )

        assert [result.passed for result in results] == [True, True]
        assert [result.grader_name for result in results] == ["sample_count", "total_reward"]

        with pytest.raises(
            ValueError,
            match=r"TrajectoryReward does not store requested trajectory filter field\(s\): task_id",
        ):
            await dispatcher.apply(
                QueryTrajectory(
                    component=TrajectoryReward,
                    world_id=world.world_id,
                    run_id=run.run_id,
                    selection=TrajectorySelection(task_ids=("reach",)),
                    storage_config=storage,
                )
            )
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_runtime_world_exposes_trajectory_query_and_grading(tmp_path) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_trajectory")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("runtime-trajectory", storage=storage)
        await world.spawn(TrajectoryReward(trajectory_id="traj-a", seq=0, reward=0.25))
        await world.spawn(TrajectoryReward(trajectory_id="traj-a", seq=1, reward=1.0))
        await world.spawn(TrajectoryReward(trajectory_id="traj-b", seq=0, reward=-1.0))
        await world.run(steps=1)
        selection = TrajectorySelection(trajectory_ids=("traj-a",))

        frame = await world.query_trajectory(TrajectoryReward, selection=selection)

        def grade_reward(rows: DataFrame) -> GraderResult:
            return state_check(
                {
                    "total_reward": sum(row["trajectoryreward__reward"] for row in _rows(rows))
                    == 1.25
                },
                name="runtime_trajectory",
            )

        results = await world.grade_trajectory(
            TrajectoryReward,
            selection=selection,
            graders=[grade_reward],
        )

        assert len(_rows(frame)) == 2
        assert results[0].passed is True


def test_sync_runtime_world_mirrors_trajectory_query(tmp_path) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="sync_trajectory")
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("sync-trajectory", storage=storage)
        world.spawn(TrajectoryReward(trajectory_id="traj-a", seq=0, reward=2.0))
        world.run(steps=1)

        frame = world.query_trajectory(
            TrajectoryReward,
            selection=TrajectorySelection(trajectory_ids=("traj-a",)),
        )

        assert _rows(frame)[0]["trajectoryreward__reward"] == 2.0
