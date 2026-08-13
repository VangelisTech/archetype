# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""TrajectoryService and runtime composition contracts."""

from __future__ import annotations

import pytest
from daft import DataFrame

from archetype import ArchetypeRuntime
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.missions._extension import get_manifest
from archetype.missions.trajectories import (
    TrajectoryReward,
    TrajectorySelection,
    TrajectoryTurn,
    trajectory,
)
from archetype.missions.trajectories.models import GradeTrajectory, QueryTrajectory
from archetype.world.models import CreateWorld, Run, Spawn
from evals.graders import exact_match, state_check
from evals.types import GraderResult
from tests._runtime import build_test_runtime


def _rows(frame: DataFrame) -> list[dict]:
    return frame.collect().to_pylist()


@pytest.mark.asyncio
async def test_service_filters_one_persisted_evidence_table(tmp_path) -> None:
    resources = build_test_runtime(tmp_path, world_libraries=(get_manifest(),))
    dispatcher = resources.dispatcher
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="trajectory")
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="trajectory"),
                storage_config=storage,
            )
        )
        for turn in (
            TrajectoryTurn(episode_id="episode-a", seq=0, role="user", content="reach"),
            TrajectoryTurn(episode_id="episode-b", seq=0, role="user", content="retry"),
        ):
            await dispatcher.apply(
                Spawn.from_components(
                    world_id=world.world_id,
                    components=[turn],
                )
            )

        run = await dispatcher.apply(
            Run(world_id=world.world_id, run_config=RunConfig(num_steps=1))
        )
        frame = await dispatcher.apply(
            QueryTrajectory(
                component=TrajectoryTurn,
                world_id=world.world_id,
                run_id=run.run_id,
                selection=TrajectorySelection(episode_ids=("episode-b",)),
                storage_config=storage,
            )
        )

        assert [row["trajectoryturn__episode_id"] for row in _rows(frame)] == ["episode-b"]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_service_composes_query_with_evaluation_graders(tmp_path) -> None:
    resources = build_test_runtime(tmp_path, world_libraries=(get_manifest(),))
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
            TrajectoryReward(episode_id="episode-a", seq=0, reward=0.25),
            TrajectoryReward(episode_id="episode-a", seq=1, reward=1.0),
            TrajectoryReward(episode_id="episode-b", seq=0, reward=-1.0),
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
                selection=TrajectorySelection(episode_ids=("episode-a",)),
                graders=(grade_total_reward,),
                storage_config=storage,
            )
        )

        assert [result.passed for result in results] == [True, True]
        assert [result.grader_name for result in results] == ["sample_count", "total_reward"]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_runtime_world_exposes_trajectory_query_and_grading(tmp_path) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_trajectory")
    async with ArchetypeRuntime(world_libraries=(get_manifest(),)) as runtime:
        world = runtime.world("runtime-trajectory", storage=storage)
        await world.spawn(TrajectoryReward(episode_id="episode-a", seq=0, reward=0.25))
        await world.spawn(TrajectoryReward(episode_id="episode-a", seq=1, reward=1.0))
        await world.spawn(TrajectoryReward(episode_id="episode-b", seq=0, reward=-1.0))
        await world.run(steps=1)
        selection = TrajectorySelection(episode_ids=("episode-a",))

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


@pytest.mark.asyncio
async def test_derived_trajectory_view_orders_persisted_evidence(tmp_path) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="derived_view")
    async with ArchetypeRuntime(world_libraries=(get_manifest(),)) as runtime:
        world = runtime.world("derived-view", storage=storage)
        # Spawn out of seq order; the derived view must restore evidence order.
        await world.spawn(TrajectoryTurn(episode_id="episode-a", seq=1, role="assistant"))
        await world.spawn(TrajectoryTurn(episode_id="episode-a", seq=0, role="user"))
        await world.spawn(TrajectoryTurn(episode_id="episode-b", seq=0, role="user"))
        await world.run(steps=1)

        frame = await world.query_trajectory(TrajectoryTurn)
        view = trajectory(frame, TrajectoryTurn, episode_id="episode-a")
        rows = _rows(view)

        assert [row["trajectoryturn__seq"] for row in rows] == [0, 1]
        assert [row["trajectoryturn__role"] for row in rows] == ["user", "assistant"]


def test_sync_runtime_world_mirrors_trajectory_query(tmp_path) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="sync_trajectory")
    with ArchetypeRuntime.sync(world_libraries=(get_manifest(),)) as runtime:
        world = runtime.world("sync-trajectory", storage=storage)
        world.spawn(TrajectoryReward(episode_id="episode-a", seq=0, reward=2.0))
        world.run(steps=1)

        frame = world.query_trajectory(
            TrajectoryReward,
            selection=TrajectorySelection(episode_ids=("episode-a",)),
        )

        assert _rows(frame)[0]["trajectoryreward__reward"] == 2.0
