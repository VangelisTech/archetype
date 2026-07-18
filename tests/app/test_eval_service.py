# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""EvaluationService contracts."""

from __future__ import annotations

import daft
import pytest
from daft import DataFrame, col

from archetype.app.container import ServiceContainer
from archetype.app.evaluation.service import EvaluationService
from archetype.app.models import EpisodeConfig
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.trajectories import Trajectory, TrajectoryReward
from evals.graders import exact_match, state_check
from evals.types import GraderResult


class Score(Component):
    value: int = 0


class IncrementScore(AsyncProcessor):
    components = (Score,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column("score__value", col("score__value") + 1)


def _score_rows(df: DataFrame) -> list[dict]:
    return df.collect().to_pylist()


@pytest.mark.asyncio
async def test_container_wires_evaluation_service():
    container = ServiceContainer()
    try:
        assert isinstance(container.evaluation_service, EvaluationService)
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_evaluation_service_queries_explicit_component_window(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval")
        world = await container.world_service.create_world(WorldConfig(name="scores"), storage)
        await world.add_processor(IncrementScore())
        await world.create_entity([Score(value=1)])

        run = await container.simulation_service.run(world.world_id, RunConfig(num_steps=3))
        df = await container.evaluation_service.query_components(
            [Score],
            world_id=world.world_id,
            run_id=run.run_id,
            ticks=[0, 1, 2],
            storage_config=storage,
        )

        assert isinstance(df, DataFrame)
        assert [row["score__value"] for row in _score_rows(df)] == [1, 2, 3]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_evaluation_service_queries_episode_dataframe(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval_episode")
        world = await container.world_service.create_world(WorldConfig(name="episode"), storage)
        await world.add_processor(IncrementScore())
        await world.create_entity([Score(value=5)])
        await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))

        episode = await container.simulation_service.run_episode(
            world.world_id,
            EpisodeConfig(max_steps=2),
        )

        df = await container.evaluation_service.query_episode(
            episode,
            components=[Score],
            storage_config=storage,
        )

        rows = _score_rows(df)
        assert episode.start_tick == 1
        assert [row["tick"] for row in rows] == [1, 2]
        assert [row["score__value"] for row in rows] == [6, 7]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_evaluation_service_filters_trajectory_component_and_runs_graders(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval_trajectory")
        world = await container.world_service.create_world(WorldConfig(name="trajectory"), storage)
        await world.create_entity(
            [
                Trajectory(
                    trajectory_id="traj-a",
                    run_id="run-a",
                    episode_id="episode-a",
                    task_id="reach",
                    trial_idx=0,
                    terminal=True,
                    outcome="success",
                )
            ]
        )
        await world.create_entity(
            [
                Trajectory(
                    trajectory_id="traj-b",
                    run_id="run-a",
                    episode_id="episode-b",
                    task_id="reach",
                    trial_idx=1,
                    terminal=True,
                    outcome="failure",
                )
            ]
        )

        run = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))
        df = await container.evaluation_service.query_trajectory_component(
            Trajectory,
            world_id=world.world_id,
            run_id=run.run_id,
            task_ids=["reach"],
            trial_idxs=[1],
            storage_config=storage,
        )

        rows = _score_rows(df)
        assert [row["trajectory__trajectory_id"] for row in rows] == ["traj-b"]

        def grade_failure(frame: DataFrame) -> GraderResult:
            graded_rows = frame.collect().to_pylist()
            return state_check(
                {
                    "one_row": len(graded_rows) == 1,
                    "selected_failure": graded_rows[0]["trajectory__outcome"] == "failure",
                },
                name="selected_trajectory",
            )

        results = await container.evaluation_service.run_graders(df, [grade_failure])

        assert len(results) == 1
        assert isinstance(results[0], GraderResult)
        assert results[0].passed is True
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_evaluation_service_grades_trajectory_component_with_custom_sampling(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval_rewards")
        world = await container.world_service.create_world(WorldConfig(name="rewards"), storage)
        await world.create_entity([TrajectoryReward(trajectory_id="traj-a", seq=0, reward=0.25)])
        await world.create_entity([TrajectoryReward(trajectory_id="traj-a", seq=1, reward=1.0)])
        await world.create_entity([TrajectoryReward(trajectory_id="traj-b", seq=0, reward=-1.0)])

        run = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))

        def grade_total_reward(frame: DataFrame) -> list[GraderResult]:
            rows = frame.collect().to_pylist()
            total_reward = sum(row["trajectoryreward__reward"] for row in rows)
            return [
                exact_match(len(rows), 2, name="sample_count"),
                exact_match(total_reward, 1.25, name="total_reward"),
            ]

        results = await container.evaluation_service.grade_trajectory_component(
            TrajectoryReward,
            world_id=world.world_id,
            run_id=run.run_id,
            trajectory_ids=["traj-a"],
            graders=[grade_total_reward],
            storage_config=storage,
        )

        assert [result.passed for result in results] == [True, True]
        assert [result.grader_name for result in results] == ["sample_count", "total_reward"]

        with pytest.raises(
            ValueError,
            match=r"TrajectoryReward does not store requested trajectory filter field\(s\): task_id",
        ):
            await container.evaluation_service.query_trajectory_component(
                TrajectoryReward,
                world_id=world.world_id,
                run_id=run.run_id,
                task_ids=["reach"],
                storage_config=storage,
            )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_evaluation_service_rejects_vacuous_grader_sets():
    container = ServiceContainer()
    try:
        df = daft.from_pydict({"value": [1]})

        with pytest.raises(ValueError, match="at least one grader"):
            await container.evaluation_service.run_graders(df, [])

        def no_results(_frame: DataFrame) -> list[object]:
            return []

        with pytest.raises(ValueError, match="returned no outputs"):
            await container.evaluation_service.run_graders(df, [no_results])
    finally:
        await container.shutdown()
