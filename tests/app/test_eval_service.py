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
