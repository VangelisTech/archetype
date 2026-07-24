# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""EvaluationService contracts."""

from __future__ import annotations

import daft
import pytest
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.evaluation.models import RunGraders
from archetype.world.models import (
    AddProcessor,
    ComponentTypeRef,
    CreateWorld,
    EpisodeConfig,
    QueryComponents,
    Run,
    RunEpisode,
    Spawn,
)
from tests._runtime import build_test_runtime


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
async def test_evaluation_service_queries_explicit_component_window(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval")
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="scores"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(AddProcessor(world_id=world.world_id, processor=IncrementScore()))
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Score(value=1)],
            )
        )

        run = await dispatcher.apply(
            Run(world_id=world.world_id, run_config=RunConfig(num_steps=3))
        )
        df = await dispatcher.apply(
            QueryComponents(
                components=(ComponentTypeRef.from_type(Score),),
                world_id=world.world_id,
                run_id=run.run_id,
                ticks=(0, 1, 2),
                storage_config=storage,
            )
        )

        assert isinstance(df, DataFrame)
        assert [row["score__value"] for row in _score_rows(df)] == [1, 2, 3]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_evaluation_service_queries_episode_dataframe(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval_episode")
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="episode"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(AddProcessor(world_id=world.world_id, processor=IncrementScore()))
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Score(value=5)],
            )
        )
        await dispatcher.apply(Run(world_id=world.world_id, run_config=RunConfig(num_steps=1)))

        episode = await dispatcher.apply(
            RunEpisode(
                world_id=world.world_id,
                config=EpisodeConfig(max_steps=2),
            )
        )

        df = await dispatcher.apply(
            QueryComponents(
                components=(ComponentTypeRef.from_type(Score),),
                world_id=episode.world_id,
                run_id=episode.run_id,
                ticks=tuple(range(episode.start_tick, episode.final_tick)),
                storage_config=storage,
            )
        )

        rows = _score_rows(df)
        assert episode.start_tick == 1
        assert [row["tick"] for row in rows] == [1, 2]
        assert [row["score__value"] for row in rows] == [6, 7]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_evaluation_operation_rejects_vacuous_grader_sets(tmp_path):
    resources = build_test_runtime(tmp_path)
    try:
        df = daft.from_pydict({"value": [1]})

        with pytest.raises(ValueError, match="at least one grader"):
            await resources.dispatcher.apply(RunGraders(df=df, graders=()))

        def no_results(_frame: DataFrame) -> list[object]:
            return []

        with pytest.raises(ValueError, match="returned no outputs"):
            await resources.dispatcher.apply(RunGraders(df=df, graders=(no_results,)))
    finally:
        await resources.aclose()
