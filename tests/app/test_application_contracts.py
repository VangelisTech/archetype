# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the actor-free application boundary."""

from __future__ import annotations

import asyncio

import pytest
from daft import DataFrame

from archetype import AsyncProcessor, Component
from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

pytestmark = [
    pytest.mark.contract("runtime.lifecycle.single_flight_and_drain"),
    pytest.mark.integration,
    pytest.mark.race,
]


class Value(Component):
    number: int = 0


class BlockingProcessor(AsyncProcessor):
    components = (Value,)

    def __init__(self, entered: asyncio.Event, release: asyncio.Event) -> None:
        self.entered = entered
        self.release = release

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        self.entered.set()
        await self.release.wait()
        return df


@pytest.mark.asyncio
async def test_same_world_steps_serialize_and_publish_distinct_manifests(tmp_path):
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-serial")
    try:
        info = await container.application.create_world(WorldConfig(name="serial"), storage)
        await container.application.create_entity(info.world_id, [Value(number=1)])
        run_id = "serialized-run"

        await asyncio.gather(
            container.application.step(info.world_id, RunConfig(run_id=run_id)),
            container.application.step(info.world_id, RunConfig(run_id=run_id)),
        )

        world = container.world_service.get_world(info.world_id)
        active_run_id = str(world.run_id)
        rows = (
            await container.application.query_components(
                [Value], str(info.world_id), active_run_id, storage
            )
        ).to_pylist()
        visible = await container.storage_service.get_control_catalog(storage).visible_tokens(
            str(info.world_id), active_run_id, [0, 1]
        )

        assert world.tick == 2
        assert sorted(row["tick"] for row in rows) == [0, 1]
        assert visible is not None
        assert sorted(visible) == [0, 1]
        assert all(len(visible[tick]) == 1 for tick in (0, 1))
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_destroy_waits_for_an_admitted_same_world_step(tmp_path):
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-destroy")
    entered = asyncio.Event()
    release = asyncio.Event()
    try:
        info = await container.application.create_world(WorldConfig(name="destroy-order"), storage)
        await container.application.create_entity(info.world_id, [Value(number=1)])
        await container.application.add_processor(
            info.world_id, BlockingProcessor(entered, release)
        )

        step = asyncio.create_task(container.application.step(info.world_id, RunConfig()))
        await entered.wait()
        destroy = asyncio.create_task(container.application.destroy_world(info.world_id))
        await asyncio.sleep(0)

        assert not destroy.done()
        release.set()
        await step
        await destroy
        assert not container.world_service.has_world(info.world_id)
    finally:
        release.set()
        await container.shutdown()


@pytest.mark.asyncio
async def test_different_world_lanes_execute_concurrently(tmp_path):
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-parallel")
    release = asyncio.Event()
    entered_a = asyncio.Event()
    entered_b = asyncio.Event()
    try:
        first = await container.application.create_world(WorldConfig(name="first"), storage)
        second = await container.application.create_world(WorldConfig(name="second"), storage)
        for info, entered in ((first, entered_a), (second, entered_b)):
            await container.application.create_entity(info.world_id, [Value(number=1)])
            await container.application.add_processor(
                info.world_id, BlockingProcessor(entered, release)
            )

        first_step = asyncio.create_task(
            container.application.step(first.world_id, RunConfig(run_id="first-run"))
        )
        second_step = asyncio.create_task(
            container.application.step(second.world_id, RunConfig(run_id="second-run"))
        )
        await asyncio.wait_for(asyncio.gather(entered_a.wait(), entered_b.wait()), timeout=2)

        release.set()
        await asyncio.gather(first_step, second_step)
    finally:
        release.set()
        await container.shutdown()
