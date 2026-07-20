# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Component-query contracts across durable schema generations."""

import pytest
from pydantic import create_model

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

pytestmark = pytest.mark.asyncio


async def test_component_query_tolerates_added_fields(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    OldNode = create_model("Node", x=(float, 0.5), __base__=Component)

    original = ServiceContainer()
    try:
        world = await original.world_service.create_world(WorldConfig(name="old-schema"), storage)
        await original.mutation_service.create_entity(world.world_id, [OldNode(x=1.0)])
        await original.simulation_service.step(world.world_id, RunConfig())
    finally:
        await original.shutdown()

    NewNode = create_model("Node", x=(float, 0.5), r=(float, 3.9999), __base__=Component)
    fresh = ServiceContainer()
    try:
        world = await fresh.world_service.create_world(WorldConfig(name="new-schema"), storage)
        await fresh.mutation_service.create_entity(world.world_id, [NewNode(x=2.0)])
        await fresh.simulation_service.step(world.world_id, RunConfig())

        frame = await fresh.query_service.query_components(
            [NewNode], str(world.world_id), str(world.run_id), storage
        )

        rows = frame.to_pylist()
        assert len(rows) == 1
        assert rows[0]["node__x"] == 2.0
        assert rows[0]["node__r"] == 3.9999
    finally:
        await fresh.shutdown()
