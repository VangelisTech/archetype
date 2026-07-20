# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Query contracts for component schema evolution."""

import pytest

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

pytestmark = pytest.mark.asyncio


async def test_component_query_tolerates_added_fields(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

    class Node(Component):
        x: float = 0.5

    historical = ServiceContainer()
    try:
        world = await historical.world_service.create_world(WorldConfig(name="historical"), storage)
        await historical.mutation_service.create_entity(world.world_id, [Node(x=1.0)])
        await historical.simulation_service.step(world.world_id, RunConfig())
    finally:
        await historical.shutdown()

    class Node(Component):
        x: float = 0.5
        r: float = 3.9999

    fresh = ServiceContainer()
    try:
        world = await fresh.world_service.create_world(WorldConfig(name="fresh"), storage)
        await fresh.mutation_service.create_entity(world.world_id, [Node(x=2.0, r=4.0)])
        await fresh.simulation_service.step(world.world_id, RunConfig())

        result = await fresh.query_service.query_components(
            [Node], str(world.world_id), str(world.run_id), storage
        )

        assert result.select("node__x", "node__r").to_pylist() == [{"node__x": 2.0, "node__r": 4.0}]
    finally:
        await fresh.shutdown()
