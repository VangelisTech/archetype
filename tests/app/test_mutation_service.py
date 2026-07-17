# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for MutationService — entity ID accuracy and mutation lifecycle."""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    hp: int = 100


@pytest.mark.asyncio
async def test_create_entity_returns_accurate_id(tmp_path):
    """create_entity returns an ID that maps to exactly the spawned entity."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)

        eid = await container.mutation_service.create_entity(
            world.world_id, [Position(x=1.0, y=2.0)]
        )

        # ID is registered immediately
        assert eid in world.entity2sig
        assert world.entity2sig[eid] is not None

        # After step, the entity is queryable in the store
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        df = await world.get_components([Position])
        rows = df.collect().to_pylist()
        entity_ids = [r["entity_id"] for r in rows]
        assert eid in entity_ids
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_create_entity_ids_are_sequential_and_unique(tmp_path):
    """Multiple create_entity calls return sequential, unique IDs."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ms = container.mutation_service

        ids = []
        for i in range(5):
            eid = await ms.create_entity(world.world_id, [Position(x=float(i))])
            ids.append(eid)

        # All unique
        assert len(set(ids)) == 5
        # Sequential
        assert ids == list(range(ids[0], ids[0] + 5))
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_remove_entity_despawns_after_step(tmp_path):
    """remove_entity marks entity for despawn; after step it's gone."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ms = container.mutation_service

        eid = await ms.create_entity(world.world_id, [Position(x=1.0)])
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        await ms.remove_entity(world.world_id, eid)
        assert eid not in world.entity2sig

        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        df = await world.get_components([Position])
        rows = df.collect().to_pylist()
        active = [r for r in rows if r.get("is_active", True)]
        assert all(r["entity_id"] != eid for r in active)
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_add_components_widens_archetype(tmp_path):
    """add_components changes the entity's archetype signature."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ms = container.mutation_service

        eid = await ms.create_entity(world.world_id, [Position(x=1.0)])
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        old_sig = world.entity2sig[eid]
        await ms.add_components(world.world_id, eid, [Velocity(vx=5.0)])
        new_sig = world.entity2sig[eid]

        assert len(new_sig) > len(old_sig)
        assert Velocity in new_sig
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_remove_components_narrows_archetype(tmp_path):
    """remove_components changes the entity's archetype signature."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ms = container.mutation_service

        eid = await ms.create_entity(world.world_id, [Position(x=1.0), Velocity(vx=5.0)])
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        await ms.remove_components(world.world_id, eid, [Velocity])
        sig = world.entity2sig[eid]

        assert Position in sig
        assert Velocity not in sig
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_add_and_remove_processor(tmp_path):
    """add_processor and remove_processor modify the world's system."""
    from daft import DataFrame

    from archetype.core.aio.async_processor import AsyncProcessor

    class NoopProcessor(AsyncProcessor):
        components = (Position,)
        priority = 0

        async def process(self, df: DataFrame, **kwargs) -> DataFrame:
            return df

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ms = container.mutation_service

        proc = NoopProcessor()
        await ms.add_processor(world.world_id, proc)
        assert proc in world.system.processors

        await ms.remove_processor(world.world_id, NoopProcessor)
        assert proc not in world.system.processors
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_entity_commands_coerce_string_entity_ids(tmp_path):
    """DESPAWN/ADD_COMPONENT/REMOVE_COMPONENT accept JSON-string entity ids (#178).

    REST payloads arrive with entity_id as a string; SPAWN and UPDATE already
    coerced with int() while these three passed the raw value through to the
    int-keyed world, silently missing the entity.
    """
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cs = container.command_service

        eid = await container.mutation_service.create_entity(world.world_id, [Position(x=1.0)])
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        await cs.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.ADD_COMPONENT,
                payload={"entity_id": str(eid), "components": [Velocity(vx=3.0)]},
            ),
        )
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))
        rows = (await world.get_components([Velocity])).collect().to_pylist()
        assert eid in [r["entity_id"] for r in rows]

        await cs.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.REMOVE_COMPONENT,
                payload={"entity_id": str(eid), "component_types": ["Velocity"]},
            ),
        )
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))
        rows = (await world.get_components([Velocity])).collect().to_pylist()
        assert eid not in [r["entity_id"] for r in rows]

        await cs.submit(
            ctx,
            world.world_id,
            Command(type=CommandType.DESPAWN, payload={"entity_id": str(eid)}),
        )
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))
        assert eid not in world.entity2sig
    finally:
        await container.shutdown()
