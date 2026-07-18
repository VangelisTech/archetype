# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for MutationService — entity ID accuracy and mutation lifecycle."""

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest
from uuid_utils import uuid7

from archetype.app.commands.service import CommandScheduler, _parse_entity_id
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
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


@pytest.mark.parametrize(
    ("value", "expected"),
    [(7, 7), ("7", 7), ("+7", 7), ("-7", -7), ("007", 7)],
)
def test_parse_entity_id_accepts_only_exact_integer_forms(value, expected):
    assert _parse_entity_id(value) == expected


def test_update_has_one_authoritative_dispatch_arm():
    """Each command type has one reachable implementation in the drain dispatcher."""
    source = inspect.getsource(CommandScheduler._apply)

    assert source.count("case CommandType.UPDATE:") == 1


@pytest.mark.parametrize("value", [True, 7.0, 7.9, "7.0", " 7", "7 ", "", None])
def test_parse_entity_id_rejects_lossy_or_ambiguous_values(value):
    with pytest.raises(TypeError, match="entity_id must be an integer"):
        _parse_entity_id(value)


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
        cs = container.command_gateway

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
                payload={"entity_id": str(eid), "component_types": [Velocity]},
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command_type", "payload", "mutation_method"),
    [
        (CommandType.UPDATE, {"components": [Position(x=2.0)]}, "update_entity"),
        (CommandType.DESPAWN, {}, "remove_entity"),
        (CommandType.ADD_COMPONENT, {"components": [Velocity()]}, "add_components"),
        (
            CommandType.REMOVE_COMPONENT,
            {"component_types": [Velocity]},
            "remove_components",
        ),
    ],
)
async def test_entity_commands_reject_fractional_ids(command_type, payload, mutation_method):
    mutations = MagicMock()
    mutation = AsyncMock()
    setattr(mutations, mutation_method, mutation)
    scheduler = CommandScheduler(MagicMock(), mutations)
    command = Command(
        type=command_type,
        payload={"entity_id": 1.9, **payload},
    )
    with pytest.raises(TypeError, match="entity_id must be an integer"):
        await scheduler._apply("world", command)
    mutation.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduled_update_is_applied(tmp_path):
    """A submitted UPDATE command changes component state after step (#193).

    UPDATE had no case in the drain dispatcher: submit() gated it, queued it,
    emitted "queued" — and _apply dropped it at a warn-level log.
    """
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        eid = await container.mutation_service.create_entity(world.world_id, [Position(x=1.0)])
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        await container.command_gateway.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.UPDATE,
                payload={"entity_id": str(eid), "components": [Position(x=99.0)]},
            ),
        )
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        rows = (await world.get_components([Position])).collect().to_pylist()
        row = next(r for r in rows if r["entity_id"] == eid)
        assert row["position__x"] == 99.0
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_same_drain_update_then_add_component_keeps_updated_state(tmp_path):
    """#193: UPDATE then ADD_COMPONENT in one drain — the widened row composes.

    _move_entity read the entity's row from tick-1 in the store, ignoring the
    freshest same-drain row parked in spawn_cache, so the migration forked
    from stale pre-update state.
    """
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cs = container.command_gateway
        eid = await container.mutation_service.create_entity(world.world_id, [Position(x=1.0)])
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        await cs.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.UPDATE,
                payload={"entity_id": str(eid), "components": [Position(x=99.0)]},
            ),
        )
        await cs.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.ADD_COMPONENT,
                payload={"entity_id": str(eid), "components": [Velocity(vx=5.0)]},
            ),
        )
        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        rows = (await world.get_components([Position, Velocity])).collect().to_pylist()
        row = next(r for r in rows if r["entity_id"] == eid)
        assert row["velocity__vx"] == 5.0
        assert row["position__x"] == 99.0, "widened row was built from stale pre-update state"
    finally:
        await container.shutdown()
