# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for world mutation behavior — entity ID accuracy and mutation lifecycle."""

from typing import Any

import pytest
from uuid_utils import uuid7

from archetype.commands.models import ActorCtx, DurableOptions
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.world.models import (
    AddComponents,
    AddProcessor,
    ComponentTypeRef,
    ComponentValue,
    CreateWorld,
    Despawn,
    RemoveComponents,
    RemoveProcessor,
    Spawn,
    Step,
    Update,
)
from tests._runtime import build_test_runtime


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


def _world_registry(dispatcher: Any) -> Any:
    return dispatcher._registry.resolve_name("step").handler.args[0]


async def _create_world(dispatcher: Any, tmp_path):
    info = await dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name="w"),
            storage_config=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
    )
    world = await _world_registry(dispatcher).live_world(str(info.world_id))
    assert world is not None
    return info, world


@pytest.mark.asyncio
async def test_spawn_returns_accurate_id(tmp_path):
    """Spawn returns an ID that maps to exactly the staged entity."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info, world = await _create_world(dispatcher, tmp_path)

        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0, y=2.0)],
            )
        )

        assert entity_id in world.entity2sig
        assert world.entity2sig[entity_id] is not None

        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig(num_steps=1)))

        rows = (await world.get_components([Position])).collect().to_pylist()
        assert entity_id in [row["entity_id"] for row in rows]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_spawn_ids_are_sequential_and_unique(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info, _world = await _create_world(dispatcher, tmp_path)

        entity_ids = [
            await dispatcher.apply(
                Spawn.from_components(
                    world_id=info.world_id,
                    components=[Position(x=float(index))],
                )
            )
            for index in range(5)
        ]

        assert len(set(entity_ids)) == 5
        assert entity_ids == list(range(entity_ids[0], entity_ids[0] + 5))
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_despawn_removes_entity_after_step(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        await dispatcher.apply(Despawn(world_id=info.world_id, entity_id=entity_id))
        assert entity_id not in world.entity2sig

        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        rows = (await world.get_components([Position])).collect().to_pylist()
        active = [row for row in rows if row.get("is_active", True)]
        assert all(row["entity_id"] != entity_id for row in active)
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_add_components_widens_archetype(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        old_signature = world.entity2sig[entity_id]
        await dispatcher.apply(
            AddComponents(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(Velocity(vx=5.0)),),
            )
        )
        new_signature = world.entity2sig[entity_id]

        assert len(new_signature) > len(old_signature)
        assert Velocity in new_signature
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_remove_components_narrows_archetype(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0), Velocity(vx=5.0)],
            )
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        await dispatcher.apply(
            RemoveComponents(
                world_id=info.world_id,
                entity_id=entity_id,
                component_types=(ComponentTypeRef.from_type(Velocity),),
            )
        )

        assert Position in world.entity2sig[entity_id]
        assert Velocity not in world.entity2sig[entity_id]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_add_and_remove_processor(tmp_path):
    from daft import DataFrame

    from archetype.core.aio.async_processor import AsyncProcessor

    class NoopProcessor(AsyncProcessor):
        components = (Position,)
        priority = 0

        async def process(self, df: DataFrame, **kwargs) -> DataFrame:
            return df

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        processor = NoopProcessor()

        await dispatcher.apply(AddProcessor(world_id=info.world_id, processor=processor))
        assert processor in world.system.processors

        await dispatcher.apply(
            RemoveProcessor(world_id=info.world_id, processor_type=NoopProcessor)
        )
        assert processor not in world.system.processors
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_deferred_entity_operations_accept_wire_string_ids(tmp_path):
    """JSON entity IDs normalize on exact operation models before admission."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        add = AddComponents.model_validate(
            {
                "world_id": info.world_id,
                "entity_id": str(entity_id),
                "components": (ComponentValue.from_component(Velocity(vx=3.0)),),
            }
        )
        assert add.entity_id == entity_id
        await dispatcher.defer_as(
            actor,
            add,
            DurableOptions(target_tick=world.tick),
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        rows = (await world.get_components([Velocity])).collect().to_pylist()
        assert entity_id in [row["entity_id"] for row in rows]

        remove = RemoveComponents.model_validate(
            {
                "world_id": info.world_id,
                "entity_id": str(entity_id),
                "component_types": (ComponentTypeRef.from_type(Velocity),),
            }
        )
        assert remove.entity_id == entity_id
        await dispatcher.defer_as(
            actor,
            remove,
            DurableOptions(target_tick=world.tick),
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        rows = (await world.get_components([Velocity])).collect().to_pylist()
        assert entity_id not in [row["entity_id"] for row in rows]

        despawn = Despawn.model_validate({"world_id": info.world_id, "entity_id": str(entity_id)})
        assert despawn.entity_id == entity_id
        await dispatcher.defer_as(
            actor,
            despawn,
            DurableOptions(target_tick=world.tick),
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        assert entity_id not in world.entity2sig
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_deferred_update_is_applied(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        await dispatcher.defer_as(
            actor,
            Update(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(Position(x=99.0)),),
            ),
            DurableOptions(target_tick=world.tick),
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        rows = (await world.get_components([Position])).collect().to_pylist()
        row = next(row for row in rows if row["entity_id"] == entity_id)
        assert row["position__x"] == 99.0
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_same_drain_update_then_add_component_keeps_updated_state(tmp_path):
    """A widened row composes from the freshest same-drain component state."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        info, world = await _create_world(dispatcher, tmp_path)
        entity_id = await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Position(x=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        target_tick = world.tick
        await dispatcher.defer_as(
            actor,
            Update(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(Position(x=99.0)),),
            ),
            DurableOptions(target_tick=target_tick),
        )
        await dispatcher.defer_as(
            actor,
            AddComponents(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(Velocity(vx=5.0)),),
            ),
            DurableOptions(target_tick=target_tick),
        )
        await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        rows = (await world.get_components([Position, Velocity])).collect().to_pylist()
        row = next(row for row in rows if row["entity_id"] == entity_id)
        assert row["velocity__vx"] == 5.0
        assert row["position__x"] == 99.0, "widened row was built from stale pre-update state"
    finally:
        await resources.aclose()
