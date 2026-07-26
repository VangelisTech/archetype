# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for module-level world mutation authority."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from archetype.core.component import Component
from archetype.world import mutation
from archetype.world.handlers import materialize_locked
from archetype.world.models import AddProcessor, Spawn

pytestmark = [pytest.mark.asyncio, pytest.mark.contract("world.mutation.locking")]


class Marker(Component):
    value: int = 0


@dataclass
class _Processor:
    priority: int = 10


class _World:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.next_entity_id = 1

    async def create_entity(self, components: list[Component]) -> int:
        self.calls.append(("create_entity", components))
        entity_id = self.next_entity_id
        self.next_entity_id += 1
        return entity_id

    async def create_entities(self, entities: list[list[Component]]) -> list[int]:
        self.calls.append(("create_entities", entities))
        start = self.next_entity_id
        self.next_entity_id += len(entities)
        return list(range(start, self.next_entity_id))

    def reserve_entity_ids(self, n: int) -> list[int]:
        self.calls.append(("reserve_entity_ids", n))
        start = self.next_entity_id
        self.next_entity_id += n
        return list(range(start, self.next_entity_id))

    async def spawn_with_reserved_id(self, entity_id: int, components: list[Component]) -> None:
        self.calls.append(("spawn_with_reserved_id", (entity_id, components)))

    async def remove_entity(self, entity_id: int) -> None:
        self.calls.append(("remove_entity", entity_id))

    async def update_entity(self, entity_id: int, components: list[Component]) -> None:
        self.calls.append(("update_entity", (entity_id, components)))

    async def add_components(self, entity_id: int, components: list[Component]) -> None:
        self.calls.append(("add_components", (entity_id, components)))

    async def remove_components(
        self, entity_id: int, component_types: list[type[Component]]
    ) -> None:
        self.calls.append(("remove_components", (entity_id, component_types)))

    async def add_processor(self, processor: object) -> None:
        self.calls.append(("add_processor", processor))

    async def remove_processor(self, proc_type: type[object]) -> None:
        self.calls.append(("remove_processor", proc_type))


class _Registry:
    def __init__(self, world: _World) -> None:
        self.world = world
        self.operation_entries = 0

    @asynccontextmanager
    async def operation(self, world_id: object):
        assert str(world_id) == "world-1"
        self.operation_entries += 1
        yield self.world


async def test_public_mutation_acquires_one_registry_operation() -> None:
    world = _World()
    registry = _Registry(world)
    marker = Marker(value=3)

    assert await mutation.create_entity(registry, "world-1", [marker]) == 1

    assert registry.operation_entries == 1
    assert world.calls == [("create_entity", [marker])]


async def test_lock_held_mutation_uses_same_transition_without_reacquiring() -> None:
    world = _World()
    registry = _Registry(world)
    marker = Marker(value=7)

    reserved = mutation._reserve_entity_ids_locked(world, 1)
    await mutation._spawn_with_reserved_id_locked(world, reserved[0], [marker])

    assert registry.operation_entries == 0
    assert world.calls == [
        ("reserve_entity_ids", 1),
        ("spawn_with_reserved_id", (1, [marker])),
    ]


async def test_public_and_lock_held_paths_preserve_batch_order() -> None:
    world = _World()
    registry = _Registry(world)
    entities = [[Marker(value=1)], [Marker(value=2)]]

    public_ids = await mutation.create_entities(registry, "world-1", entities)
    locked_ids = await mutation._create_entities_locked(world, entities)

    assert public_ids == [1, 2]
    assert locked_ids == [3, 4]
    assert registry.operation_entries == 1
    assert [name for name, _value in world.calls] == [
        "create_entities",
        "create_entities",
    ]


async def test_atomic_batch_rolls_back_after_a_prior_spawn_hook_ran() -> None:
    class _HookedWorld:
        def __init__(self) -> None:
            self.next_entity_id = 2
            self.tick = 7
            self.entity2sig = {1: (Marker,)}
            self.spawn_cache = {(Marker,): [{"entity_id": 1, "marker__value": 99}]}
            self.despawn_cache: dict[tuple[type[Component], ...], list[int]] = {}
            self.hook_effects: list[int] = []

        async def create_entities(
            self,
            entities: list[list[Component]],
        ) -> list[int]:
            ids: list[int] = []
            for components in entities:
                entity_id = self.next_entity_id
                self.next_entity_id += 1
                ids.append(entity_id)
                signature = tuple(type(component) for component in components)
                self.entity2sig[entity_id] = signature
                self.spawn_cache.setdefault(signature, []).append({"entity_id": entity_id})
                # OnSpawn handlers are consequential advisory callbacks. A
                # prior callback can run, but its world mutation prefix must
                # not survive a later callback failure.
                self.hook_effects.append(entity_id)
                if entity_id == 3:
                    raise RuntimeError("later OnSpawn failed")
            return ids

    world = _HookedWorld()
    original_entity2sig = dict(world.entity2sig)
    original_spawn_cache = {signature: list(rows) for signature, rows in world.spawn_cache.items()}

    with pytest.raises(RuntimeError, match="later OnSpawn"):
        await mutation._create_entities_atomically_locked(  # type: ignore[arg-type]
            world,
            [[Marker(value=1)], [Marker(value=2)], [Marker(value=3)]],
        )

    # The callback itself cannot be undone, so hooks must not own Mission
    # correctness. The world cache, entity sequence, and tick transition are
    # restored synchronously before the error escapes.
    assert world.hook_effects == [2, 3]
    assert world.next_entity_id == 2
    assert world.tick == 7
    assert world.entity2sig == original_entity2sig
    assert world.spawn_cache == original_spawn_cache
    assert world.despawn_cache == {}


async def test_atomic_batch_rolls_back_an_unexpected_identity_sequence() -> None:
    class _MisnumberedWorld:
        def __init__(self) -> None:
            self.next_entity_id = 2
            self.entity2sig = {1: (Marker,)}
            self.spawn_cache = {(Marker,): [{"entity_id": 1, "marker__value": 99}]}
            self.despawn_cache: dict[tuple[type[Component], ...], list[int]] = {}

        async def create_entities(
            self,
            entities: list[list[Component]],
        ) -> list[int]:
            self.next_entity_id += len(entities) + 1
            self.entity2sig[3] = (Marker,)
            self.spawn_cache[(Marker,)].append({"entity_id": 3, "marker__value": 1})
            return [3]

    world = _MisnumberedWorld()
    original_entity2sig = dict(world.entity2sig)
    original_spawn_cache = {signature: list(rows) for signature, rows in world.spawn_cache.items()}

    with pytest.raises(RuntimeError, match="atomic batch identity reservation"):
        await mutation._create_entities_atomically_locked(  # type: ignore[arg-type]
            world,
            [[Marker(value=1)]],
        )

    assert world.next_entity_id == 2
    assert world.entity2sig == original_entity2sig
    assert world.spawn_cache == original_spawn_cache
    assert world.despawn_cache == {}


async def test_processor_mutation_preserves_live_capability_identity() -> None:
    world = _World()
    registry = _Registry(world)
    processor = _Processor()

    await mutation.add_processor(registry, "world-1", processor)
    await mutation._remove_processor_locked(world, _Processor)

    assert world.calls == [
        ("add_processor", processor),
        ("remove_processor", _Processor),
    ]
    assert world.calls[0][1] is processor


async def test_portable_materializer_uses_value_snapshot_and_fails_closed() -> None:
    world = _World()
    marker = Marker(value=11)
    operation = Spawn.from_components(world_id="world-1", components=[marker])
    marker.value = 99

    await materialize_locked(world, operation)

    call_name, components = world.calls[0]
    assert call_name == "create_entity"
    assert isinstance(components, list)
    assert len(components) == 1
    assert isinstance(components[0], Marker)
    assert components[0].value == 11
    assert components[0] is not marker

    with pytest.raises(TypeError, match="no portable lock-held materializer"):
        await materialize_locked(  # type: ignore[arg-type]
            world,
            AddProcessor(world_id="world-1", processor=_Processor()),
        )
