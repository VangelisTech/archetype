# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World-owned mutation behavior.

Public functions acquire the registry's exact-world operation lease.  The
``_..._locked`` variants are the only entry points for callers, such as the
durable tick materializer, that already hold that lease.  Keeping both shapes
in this module makes the state transition itself single-sourced without
introducing reentrant lock authority.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from uuid_utils import UUID

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.hooks import (
    AsyncHookHandler,
    FireMode,
    HookEvent,
    HookHandle,
)

if TYPE_CHECKING:
    from archetype.world.interfaces import iWorldRegistry


async def create_entity(
    registry: iWorldRegistry,
    world_id: str | UUID,
    components: list[Component],
) -> int:
    """Stage one entity spawn under the world's operation lease."""
    async with registry.operation(world_id) as world:
        return await _create_entity_locked(world, components)


async def _create_entity_locked(world: AsyncWorld, components: list[Component]) -> int:
    """Stage one entity spawn when the caller already holds the world lock."""
    return await world.create_entity(components)


async def create_entities(
    registry: iWorldRegistry,
    world_id: str | UUID,
    entities: list[list[Component]],
) -> list[int]:
    """Stage a batch of entity spawns, preserving caller order."""
    async with registry.operation(world_id) as world:
        return await _create_entities_locked(world, entities)


async def _create_entities_locked(
    world: AsyncWorld,
    entities: list[list[Component]],
) -> list[int]:
    """Stage an ordered entity batch while the world lock is already held."""
    return await world.create_entities(entities)


async def reserve_entity_ids(
    registry: iWorldRegistry,
    world_id: str | UUID,
    n: int,
) -> list[int]:
    """Reserve IDs from the world's monotonic sequence under its lock."""
    async with registry.operation(world_id) as world:
        return _reserve_entity_ids_locked(world, n)


def _reserve_entity_ids_locked(world: AsyncWorld, n: int) -> list[int]:
    """Reserve IDs while the caller already holds the world lock."""
    return world.reserve_entity_ids(n)


async def spawn_with_reserved_id(
    registry: iWorldRegistry,
    world_id: str | UUID,
    entity_id: int,
    components: list[Component],
) -> None:
    """Stage a spawn for a previously reserved entity ID."""
    async with registry.operation(world_id) as world:
        await _spawn_with_reserved_id_locked(world, entity_id, components)


async def _spawn_with_reserved_id_locked(
    world: AsyncWorld,
    entity_id: int,
    components: list[Component],
) -> None:
    """Stage a reserved spawn while the world lock is already held."""
    await world.spawn_with_reserved_id(entity_id, components)


async def remove_entity(
    registry: iWorldRegistry,
    world_id: str | UUID,
    entity_id: int,
) -> None:
    """Stage an entity removal."""
    async with registry.operation(world_id) as world:
        await _remove_entity_locked(world, entity_id)


async def _remove_entity_locked(world: AsyncWorld, entity_id: int) -> None:
    """Stage an entity removal while the world lock is already held."""
    await world.remove_entity(entity_id)


async def update_entity(
    registry: iWorldRegistry,
    world_id: str | UUID,
    entity_id: int,
    components: list[Component],
) -> None:
    """Overlay values on an entity without changing its signature."""
    async with registry.operation(world_id) as world:
        await _update_entity_locked(world, entity_id, components)


async def _update_entity_locked(
    world: AsyncWorld,
    entity_id: int,
    components: list[Component],
) -> None:
    """Overlay entity values while the world lock is already held."""
    await world.update_entity(entity_id, components)


async def add_components(
    registry: iWorldRegistry,
    world_id: str | UUID,
    entity_id: int,
    components: list[Component],
) -> None:
    """Stage components that extend an entity's signature."""
    async with registry.operation(world_id) as world:
        await _add_components_locked(world, entity_id, components)


async def _add_components_locked(
    world: AsyncWorld,
    entity_id: int,
    components: list[Component],
) -> None:
    """Extend an entity signature while the world lock is already held."""
    await world.add_components(entity_id, components)


async def remove_components(
    registry: iWorldRegistry,
    world_id: str | UUID,
    entity_id: int,
    component_types: list[type[Component]],
) -> None:
    """Stage component removals from an entity."""
    async with registry.operation(world_id) as world:
        await _remove_components_locked(world, entity_id, component_types)


async def _remove_components_locked(
    world: AsyncWorld,
    entity_id: int,
    component_types: list[type[Component]],
) -> None:
    """Remove components while the world lock is already held."""
    await world.remove_components(entity_id, component_types)


async def add_processor(
    registry: iWorldRegistry,
    world_id: str | UUID,
    processor: Any,
) -> None:
    """Attach one live processor directly to a world."""
    async with registry.operation(world_id) as world:
        await _add_processor_locked(world, processor)


async def _add_processor_locked(world: AsyncWorld, processor: Any) -> None:
    """Attach a processor while the world lock is already held."""
    await world.add_processor(processor)


async def remove_processor(
    registry: iWorldRegistry,
    world_id: str | UUID,
    proc_type: type[Any],
) -> None:
    """Remove processors of the requested type."""
    async with registry.operation(world_id) as world:
        await _remove_processor_locked(world, proc_type)


async def _remove_processor_locked(world: AsyncWorld, proc_type: type[Any]) -> None:
    """Remove a processor type while the world lock is already held."""
    await world.remove_processor(proc_type)


async def add_resource(
    registry: iWorldRegistry,
    world_id: str | UUID,
    resource: object,
) -> None:
    """Attach an explicitly direct-only live resource."""
    async with registry.operation(world_id) as world:
        _add_resource_locked(world, resource)


def _add_resource_locked(world: AsyncWorld, resource: object) -> None:
    """Attach a live resource while the world lock is already held."""
    world.resources.insert(resource)


async def add_hook(
    registry: iWorldRegistry,
    world_id: str | UUID,
    event_type: type[HookEvent],
    fn: AsyncHookHandler[Any],
    *,
    mode: FireMode = "blocking",
) -> HookHandle:
    """Register an advisory hook under the world operation lease."""
    async with registry.operation(world_id) as world:
        return _add_hook_locked(world, event_type, fn, mode=mode)


def _add_hook_locked(
    world: AsyncWorld,
    event_type: type[HookEvent],
    fn: AsyncHookHandler[Any],
    *,
    mode: FireMode = "blocking",
) -> HookHandle:
    """Register an advisory hook while the world lock is already held."""
    return world.add_hook(event_type, fn, mode=mode)


async def remove_hook(
    registry: iWorldRegistry,
    world_id: str | UUID,
    handle: HookHandle,
) -> None:
    """Remove an advisory hook under the world operation lease."""
    async with registry.operation(world_id) as world:
        _remove_hook_locked(world, handle)


def _remove_hook_locked(world: AsyncWorld, handle: HookHandle) -> None:
    """Remove an advisory hook while the world lock is already held."""
    world.remove_hook(handle)
