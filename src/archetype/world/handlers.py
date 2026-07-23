# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Thin exact-model adapters for externally operable world behavior.

Dependencies are ordinary explicit parameters.  The commands family may bind
them during composition, but neither dispatch nor policy semantics enter this
family.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.world import mutation, query, simulation
from archetype.world.models import (
    WORLD_OPERATION_TYPES,
    AddComponents,
    AddHook,
    AddProcessor,
    AddResource,
    ComponentTypeRef,
    ComponentValue,
    CreateEntities,
    CreateWorld,
    Despawn,
    DestroyWorld,
    DiscoverWorlds,
    ForkWorld,
    GetWorldInfo,
    HookInfo,
    ListHooks,
    ListProcessors,
    ListResources,
    ListSignatures,
    ListWorlds,
    OpenWorldReadonly,
    PortableTickOperation,
    ProcessorInfo,
    QueryArchetype,
    QueryComponents,
    RemoveComponents,
    RemoveHook,
    RemoveProcessor,
    ReserveEntityIds,
    ResourceInfo,
    ResumeWorld,
    Run,
    RunEpisode,
    RunRollout,
    Spawn,
    SpawnReserved,
    Step,
    Update,
    WorldInfo,
)

if TYPE_CHECKING:
    from archetype.world.registry import WorldRegistry

    from archetype.storage.service import StorageService

LifecycleCallable = Callable[..., Awaitable[Any]]
WorldResolver = Callable[[object], Any]
WorldLister = Callable[[], list[Any]]


def _components(values: tuple[ComponentValue, ...]) -> list[Component]:
    return [value.materialize() for value in values]


def _component_types(
    values: tuple[ComponentTypeRef, ...],
) -> list[type[Component]]:
    return [value.resolve() for value in values]


def _input_kwargs(payload_json: str) -> dict[str, Any]:
    decoded = json.loads(payload_json)
    if not isinstance(decoded, dict):
        raise ValueError("input_kwargs_json must decode to an object")
    return decoded


def _world_info(world: Any) -> WorldInfo:
    return WorldInfo(
        world_id=world.world_id,
        name=world.name,
        tick=world.tick,
        run_id=world.run_id,
    )


async def spawn(registry: WorldRegistry, operation: Spawn) -> int:
    return await mutation.create_entity(
        registry,
        operation.world_id,
        _components(operation.components),
    )


async def create_entities(
    registry: WorldRegistry,
    operation: CreateEntities,
) -> list[int]:
    entities = [_components(values) for values in operation.entities]
    return await mutation.create_entities(registry, operation.world_id, entities)


async def reserve_entity_ids(
    registry: WorldRegistry,
    operation: ReserveEntityIds,
) -> list[int]:
    return await mutation.reserve_entity_ids(
        registry,
        operation.world_id,
        operation.count,
    )


async def spawn_reserved(
    registry: WorldRegistry,
    operation: SpawnReserved,
) -> None:
    await mutation.spawn_with_reserved_id(
        registry,
        operation.world_id,
        operation.entity_id,
        _components(operation.components),
    )


async def despawn(registry: WorldRegistry, operation: Despawn) -> None:
    await mutation.remove_entity(registry, operation.world_id, operation.entity_id)


async def update(registry: WorldRegistry, operation: Update) -> None:
    await mutation.update_entity(
        registry,
        operation.world_id,
        operation.entity_id,
        _components(operation.components),
    )


async def add_components(
    registry: WorldRegistry,
    operation: AddComponents,
) -> None:
    await mutation.add_components(
        registry,
        operation.world_id,
        operation.entity_id,
        _components(operation.components),
    )


async def remove_components(
    registry: WorldRegistry,
    operation: RemoveComponents,
) -> None:
    await mutation.remove_components(
        registry,
        operation.world_id,
        operation.entity_id,
        _component_types(operation.component_types),
    )


async def add_processor(
    registry: WorldRegistry,
    operation: AddProcessor,
) -> None:
    await mutation.add_processor(registry, operation.world_id, operation.processor)


async def remove_processor(
    registry: WorldRegistry,
    operation: RemoveProcessor,
) -> None:
    await mutation.remove_processor(
        registry,
        operation.world_id,
        operation.processor_type,
    )


async def add_resource(registry: WorldRegistry, operation: AddResource) -> None:
    await mutation.add_resource(registry, operation.world_id, operation.resource)


async def add_hook(registry: WorldRegistry, operation: AddHook) -> Any:
    return await mutation.add_hook(
        registry,
        operation.world_id,
        operation.event_type,
        operation.handler,
        mode=operation.mode,
    )


async def remove_hook(registry: WorldRegistry, operation: RemoveHook) -> None:
    await mutation.remove_hook(registry, operation.world_id, operation.handle)


async def _spawn_locked(world: AsyncWorld, operation: Spawn) -> None:
    await mutation._create_entity_locked(world, _components(operation.components))


async def _spawn_reserved_locked(
    world: AsyncWorld,
    operation: SpawnReserved,
) -> None:
    await mutation._spawn_with_reserved_id_locked(
        world,
        operation.entity_id,
        _components(operation.components),
    )


async def _despawn_locked(world: AsyncWorld, operation: Despawn) -> None:
    await mutation._remove_entity_locked(world, operation.entity_id)


async def _update_locked(world: AsyncWorld, operation: Update) -> None:
    await mutation._update_entity_locked(
        world,
        operation.entity_id,
        _components(operation.components),
    )


async def _add_components_locked(
    world: AsyncWorld,
    operation: AddComponents,
) -> None:
    await mutation._add_components_locked(
        world,
        operation.entity_id,
        _components(operation.components),
    )


async def _remove_components_locked(
    world: AsyncWorld,
    operation: RemoveComponents,
) -> None:
    await mutation._remove_components_locked(
        world,
        operation.entity_id,
        _component_types(operation.component_types),
    )


_PORTABLE_MATERIALIZERS: dict[
    type[PortableTickOperation],
    Callable[[AsyncWorld, Any], Awaitable[None]],
] = {
    Spawn: _spawn_locked,
    SpawnReserved: _spawn_reserved_locked,
    Despawn: _despawn_locked,
    Update: _update_locked,
    AddComponents: _add_components_locked,
    RemoveComponents: _remove_components_locked,
}


async def materialize_locked(
    world: AsyncWorld,
    operation: PortableTickOperation,
) -> None:
    """Apply an exact portable mutation without reacquiring the world lock."""
    materializer = _PORTABLE_MATERIALIZERS.get(type(operation))
    if materializer is None:
        raise TypeError(f"{type(operation).__name__} has no portable lock-held materializer")
    await materializer(world, operation)


async def create_world(
    create: LifecycleCallable,
    operation: CreateWorld,
) -> WorldInfo:
    world = await create(
        operation.config,
        operation.storage_config,
        operation.cache_config,
    )
    return _world_info(world)


async def fork_world(
    fork: LifecycleCallable,
    operation: ForkWorld,
) -> WorldInfo:
    world = await fork(
        operation.source_world_id,
        name=operation.name,
        storage_config=operation.storage_config,
        cache_config=operation.cache_config,
    )
    return _world_info(world)


async def destroy_world(
    destroy: LifecycleCallable,
    operation: DestroyWorld,
) -> None:
    await destroy(operation.world_id)


async def get_world_info(
    resolve: WorldResolver,
    operation: GetWorldInfo,
) -> WorldInfo:
    return _world_info(resolve(operation.world_id))


async def list_worlds(
    list_live: WorldLister,
    operation: ListWorlds,
) -> list[WorldInfo]:
    del operation
    return [_world_info(world) for world in list_live()]


async def discover_worlds(
    discover: LifecycleCallable,
    operation: DiscoverWorlds,
) -> list[WorldInfo]:
    return await discover(operation.storage_config)


async def open_world_readonly(
    open_readonly: LifecycleCallable,
    operation: OpenWorldReadonly,
) -> WorldInfo:
    return await open_readonly(operation.storage_config, operation.world_id)


async def resume_world(
    resume: LifecycleCallable,
    operation: ResumeWorld,
) -> WorldInfo:
    world = await resume(operation.storage_config, operation.world_id)
    return _world_info(world)


async def step(registry: WorldRegistry, operation: Step) -> int:
    return await simulation.step(
        registry,
        operation.world_id,
        operation.run_config,
        **_input_kwargs(operation.input_kwargs_json),
    )


async def run(registry: WorldRegistry, operation: Run):
    return await simulation.run(
        registry,
        operation.world_id,
        operation.run_config,
        **_input_kwargs(operation.input_kwargs_json),
    )


async def run_episode(
    registry: WorldRegistry,
    storage: StorageService,
    operation: RunEpisode,
):
    return await simulation.run_episode(
        registry,
        storage,
        operation.world_id,
        operation.config,
        **_input_kwargs(operation.input_kwargs_json),
    )


async def run_rollout(
    registry: WorldRegistry,
    storage: StorageService,
    fork: simulation.ForkWorldCallable,
    destroy: simulation.DestroyWorldCallable,
    operation: RunRollout,
):
    return await simulation.run_rollout(
        registry,
        storage,
        fork,
        destroy,
        operation.world_id,
        operation.config,
        **_input_kwargs(operation.input_kwargs_json),
    )


async def query_components(
    storage: StorageService,
    operation: QueryComponents,
):
    return await query.query_components(
        storage,
        _component_types(operation.components),
        str(operation.world_id),
        str(operation.run_id),
        operation.storage_config,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
        lineage=list(operation.lineage) if operation.lineage is not None else None,
        visibility_tokens=(
            list(operation.visibility_tokens) if operation.visibility_tokens is not None else None
        ),
    )


async def query_archetype(
    storage: StorageService,
    operation: QueryArchetype,
):
    return await query.query_archetype(
        storage,
        tuple(_component_types(operation.signature)),
        str(operation.world_id),
        str(operation.run_id),
        operation.storage_config,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
        components=(
            _component_types(operation.components) if operation.components is not None else None
        ),
        lineage=list(operation.lineage) if operation.lineage is not None else None,
    )


async def list_signatures(
    storage: StorageService,
    operation: ListSignatures,
):
    return await query.list_signatures(storage, operation.storage_config)


async def list_processors(
    registry: WorldRegistry,
    operation: ListProcessors,
) -> list[ProcessorInfo]:
    async with registry.operation(operation.world_id) as world:
        return [
            ProcessorInfo(
                qualname=f"{type(processor).__module__}.{type(processor).__qualname__}",
                priority=getattr(processor, "priority", 0),
                components=tuple(
                    f"{component.__module__}.{component.__qualname__}"
                    for component in getattr(processor, "components", ())
                ),
            )
            for processor in world.system.processors
        ]


async def list_hooks(
    registry: WorldRegistry,
    operation: ListHooks,
) -> list[HookInfo]:
    async with registry.operation(operation.world_id) as world:
        return [
            HookInfo(
                event_type=event_type.__name__,
                handler_qualname=getattr(handler, "__qualname__", str(handler)),
                mode=mode,
                handle_id=handle.id,
            )
            for event_type, handle, handler, mode in world.hooks.items()
        ]


async def list_resources(
    registry: WorldRegistry,
    operation: ListResources,
) -> list[ResourceInfo]:
    async with registry.operation(operation.world_id) as world:
        return [
            ResourceInfo(qualname=f"{resource_type.__module__}.{resource_type.__qualname__}")
            for resource_type, _resource in world.resources.items()
        ]


WORLD_OPERATION_HANDLERS = {
    Spawn: spawn,
    CreateEntities: create_entities,
    ReserveEntityIds: reserve_entity_ids,
    SpawnReserved: spawn_reserved,
    Despawn: despawn,
    Update: update,
    AddComponents: add_components,
    RemoveComponents: remove_components,
    AddProcessor: add_processor,
    RemoveProcessor: remove_processor,
    CreateWorld: create_world,
    ForkWorld: fork_world,
    DestroyWorld: destroy_world,
    GetWorldInfo: get_world_info,
    ListWorlds: list_worlds,
    DiscoverWorlds: discover_worlds,
    OpenWorldReadonly: open_world_readonly,
    ResumeWorld: resume_world,
    Step: step,
    Run: run,
    RunEpisode: run_episode,
    RunRollout: run_rollout,
    QueryComponents: query_components,
    QueryArchetype: query_archetype,
    ListSignatures: list_signatures,
    AddResource: add_resource,
    AddHook: add_hook,
    RemoveHook: remove_hook,
    ListProcessors: list_processors,
    ListHooks: list_hooks,
    ListResources: list_resources,
}

if frozenset(WORLD_OPERATION_HANDLERS) != frozenset(WORLD_OPERATION_TYPES):
    raise RuntimeError("world operation model/handler inventory is incomplete")
