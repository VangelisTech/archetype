# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Thin exact-model adapters for externally operable world behavior.

Dependencies are ordinary explicit parameters.  The commands family may bind
them during composition, but neither dispatch nor policy semantics enter this
family.
"""

from __future__ import annotations

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
    ListWorldSignatures,
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
    from archetype.core.config import StorageConfig
    from archetype.storage.interfaces import iStorageService
    from archetype.world.interfaces import iWorldRegistry

LifecycleCallable = Callable[..., Awaitable[Any]]


def _components(values: tuple[ComponentValue, ...]) -> list[Component]:
    return [value.materialize() for value in values]


def _component_types(
    values: tuple[ComponentTypeRef, ...],
) -> list[type[Component]]:
    return [value.resolve() for value in values]


def _world_info(world: Any) -> WorldInfo:
    return WorldInfo(
        world_id=world.world_id,
        name=world.name,
        tick=world.tick,
        run_id=world.run_id,
    )


async def spawn(registry: iWorldRegistry, operation: Spawn) -> int:
    return await mutation.create_entity(
        registry,
        operation.world_id,
        _components(operation.components),
    )


async def create_entities(
    registry: iWorldRegistry,
    operation: CreateEntities,
) -> list[int]:
    entities = [_components(values) for values in operation.entities]
    return await mutation.create_entities(registry, operation.world_id, entities)


async def reserve_entity_ids(
    registry: iWorldRegistry,
    operation: ReserveEntityIds,
) -> list[int]:
    return await mutation.reserve_entity_ids(
        registry,
        operation.world_id,
        operation.count,
    )


async def spawn_reserved(
    registry: iWorldRegistry,
    operation: SpawnReserved,
) -> None:
    await mutation.spawn_with_reserved_id(
        registry,
        operation.world_id,
        operation.entity_id,
        _components(operation.components),
    )


async def despawn(registry: iWorldRegistry, operation: Despawn) -> None:
    await mutation.remove_entity(registry, operation.world_id, operation.entity_id)


async def update(registry: iWorldRegistry, operation: Update) -> None:
    await mutation.update_entity(
        registry,
        operation.world_id,
        operation.entity_id,
        _components(operation.components),
    )


async def add_components(
    registry: iWorldRegistry,
    operation: AddComponents,
) -> None:
    await mutation.add_components(
        registry,
        operation.world_id,
        operation.entity_id,
        _components(operation.components),
    )


async def remove_components(
    registry: iWorldRegistry,
    operation: RemoveComponents,
) -> None:
    await mutation.remove_components(
        registry,
        operation.world_id,
        operation.entity_id,
        _component_types(operation.component_types),
    )


async def add_processor(
    registry: iWorldRegistry,
    operation: AddProcessor,
) -> None:
    await mutation.add_processor(registry, operation.world_id, operation.processor)


async def remove_processor(
    registry: iWorldRegistry,
    operation: RemoveProcessor,
) -> None:
    await mutation.remove_processor(
        registry,
        operation.world_id,
        operation.processor_type,
    )


async def add_resource(registry: iWorldRegistry, operation: AddResource) -> None:
    await mutation.add_resource(registry, operation.world_id, operation.resource)


async def add_hook(registry: iWorldRegistry, operation: AddHook) -> Any:
    return await mutation.add_hook(
        registry,
        operation.world_id,
        operation.event_type,
        operation.handler,
        mode=operation.mode,
    )


async def remove_hook(registry: iWorldRegistry, operation: RemoveHook) -> None:
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
    registry: iWorldRegistry,
    operation: GetWorldInfo,
) -> WorldInfo:
    world_id = str(operation.world_id)
    async with registry.operation(world_id) as world:
        await simulation.reconcile_committed_work_locked(
            registry,
            world_id,
            world,
        )
        return _world_info(world)


async def list_worlds(
    registry: iWorldRegistry,
    operation: ListWorlds,
) -> list[WorldInfo]:
    del operation
    snapshot = await registry.list_worlds()
    world_ids = [str(world.world_id) for world in snapshot]
    infos: list[WorldInfo] = []
    # Recovery may invoke user hooks or required projectors that target a
    # sibling. Acquire only one exact-world lock at a time and fail closed if
    # a snapshotted world begins closing before its turn.
    for world_id in world_ids:
        async with registry.operation(world_id) as world:
            await simulation.reconcile_committed_work_locked(
                registry,
                world_id,
                world,
            )
            infos.append(_world_info(world))
    return infos


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


async def step(registry: iWorldRegistry, operation: Step) -> int:
    return await simulation.step(
        registry,
        operation.world_id,
        operation.run_config,
        **operation.input_kwargs,
    )


async def run(registry: iWorldRegistry, operation: Run):
    return await simulation.run(
        registry,
        operation.world_id,
        operation.run_config,
        **operation.input_kwargs,
    )


async def run_episode(
    registry: iWorldRegistry,
    storage: iStorageService,
    operation: RunEpisode,
):
    return await simulation.run_episode(
        registry,
        storage,
        operation.world_id,
        operation.config,
        **operation.input_kwargs,
    )


async def run_rollout(
    registry: iWorldRegistry,
    storage: iStorageService,
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
        **operation.input_kwargs,
    )


async def query_components(
    registry: iWorldRegistry,
    storage: iStorageService,
    operation: QueryComponents,
):
    storage_config = await _resolve_storage(
        registry,
        operation.world_id,
        operation.storage_config,
    )
    lineage = list(operation.lineage) if operation.lineage is not None else None
    if operation.lineage is None and operation.visibility_tokens is None:
        lineage = await _resolve_lineage(
            registry,
            storage,
            operation.world_id,
            operation.run_id,
            storage_config,
        )
    return await query.query_components(
        storage,
        _component_types(operation.components),
        str(operation.world_id),
        str(operation.run_id),
        storage_config,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
        lineage=lineage,
        visibility_tokens=(
            list(operation.visibility_tokens) if operation.visibility_tokens is not None else None
        ),
    )


async def query_archetype(
    registry: iWorldRegistry,
    storage: iStorageService,
    operation: QueryArchetype,
):
    storage_config = await _resolve_storage(
        registry,
        operation.world_id,
        operation.storage_config,
    )
    lineage = (
        list(operation.lineage)
        if operation.lineage is not None
        else await _resolve_lineage(
            registry,
            storage,
            operation.world_id,
            operation.run_id,
            storage_config,
        )
    )
    return await query.query_archetype(
        storage,
        tuple(_component_types(operation.signature)),
        str(operation.world_id),
        str(operation.run_id),
        storage_config,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
        components=(
            _component_types(operation.components) if operation.components is not None else None
        ),
        lineage=lineage,
    )


async def _resolve_storage(
    registry: iWorldRegistry,
    world_id: object,
    storage_config: StorageConfig | None,
) -> StorageConfig | None:
    if storage_config is not None:
        return storage_config
    record = await registry.storage_record(str(world_id))
    return record[0] if record is not None else None


async def _resolve_lineage(
    registry: iWorldRegistry,
    storage: iStorageService,
    world_id: object,
    run_id: object,
    storage_config: StorageConfig | None,
) -> list[tuple[str, str, int]] | None:
    # A live-but-closing world raises RuntimeError and must not be silently
    # reclassified as a cold durable read.
    try:
        async with registry.operation(str(world_id)) as world:
            lineage = getattr(world, "lineage", None)
            return list(lineage) if lineage else None
    except KeyError:
        return await query.get_lineage(
            storage,
            str(world_id),
            str(run_id),
            storage_config,
        )


async def list_signatures(
    storage: iStorageService,
    operation: ListSignatures,
):
    return await query.list_signatures(storage, operation.storage_config)


async def list_world_signatures(
    registry: iWorldRegistry,
    storage: iStorageService,
    operation: ListWorldSignatures,
):
    storage_config = await _resolve_storage(
        registry,
        operation.world_id,
        operation.storage_config,
    )
    return await query.list_signatures(storage, storage_config)


async def list_processors(
    registry: iWorldRegistry,
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
    registry: iWorldRegistry,
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
    registry: iWorldRegistry,
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
    ListWorldSignatures: list_world_signatures,
    AddResource: add_resource,
    AddHook: add_hook,
    RemoveHook: remove_hook,
    ListProcessors: list_processors,
    ListHooks: list_hooks,
    ListResources: list_resources,
}

if frozenset(WORLD_OPERATION_HANDLERS) != frozenset(WORLD_OPERATION_TYPES):
    raise RuntimeError("world operation model/handler inventory is incomplete")
