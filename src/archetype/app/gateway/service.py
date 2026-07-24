# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Thin untrusted adapter over the commands-owned dispatcher."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING

from archetype._obs import instrument
from archetype.app.application.interfaces import iRuntimeApplication
from archetype.app.gateway._pr3_commands_bridge import (
    preauthorize_pr3_bridge_actor_call,
)
from archetype.app.models import Command, deferred_operation
from archetype.commands.models import DeferredItem, DurableOptions, GetAuditHistory
from archetype.errors import WorldNotFoundError
from archetype.world.errors import WorldClosingError
from archetype.world.models import (
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
    ListHooks,
    ListProcessors,
    ListResources,
    ListSignatures,
    ListWorlds,
    ListWorldSignatures,
    OpenWorldReadonly,
    QueryArchetype,
    QueryComponents,
    RemoveComponents,
    RemoveHook,
    RemoveProcessor,
    ReserveEntityIds,
    ResumeWorld,
    Run,
    RunEpisode,
    RunRollout,
    Spawn,
    SpawnReserved,
    Step,
    Update,
)

if TYPE_CHECKING:
    from archetype.app.gateway.auth.models import ActorCtx
    from archetype.commands.dispatch import CommandDispatcher
    from archetype.commands.policy import Policy

_DURABLE_TARGET_TICK = 0


def _component_values(components) -> tuple[ComponentValue, ...]:
    return tuple(ComponentValue.from_component(component) for component in components)


def _component_types(component_types) -> tuple[ComponentTypeRef, ...]:
    return tuple(ComponentTypeRef.from_type(component_type) for component_type in component_types)


def _input_kwargs_json(input_kwargs: dict) -> str:
    return json.dumps(
        input_kwargs,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


class CommandGateway:
    """Construct exact operation models and delegate actor-aware entry."""

    def __init__(
        self,
        application: iRuntimeApplication,
        dispatcher: CommandDispatcher,
        policy: Policy,
        *,
        target_tick_for_world: Callable[[object], int] | None = None,
    ) -> None:
        self._application = application
        self._dispatcher = dispatcher
        self._policy = policy
        self._target_tick_for_world = target_tick_for_world

    def _world_target_tick(self, world_id: object) -> int:
        if self._target_tick_for_world is None:
            raise RuntimeError("temporary bridge calls require an explicit target-tick resolver")
        return self._target_tick_for_world(world_id)

    def _authorize_bridge_world(
        self,
        actor: ActorCtx,
        *,
        operation: str,
        world_id: object,
        token_cost: int,
        durable: bool,
    ) -> None:
        preauthorize_pr3_bridge_actor_call(
            self._policy,
            actor,
            operation=operation,
        )
        try:
            target_tick = self._world_target_tick(world_id)
        except (KeyError, WorldClosingError, WorldNotFoundError):
            if not durable:
                raise
            target_tick = _DURABLE_TARGET_TICK
        self._policy.authorize(
            actor,
            permission=operation,
            world_id=world_id,
            target_tick=target_tick,
            token_cost=token_cost,
        )

    # Mutations ------------------------------------------------------

    @instrument("gateway.create_entity")
    async def create_entity(self, ctx, world_id, components):
        return await self._dispatcher.apply_as(
            ctx,
            Spawn.from_components(world_id=world_id, components=components),
        )

    async def create_entities(self, ctx, world_id, entities):
        return await self._dispatcher.apply_as(
            ctx,
            CreateEntities.from_entities(world_id=world_id, entities=entities),
        )

    async def reserve_entity_ids(self, ctx, world_id, n):
        return await self._dispatcher.apply_as(
            ctx,
            ReserveEntityIds(world_id=world_id, count=n),
        )

    async def spawn_with_reserved_id(self, ctx, world_id, entity_id, components):
        return await self._dispatcher.apply_as(
            ctx,
            SpawnReserved(
                world_id=world_id,
                entity_id=entity_id,
                components=_component_values(components),
            ),
        )

    async def remove_entity(self, ctx, world_id, entity_id):
        return await self._dispatcher.apply_as(
            ctx,
            Despawn(world_id=world_id, entity_id=entity_id),
        )

    async def update_entity(self, ctx, world_id, entity_id, components):
        return await self._dispatcher.apply_as(
            ctx,
            Update(
                world_id=world_id,
                entity_id=entity_id,
                components=_component_values(components),
            ),
        )

    async def add_components(self, ctx, world_id, entity_id, components):
        return await self._dispatcher.apply_as(
            ctx,
            AddComponents(
                world_id=world_id,
                entity_id=entity_id,
                components=_component_values(components),
            ),
        )

    async def remove_components(self, ctx, world_id, entity_id, component_types):
        return await self._dispatcher.apply_as(
            ctx,
            RemoveComponents(
                world_id=world_id,
                entity_id=entity_id,
                component_types=_component_types(component_types),
            ),
        )

    async def add_processor(self, ctx, world_id, processor):
        return await self._dispatcher.apply_as(
            ctx,
            AddProcessor(world_id=world_id, processor=processor),
        )

    async def remove_processor(self, ctx, world_id, proc_type):
        return await self._dispatcher.apply_as(
            ctx,
            RemoveProcessor(world_id=world_id, processor_type=proc_type),
        )

    # Lifecycle ------------------------------------------------------

    @instrument("gateway.create_world")
    async def create_world(self, ctx, config, storage_config=None, cache_config=None):
        return await self._dispatcher.apply_as(
            ctx,
            CreateWorld(
                config=config,
                storage_config=storage_config,
                cache_config=cache_config,
            ),
        )

    async def fork_world(
        self,
        ctx,
        source_world_id,
        name=None,
        *,
        storage_config=None,
        cache_config=None,
    ):
        return await self._dispatcher.apply_as(
            ctx,
            ForkWorld(
                source_world_id=source_world_id,
                name=name,
                storage_config=storage_config,
                cache_config=cache_config,
            ),
        )

    async def destroy_world(self, ctx, world_id):
        return await self._dispatcher.apply_as(ctx, DestroyWorld(world_id=world_id))

    @instrument("gateway.get_world_info")
    async def get_world_info(self, ctx, world_id):
        return await self._dispatcher.apply_as(ctx, GetWorldInfo(world_id=world_id))

    async def list_worlds(self, ctx):
        return await self._dispatcher.apply_as(ctx, ListWorlds())

    async def discover_worlds(self, ctx, storage_config):
        return await self._dispatcher.apply_as(
            ctx,
            DiscoverWorlds(storage_config=storage_config),
        )

    async def open_world_readonly(self, ctx, storage_config, world_id):
        return await self._dispatcher.apply_as(
            ctx,
            OpenWorldReadonly(storage_config=storage_config, world_id=world_id),
        )

    async def resume_world(self, ctx, storage_config, world_id):
        return await self._dispatcher.apply_as(
            ctx,
            ResumeWorld(storage_config=storage_config, world_id=world_id),
        )

    # Simulation and workflows --------------------------------------

    async def step(self, ctx, world_id, run_config, **input_kwargs):
        return await self._dispatcher.apply_as(
            ctx,
            Step(
                world_id=world_id,
                run_config=run_config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            ),
        )

    async def run(self, ctx, world_id, run_config, **input_kwargs):
        return await self._dispatcher.apply_as(
            ctx,
            Run(
                world_id=world_id,
                run_config=run_config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            ),
        )

    async def run_episode(self, ctx, world_id, config, **input_kwargs):
        return await self._dispatcher.apply_as(
            ctx,
            RunEpisode(
                world_id=world_id,
                config=config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            ),
        )

    async def run_rollout(self, ctx, world_id, config, **input_kwargs):
        return await self._dispatcher.apply_as(
            ctx,
            RunRollout(
                world_id=world_id,
                config=config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            ),
        )

    async def autoresearch(
        self,
        ctx,
        world_id,
        config,
        evaluator,
        *,
        prepare_candidate=None,
        lab_world_id=None,
        on_iteration=None,
    ):
        self._authorize_bridge_world(
            ctx,
            operation="autoresearch",
            world_id=world_id,
            token_cost=200 * max(int(config.max_iterations), 1),
            durable=False,
        )
        return await self._application.autoresearch(
            world_id,
            config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id=lab_world_id,
            on_iteration=on_iteration,
        )

    # Queries --------------------------------------------------------

    async def query_components(
        self,
        ctx,
        components,
        world_id,
        run_id,
        storage_config=None,
        *,
        ticks=None,
        entity_ids=None,
    ):
        return await self._dispatcher.apply_as(
            ctx,
            QueryComponents(
                components=_component_types(components),
                world_id=world_id,
                run_id=run_id,
                storage_config=storage_config,
                ticks=tuple(ticks) if ticks is not None else None,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
            ),
        )

    async def query_archetype(
        self,
        ctx,
        sig,
        world_id,
        run_id,
        storage_config=None,
        *,
        ticks=None,
        entity_ids=None,
        components=None,
    ):
        return await self._dispatcher.apply_as(
            ctx,
            QueryArchetype(
                signature=_component_types(sig),
                world_id=world_id,
                run_id=run_id,
                storage_config=storage_config,
                ticks=tuple(ticks) if ticks is not None else None,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
                components=(_component_types(components) if components is not None else None),
            ),
        )

    async def list_signatures(self, ctx, storage_config=None, *, world_id=None):
        if world_id is None:
            return await self._dispatcher.apply_as(
                ctx,
                ListSignatures(storage_config=storage_config),
            )
        return await self._dispatcher.apply_as(
            ctx,
            ListWorldSignatures(
                world_id=world_id,
                storage_config=storage_config,
            ),
        )

    async def get_audit_history(self, ctx, world_id=None, **filters):
        if world_id is None:
            raise ValueError("world_id is required for command audit history")
        return await self._dispatcher.apply_as(
            ctx,
            GetAuditHistory(world_id=world_id, **filters),
        )

    # World wiring/introspection ------------------------------------

    async def add_resource(self, ctx, world_id, resource):
        return await self._dispatcher.apply_as(
            ctx,
            AddResource(world_id=world_id, resource=resource),
        )

    async def add_hook(self, ctx, world_id, event_type, fn, *, mode="blocking"):
        return await self._dispatcher.apply_as(
            ctx,
            AddHook(
                world_id=world_id,
                event_type=event_type,
                handler=fn,
                mode=mode,
            ),
        )

    async def remove_hook(self, ctx, world_id, handle):
        return await self._dispatcher.apply_as(
            ctx,
            RemoveHook(world_id=world_id, handle=handle),
        )

    async def list_processors(self, ctx, world_id):
        return await self._dispatcher.apply_as(ctx, ListProcessors(world_id=world_id))

    async def list_hooks(self, ctx, world_id):
        return await self._dispatcher.apply_as(ctx, ListHooks(world_id=world_id))

    async def list_resources(self, ctx, world_id):
        return await self._dispatcher.apply_as(ctx, ListResources(world_id=world_id))

    # Artifacts and evaluation --------------------------------------

    async def ingest_artifacts(self, ctx, world_id, sources, *, storage_config=None):
        self._authorize_bridge_world(
            ctx,
            operation="ingest_artifacts",
            world_id=world_id,
            token_cost=10,
            durable=False,
        )
        return await self._application.ingest_artifacts(
            world_id, sources, storage_config=storage_config
        )

    async def query_artifacts(self, ctx, world_id, *, storage_config=None):
        self._authorize_bridge_world(
            ctx,
            operation="query_artifacts",
            world_id=world_id,
            token_cost=5,
            durable=True,
        )
        return await self._application.query_artifacts(
            world_id,
            storage_config=storage_config,
        )

    async def evaluate(self, ctx, world_id, components, **kwargs):
        self._authorize_bridge_world(
            ctx,
            operation="evaluate",
            world_id=world_id,
            token_cost=10,
            durable=True,
        )
        return await self._application.evaluate(world_id, components, **kwargs)

    # Deferred command acceptance ----------------------------------

    async def submit(self, ctx, world_id, command: Command):
        operation, options = deferred_operation(world_id, command)
        return await self._dispatcher.defer_as(
            ctx,
            operation,
            options,
            command_id=command.id,
            version=command.version,
        )

    async def submit_batch(self, ctx, world_id, commands: list[Command]):
        items = tuple(
            DeferredItem(
                operation=operation,
                options=options,
                command_id=command.id,
                version=command.version,
            )
            for command in commands
            for operation, options in (deferred_operation(world_id, command),)
        )
        return await self._dispatcher.defer_batch_as(ctx, items)

    async def submit_spawn(self, ctx, world_id, components, *, tick=0, priority=0):
        entity_id, _command_id = await self._dispatcher.defer_spawn_as(
            ctx,
            Spawn.from_components(world_id=world_id, components=components),
            DurableOptions(target_tick=tick, priority=priority),
        )
        return entity_id
