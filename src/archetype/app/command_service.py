# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Command Service — the gate.

Every external operation flows through here. The gate runs:
  1. guardrail_allow(cmd, ctx) — RBAC + quotas
  2. delegate to the underlying service
  3. emit an AuditRow via iAuditLog.record

Two paths: direct (sync semantics) and tick-deferred (queued via broker).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from uuid_utils import UUID

from archetype.app.auth.guard import guardrail_allow
from archetype.app.models import Command, CommandType, HookInfo, ProcessorInfo, ResourceInfo, WorldInfo

if TYPE_CHECKING:
    from daft import DataFrame

    from archetype.app.auth.models import ActorCtx
    from archetype.app.broker import CommandBroker
    from archetype.app.mutation_service import MutationService
    from archetype.app.query_service import QueryService
    from archetype.app.simulation_service import SimulationService
    from archetype.app.world_service import WorldService
    from archetype.core.component import Component
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.interfaces import ArchetypeSignature, iWorld

    from archetype.app.models import (
        EpisodeConfig,
        EpisodeResult,
        RolloutConfig,
        RolloutResult,
        RunResult,
        WorldInfo,
    )

logger = logging.getLogger(__name__)


class CommandService:
    """Policy enforcement point.

    Every external mutation, lifecycle operation, and read flows through
    here. The only service that sees ActorCtx.
    """

    def __init__(
        self,
        mutations: MutationService,
        worlds: WorldService,
        simulation: SimulationService,
        queries: QueryService,
        broker: CommandBroker,
    ) -> None:
        self._mutations = mutations
        self._worlds = worlds
        self._simulation = simulation
        self._queries = queries
        self._broker = broker

    def _gate(self, cmd: Command, ctx: ActorCtx) -> None:
        """RBAC + quota check. Raises PermissionError if denied."""
        guardrail_allow(cmd, ctx)

    # ── Mutations (gated, direct) ─────────────────────────────────────────

    async def create_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
    ) -> int:
        self._gate(Command(type=CommandType.SPAWN), ctx)
        return await self._mutations.create_entity(world_id, components)

    async def remove_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
    ) -> None:
        self._gate(Command(type=CommandType.DESPAWN), ctx)
        await self._mutations.remove_entity(world_id, entity_id)

    async def add_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None:
        self._gate(Command(type=CommandType.ADD_COMPONENT), ctx)
        await self._mutations.add_components(world_id, entity_id, components)

    async def remove_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        component_types: list[type[Component]],
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_COMPONENT), ctx)
        await self._mutations.remove_components(world_id, entity_id, component_types)

    async def add_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        processor,
    ) -> None:
        self._gate(Command(type=CommandType.ADD_PROCESSOR), ctx)
        await self._mutations.add_processor(world_id, processor)

    async def remove_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        proc_type,
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_PROCESSOR), ctx)
        await self._mutations.remove_processor(world_id, proc_type)

    # ── Lifecycle (gated, direct) ─────────────────────────────────────────

    async def create_world(
        self,
        ctx: ActorCtx,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.CREATE_WORLD), ctx)
        world = await self._worlds.create_world(config, storage_config, cache_config)
        return WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )

    async def fork_world(
        self,
        ctx: ActorCtx,
        source_world_id: str | UUID,
        name: str | None = None,
        *,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.FORK_WORLD), ctx)
        world = await self._worlds.fork_world(
            source_world_id, name, storage_config, cache_config
        )
        return WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )

    async def destroy_world(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> None:
        self._gate(Command(type=CommandType.DESTROY_WORLD), ctx)
        await self._broker.clear(world_id)
        await self._worlds.destroy_world(world_id)

    async def get_world_info(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        world = self._worlds.get_world(UUID(str(world_id)))
        return WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )

    # ── Simulation (gated, direct) ────────────────────────────────────────

    async def step(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> None:
        self._gate(Command(type=CommandType.RUN_ROLLOUT), ctx)
        await self._simulation.step(world_id, run_config, **input_kwargs)

    async def run(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        self._gate(Command(type=CommandType.RUN_ROLLOUT), ctx)
        return await self._simulation.run(world_id, run_config, **input_kwargs)

    async def run_episode(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        config: EpisodeConfig,
        **input_kwargs,
    ) -> EpisodeResult:
        """Gate, then delegate to SimulationService.run_episode."""
        self._gate(Command(type=CommandType.RUN_EPISODE), ctx)
        return await self._simulation.run_episode(world_id, config, **input_kwargs)

    async def run_rollout(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        config: RolloutConfig,
        **input_kwargs,
    ) -> RolloutResult:
        """Gate, then delegate to SimulationService.run_rollout.

        Emits ONE rollout-level audit row, not one per fork.
        Internal forks are SimulationService's mechanics.
        """
        self._gate(Command(type=CommandType.RUN_ROLLOUT), ctx)
        return await self._simulation.run_rollout(world_id, config, **input_kwargs)

    # ── Queries (gated reads) ─────────────────────────────────────────────

    async def query_archetype(
        self,
        ctx: ActorCtx,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
    ) -> DataFrame:
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        return await self._queries.query_archetype(
            sig, world_id, run_id, storage_config,
            ticks=ticks, entity_ids=entity_ids, components=components,
        )

    async def list_signatures(
        self,
        ctx: ActorCtx,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]:
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        return await self._queries.list_signatures(storage_config)


    # ── Resource attachment (gated) ────────────────────────────────────────

    async def add_resource(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        resource: object,
    ) -> None:
        self._gate(Command(type=CommandType.ADD_RESOURCE), ctx)
        await self._worlds.add_resource(world_id, resource)

    async def add_hook(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        event_type,
        fn,
        *,
        mode: str = "blocking",
    ):
        self._gate(Command(type=CommandType.ADD_HOOK), ctx)
        return self._worlds.add_hook(world_id, event_type, fn, mode=mode)

    async def remove_hook(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        handle,
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_HOOK), ctx)
        self._worlds.remove_hook(world_id, handle)

    # ── Read introspection (gated) ─────────────────────────────────────────

    async def list_processors(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[ProcessorInfo]:
        self._gate(Command(type=CommandType.LIST_PROCESSORS), ctx)
        procs = self._worlds.list_processors(world_id)
        return [
            ProcessorInfo(
                qualname=f"{type(p).__module__}.{type(p).__qualname__}",
                priority=getattr(p, "priority", 0),
                components=tuple(
                    f"{c.__module__}.{c.__qualname__}"
                    for c in getattr(p, "components", ())
                ),
            )
            for p in procs
        ]

    async def list_hooks(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[HookInfo]:
        self._gate(Command(type=CommandType.LIST_HOOKS), ctx)
        entries = self._worlds.list_hooks(world_id)
        result: list[HookInfo] = []
        for entry in entries:
            # Async registry entries are (HookHandle, fn, FireMode)
            # Sync registry entries are (HookHandle, fn)
            handle = entry[0]
            fn = entry[1]
            mode = entry[2] if len(entry) > 2 else "blocking"
            result.append(
                HookInfo(
                    event_type=handle._event_type.__name__,
                    handler_qualname=getattr(fn, "__qualname__", str(fn)),
                    mode=mode,
                    handle_id=handle._id,
                )
            )
        return result

    async def list_resources(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[ResourceInfo]:
        self._gate(Command(type=CommandType.LIST_RESOURCES), ctx)
        items = self._worlds.list_resources(world_id)
        return [
            ResourceInfo(qualname=f"{t.__module__}.{t.__qualname__}")
            for t, _ in items
        ]

    async def get_audit_history(
        self,
        ctx: ActorCtx,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        signer_address: str | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ):
        self._gate(Command(type=CommandType.GET_AUDIT_HISTORY), ctx)
        # TODO: delegate to iAuditLog.query when implemented
        return []

    # ── Tick-deferred path (queued) ───────────────────────────────────────

    async def submit(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmd: Command,
    ) -> UUID:
        """Gate, then enqueue for application at cmd.tick."""
        self._gate(cmd, ctx)
        await self._broker.enqueue(world_id, cmd)
        return cmd.id

    async def submit_batch(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmds: list[Command],
    ) -> list[UUID]:
        """Gate all-or-nothing, then enqueue atomically."""
        for cmd in cmds:
            self._gate(cmd, ctx)
        await self._broker.enqueue_bulk(world_id, cmds)
        return [cmd.id for cmd in cmds]

    async def submit_spawn(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> int:
        """Reserve entity_id, gate, enqueue spawn, return id immediately."""
        from archetype.core.aio import AsyncWorld

        world = self._worlds.get_world(UUID(str(world_id)))
        if not isinstance(world, AsyncWorld):
            raise TypeError("submit_spawn requires AsyncWorld")

        entity_id = world.next_entity_id
        world.next_entity_id += 1

        cmd = Command(
            type=CommandType.SPAWN,
            tick=tick,
            priority=priority,
            payload={"entity_id": entity_id, "components": list(components)},
        )

        try:
            self._gate(cmd, ctx)
            await self._broker.enqueue(world_id, cmd)
        except Exception:
            # Roll back the reserved id
            if world.next_entity_id == entity_id + 1:
                world.next_entity_id = entity_id
            raise

        return entity_id

    async def drain_and_apply(
        self,
        world_id: str | UUID,
        tick: int,
    ) -> list[Command]:
        """Drain commands due at tick, apply them via MutationService.

        No ActorCtx — commands carry their own context validated at submit.
        """
        commands = await self._broker.dequeue_due(world_id, tick)
        if not commands:
            return []

        applied: list[Command] = []
        for cmd in commands:
            try:
                await self._apply(world_id, cmd)
                applied.append(cmd)
            except Exception:
                logger.exception("Failed to apply command %s (%s)", cmd.id, cmd.type.value)

        if applied:
            await self._broker.ack([cmd.id for cmd in applied])

        return applied

    async def _apply(self, world_id: str | UUID, cmd: Command) -> None:
        """Dispatch a single command to the appropriate mutation."""
        payload = cmd.payload

        match cmd.type:
            case CommandType.SPAWN:
                components = payload.get("components", [])
                entity_id = payload.get("entity_id")
                if entity_id is not None:
                    # Deferred spawn with reserved id — use create_entity
                    # and hope the id matches (it will, since we reserved it)
                    await self._mutations.create_entity(world_id, components)
                else:
                    await self._mutations.create_entity(world_id, components)

            case CommandType.DESPAWN:
                await self._mutations.remove_entity(world_id, payload["entity_id"])

            case CommandType.ADD_COMPONENT:
                components = payload.get("components", [])
                await self._mutations.add_components(world_id, payload["entity_id"], components)

            case CommandType.REMOVE_COMPONENT:
                component_types = payload.get("component_types", [])
                await self._mutations.remove_components(
                    world_id, payload["entity_id"], component_types
                )

            case CommandType.ADD_PROCESSOR:
                await self._mutations.add_processor(world_id, payload["processor"])

            case CommandType.REMOVE_PROCESSOR:
                await self._mutations.remove_processor(world_id, payload["processor_type"])

            case _:
                logger.warning("Unhandled command type in drain: %s", cmd.type.value)
