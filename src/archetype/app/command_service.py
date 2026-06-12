# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Command Service — the gate.

Every external operation flows through here. The gate runs:
  1. guardrail_allow(cmd, ctx) — RBAC + quotas
  2. delegate to the underlying service
  3. emit an AuditRow via iAuditLog.record

Two paths: direct (sync semantics) and tick-deferred (queued via broker).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import logfire
from uuid_utils import UUID

from archetype.app.auth.guard import guardrail_allow
from archetype.app.models import (
    Command,
    CommandType,
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    WorldInfo,
)

if TYPE_CHECKING:
    from daft import DataFrame

    from archetype.app.audit_log import AuditLog
    from archetype.app.auth.models import ActorCtx
    from archetype.app.broker import CommandBroker
    from archetype.app.models import (
        EpisodeConfig,
        EpisodeResult,
        RolloutConfig,
        RolloutResult,
        RunResult,
        WorldInfo,
    )
    from archetype.app.mutation_service import MutationService
    from archetype.app.query_service import QueryService
    from archetype.app.simulation_service import SimulationService
    from archetype.app.world_service import WorldService
    from archetype.core.component import Component
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.interfaces import ArchetypeSignature

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
        audit: AuditLog | None = None,
    ) -> None:
        self._mutations = mutations
        self._worlds = worlds
        self._simulation = simulation
        self._queries = queries
        self._broker = broker
        self._audit = audit

    def _gate(self, cmd: Command, ctx: ActorCtx) -> None:
        """RBAC + quota check. Raises GuardrailError if denied."""
        guardrail_allow(cmd, ctx)

    def _require_world(self, world_id: str | UUID) -> None:
        """Reject submissions to worlds not in the registry.

        Per ``docs/guide/specification.md`` "Required Hardening Work" item 3
        and the "CommandService" CURRENT GAPS list, ``submit*`` MUST reject
        commands targeted at unknown ``world_id`` before quota debit, broker
        enqueue, or audit emit.
        """
        from archetype.app.errors import WorldNotFoundError

        if not self._worlds.has_world(world_id):
            raise WorldNotFoundError(world_id)

    async def _emit(self, ctx: ActorCtx, command_type: str, world_id=None, **kw) -> None:
        """Emit one audit row. Best-effort — never raises."""
        if self._audit is None:
            return
        try:
            from archetype.app.audit_log import make_audit_row

            row = make_audit_row(ctx, command_type, world_id, **kw)
            await self._audit.record(row)
        except Exception:
            logger.debug("audit emission failed", exc_info=True)

    # ── Mutations (gated, direct) ─────────────────────────────────────────

    @logfire.instrument("gate.create_entity")
    async def create_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
    ) -> int:
        self._gate(Command(type=CommandType.SPAWN), ctx)
        result = await self._mutations.create_entity(world_id, components)
        await self._emit(ctx, "spawn", world_id)
        return result

    @logfire.instrument("gate.remove_entity")
    async def remove_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
    ) -> None:
        self._gate(Command(type=CommandType.DESPAWN), ctx)
        await self._mutations.remove_entity(world_id, entity_id)
        await self._emit(ctx, "despawn", world_id)

    @logfire.instrument("gate.update_entity")
    async def update_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None:
        """Overlay values on existing components (same archetype)."""
        self._gate(Command(type=CommandType.UPDATE), ctx)
        await self._mutations.update_entity(world_id, entity_id, components)
        await self._emit(ctx, "update", world_id)

    @logfire.instrument("gate.add_components")
    async def add_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None:
        self._gate(Command(type=CommandType.ADD_COMPONENT), ctx)
        await self._mutations.add_components(world_id, entity_id, components)
        await self._emit(ctx, "add_component", world_id)

    @logfire.instrument("gate.remove_components")
    async def remove_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        component_types: list[type[Component]],
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_COMPONENT), ctx)
        await self._mutations.remove_components(world_id, entity_id, component_types)
        await self._emit(ctx, "remove_component", world_id)

    @logfire.instrument("gate.add_processor")
    async def add_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        processor,
    ) -> None:
        self._gate(Command(type=CommandType.ADD_PROCESSOR), ctx)
        await self._mutations.add_processor(world_id, processor)
        await self._emit(ctx, "add_processor", world_id)

    @logfire.instrument("gate.remove_processor")
    async def remove_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        proc_type,
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_PROCESSOR), ctx)
        await self._mutations.remove_processor(world_id, proc_type)
        await self._emit(ctx, "remove_processor", world_id)

    # ── Lifecycle (gated, direct) ─────────────────────────────────────────

    @logfire.instrument("gate.create_world")
    async def create_world(
        self,
        ctx: ActorCtx,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.CREATE_WORLD), ctx)
        world = await self._worlds.create_world(config, storage_config, cache_config)
        info = WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )
        await self._emit(ctx, "create_world", info.world_id)
        return info

    @logfire.instrument("gate.fork_world")
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
        world = await self._worlds.fork_world(source_world_id, name, storage_config, cache_config)
        info = WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )
        await self._emit(ctx, "fork_world", info.world_id)
        return info

    @logfire.instrument("gate.destroy_world")
    async def destroy_world(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> None:
        self._gate(Command(type=CommandType.DESTROY_WORLD), ctx)
        if self._audit:
            await self._audit.flush()
        await self._broker.clear(world_id)
        await self._worlds.destroy_world(world_id)
        await self._emit(ctx, "destroy_world", world_id)

    @logfire.instrument("gate.get_world_info")
    async def get_world_info(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.GET_WORLD_INFO), ctx)
        world = self._worlds.get_world(UUID(str(world_id)))
        info = WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )
        await self._emit(ctx, "get_world_info", world_id)
        return info

    async def list_worlds(self, ctx: ActorCtx) -> list[WorldInfo]:
        self._gate(Command(type=CommandType.LIST_WORLDS), ctx)
        worlds = [
            WorldInfo(
                world_id=world.world_id,
                name=world.name,
                tick=getattr(world, "tick", 0),
                run_id=getattr(world, "run_id", None),
            )
            for world in self._worlds.list_worlds()
        ]
        await self._emit(ctx, "list_worlds")
        return worlds

    # ── Simulation (gated, direct) ────────────────────────────────────────

    @logfire.instrument("gate.step")
    async def step(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> int:
        self._gate(Command(type=CommandType.STEP), ctx)
        commands_applied = await self._simulation.step(world_id, run_config, **input_kwargs)
        await self._emit(ctx, "step", world_id)
        return commands_applied

    @logfire.instrument("gate.run")
    async def run(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        self._gate(Command(type=CommandType.RUN), ctx)
        result = await self._simulation.run(world_id, run_config, **input_kwargs)
        await self._emit(ctx, "run", world_id)
        return result

    @logfire.instrument("gate.run_episode")
    async def run_episode(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        config: EpisodeConfig,
        **input_kwargs,
    ) -> EpisodeResult:
        """Gate, then delegate to SimulationService.run_episode."""
        self._gate(Command(type=CommandType.RUN_EPISODE), ctx)
        result = await self._simulation.run_episode(world_id, config, **input_kwargs)
        await self._emit(
            ctx,
            "run_episode",
            world_id,
            payload_json=json.dumps(
                {
                    "episode_id": str(result.episode_id),
                    "final_tick": result.final_tick,
                    "terminated": result.terminated,
                    "duration_steps": result.duration_steps,
                }
            ),
        )
        return result

    @logfire.instrument("gate.run_rollout")
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
        result = await self._simulation.run_rollout(world_id, config, **input_kwargs)
        await self._emit(
            ctx,
            "run_rollout",
            world_id,
            payload_json=json.dumps(
                {
                    "num_episodes": result.num_episodes,
                    "total_duration_steps": result.total_duration_steps,
                    "episode_world_ids": [str(ep.world_id) for ep in result.episodes],
                }
            ),
        )
        return result

    # ── Queries (gated reads) ─────────────────────────────────────────────

    @logfire.instrument("gate.query_components")
    async def query_components(
        self,
        ctx: ActorCtx,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """Query entities by component subset. Gated read."""
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        storage_config = self._resolve_storage(world_id, storage_config)
        result = await self._queries.query_components(
            components=components,
            world_id=world_id,
            run_id=run_id,
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=await self._resolve_lineage(world_id, run_id, storage_config),
        )
        await self._emit(ctx, "query_world", world_id)
        return result

    def _resolve_storage(
        self,
        world_id: str | UUID,
        storage_config: StorageConfig | None,
    ) -> StorageConfig | None:
        """Resolve which store holds a world's rows.

        An explicit storage_config is an override and wins. Otherwise the
        world's recorded storage is used, so readers find the rows wherever
        the world actually wrote them — forks included.
        """
        if storage_config is not None:
            return storage_config
        record = self._worlds.storage_record(world_id)
        return record[0] if record is not None else None

    async def _resolve_lineage(
        self,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None,
    ) -> list[tuple[str, str, int]] | None:
        """Fork ancestry for a world, so reads cover pre-fork ticks.

        Live worlds carry lineage in memory; destroyed worlds fall back to
        the lineage rows persisted at fork time (append-only, never lost).
        """
        try:
            world = self._worlds.get_world(UUID(str(world_id)))
        except Exception:
            world = None
        if world is not None:
            lineage = getattr(world, "lineage", None)
            return list(lineage) if lineage else None
        return await self._queries.get_lineage(world_id, run_id, storage_config)

    @logfire.instrument("gate.query_archetype")
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
        storage_config = self._resolve_storage(world_id, storage_config)
        result = await self._queries.query_archetype(
            sig,
            world_id,
            run_id,
            storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            components=components,
            lineage=await self._resolve_lineage(world_id, run_id, storage_config),
        )
        await self._emit(ctx, "query_world", world_id)
        return result

    async def list_signatures(
        self,
        ctx: ActorCtx,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]:
        self._gate(Command(type=CommandType.LIST_SIGNATURES), ctx)
        result = await self._queries.list_signatures(storage_config)
        await self._emit(ctx, "list_signatures")
        return result

    # ── Resource attachment (gated) ────────────────────────────────────────

    async def add_resource(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        resource: object,
    ) -> None:
        self._gate(Command(type=CommandType.ADD_RESOURCE), ctx)
        await self._worlds.add_resource(world_id, resource)
        await self._emit(ctx, "add_resource", world_id)

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
        handle = self._worlds.add_hook(world_id, event_type, fn, mode=mode)
        await self._emit(ctx, "add_hook", world_id)
        return handle

    async def remove_hook(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        handle,
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_HOOK), ctx)
        self._worlds.remove_hook(world_id, handle)
        await self._emit(ctx, "remove_hook", world_id)

    # ── Read introspection (gated) ─────────────────────────────────────────

    async def list_processors(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[ProcessorInfo]:
        self._gate(Command(type=CommandType.LIST_PROCESSORS), ctx)
        procs = self._worlds.list_processors(world_id)
        result = [
            ProcessorInfo(
                qualname=f"{type(p).__module__}.{type(p).__qualname__}",
                priority=getattr(p, "priority", 0),
                components=tuple(
                    f"{c.__module__}.{c.__qualname__}" for c in getattr(p, "components", ())
                ),
            )
            for p in procs
        ]
        await self._emit(ctx, "list_processors", world_id)
        return result

    async def list_hooks(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[HookInfo]:
        self._gate(Command(type=CommandType.LIST_HOOKS), ctx)
        entries = self._worlds.list_hooks(world_id)
        result: list[HookInfo] = []
        # Hook bus items() yields uniform (event_type, handle, fn, mode) rows.
        for event_type, handle, fn, mode in entries:
            result.append(
                HookInfo(
                    event_type=event_type.__name__,
                    handler_qualname=getattr(fn, "__qualname__", str(fn)),
                    mode=mode,
                    handle_id=handle.id,
                )
            )
        await self._emit(ctx, "list_hooks", world_id)
        return result

    async def list_resources(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[ResourceInfo]:
        self._gate(Command(type=CommandType.LIST_RESOURCES), ctx)
        items = self._worlds.list_resources(world_id)
        result = [ResourceInfo(qualname=f"{t.__module__}.{t.__qualname__}") for t, _ in items]
        await self._emit(ctx, "list_resources", world_id)
        return result

    async def get_audit_history(
        self,
        ctx: ActorCtx,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ):
        self._gate(Command(type=CommandType.GET_AUDIT_HISTORY), ctx)
        if self._audit is None:
            return []
        result = await self._audit.query(
            world_id=world_id,
            tick_range=tick_range,
            actor_id=actor_id,
            idempotency_key=idempotency_key,
            limit=limit,
        )
        await self._emit(ctx, "get_audit_history", world_id)
        return result

    # ── Tick-deferred path (queued) ───────────────────────────────────────

    @staticmethod
    def _normalize_submit_args(ctx, world_id, command_or_commands):
        """Accept canonical and pre-refactor positional submit ordering."""
        from archetype.app.auth.models import ActorCtx

        if isinstance(ctx, ActorCtx):
            return ctx, world_id, command_or_commands
        if isinstance(command_or_commands, ActorCtx):
            return command_or_commands, ctx, world_id
        raise TypeError("submit expects (ctx, world_id, command) or (world_id, command, ctx)")

    async def submit(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmd: Command,
    ) -> UUID:
        """Gate, then enqueue for application at cmd.tick."""
        ctx, world_id, cmd = self._normalize_submit_args(ctx, world_id, cmd)
        self._require_world(world_id)
        self._gate(cmd, ctx)
        await self._broker.enqueue(world_id, cmd)
        await self._emit(ctx, cmd.type.value, world_id, command_id=cmd.id, status="queued")
        return cmd.id

    async def submit_batch(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmds: list[Command],
    ) -> list[UUID]:
        """Gate all-or-nothing, then enqueue atomically."""
        ctx, world_id, cmds = self._normalize_submit_args(ctx, world_id, cmds)
        self._require_world(world_id)
        for cmd in cmds:
            self._gate(cmd, ctx)
        await self._broker.enqueue_bulk(world_id, cmds)
        for cmd in cmds:
            await self._emit(ctx, cmd.type.value, world_id, command_id=cmd.id, status="queued")
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

        self._require_world(world_id)
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
            await self._emit(ctx, "spawn", world_id, command_id=cmd.id, status="queued")
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

    @staticmethod
    def _hydrate_components(payload_components) -> list[Component]:
        from archetype.core.component import Component

        return [
            component if isinstance(component, Component) else Component.from_dict(component)
            for component in payload_components
        ]

    @staticmethod
    def _hydrate_component_types(payload_types) -> list[type[Component]]:
        from archetype.core.component import Component

        return [
            component_type
            if isinstance(component_type, type) and issubclass(component_type, Component)
            else Component.get_type_by_name(str(component_type))
            for component_type in payload_types
        ]

    async def _apply(self, world_id: str | UUID, cmd: Command) -> None:
        """Dispatch a single command to the appropriate mutation."""
        payload = cmd.payload

        match cmd.type:
            case CommandType.SPAWN:
                components = self._hydrate_components(payload.get("components", []))
                entity_id = payload.get("entity_id")
                if entity_id is not None:
                    # Deferred spawn with reserved id — register directly
                    # on the world so the pre-reserved ID is honored.
                    from archetype.core.aio import AsyncWorld

                    world = self._worlds.get_world(UUID(str(world_id)))
                    if isinstance(world, AsyncWorld):
                        await world._register_entity(int(entity_id), components)
                else:
                    await self._mutations.create_entity(world_id, components)

            case CommandType.DESPAWN:
                await self._mutations.remove_entity(world_id, payload["entity_id"])

            case CommandType.ADD_COMPONENT:
                components = self._hydrate_components(payload.get("components", []))
                await self._mutations.add_components(world_id, payload["entity_id"], components)

            case CommandType.REMOVE_COMPONENT:
                component_types = self._hydrate_component_types(payload.get("component_types", []))
                await self._mutations.remove_components(
                    world_id, payload["entity_id"], component_types
                )

            case CommandType.ADD_PROCESSOR:
                await self._mutations.add_processor(world_id, payload["processor"])

            case CommandType.REMOVE_PROCESSOR:
                await self._mutations.remove_processor(world_id, payload["processor_type"])

            case _:
                logger.warning("Unhandled command type in drain: %s", cmd.type.value)
