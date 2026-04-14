# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Command Service

The nerve center. Every mutation from an external actor flows through here.
Accepts commands, routes them through the CommandBroker for auth and ordering,
then dispatches to the appropriate world mutation.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.auth.guard import guardrail_allow
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType

if TYPE_CHECKING:
    from archetype.app.world_service import WorldService
    from archetype.core.aio import AsyncWorld
    from archetype.core.interfaces import iWorld

logger = logging.getLogger(__name__)


class CommandService:
    """
    Auth routing + command dispatch.

    All mutations from external actors flow through submit() → broker → enqueue.
    SimulationService calls drain_and_apply() during each tick to process queued commands.
    """

    def __init__(self, broker: CommandBroker, world_service: WorldService):
        self._broker = broker
        self._world_service = world_service
        self._spawn_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def submit(
        self,
        world_id: str | UUID,
        cmd: Command,
        ctx: ActorCtx,
    ) -> UUID:
        """
        Submit a command for a world.
        Broker validates RBAC + quotas, then enqueues.
        Returns the command ID.
        """
        await self._broker.enqueue(world_id, cmd, ctx)
        return cmd.id

    async def submit_batch(
        self,
        world_id: str | UUID,
        cmds: list[Command],
        ctx: ActorCtx,
    ) -> list[UUID]:
        """Submit multiple commands. All-or-nothing RBAC validation."""
        await self._broker.enqueue_bulk(world_id, cmds, ctx)
        return [cmd.id for cmd in cmds]

    async def submit_spawn(
        self,
        world_id: str | UUID,
        components: list,
        ctx: ActorCtx,
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> int:
        """
        Reserve an entity ID for a spawn command and enqueue it.

        The returned entity ID is stable across the full command lifecycle:
        submit -> broker queue -> drain_and_apply. The entity is still
        materialized at the broker drain boundary, preserving the service
        layer's audit trail and ordering semantics.
        """
        world_key = str(world_id)
        lock = self._spawn_locks[world_key]

        async with lock:
            world = self._world_service.get_world(UUID(world_key))
            entity_id = self._reserve_entity_id(world)
            cmd = Command(
                type=CommandType.SPAWN,
                tick=tick,
                priority=priority,
                payload={
                    "entity_id": entity_id,
                    # Keep live component instances through the in-process broker
                    # so type identity is preserved even when different modules
                    # define test-only components with the same class name.
                    "components": list(components),
                },
            )

            try:
                guardrail_allow(cmd, ctx)
                await self._broker.enqueue(world_id, cmd, None)
            except Exception:
                self._release_entity_id(world, entity_id)
                raise

            return entity_id

    async def drain_and_apply(
        self,
        world_id: str | UUID,
        tick: int,
    ) -> list[Command]:
        """
        Dequeue all commands due for this world at this tick,
        apply them to the target world, ack on success.
        Returns applied commands for audit.
        """
        commands = await self._broker.dequeue_due(world_id, tick)
        if not commands:
            return []

        world = self._world_service.get_world(UUID(str(world_id)))
        applied_ids = []

        for cmd in commands:
            try:
                await self.apply(world, cmd)
                applied_ids.append(cmd.id)
            except Exception:
                logger.exception(f"Failed to apply command {cmd.id} ({cmd.type.value})")

        if applied_ids:
            await self._broker.ack(applied_ids)

        return commands

    @staticmethod
    def _hydrate_components(raw: list) -> list:
        """Convert dicts in a component list back to typed Component instances.

        Each dict entry must include a ``"type"`` key naming a concrete
        Component subclass. Use ``Component.to_payload()`` to serialize an
        instance to the expected form. Raw Component instances pass through.
        """
        from archetype.core.component import Component

        result = []
        for item in raw:
            if isinstance(item, Component):
                result.append(item)
                continue
            if isinstance(item, dict):
                # copy to avoid mutating the caller's payload
                result.append(Component.from_dict(dict(item)))
                continue
            raise TypeError(
                f"Component payload entry must be a Component instance or dict "
                f"with a 'type' key, got {type(item).__name__}"
            )
        return result

    @staticmethod
    def _hydrate_component_types(raw: list) -> list:
        """Resolve a payload's ``component_types`` list into Component classes.

        REMOVE_COMPONENT payloads cross JSON boundaries (REST/CLI/MCP) so the
        wire format is a list of strings naming Component subclasses. Pass
        them straight to ``Archetype.remove_components`` and the set
        difference compares strings against class objects, returns the full
        signature unchanged, and the command silently no-ops. Resolve names
        to concrete subclasses here so downstream comparisons work.
        """
        from archetype.core.component import Component

        result: list[type[Component]] = []
        for item in raw:
            if isinstance(item, type) and issubclass(item, Component):
                result.append(item)
                continue
            if isinstance(item, str):
                result.append(Component.get_type_by_name(item))
                continue
            raise TypeError(
                f"component_types entries must be a Component subclass or string "
                f"naming one, got {type(item).__name__}"
            )
        return result

    @staticmethod
    def _reserve_entity_id(world: AsyncWorld) -> int:
        entity_id = world._next_entity_id
        world._next_entity_id += 1
        return entity_id

    @staticmethod
    def _release_entity_id(world: AsyncWorld, entity_id: int) -> None:
        if world._next_entity_id == entity_id + 1:
            world._next_entity_id = entity_id

    @staticmethod
    async def _apply_update(world: AsyncWorld, entity_id: int, components: list) -> None:
        """Apply an UPDATE: overlay component values on an existing entity.

        Unlike ``ADD_COMPONENT``, UPDATE must mutate component values even when
        the entity already has those component types (same archetype signature).
        ``AsyncWorld.add_components`` early-returns in that case, which made
        UPDATE a silent no-op for the natural use case. Here we detect the
        same-signature path and push an overlaid row through the spawn cache
        directly; type-widening updates still delegate to ``add_components``.
        """
        from archetype.core.archetype import Archetype

        old_sig = world._entity2sig.get(entity_id)
        if old_sig is None:
            logger.warning("UPDATE: entity %s not found", entity_id)
            return

        if not components:
            return

        new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
        if new_sig != old_sig:
            # New component types introduced — archetype move already handles
            # overlaying the supplied values via _move_entity.
            await world.add_components(entity_id, components)
            return

        row = await world._move_entity(entity_id, old_sig, new_sig, components)
        if not row:
            logger.warning("UPDATE: entity %s has no prior row to update", entity_id)
            return

        # Mark the prior row for this entity inactive and append the overlaid
        # row under the same signature. Mirrors the archetype-move bookkeeping
        # in ``AsyncWorld.add_components`` so that queries latched onto "latest
        # active row" return the updated values, not the original.
        world._despawn_cache.setdefault(old_sig, []).append(entity_id)
        world._spawn_cache.setdefault(new_sig, []).append(row)

    @staticmethod
    def _apply_reserved_spawn(world: AsyncWorld, entity_id: int, components: list) -> None:
        from archetype.core.archetype import Archetype

        if entity_id in world._entity2sig:
            raise ValueError(f"Entity {entity_id} already exists in world {world.world_id}")

        sig = Archetype.sig_from_components(components)
        world._entity2sig[entity_id] = sig
        world._next_entity_id = max(world._next_entity_id, entity_id + 1)
        row_dict = Archetype.to_row_dict(
            entity_id, world.tick, components, world.world_id, run_id=""
        )
        world._spawn_cache.setdefault(sig, []).append(row_dict)

    async def apply_world_lifecycle(self, cmd: Command) -> iWorld | None:
        """Dispatch a world-level lifecycle command (create/destroy/fork).

        These commands operate on ``WorldService`` directly and don't require
        a pre-existing world instance, so they are separated from the
        per-world ``apply()`` path.

        Returns the created/forked world for CREATE and FORK, None for DESTROY.

        Lifecycle commands are not tick-scheduled, so the broker's
        ``drain_and_apply`` loop never sees them. Drain the command from
        ``__global__`` here (in a ``finally`` so failures don't leak either)
        to keep ``broker._pending`` and ``broker._queues['__global__']`` from
        growing without bound across the process lifetime.
        """
        payload = cmd.payload

        try:
            match cmd.type:
                case CommandType.CREATE_WORLD:
                    from archetype.core.config import StorageConfig, WorldConfig

                    cfg = payload.get("config", {})
                    world_config = WorldConfig(**cfg) if isinstance(cfg, dict) else cfg
                    storage_config = StorageConfig(
                        uri=payload.get("storage_uri", "./archetype_data"),
                        namespace=payload.get("namespace", "archetypes"),
                    )
                    return await self._world_service.create_world(world_config, storage_config)

                case CommandType.DESTROY_WORLD:
                    target_id = UUID(str(payload["world_id"]))
                    await self._world_service.remove_world(target_id)
                    return None

                case CommandType.FORK_WORLD:
                    from archetype.core.config import StorageConfig

                    source_id = UUID(str(payload["source_world_id"]))
                    fork_name = payload.get("name") or payload.get("config", {}).get("name")
                    return await self._world_service.fork_world(
                        source_id, fork_name, StorageConfig()
                    )

                case _:
                    raise ValueError(f"apply_world_lifecycle does not handle {cmd.type.value}")
        finally:
            await self._broker.remove("__global__", cmd.id)

    async def apply(self, world: AsyncWorld, cmd: Command) -> None:
        """
        Dispatch a single entity/processor-level command to a world.
        """
        payload = cmd.payload

        match cmd.type:
            case CommandType.SPAWN:
                components = self._hydrate_components(payload.get("components", []))
                entity_id = payload.get("entity_id")
                if entity_id is None:
                    await world.create_entity(components)
                else:
                    self._apply_reserved_spawn(world, int(entity_id), components)

            case CommandType.DESPAWN:
                entity_id = payload["entity_id"]
                await world.remove_entity(entity_id)

            case CommandType.UPDATE:
                entity_id = payload["entity_id"]
                components = self._hydrate_components(payload.get("components", []))
                await self._apply_update(world, int(entity_id), components)

            case CommandType.ADD_COMPONENT:
                entity_id = payload["entity_id"]
                components = self._hydrate_components(payload.get("components", []))
                await world.add_components(entity_id, components)

            case CommandType.REMOVE_COMPONENT:
                entity_id = payload["entity_id"]
                component_types = self._hydrate_component_types(payload.get("component_types", []))
                await world.remove_components(entity_id, component_types)

            case CommandType.ADD_PROCESSOR:
                processor = payload["processor"]
                await world.add_processor(processor)

            case CommandType.REMOVE_PROCESSOR:
                proc_type = payload["processor_type"]
                await world.remove_processor(proc_type)

            case CommandType.CREATE_WORLD | CommandType.DESTROY_WORLD | CommandType.FORK_WORLD:
                await self.apply_world_lifecycle(cmd)

            case CommandType.MESSAGE:
                # Message delivery — future extension point
                # Processors can read MESSAGE commands from broker history
                pass

            case CommandType.QUERY_WORLD:
                pass  # Read-only, delegated to QueryService

            case CommandType.CUSTOM:
                pass  # No default handler

            case _:
                logger.warning(f"Unhandled command type: {cmd.type.value}")
