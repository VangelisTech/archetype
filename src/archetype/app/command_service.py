# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Command Service

The nerve center. Every mutation from an external actor flows through here.
Accepts commands, routes them through the CommandBroker for auth and ordering,
then dispatches to the appropriate world mutation.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType

if TYPE_CHECKING:
    from archetype.app.world_service import WorldService
    from archetype.core.aio import AsyncWorld

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

    async def apply(self, world: AsyncWorld, cmd: Command) -> None:
        """
        Dispatch a single command to the world.
        """
        payload = cmd.payload

        match cmd.type:
            case CommandType.SPAWN:
                components = payload.get("components", [])
                await world.create_entity(components)

            case CommandType.DESPAWN:
                entity_id = payload["entity_id"]
                await world.remove_entity(entity_id)

            case CommandType.UPDATE | CommandType.ADD_COMPONENT:
                entity_id = payload["entity_id"]
                components = payload.get("components", [])
                await world.add_components(entity_id, components)

            case CommandType.REMOVE_COMPONENT:
                entity_id = payload["entity_id"]
                component_types = payload.get("component_types", [])
                await world.remove_components(entity_id, component_types)

            case CommandType.ADD_PROCESSOR:
                processor = payload["processor"]
                world.add_processor(processor)

            case CommandType.REMOVE_PROCESSOR:
                proc_type = payload["processor_type"]
                world.remove_processor(proc_type)

            case CommandType.CREATE_WORLD:
                from archetype.core.config import StorageConfig, WorldConfig

                cfg = payload.get("config", {})
                world_config = WorldConfig(**cfg) if isinstance(cfg, dict) else cfg
                storage_config = StorageConfig()
                await self._world_service.create_world(world_config, storage_config)

            case CommandType.DESTROY_WORLD:
                target_id = UUID(str(payload["world_id"]))
                self._world_service.remove_world(target_id)

            case CommandType.FORK_WORLD:
                from archetype.core.config import StorageConfig, WorldConfig

                source_id = UUID(str(payload["source_world_id"]))
                cfg = payload.get("config", {})
                world_config = WorldConfig(**cfg) if isinstance(cfg, dict) else cfg
                await self._world_service.fork_world(source_id, world_config, StorageConfig())

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
