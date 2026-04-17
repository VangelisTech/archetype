# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Simulation Service

Owns execution. A simulation maps to Runs.
The world is just the container — SimulationService drives what happens inside it.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.auth.guard import reset_tick_counters
from archetype.app.models import ProcessorInfo, RunResult
from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig

if TYPE_CHECKING:
    from archetype.app.command_service import CommandService
    from archetype.app.world_service import WorldService


class SimulationService:
    """
    Execution engine. Drives world stepping with broker drain.
    """

    def __init__(self, world_service: WorldService, command_service: CommandService):
        self._world_service = world_service
        self._command_service = command_service

    async def step(
        self,
        world_id: UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> int:
        """Drain queued commands, advance the world one tick. Returns applied count."""
        world = self._world_service.get_world(world_id)
        tick = getattr(world, "tick", 0)

        applied = await self._command_service.drain_and_apply(world_id, tick)
        reset_tick_counters()

        if isinstance(world, AsyncWorld):
            await world.step(run_config, **input_kwargs)

        return len(applied)

    async def run(
        self,
        world_id: UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        """Execute ``run_config.num_steps`` ticks and return the RunResult."""
        world = self._world_service.get_world(world_id)

        if hasattr(world, "run_id"):
            world.run_id = str(run_config.run_id)

        total_commands = 0

        for _ in range(run_config.num_steps):
            cmds = await self.step(world_id, run_config, **input_kwargs)
            total_commands += cmds

        return RunResult(
            run_id=run_config.run_id,
            world_id=world_id,
            ticks_completed=run_config.num_steps,
            commands_applied=total_commands,
            final_tick=getattr(world, "tick", 0),
        )

    async def run_all(
        self,
        run_config: RunConfig,
        **input_kwargs,
    ) -> list[RunResult]:
        """Step all worlds concurrently."""
        worlds = self._world_service.list_worlds()
        tasks = [self.run(w.world_id, run_config, **input_kwargs) for w in worlds]
        if tasks:
            return list(await asyncio.gather(*tasks))
        return []

    async def add_processor(self, world_id: UUID, processor) -> None:
        """Add a processor to a world's system."""
        world = self._world_service.get_world(world_id)
        await world.add_processor(processor)

    async def remove_processor(self, world_id: UUID, proc_type) -> None:
        """Remove a processor from a world."""
        world = self._world_service.get_world(world_id)
        await world.remove_processor(proc_type)

    def list_processors(self, world_id: UUID) -> list[ProcessorInfo]:
        """List processors in a world."""
        world = self._world_service.get_world(world_id)
        result = []
        if hasattr(world, "system") and hasattr(world.system, "processors"):
            for proc in world.system.processors:
                result.append(
                    ProcessorInfo(
                        name=type(proc).__name__,
                        priority=getattr(proc, "priority", 0),
                        components=[
                            c.__name__ if hasattr(c, "__name__") else str(c)
                            for c in getattr(proc, "components", ())
                        ],
                    )
                )
        return result
