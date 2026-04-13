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
        run_config: RunConfig | None = None,
        **input_kwargs,
    ) -> int:
        """
        One tick:
        1. drain_and_apply(world_id, world.tick)
        2. reset_tick_counters()
        3. world.step(run_config, **input_kwargs)

        Returns number of commands applied.
        """
        world = self._world_service.get_world(world_id)
        tick = getattr(world, "tick", 0)

        applied = await self._command_service.drain_and_apply(world_id, tick)
        reset_tick_counters()

        if run_config is None:
            run_config = RunConfig(num_steps=1)

        if isinstance(world, AsyncWorld):
            await world.step(run_config, **input_kwargs)

        return len(applied)

    async def run(
        self,
        world_id: UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        """
        Execute run_config.num_steps ticks.
        Returns RunResult with run_id, ticks completed, final state.

        The user's ``run_config`` is threaded into every per-tick step so that
        ``run_id``, ``prefer_live_reads``, ``debug``, ``suite``, ``trial``,
        ``metadata`` and other fields reach the world. Mirroring
        ``AsyncWorld.run``, the world's ``run_id`` pointer is set once up-front
        so default-run queries see the same identifier as ``RunResult.run_id``.
        """
        world = self._world_service.get_world(world_id)

        # Mirror AsyncWorld.run: pin the world's current run identifier so
        # default-run queries resolve to the user's run_id.
        if hasattr(world, "run_id"):
            world.run_id = str(run_config.run_id)

        total_commands = 0

        for _ in range(run_config.num_steps):
            # Pass the user's run_config through unchanged so fields like
            # prefer_live_reads, debug, run_id, suite, trial, metadata reach
            # world.step. RunConfig is frozen, so sharing the instance is safe.
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

    def add_processor(self, world_id: UUID, processor) -> None:
        """Add a processor to a world's system."""
        world = self._world_service.get_world(world_id)
        world.add_processor(processor)

    def remove_processor(self, world_id: UUID, proc_type) -> None:
        """Remove a processor from a world."""
        world = self._world_service.get_world(world_id)
        world.remove_processor(proc_type)

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
