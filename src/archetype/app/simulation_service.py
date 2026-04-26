# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Simulation Service

Owns execution. Drives world stepping via RunConfig.
The world is the container — SimulationService drives what happens inside it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.models import RunResult
from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig

if TYPE_CHECKING:
    from archetype.app.world_service import WorldService


class SimulationService:
    """Execution engine. Takes a RunConfig and drives worlds through ticks."""

    def __init__(self, world_service: WorldService) -> None:
        self._world_service = world_service

    async def step(
        self,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> None:
        """Advance a world by one tick."""
        world = self._world_service.get_world(UUID(str(world_id)))
        if isinstance(world, AsyncWorld):
            await world.step(run_config, **input_kwargs)

    async def run(
        self,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        """Execute ``run_config.num_steps`` ticks and return a RunResult."""
        world = self._world_service.get_world(UUID(str(world_id)))

        if isinstance(world, AsyncWorld) and world.run_id is None:
            world.run_id = str(run_config.run_id)

        for _ in range(run_config.num_steps):
            await self.step(world_id, run_config, **input_kwargs)

        return RunResult(
            run_id=run_config.run_id,
            world_id=world_id,
            ticks_completed=run_config.num_steps,
            commands_applied=0,
            final_tick=getattr(world, "tick", 0),
        )
