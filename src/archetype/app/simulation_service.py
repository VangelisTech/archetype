# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Simulation Service

Owns execution semantics: step, run, run_episode, run_rollout.
Internal forks go through WorldService directly — not gated.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.models import EpisodeResult, RolloutResult, RunResult
from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig

if TYPE_CHECKING:
    from archetype.app.models import EpisodeConfig, RolloutConfig
    from archetype.app.world_service import WorldService


class SimulationService:
    """Execution engine. Takes configs and drives worlds through ticks.

    run_episode and run_rollout are owned here. Internal forks inside
    run_rollout go through WorldService directly — they are not
    separately gated. The audit unit is the rollout call, not each fork.
    """

    def __init__(self, world_service: WorldService) -> None:
        self._world_service = world_service
        self._drain_commands: Callable[[str | UUID, int], Awaitable[object]] | None = None

    def set_command_drain(
        self,
        drain_commands: Callable[[str | UUID, int], Awaitable[object]],
    ) -> None:
        self._drain_commands = drain_commands

    async def step(
        self,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> int:
        """Advance a world by one tick."""
        world = self._world_service.get_world(UUID(str(world_id)))
        commands_applied = 0
        if self._drain_commands is not None:
            applied = await self._drain_commands(world_id, getattr(world, "tick", 0))
            commands_applied = len(applied) if hasattr(applied, "__len__") else 0
        if isinstance(world, AsyncWorld):
            await world.step(run_config, **input_kwargs)
        return commands_applied

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

        commands_applied = 0
        for _ in range(run_config.num_steps):
            commands_applied += await self.step(world_id, run_config, **input_kwargs)

        return RunResult(
            run_id=run_config.run_id,
            world_id=world_id,
            ticks_completed=run_config.num_steps,
            commands_applied=commands_applied,
            final_tick=getattr(world, "tick", 0),
        )

    async def run_episode(
        self,
        world_id: str | UUID,
        config: EpisodeConfig,
        **input_kwargs,
    ) -> EpisodeResult:
        """Run a bounded episode on a world until termination or max_steps.

        Does NOT fork — runs on the given world_id directly.
        Termination: terminal_component check, termination callable, or max_steps cap.
        """
        world = self._world_service.get_world(UUID(str(world_id)))
        if not isinstance(world, AsyncWorld):
            raise TypeError("run_episode requires AsyncWorld")

        start_tick = world.tick
        terminated = False
        step_count = 0

        while step_count < config.max_steps:
            # Check terminal_component
            if config.terminal_component is not None:
                for sig in world.entity2sig.values():
                    if config.terminal_component in sig:
                        terminated = True
                        break
                if terminated:
                    break

            # Check termination callable
            if config.termination is not None and config.termination(world):
                terminated = True
                break

            await self.step(world_id, config.run_config, **input_kwargs)
            step_count += 1

        return EpisodeResult(
            episode_id=config.episode_id,
            world_id=world_id,
            final_tick=world.tick,
            terminated=terminated,
            duration_steps=world.tick - start_tick,
        )

    async def run_rollout(
        self,
        world_id: str | UUID,
        config: RolloutConfig,
        **input_kwargs,
    ) -> RolloutResult:
        """Run N episodes, each on a fork of the base world.

        Forks go through WorldService directly — not gated.
        The rollout is the audit unit, not each fork.
        """
        base = self._world_service.get_world(UUID(str(world_id)))

        async def _run_one(i: int) -> EpisodeResult:
            fork = await self._world_service.fork_world(
                world_id,
                name=f"{base.name}:{config.name_prefix}:{i}",
            )
            result = await self.run_episode(fork.world_id, config.episode_config, **input_kwargs)
            if config.destroy_forks_on_complete:
                await self._world_service.destroy_world(fork.world_id)
            return result

        if config.parallel:
            results = list(await asyncio.gather(*[_run_one(i) for i in range(config.num_episodes)]))
        else:
            results = [await _run_one(i) for i in range(config.num_episodes)]

        return RolloutResult(
            rollout_id=config.rollout_id,
            base_world_id=world_id,
            episodes=results,
            num_episodes=len(results),
        )
