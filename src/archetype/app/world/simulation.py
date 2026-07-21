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
Simulation Service

Owns execution semantics: step, run, run_episode, run_rollout.
Internal forks go through WorldService directly — not gated.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sized
from typing import TYPE_CHECKING

from daft import DataType, col
from uuid_utils import UUID

from archetype.app.models import EpisodeResult, RolloutResult, RunResult
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig

if TYPE_CHECKING:
    from archetype.app.models import EpisodeConfig, RolloutConfig
    from archetype.app.storage.interfaces import iStorageService
    from archetype.app.world.interfaces import iWorldService


class SimulationService:
    """Execution engine. Takes configs and drives worlds through ticks.

    run_episode and run_rollout are owned here. Internal forks inside
    run_rollout go through WorldService directly — they are not
    separately gated. The audit unit is the rollout call, not each fork.
    """

    def __init__(
        self,
        world_service: iWorldService,
        storage_service: iStorageService,
    ) -> None:
        self._world_service = world_service
        self._storage_service = storage_service
        self._drain_commands: Callable[[str | UUID, int], Awaitable[object]] | None = None
        self._reset_quota: Callable[[], None] | None = None

    def set_command_drain(
        self,
        drain_commands: Callable[[str | UUID, int], Awaitable[object]],
    ) -> None:
        self._drain_commands = drain_commands

    def set_quota_reset(self, reset_quota: Callable[[], None]) -> None:
        """Inject the per-tick RBAC quota reset.

        Wired by the container to ``auth.reset_tick_counters`` so the
        gate's per-tick command counter starts fresh each tick instead of
        accumulating process-wide. Left unset in gate-less service tests
        (no quota is debited without the gate, so there is nothing to
        reset). Keeps SimulationService free of an auth import — the same
        decoupling pattern as ``set_command_drain``.
        """
        self._reset_quota = reset_quota

    async def step(
        self,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> int:
        """Advance a world by one tick."""
        # A new tick begins: clear the per-actor per-tick command quota so the
        # gate's budget is per-tick, not per-process. The gate debits the
        # counter at submit time and nothing else clears it (bug B1) — the
        # LIBERO driver used to reset it by hand before every step.
        if self._reset_quota is not None:
            self._reset_quota()
        world = self._world_service.get_world(UUID(str(world_id)))
        commands_applied = 0
        if self._drain_commands is not None:
            applied = await self._drain_commands(world_id, getattr(world, "tick", 0))
            commands_applied = len(applied) if isinstance(applied, Sized) else 0
        if isinstance(world, AsyncWorld):
            await world.step(run_config, **input_kwargs)
            # Advance the world's advisory catalog head + register any new
            # signatures (issue #272). One catalog transaction per tick;
            # advisory until A2 manifests, so failures log, never fail a
            # tick whose data-plane writes already committed.
            await self._world_service.record_step(world_id)
        return commands_applied

    async def run(
        self,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        """Execute ``run_config.num_steps`` ticks and return a RunResult.

        Inv O3: the returned ``run_id`` is the world's active run_id —
        i.e. the run_id actually stamped on persisted rows during this
        run — so callers can round-trip ``RunResult.run_id`` back into a
        query and find the data they just wrote. ``RunConfig.run_id``
        seeds the world's run_id only when the world has none set yet
        (per the "first run pins" semantics that keep cross-run state
        continuity intact).
        """
        world = self._world_service.get_world(UUID(str(world_id)))

        if isinstance(world, AsyncWorld) and not world.run_id:
            world.run_id = str(run_config.run_id)

        commands_applied = 0
        for _ in range(run_config.num_steps):
            commands_applied += await self.step(world_id, run_config, **input_kwargs)

        active_run_id = getattr(world, "run_id", None) or run_config.run_id
        return RunResult(
            run_id=active_run_id,
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
        Termination strategies and their order are documented on
        ``EpisodeConfig``: structural / callable are checked before each step;
        the value-based "all done" contract is checked after each step against
        persisted rows.
        """
        world = self._world_service.get_world(UUID(str(world_id)))
        if not isinstance(world, AsyncWorld):
            raise TypeError("run_episode requires AsyncWorld")

        if not world.run_id:
            world.run_id = str(config.run_config.run_id)

        start_tick = world.tick
        terminated = False
        step_count = 0
        value_based = config.terminal_component is not None and config.terminal_field is not None

        while step_count < config.max_steps:
            # Pre-step structural check. Suppressed when terminal_field is set —
            # then terminal_component names the value-carrier, not a marker whose
            # mere presence ends the episode.
            if not value_based and config.terminal_component is not None:
                if any(config.terminal_component in sig for sig in world.entity2sig.values()):
                    terminated = True
                    break

            # Pre-step callable check.
            if config.termination is not None and config.termination(world):
                terminated = True
                break

            await self.step(world_id, config.run_config, **input_kwargs)
            step_count += 1

            # Post-step value-based check: terminate once entities carrying
            # terminal_component have terminal_field latched (all, or any).
            if value_based and await self._entities_terminal(
                world,
                config.terminal_component,
                config.terminal_field,
                require_all=config.terminal_all,
            ):
                terminated = True
                break

        return EpisodeResult(
            episode_id=config.episode_id,
            world_id=world_id,
            run_id=world.run_id,
            start_tick=start_tick,
            final_tick=world.tick,
            terminated=terminated,
            duration_steps=world.tick - start_tick,
        )

    async def _entities_terminal(
        self,
        world: AsyncWorld,
        component: type[Component],
        field: str,
        *,
        require_all: bool,
    ) -> bool:
        """Have entities carrying ``component`` latched the boolean ``field``?

        ``require_all`` True → every such entity; False → at least one. Reads
        the latest committed frame (``get_components`` reads tick-1) and reduces
        in Daft to a single row, so cost is one boolean column scan per tick
        regardless of entity count.
        """
        df = await world.get_components([component])
        flag = f"{component.get_prefix()}{field}"
        summary = df.agg(
            col(flag).cast(DataType.int64()).sum().alias("n_done"),
            col(flag).count().alias("n_total"),
        )
        # lazy_audit: the all/any decision must enter Python control flow to end
        # the episode loop; a Python bool cannot remain a lazy Daft expression.
        # Reduced to one row above, so this materializes a single scalar pair.
        materialized = await self._storage_service.materialize(summary)
        stats = materialized.to_pylist()
        if not stats:
            return False
        n_total = stats[0]["n_total"] or 0
        n_done = stats[0]["n_done"] or 0
        if n_total == 0:
            return False
        return n_done == n_total if require_all else n_done >= 1

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
            total_duration_steps=sum(r.duration_steps for r in results),
        )
