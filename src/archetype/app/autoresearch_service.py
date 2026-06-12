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
AutoResearch Service

The loop controller. Reads the current BranchHead, launches a rollout,
evaluates results, and advances the head on improvement.

The service is deliberately scoring-agnostic: the user provides an
evaluator callable that takes rollout results and returns a score.
The service compares scores and advances the BranchHead when the
new score beats the incumbent.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from uuid_utils import UUID

from archetype.app.models import EpisodeConfig, RolloutConfig, RolloutResult
from archetype.core.config import RunConfig, WorldConfig
from archetype.experiments.components import BranchHead, Experiment, Result, Run, RunStatus

if TYPE_CHECKING:
    from archetype.app.simulation_service import SimulationService
    from archetype.app.world_service import WorldService

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AutoResearchConfig:
    """Configuration for one autoresearch loop.

    The evaluator takes a RolloutResult and returns a float score.
    Higher is better. The loop advances the BranchHead when the
    new score exceeds the incumbent by at least `improvement_threshold`.
    """

    experiment_name: str
    episode_config: EpisodeConfig = field(default_factory=EpisodeConfig)
    num_episodes: int = 10
    parallel: bool = False
    max_iterations: int = 100
    improvement_threshold: float = 0.0
    destroy_forks_on_complete: bool = True
    # The loop's own state lives on the ledger: a lab world named
    # "autoresearch:{experiment_name}" whose ticks are the loop's iterations.
    record_to_ledger: bool = True


@dataclass(frozen=True)
class IterationResult:
    """Result of one autoresearch iteration."""

    iteration: int
    rollout: RolloutResult
    score: float
    improved: bool
    incumbent_score: float


@dataclass(frozen=True)
class AutoResearchResult:
    """Result of the full autoresearch loop."""

    experiment_name: str
    iterations_completed: int
    final_score: float
    initial_score: float
    iterations: list[IterationResult] = field(default_factory=list)
    # World id of the experiment's ledger (lab world); "" when recording
    # was disabled. Query it like any other world: Experiment at tick 0,
    # one tick per iteration, BranchHead history at every tick.
    lab_world_id: str = ""

    @property
    def improved(self) -> bool:
        return self.final_score > self.initial_score


# ─────────────────────────────────────────────────────────────────────────────
# Evaluator protocol
# ─────────────────────────────────────────────────────────────────────────────

# An evaluator takes a RolloutResult and returns a score (float).
# The service doesn't interpret the score beyond comparison.
Evaluator = Callable[[RolloutResult], float | Awaitable[float]]


# ─────────────────────────────────────────────────────────────────────────────
# Service
# ─────────────────────────────────────────────────────────────────────────────


class AutoResearchService:
    """Loop controller for autoresearch.

    Depends on:
      - WorldService for world lifecycle (fork, destroy, get_world)
      - SimulationService for run_rollout execution
    """

    def __init__(
        self,
        world_service: WorldService,
        simulation_service: SimulationService,
    ) -> None:
        self._world_service = world_service
        self._simulation_service = simulation_service

    async def run(
        self,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        """Run the autoresearch loop.

        1. Get the base world state
        2. For each iteration:
           a. Run a rollout (N episodes on forks of the base)
           b. Evaluate the rollout result → score
           c. If score > incumbent + threshold → advance (record improvement)
           d. Call on_iteration callback if provided
        3. Return the aggregate result

        The base world is NOT mutated by the loop. Each iteration forks
        from the base, runs episodes on the forks, and optionally destroys
        them. The base world's state is the "seed" for every iteration.

        The loop's own state lives on the ledger (unless
        config.record_to_ledger is False): a lab world named
        "autoresearch:{experiment_name}" whose tick 0 is the genesis
        (Experiment + seed BranchHead as initial conditions) and whose
        every subsequent tick is one iteration — a Run row, a Result row,
        and the BranchHead advance when the iteration improved. The
        incumbent is read from the ledger, never from memory, so a second
        run of the same experiment resumes from the last declared best.
        """
        # Validate the base world exists (raises if not); the loop forks
        # from world_id rather than mutating this instance.
        self._world_service.get_world(UUID(str(world_id)))

        lab = None
        head_entity_id: int | None = None
        incumbent_score = float("-inf")
        start_iteration = 0
        if config.record_to_ledger:
            lab, head_entity_id, incumbent_score, start_iteration = await self._attach_ledger(
                world_id, config
            )

        initial_score = float("-inf")
        iterations: list[IterationResult] = []

        for i in range(start_iteration, start_iteration + config.max_iterations):
            # Build rollout config
            rollout_config = RolloutConfig(
                episode_config=config.episode_config,
                num_episodes=config.num_episodes,
                parallel=config.parallel,
                destroy_forks_on_complete=config.destroy_forks_on_complete,
                name_prefix=f"{config.experiment_name}:iter{i}",
            )

            # Run the rollout
            started_at_ms = int(time.time() * 1000)
            rollout_result = await self._simulation_service.run_rollout(world_id, rollout_config)

            # Evaluate
            score = evaluator(rollout_result)
            if hasattr(score, "__await__"):
                score = await score  # ty: ignore[invalid-await]  # runtime-checked awaitable
            score = float(score)

            if i == start_iteration:
                initial_score = score

            # Compare to incumbent
            improved = score > incumbent_score + config.improvement_threshold
            if improved:
                incumbent_score = score

            if lab is not None:
                await self._record_iteration(
                    lab,
                    head_entity_id,
                    config,
                    iteration=i,
                    rollout=rollout_result,
                    score=score,
                    improved=improved,
                    started_at_ms=started_at_ms,
                )

            iteration_result = IterationResult(
                iteration=i,
                rollout=rollout_result,
                score=score,
                improved=improved,
                incumbent_score=incumbent_score,
            )
            iterations.append(iteration_result)

            logger.info(
                "autoresearch %s iter=%d score=%.4f incumbent=%.4f improved=%s",
                config.experiment_name,
                i,
                score,
                incumbent_score,
                improved,
            )

            # Callback
            if on_iteration is not None:
                result = on_iteration(iteration_result)
                if hasattr(result, "__await__"):
                    await result

        return AutoResearchResult(
            experiment_name=config.experiment_name,
            iterations_completed=len(iterations),
            final_score=incumbent_score,
            initial_score=initial_score,
            iterations=iterations,
            lab_world_id=str(lab.world_id) if lab is not None else "",
        )

    # ── Ledger: the loop's own state as world rows ─────────────────────────

    async def _attach_ledger(
        self,
        base_world_id: str | UUID,
        config: AutoResearchConfig,
    ) -> tuple[Any, int, float, int]:
        """Create or resume the experiment's lab world.

        Returns (lab_world, head_entity_id, incumbent_score, start_iteration).

        Genesis (new lab world): spawn Experiment + a seed BranchHead and
        step once, so tick 0 holds the experiment's initial conditions. The
        lab world inherits the base world's storage — the record lives next
        to the data it is about.

        Resume (existing lab world): the latest BranchHead row IS the
        incumbent; the next iteration index is derived from the lab tick
        (one iteration per tick, genesis at tick 0).
        """
        name = f"autoresearch:{config.experiment_name}"
        try:
            lab = self._world_service.get_world_by_name(name)
        except KeyError:
            record = self._world_service.storage_record(base_world_id)
            storage_config = record[0] if record is not None else None
            cache_config = record[1] if record is not None else None
            lab = await self._world_service.create_world(
                WorldConfig(name=name), storage_config, cache_config
            )

        head_row = await self._read_head(lab)
        if head_row is None:
            await lab.create_entity(
                [
                    Experiment.make(
                        config.experiment_name,
                        "",
                        metadata={"base_world_id": str(base_world_id)},
                    )
                ]
            )
            head_entity_id = await lab.create_entity(
                [BranchHead.make(config.experiment_name, "", descriptor={"score": None})]
            )
            await lab.step(RunConfig())  # genesis: initial conditions at tick 0
            return lab, head_entity_id, float("-inf"), max(lab.tick - 1, 0)

        descriptor = json.loads(head_row["branchhead__descriptor_json"])
        score = descriptor.get("score")
        incumbent = float(score) if score is not None else float("-inf")
        return lab, int(head_row["entity_id"]), incumbent, max(lab.tick - 1, 0)

    async def _read_head(self, lab) -> dict | None:
        """The latest persisted BranchHead row, or None for a fresh world."""
        if lab.tick == 0:
            return None
        df = await lab.query_archetype(sig=(BranchHead,), ticks=[lab.tick - 1])
        rows = df.to_pylist()
        return rows[0] if rows else None

    async def _record_iteration(
        self,
        lab,
        head_entity_id: int | None,
        config: AutoResearchConfig,
        *,
        iteration: int,
        rollout: RolloutResult,
        score: float,
        improved: bool,
        started_at_ms: int,
    ) -> None:
        """Append one tick of loop history to the lab world.

        One iteration = one tick: a Run row (the attempt), a Result row
        (the evaluation), and — when the iteration improved on the
        incumbent — the BranchHead advance. Every advance is an append;
        the head's full history stays queryable at every tick.
        """
        run_id = f"{config.experiment_name}:iter{iteration}"
        await lab.create_entity(
            [
                Run(
                    run_id=run_id,
                    experiment_name=config.experiment_name,
                    status=RunStatus.STOPPED.value,
                    task="rollout",
                    started_at_ms=started_at_ms,
                    finished_at_ms=int(time.time() * 1000),
                )
            ]
        )
        await lab.create_entity(
            [
                Result.make(
                    run_id,
                    outputs={
                        "score": score,
                        "improved": improved,
                        "iteration": iteration,
                        "num_episodes": rollout.num_episodes,
                        "total_duration_steps": rollout.total_duration_steps,
                        "episode_world_ids": [str(ep.world_id) for ep in rollout.episodes],
                    },
                    evaluator="autoresearch",
                )
            ]
        )
        if improved and head_entity_id is not None:
            await lab.update_entity(
                head_entity_id,
                [
                    BranchHead.make(
                        config.experiment_name,
                        "",
                        run_id=run_id,
                        descriptor={"score": score, "iteration": iteration},
                    )
                ],
            )
        await lab.step(RunConfig())

    async def sweep(
        self,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        param_grid: dict[str, list[Any]],
        *,
        setup_fn: Callable | None = None,
    ) -> dict[tuple, AutoResearchResult]:
        """Parameter sweep: run the autoresearch loop for each point in a grid.

        For each combination of parameters in param_grid:
        1. Fork the base world
        2. Call setup_fn(fork_world_id, params) to configure the fork
        3. Run the autoresearch loop on the fork
        4. Collect results keyed by parameter tuple

        Returns a dict mapping param tuples to AutoResearchResults.
        """
        import itertools

        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        results: dict[tuple, AutoResearchResult] = {}

        for combo in itertools.product(*param_values):
            params = dict(zip(param_names, combo, strict=False))

            # Fork the base world for this parameter point
            fork = await self._world_service.fork_world(
                world_id,
                name=f"{config.experiment_name}:sweep:{combo}",
            )

            # Let the caller configure the fork with these params
            if setup_fn is not None:
                result = setup_fn(fork.world_id, params)
                if hasattr(result, "__await__"):
                    await result

            # Run the loop on this fork
            sweep_config = AutoResearchConfig(
                experiment_name=f"{config.experiment_name}:{combo}",
                episode_config=config.episode_config,
                num_episodes=config.num_episodes,
                parallel=config.parallel,
                max_iterations=config.max_iterations,
                improvement_threshold=config.improvement_threshold,
                destroy_forks_on_complete=config.destroy_forks_on_complete,
                record_to_ledger=config.record_to_ledger,
            )

            result = await self.run(fork.world_id, sweep_config, evaluator)
            results[combo] = result

            logger.info(
                "autoresearch sweep %s params=%s final_score=%.4f",
                config.experiment_name,
                params,
                result.final_score,
            )

        return results
