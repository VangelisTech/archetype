# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from uuid_utils import UUID, uuid7

from archetype.app.models import EpisodeConfig, EpisodeResult, RolloutConfig, RolloutResult
from archetype.core.config import RunConfig
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
        """
        world = self._world_service.get_world(UUID(str(world_id)))

        # Track the incumbent score
        incumbent_score = float("-inf")
        initial_score = float("-inf")
        iterations: list[IterationResult] = []

        for i in range(config.max_iterations):
            # Build rollout config
            rollout_config = RolloutConfig(
                episode_config=config.episode_config,
                num_episodes=config.num_episodes,
                parallel=config.parallel,
                destroy_forks_on_complete=config.destroy_forks_on_complete,
                name_prefix=f"{config.experiment_name}:iter{i}",
            )

            # Run the rollout
            rollout_result = await self._simulation_service.run_rollout(
                world_id, rollout_config
            )

            # Evaluate
            score = evaluator(rollout_result)
            if hasattr(score, "__await__"):
                score = await score

            if i == 0:
                initial_score = score

            # Compare to incumbent
            improved = score > incumbent_score + config.improvement_threshold
            if improved:
                incumbent_score = score

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
        )

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
