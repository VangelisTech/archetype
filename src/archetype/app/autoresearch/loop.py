# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
AutoResearch Loop — the experiment orchestrator.

Drives: propose → execute → evaluate → select → advance

This is the app-layer controller. It creates worlds, spawns experiments,
runs the pipeline, collects results, and advances the frontier.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from archetype.app.container import ServiceContainer
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

from .components import BranchHead, Commit, Experiment, Result, Run

logger = logging.getLogger(__name__)


@dataclass
class ExperimentResult:
    """Result from a single experiment evaluation."""

    experiment_id: str
    strategy: str
    exec_score: float
    judge_score: float
    combined_score: float
    reasoning: str
    kept: bool


@dataclass
class GenerationResult:
    """Result from a full generation of experiments."""

    generation: int
    experiments: list[ExperimentResult]
    frontier_advanced: bool
    frontier_score: float
    frontier_strategy: str


@dataclass
class AutoResearchLoop:
    """
    Karpathy-style autonomous optimization loop.

    Usage:
        loop = AutoResearchLoop(
            processors=[ProposeProcessor(), EvaluateProcessor()],
            evaluate_fn=my_eval_function,
            parse_judge_fn=my_parse_function,
        )
        results = await loop.run(
            initial_strategy="lambda text: len(text) / 10.0",
            initial_score=5.4,
            num_generations=5,
            experiments_per_gen=3,
        )
    """

    processors: list[AsyncProcessor]
    evaluate_fn: Callable[[str], float]  # strategy_text → score
    parse_judge_fn: Callable[[str], float]  # reasoning_text → score
    storage_uri: str = "./autoresearch_data"
    exec_weight: float = 0.6
    judge_weight: float = 0.4
    _container: ServiceContainer | None = field(default=None, init=False, repr=False)

    async def run(
        self,
        initial_strategy: str,
        initial_score: float,
        num_generations: int = 5,
        experiments_per_gen: int = 3,
    ) -> list[GenerationResult]:
        """Run the full autoresearch loop. Returns results per generation."""

        self._container = ServiceContainer()
        frontier_strategy = initial_strategy
        frontier_score = initial_score
        generation_results = []

        try:
            for gen in range(num_generations):
                gen_result = await self._run_generation(
                    gen, frontier_strategy, frontier_score, experiments_per_gen
                )
                generation_results.append(gen_result)

                if gen_result.frontier_advanced:
                    frontier_strategy = gen_result.frontier_strategy
                    frontier_score = gen_result.frontier_score
                    logger.info(
                        f"Gen {gen}: ADVANCED {frontier_score:.2f} "
                        f"with: {frontier_strategy[:80]}"
                    )
                else:
                    logger.info(f"Gen {gen}: HELD at {frontier_score:.2f}")

        finally:
            await self._container.shutdown()

        return generation_results

    async def _run_generation(
        self,
        gen: int,
        frontier_strategy: str,
        frontier_score: float,
        num_experiments: int,
    ) -> GenerationResult:
        """Run a single generation: create world → spawn → step → collect → select."""

        world = await self._container.world_service.create_world(
            WorldConfig(name=f"gen-{gen}"),
            StorageConfig(uri=self.storage_uri),
        )

        # Register processors
        for proc in self.processors:
            await world.system.add_processor(proc)

        # Spawn competing experiments
        for i in range(num_experiments):
            await world.create_entity([
                BranchHead(
                    frontier_metric_value=frontier_score,
                    generation=gen,
                ),
                Experiment(
                    experiment_id=f"gen{gen}-exp{i}",
                    strategy=frontier_strategy,
                    status="pending",
                ),
                Run(run_id=f"run-gen{gen}-exp{i}", status="pending"),
                Commit(),
                Result(),
            ])

        # One tick = propose + evaluate
        await world.run(RunConfig(num_steps=1, prefer_live_reads=True))

        # Collect results
        experiments = []
        for _sig, df in world._live.items():
            rows = df.collect().to_pylist()
            for row in rows:
                hypothesis = row.get("experiment__strategy", "")
                # Check if the LLM wrote a new strategy into the hypothesis field
                # (processors may write to different fields depending on implementation)
                for field_name in ["experiment__hypothesis", "experiment__strategy"]:
                    val = row.get(field_name, "")
                    if val and "lambda" in val:
                        hypothesis = val
                        break

                reasoning = row.get("result__reasoning", "")
                judge_score = self.parse_judge_fn(reasoning)
                exec_score = self.evaluate_fn(hypothesis)
                combined = exec_score * self.exec_weight + judge_score * self.judge_weight

                experiments.append(ExperimentResult(
                    experiment_id=row.get("experiment__experiment_id", "?"),
                    strategy=hypothesis,
                    exec_score=exec_score,
                    judge_score=judge_score,
                    combined_score=combined,
                    reasoning=reasoning,
                    kept=combined > frontier_score,
                ))

        # Sort best first
        experiments.sort(key=lambda e: e.combined_score, reverse=True)

        # Advance frontier
        best = experiments[0] if experiments else None
        advanced = best is not None and best.combined_score > frontier_score

        return GenerationResult(
            generation=gen,
            experiments=experiments,
            frontier_advanced=advanced,
            frontier_score=best.combined_score if advanced else frontier_score,
            frontier_strategy=best.strategy if advanced else frontier_strategy,
        )
