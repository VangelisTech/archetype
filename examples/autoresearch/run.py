#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
AutoResearch — Karpathy-style Autonomous Optimization on Archetype
==================================================================

Runs an experiment loop that evolves scoring strategies:

    frontier → propose → execute → evaluate → keep|discard → advance

Each generation, 3 competing experiments propose improvements to a
text-interestingness scoring function. An LLM judge evaluates each.
The best strategy advances the frontier.

Usage:
    cd /Users/everettkleven/git/other/archetype
    source .env
    .venv/bin/python -m examples.autoresearch.run
"""

import asyncio
import re
from pathlib import Path

from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

from .components import BranchHead, Experiment, Result, Run
from .processors import EvaluateProcessor, ProposeProcessor


# ── Test corpus for evaluating strategies ──

TEST_TEXTS = [
    "The quick brown fox jumps over the lazy dog.",
    "Why do we build systems that understand themselves? Because recursion is the point.",
    "SELECT * FROM users WHERE active = true AND last_login > NOW() - INTERVAL '30 days';",
    "I've never published to PyPI before.",
    "The ECS decouples compute from data — processors are pure DataFrame transforms that parallelize on Ray.",
    "",
    "ok",
    "Archetype is a lazily-evaluated Virtual Data Architecture at your agent's commands.",
]


def safe_eval_strategy(strategy_text: str, test_texts: list[str]) -> float:
    """Safely evaluate a lambda strategy against test texts. Returns average score."""
    # Extract lambda from LLM output
    match = re.search(r"(lambda\s+text\s*:.+?)(?:\n|$)", strategy_text)
    if not match:
        return 0.0

    lambda_str = match.group(1).strip()

    try:
        func = eval(lambda_str)  # noqa: S307 — sandboxed test
    except Exception:
        return 0.0

    scores = []
    for text in test_texts:
        try:
            score = float(func(text))
            scores.append(max(0.0, min(10.0, score)))
        except Exception:
            scores.append(0.0)

    return sum(scores) / len(scores) if scores else 0.0


def parse_judge_score(reasoning: str) -> float:
    """Extract SCORE: X.X from judge output."""
    match = re.search(r"SCORE:\s*([\d.]+)", reasoning or "")
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return 0.0
    return 0.0


async def main():
    container = ServiceContainer()
    storage_uri = str(Path.cwd() / "autoresearch_data")

    NUM_EXPERIMENTS = 3
    NUM_GENERATIONS = 3

    # ── Frontier state ──
    frontier_strategy = "lambda text: len(text) / 10.0"
    frontier_score = safe_eval_strategy(frontier_strategy, TEST_TEXTS)
    print(f"Initial frontier: score={frontier_score:.2f}")
    print(f"  strategy: {frontier_strategy}\n")

    for gen in range(NUM_GENERATIONS):
        print(f"{'='*60}")
        print(f"GENERATION {gen + 1}")
        print(f"{'='*60}\n")

        # Create a fresh world per generation
        world = await container.world_service.create_world(
            WorldConfig(name=f"gen-{gen}"),
            StorageConfig(uri=storage_uri),
        )
        await world.system.add_processor(ProposeProcessor())
        await world.system.add_processor(EvaluateProcessor())

        # Spawn N competing experiments
        for i in range(NUM_EXPERIMENTS):
            await world.create_entity([
                BranchHead(
                    frontier_metric=frontier_score,
                    generation=gen,
                ),
                Experiment(
                    experiment_id=f"gen{gen}-exp{i}",
                    strategy=frontier_strategy,
                    status="pending",
                    generation=gen,
                ),
                Run(run_id=f"run-gen{gen}-exp{i}", status="pending", budget_steps=1),
                Result(),
            ])

        # One tick = propose + execute
        print(f"Running {NUM_EXPERIMENTS} experiments...")
        await world.run(RunConfig(num_steps=1, prefer_live_reads=True))

        # ── Collect and evaluate results ──
        experiments = []
        for _sig, df in world._live.items():
            rows = df.collect().to_pylist()
            for row in rows:
                # The LLM writes the proposed strategy into hypothesis
                hypothesis = row.get("experiment__hypothesis", "")
                strategy = hypothesis if "lambda" in hypothesis else row.get("experiment__strategy", "")
                judge_reasoning = row.get("result__reasoning", "")
                judge_score = parse_judge_score(judge_reasoning)
                exec_score = safe_eval_strategy(strategy, TEST_TEXTS)

                # Combined score: 60% execution, 40% judge
                combined = exec_score * 0.6 + judge_score * 0.4

                experiments.append({
                    "id": row.get("experiment__experiment_id", "?"),
                    "strategy": strategy,
                    "exec_score": exec_score,
                    "judge_score": judge_score,
                    "combined": combined,
                    "reasoning": judge_reasoning,
                })

        # Sort by combined score
        experiments.sort(key=lambda e: e["combined"], reverse=True)

        for exp in experiments:
            status = "KEPT" if exp["combined"] > frontier_score else "DISCARDED"
            print(f"\n  [{status}] {exp['id']}: combined={exp['combined']:.2f} "
                  f"(exec={exp['exec_score']:.2f}, judge={exp['judge_score']:.1f})")
            # Show first 120 chars of strategy
            strat_preview = exp["strategy"].replace("\n", " ")[:120]
            print(f"    strategy: {strat_preview}")

        # ── Advance frontier if best experiment beats it ──
        best = experiments[0]
        if best["combined"] > frontier_score:
            print(f"\n  >> FRONTIER ADVANCED: {frontier_score:.2f} → {best['combined']:.2f}")
            frontier_score = best["combined"]
            frontier_strategy = best["strategy"]
        else:
            print(f"\n  >> FRONTIER HELD at {frontier_score:.2f}")

    # ── Final report ──
    print(f"\n{'='*60}")
    print("FINAL FRONTIER")
    print(f"{'='*60}")
    print(f"Score: {frontier_score:.2f}")
    print(f"Strategy: {frontier_strategy}")
    print(f"Generations: {NUM_GENERATIONS}")
    print(f"Total experiments: {NUM_GENERATIONS * NUM_EXPERIMENTS}")

    await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
