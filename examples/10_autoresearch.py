# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
AutoResearch: Save-State Optimization
======================================

Worlds are save states. This example tunes a parameter by replaying
candidate lines from one base save:

1. Fork the base world and set a candidate value (prepare)
2. Roll the candidate forward (rollout)
3. Score it by loading the episode save and grading what happened (evaluate)
4. Keep the best route on the experiment's ledger (BranchHead)

Every attempt — including crashes — is recorded on the experiment's own
lab world, so the search itself is a queryable archetype simulation.
Rerunning with the same experiment_id resumes from the persisted best.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/10_autoresearch.py
"""

import asyncio

from daft import col

from archetype import ArchetypeRuntime, AutoResearchConfig, EvaluationResult
from archetype.app.models import EpisodeConfig
from archetype.app.research.models import Run
from archetype.core.component import Component
from archetype.core.config import StorageConfig

TARGET = 3.0


class Knob(Component):
    x: float = 0.0


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="autoresearch_demo")

    async with ArchetypeRuntime() as runtime:
        base = runtime.world("knob-base", storage=storage)
        await base.spawn(Knob(x=0.0))
        await base.run(steps=1)
        print(f"Base save state: {base.world_id}\n")

        async def prepare(ctx):
            """Fork the base save and try x = iteration index."""
            fork = await base.fork(f"candidate-x{ctx.iteration}")
            rows = (await fork.query(Knob)).to_pylist()
            await fork.update(int(rows[0]["entity_id"]), Knob(x=float(ctx.iteration)))
            await fork.run(steps=1)
            return fork.world_id

        async def evaluate(rollout) -> EvaluationResult:
            """Load each episode save and score its final Knob state."""
            xs = []
            for ep in rollout.episodes:
                episode = runtime.attach(ep.world_id, name="episode")
                final_tick = (await episode.info()).tick - 1

                def final_x(df, t=final_tick):
                    latest = df.where(col("tick") == t)
                    return latest.agg(col("knob__x").mean().alias("x")).to_pylist()[0]["x"]

                outputs = await episode.grade(Knob, graders=[final_x])
                xs.append(outputs[0])
            dist = sum((x - TARGET) ** 2 for x in xs) / len(xs)
            score = -dist if dist else 0.0
            return EvaluationResult(
                score=score,
                evaluator="knob-distance-v1",
                evidence={"xs": xs, "target": TARGET},
            )

        config = AutoResearchConfig(
            experiment_name="knob-tuning",
            experiment_id="knob-tuning-demo",
            evaluator_id="knob-distance-v1",
            rollout_contract_id="knob-1ep-1step-v1",
            episode_config=EpisodeConfig(max_steps=1),
            num_episodes=1,
            max_iterations=4,
        )

        result = await base.autoresearch(config, evaluate, prepare_candidate=prepare)

        for it in result.iterations:
            marker = "ADVANCE" if it.improved else "hold"
            print(
                f"iter {it.iteration}: x={float(it.iteration):.0f} "
                f"score={it.score:+.1f} -> {marker} (best={it.incumbent_score:+.1f})"
            )
        print(f"\nBest score {result.final_score:+.1f} at x={TARGET:.0f}, ", end="")
        print(f"improved={result.improved}")

        # The search itself is a simulation: load the ledger save and audit it.
        lab = runtime.attach(result.lab_world_id, name="lab")
        attempts = await lab.query(Run)
        info = await lab.info()
        latest = attempts.where(col("tick") == info.tick - 1)
        print(f"\nExperiment ledger ({result.lab_world_id}), attempts at final tick:")
        latest.select("run__run_id", "run__status").show()


if __name__ == "__main__":
    asyncio.run(main())
