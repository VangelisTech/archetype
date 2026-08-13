# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Cooperative Emergence Under Resource Scarcity
==============================================

Auto-research experiment: sweep the parameter space to find where
cooperation transitions from unstable to dominant in a multi-agent
commons with depletable resources.

Usage:
    uv run python experiments/cooperative_emergence.py
"""

from __future__ import annotations

import asyncio

from daft import DataFrame, col
from daft.functions import when

from archetype import (
    ArchetypeRuntime,
    AsyncProcessor,
    Component,
    EpisodeConfig,
    RolloutResult,
)
from archetype.core.config import RunConfig
from archetype.core.hooks import PostTick
from archetype.research import AutoResearchConfig, Research

# ── Components ────────────────────────────────────────────────────────────


class Strategy(Component):
    kind: str = "cooperative"  # cooperative | greedy | conditional
    energy: float = 100.0
    lifetime_harvest: float = 0.0


class Pool(Component):
    current: float = 1000.0
    capacity: float = 1000.0
    regen_rate: float = 0.05
    contribution_cost: float = 10.0


class Terminal(Component):
    reason: str = ""


# ── Processors ────────────────────────────────────────────────────────────


class HarvestProcessor(AsyncProcessor):
    """Each agent harvests from or contributes to the pool based on strategy.

    Greedy: extracts 20 units, gains energy.
    Cooperative: extracts 5 units, contributes `contribution_cost` back,
                 pays an energy cost.
    Conditional: cooperates if pool > 50% capacity, otherwise greedy.
    """

    components = (Strategy,)
    priority = 10

    async def process(self, df: DataFrame, **kw) -> DataFrame:
        # Greedy: +15 energy, +20 harvest
        # Cooperative: -2 energy (net of contribution cost), +5 harvest
        # Conditional: same as cooperative if pool healthy, else greedy
        return df.with_columns(
            {
                "strategy__energy": when(
                    col("strategy__kind") == "greedy",
                    col("strategy__energy") + 15.0,
                )
                .when(
                    col("strategy__kind") == "cooperative",
                    col("strategy__energy") - 2.0,
                )
                .otherwise(
                    # conditional: assume cooperative for now
                    col("strategy__energy") - 2.0,
                ),
                "strategy__lifetime_harvest": when(
                    col("strategy__kind") == "greedy",
                    col("strategy__lifetime_harvest") + 20.0,
                ).otherwise(
                    col("strategy__lifetime_harvest") + 5.0,
                ),
            }
        )


class RegrowthProcessor(AsyncProcessor):
    """Pool regenerates logistically each tick.

    new_pool = pool + regen_rate * pool * (1 - pool/capacity)
    Capped at capacity.
    """

    components = (Pool,)
    priority = 20

    async def process(self, df: DataFrame, **kw) -> DataFrame:
        regrowth = col("pool__current") + col("pool__regen_rate") * col("pool__current") * (
            1.0 - col("pool__current") / col("pool__capacity")
        )
        return df.with_columns(
            {
                "pool__current": when(
                    regrowth > col("pool__capacity"),
                    col("pool__capacity"),
                ).otherwise(regrowth),
            }
        )


class DepletionProcessor(AsyncProcessor):
    """Drain the pool based on total agent extraction.

    Each greedy agent drains 20, each cooperative drains 5.
    Applied as a flat deduction from the pool each tick.
    Pool floors at 0.
    """

    components = (Pool,)
    priority = 30

    async def process(self, df: DataFrame, **kw) -> DataFrame:
        # Simplified: deduct a fixed amount per tick
        # In a real sim, this would count active agents
        return df.with_columns(
            {
                "pool__current": when(
                    col("pool__current") - 50.0 < 0.0,
                    0.0,
                ).otherwise(col("pool__current") - 50.0),
            }
        )


# ── Termination ───────────────────────────────────────────────────────────

# World ids whose pool has hit zero. The PostTick hook below is registered
# on the base world and carried onto every fork (fork deep-copies the hook
# registry), so episode forks report collapse here under their own world_id.
_collapsed_worlds: set[str] = set()


async def detect_collapse(event: PostTick) -> None:
    """Flag the world as collapsed once its pool reaches zero."""
    for df in event.results.values():
        if "pool__current" not in df.column_names:
            continue
        values = df.to_pydict().get("pool__current", [])
        if any(v is not None and v <= 0.0 for v in values):
            _collapsed_worlds.add(str(event.world_id))


def pool_collapsed(world) -> bool:
    """EpisodeConfig.termination predicate — must be synchronous."""
    return str(world.world_id) in _collapsed_worlds


# ── Evaluator ─────────────────────────────────────────────────────────────


def survival_rate(result: RolloutResult) -> float:
    """Score = fraction of episodes that didn't terminate early (survived)."""
    if not result.episodes:
        return 0.0
    survived = sum(1 for ep in result.episodes if not ep.terminated)
    return survived / len(result.episodes)


def make_cooperation_score(max_steps: int):
    """Score = average episode duration / max_steps.

    Longer episodes mean the commons sustained longer.
    1.0 = all episodes ran to max_steps (sustainable).
    0.0 = all episodes collapsed immediately.

    Normalizes by the configured step budget, not the longest observed
    episode — otherwise uniform collapse scores a perfect 1.0.
    """

    def cooperation_score(result: RolloutResult) -> float:
        if not result.episodes:
            return 0.0
        avg_duration = sum(ep.duration_steps for ep in result.episodes) / len(result.episodes)
        return avg_duration / max_steps

    return cooperation_score


# ── Main ──────────────────────────────────────────────────────────────────


async def main():
    print("=" * 60)
    print("Cooperative Emergence Under Resource Scarcity")
    print("=" * 60)
    print()

    async with ArchetypeRuntime() as rt:
        # Create the base world with processors
        world = rt.world(
            "commons",
            processors=[HarvestProcessor(), RegrowthProcessor(), DepletionProcessor()],
        )

        # Seed: 4 cooperative + 3 greedy agents + 1 pool
        for _ in range(4):
            await world.spawn(Strategy(kind="cooperative", energy=100.0))
        for _ in range(3):
            await world.spawn(Strategy(kind="greedy", energy=100.0))
        pool_id = await world.spawn(Pool(current=1000.0, capacity=1000.0, regen_rate=0.05))
        # Hooks registered before forking are deep-copied onto every fork,
        # including the per-episode forks created by run_rollout.
        await world.add_hook(PostTick, detect_collapse)
        await world.step()  # materialize the seed state

        info = await world.info()
        print(f"Base world: {info.world_id}")
        print("  4 cooperative + 3 greedy agents, pool=1000")
        print()

        # ── Parameter Sweep ───────────────────────────────────────────────

        regen_rates = [0.01, 0.03, 0.05, 0.10, 0.20]
        episodes_per_point = 5
        max_steps = 50

        print(f"Sweeping {len(regen_rates)} regen rates × {episodes_per_point} episodes each")
        print(f"Max steps per episode: {max_steps}")
        print()

        results: dict[float, float] = {}

        for regen in regen_rates:
            # Fork the base for this parameter point. Fork lineage makes
            # the parent's materialized rows readable from the fork, so
            # the update below finds its prior row and episode forks see
            # the full seed state.
            fork = await world.fork(f"regen-{regen}")
            await fork.update(pool_id, Pool(current=1000.0, capacity=1000.0, regen_rate=regen))
            await fork.step()  # materialize the regen update before episodes fork

            config = AutoResearchConfig(
                experiment_name=f"commons-regen-{regen}",
                experiment_id=f"commons-regen-{regen}",
                evaluator_id="cooperation-score-v1",
                rollout_contract_id="commons-dynamics-v1",
                episode_config=EpisodeConfig(
                    run_config=RunConfig(),
                    max_steps=max_steps,
                    termination=pool_collapsed,
                ),
                num_episodes=episodes_per_point,
                max_iterations=1,  # single iteration per param point
                destroy_forks_on_complete=True,
            )

            result = await Research(fork).autoresearch(
                config,
                make_cooperation_score(max_steps),
            )

            score = result.final_score
            results[regen] = score

            # Show per-episode details
            if result.iterations:
                rollout = result.iterations[0].rollout
                durations = [ep.duration_steps for ep in rollout.episodes]
                terminated = sum(1 for ep in rollout.episodes if ep.terminated)
                print(
                    f"  regen={regen:.2f}: score={score:.3f} "
                    f"durations={durations} "
                    f"terminated={terminated}/{len(rollout.episodes)}"
                )

        # ── Results ───────────────────────────────────────────────────────

        print()
        print("=" * 60)
        print("RESULTS: Cooperation Score by Regeneration Rate")
        print("=" * 60)
        print()
        print(f"  {'regen_rate':>12}  {'score':>8}  {'assessment'}")
        print(f"  {'─' * 12}  {'─' * 8}  {'─' * 20}")

        for regen, score in sorted(results.items()):
            if score >= 0.9:
                assessment = "SUSTAINABLE"
            elif score >= 0.5:
                assessment = "stressed"
            else:
                assessment = "COLLAPSED"
            print(f"  {regen:>12.2f}  {score:>8.3f}  {assessment}")

        # Find the phase boundary
        sorted_results = sorted(results.items())
        boundary = None
        for i in range(len(sorted_results) - 1):
            r1, s1 = sorted_results[i]
            r2, s2 = sorted_results[i + 1]
            if s1 < 0.5 and s2 >= 0.5:
                boundary = (r1, r2)
                break

        print()
        if boundary:
            print(
                f"Phase boundary detected: between regen={boundary[0]:.2f} and regen={boundary[1]:.2f}"
            )
            print("Cooperation transitions from collapse to sustainability in this range.")
        else:
            all_scores = [s for _, s in sorted_results]
            if all(s >= 0.5 for s in all_scores):
                print("All parameter points sustained — try lower regen rates to find collapse.")
            else:
                print(
                    "All parameter points collapsed — try higher regen rates to find sustainability."
                )

        print()
        print("Done. All results queryable via QueryService (append-only).")


if __name__ == "__main__":
    asyncio.run(main())
