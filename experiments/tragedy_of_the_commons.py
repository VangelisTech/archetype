# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tragedy of the Commons
=======================

A multi-agent simulation exploring how different harvesting strategies
affect a shared renewable resource (the "commons").

Each agent has a strategy:
  - greedy:       harvests 12% of remaining pool per tick
  - moderate:     harvests 5% of remaining pool per tick
  - cooperative:  harvests 2% of remaining pool per tick

The pool regenerates via logistic growth each tick:
  growth = growth_rate * amount * (1 - amount / max_capacity)
  with growth_rate=1.0, giving max regeneration of 250/tick at half capacity

Equilibrium analysis (accounting for harvest-before-regen discrete dynamics):
  - 6 cooperative (total 12%): equilibrium at ~981 (very healthy)
  - Mixed 2+2+2   (total 38%): equilibrium at ~625 (stressed but surviving)
  - 6 greedy      (total 72%): no equilibrium (collapse)

Three scenarios are compared:
  1. All cooperative  (6 cooperative agents)
  2. Mixed society    (2 greedy + 2 moderate + 2 cooperative)
  3. All greedy       (6 greedy agents)

Run for 50 ticks each and compare pool depletion curves, per-agent
harvest totals, and sustainability outcomes.

Usage:
    uv run python experiments/tragedy_of_the_commons.py
"""

import asyncio
from dataclasses import dataclass, field

import daft
import pyarrow as pa
from daft import DataFrame, col
from daft.functions import when

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.resources import Resources

# ---------------------------------------------------------------------------
# In-memory infrastructure (same pattern as examples/simulation_script.py)
# ---------------------------------------------------------------------------


class InMemoryQuerier:
    def __init__(self):
        self._store: dict[tuple, DataFrame] = {}

    async def query_archetype(self, **kwargs):
        sig = kwargs["sig"]
        ticks = kwargs.get("ticks", [0])
        key = (sig, ticks[0] if ticks else 0)
        if key in self._store:
            return self._store[key]
        schema = Archetype.get_archetype_schema(sig)
        return daft.from_arrow(pa.Table.from_batches([], schema=schema))

    def store(self, sig, tick, df):
        self._store[(sig, tick)] = df


class InMemoryUpdater:
    def __init__(self, querier: InMemoryQuerier):
        self._querier = querier

    async def update(self, df, sig, tick, world_id, run_id):
        df = (
            df.with_column("tick", daft.lit(tick))
            .with_column("world_id", daft.lit(str(world_id)))
            .with_column("run_id", daft.lit(str(run_id)))
        )
        df_mat = df.collect()
        self._querier.store(sig, tick, df_mat)
        return df_mat


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------


class Gatherer(Component):
    """An agent that harvests from the shared commons."""

    name: str = ""
    strategy: str = "cooperative"  # "greedy", "moderate", "cooperative"
    harvest_rate: float = 0.02  # fraction of pool taken per tick
    energy: float = 50.0  # current energy (fed by harvesting)
    total_harvested: float = 0.0  # lifetime harvest


# ---------------------------------------------------------------------------
# Shared Resource (via Resources DI, not entity data)
# ---------------------------------------------------------------------------

STRATEGY_RATES = {
    "greedy": 0.12,
    "moderate": 0.05,
    "cooperative": 0.02,
}


@dataclass
class CommonPool:
    """The shared renewable resource."""

    amount: float = 1000.0
    max_capacity: float = 1000.0
    growth_rate: float = 1.0  # logistic growth parameter (max regen = r*K/4 = 250/tick)
    history: list[float] = field(default_factory=list)

    def regenerate(self):
        """Logistic regrowth: fast when pool is mid-range, slow near 0 or max."""
        growth = self.growth_rate * self.amount * (1.0 - self.amount / self.max_capacity)
        self.amount = min(self.amount + growth, self.max_capacity)

    def record(self):
        self.history.append(round(self.amount, 2))


@dataclass
class SimMetrics:
    """Per-tick metrics for analysis."""

    tick_harvests: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Processors
# ---------------------------------------------------------------------------


class HarvestProcessor(AsyncProcessor):
    """
    Each gatherer harvests from the shared pool proportional to their strategy.

    Requires a collect (justified: cross-entity coordination for shared resource).
    The pool is finite — if multiple agents try to harvest more than what's left,
    they split the remainder proportionally.
    """

    components = (Gatherer,)
    priority = 10

    async def process(
        self, df: DataFrame, tick: int = 0, resources: Resources = None, **kwargs
    ) -> DataFrame:
        pool = resources.require(CommonPool)
        metrics = resources.require(SimMetrics)

        # Collect to coordinate shared resource depletion (justified: shared pool)
        df_mat = df.collect()
        arrow_table = df_mat.to_arrow()
        rows = arrow_table.to_pylist()

        if not rows or pool.amount <= 0.01:
            # Pool is effectively empty — agents get nothing, lose energy
            energy = col("gatherer__energy") - 2.0
            return df.with_column(
                "gatherer__energy",
                when(energy > 0.0, energy).otherwise(0.0),
            )

        # Calculate desired harvests
        desired = []
        for row in rows:
            rate = row["gatherer__harvest_rate"]
            want = rate * pool.amount
            desired.append(want)

        total_desired = sum(desired)
        available = pool.amount

        # If total demand exceeds supply, allocate proportionally
        if total_desired > available:
            scale = available / total_desired
        else:
            scale = 1.0

        # Apply harvests
        updated_rows = []
        total_taken = 0.0
        for row, want in zip(rows, desired, strict=True):
            actual = want * scale
            total_taken += actual
            row["gatherer__total_harvested"] = row["gatherer__total_harvested"] + actual
            # Energy: gain from harvest, lose metabolic cost
            row["gatherer__energy"] = row["gatherer__energy"] + actual * 0.5 - 2.0
            row["gatherer__energy"] = max(row["gatherer__energy"], 0.0)
            updated_rows.append(row)

            metrics.tick_harvests.append(
                {
                    "tick": tick,
                    "name": row["gatherer__name"],
                    "strategy": row["gatherer__strategy"],
                    "harvested": round(actual, 2),
                    "energy": round(row["gatherer__energy"], 2),
                }
            )

        # Deduct from pool
        pool.amount = max(pool.amount - total_taken, 0.0)

        # Rebuild DataFrame using the original Arrow schema
        new_arrow = pa.Table.from_pylist(updated_rows, schema=arrow_table.schema)
        return daft.from_arrow(new_arrow)


class RegenerationProcessor(AsyncProcessor):
    """Logistic regrowth of the commons after harvesting."""

    components = (Gatherer,)
    priority = 20

    async def process(self, df: DataFrame, resources: Resources = None, **kwargs) -> DataFrame:
        pool = resources.require(CommonPool)
        pool.regenerate()
        pool.record()
        # This processor only mutates the Resource; DataFrame passes through
        return df


# ---------------------------------------------------------------------------
# Scenario builder
# ---------------------------------------------------------------------------


def make_agents(scenario: str) -> list[tuple[str, str]]:
    """Return (name, strategy) pairs for a scenario."""
    if scenario == "all_cooperative":
        return [(f"Coop-{i + 1}", "cooperative") for i in range(6)]
    elif scenario == "mixed":
        return (
            [(f"Greedy-{i + 1}", "greedy") for i in range(2)]
            + [(f"Moderate-{i + 1}", "moderate") for i in range(2)]
            + [(f"Coop-{i + 1}", "cooperative") for i in range(2)]
        )
    elif scenario == "all_greedy":
        return [(f"Greedy-{i + 1}", "greedy") for i in range(6)]
    else:
        raise ValueError(f"Unknown scenario: {scenario}")


async def run_scenario(scenario: str, num_ticks: int = 50) -> dict:
    """Run a single scenario and return results."""
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    world = AsyncWorld(
        world_config=WorldConfig(name=f"commons-{scenario}"),
        querier=querier,
        updater=updater,
        system=system,
    )

    # Shared resources
    pool = CommonPool(amount=1000.0, max_capacity=1000.0, growth_rate=1.0)
    metrics = SimMetrics()
    world.resources.insert(pool)
    world.resources.insert(metrics)

    # Register processors
    await system.add_processor(HarvestProcessor())
    await system.add_processor(RegenerationProcessor())

    # Spawn agents
    agents = make_agents(scenario)
    for name, strategy in agents:
        await world.create_entity(
            [Gatherer(name=name, strategy=strategy, harvest_rate=STRATEGY_RATES[strategy])]
        )

    # Run simulation
    await world.run(RunConfig(num_steps=num_ticks))

    # Collect final state
    final_agents = []
    for _sig, df in world._live.items():
        rows = df.collect().to_pylist()
        for row in rows:
            final_agents.append(
                {
                    "name": row["gatherer__name"],
                    "strategy": row["gatherer__strategy"],
                    "total_harvested": round(row["gatherer__total_harvested"], 2),
                    "energy": round(row["gatherer__energy"], 2),
                }
            )

    return {
        "scenario": scenario,
        "pool_history": pool.history,
        "final_pool": round(pool.amount, 2),
        "agents": final_agents,
        "metrics": metrics,
    }


# ---------------------------------------------------------------------------
# Visualization (text-based charts)
# ---------------------------------------------------------------------------


def spark_line(values: list[float], max_val: float, width: int = 50) -> str:
    """Render a simple text sparkline."""
    blocks = " _.,:-=!#"
    if max_val == 0:
        return " " * width
    chars = []
    step = max(1, len(values) // width)
    sampled = values[::step][:width]
    for v in sampled:
        idx = int((v / max_val) * (len(blocks) - 1))
        idx = max(0, min(idx, len(blocks) - 1))
        chars.append(blocks[idx])
    return "".join(chars)


def render_bar(value: float, max_val: float, width: int = 30) -> str:
    """Render a horizontal bar."""
    if max_val == 0:
        return " " * width
    filled = int((value / max_val) * width)
    filled = max(0, min(filled, width))
    return "#" * filled + "." * (width - filled)


def print_results(results: list[dict]):
    """Print comparative analysis of all scenarios."""
    print("\n" + "=" * 72)
    print("  TRAGEDY OF THE COMMONS  -  Simulation Results")
    print("=" * 72)

    max_pool = 1000.0

    for res in results:
        scenario = res["scenario"].replace("_", " ").title()
        history = res["pool_history"]
        final = res["final_pool"]
        agents = res["agents"]
        recorded_ticks = len(history)
        first_tick = 1 if recorded_ticks else 0
        first_label = f"tick {first_tick}"
        last_label = f"tick {recorded_ticks}"

        print(f"\n{'─' * 72}")
        print(f"  Scenario: {scenario}")
        print(f"  Final pool: {final:.1f} / {max_pool:.0f}")
        print(f"{'─' * 72}")

        # Pool depletion curve
        print(f"\n  Pool over time ({recorded_ticks} recorded ticks):")
        print(f"  1000 |{spark_line(history, max_pool, 50)}|")
        print(f"     0 |{'_' * 50}|")
        print(f"        {first_label:<25}{last_label:>25}")

        # Per-agent summary
        print(f"\n  {'Agent':<14} {'Strategy':<13} {'Harvested':>10} {'Energy':>8}  Bar")
        print(f"  {'─' * 14} {'─' * 13} {'─' * 10} {'─' * 8}  {'─' * 30}")

        max_harvest = max((a["total_harvested"] for a in agents), default=1)
        for a in sorted(agents, key=lambda x: -x["total_harvested"]):
            bar = render_bar(a["total_harvested"], max_harvest)
            starving = " [STARVING]" if a["energy"] <= 0.01 else ""
            print(
                f"  {a['name']:<14} {a['strategy']:<13} {a['total_harvested']:>10.1f} "
                f"{a['energy']:>8.1f}  {bar}{starving}"
            )

        # Sustainability verdict
        if final > 500:
            verdict = "SUSTAINABLE - Pool is healthy"
        elif final > 100:
            verdict = "STRESSED - Pool is degraded but surviving"
        elif final > 1:
            verdict = "CRITICAL - Pool near collapse"
        else:
            verdict = "COLLAPSED - Commons destroyed"
        print(f"\n  Verdict: {verdict}")

    # Cross-scenario comparison
    print(f"\n{'=' * 72}")
    print("  CROSS-SCENARIO COMPARISON")
    print(f"{'=' * 72}")
    print(f"\n  {'Scenario':<20} {'Final Pool':>11} {'Avg Harvest':>12} {'Avg Energy':>11}")
    print(f"  {'─' * 20} {'─' * 11} {'─' * 12} {'─' * 11}")

    for res in results:
        scenario = res["scenario"].replace("_", " ").title()
        final = res["final_pool"]
        agents = res["agents"]
        avg_h = sum(a["total_harvested"] for a in agents) / len(agents) if agents else 0
        avg_e = sum(a["energy"] for a in agents) / len(agents) if agents else 0
        print(f"  {scenario:<20} {final:>11.1f} {avg_h:>12.1f} {avg_e:>11.1f}")

    # The key insight
    print(f"\n{'─' * 72}")
    print("  KEY INSIGHTS:")
    coop = next(r for r in results if r["scenario"] == "all_cooperative")
    greedy = next(r for r in results if r["scenario"] == "all_greedy")
    mixed = next(r for r in results if r["scenario"] == "mixed")
    coop_total = sum(a["total_harvested"] for a in coop["agents"])
    greedy_total = sum(a["total_harvested"] for a in greedy["agents"])
    mixed_total = sum(a["total_harvested"] for a in mixed["agents"])
    print(f"\n  Total harvest over {len(coop['pool_history'])} ticks:")
    print(f"    All cooperative: {coop_total:>10.1f}  (pool healthy at {coop['final_pool']:.0f})")
    print(
        f"    Mixed society:   {mixed_total:>10.1f}  (pool stressed at {mixed['final_pool']:.0f})"
    )
    print(f"    All greedy:      {greedy_total:>10.1f}  (pool collapsed)")

    if coop_total > greedy_total:
        ratio = coop_total / greedy_total if greedy_total > 0 else float("inf")
        print(f"\n  1. Cooperation yields {ratio:.1f}x more than universal greed.")

    if mixed_total > coop_total:
        print(f"  2. BUT the mixed society harvests the most overall ({mixed_total:.0f})!")
        print("     This is the free-rider paradox: greedy agents exploit a pool")
        print("     kept alive by cooperators, extracting more individually.")

        # Show the per-agent inequity in mixed
        mixed_greedy = [a for a in mixed["agents"] if a["strategy"] == "greedy"]
        mixed_coop = [a for a in mixed["agents"] if a["strategy"] == "cooperative"]
        coop_in_coop = coop["agents"][0]["total_harvested"]
        if mixed_greedy and mixed_coop:
            g_avg = mixed_greedy[0]["total_harvested"]
            c_avg = mixed_coop[0]["total_harvested"]
            print(
                f"  3. Inequity: greedy agents harvest {g_avg:.0f} each vs {c_avg:.0f} for cooperators."
            )
            print(
                f"     Cooperators earn LESS ({c_avg:.0f}) than in an all-cooperative world ({coop_in_coop:.0f})."
            )
            print(f"     Greedy agents free-ride: {g_avg / c_avg:.1f}x the harvest of cooperators.")

    print(f"{'─' * 72}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run_experiment(num_ticks: int = 50) -> list[dict]:
    """Run all three scenarios and return results list."""
    scenarios = ["all_cooperative", "mixed", "all_greedy"]
    results = []
    for scenario in scenarios:
        result = await run_scenario(scenario, num_ticks=num_ticks)
        results.append(result)
    return results


async def main():
    print("Tragedy of the Commons - Multi-Agent Resource Simulation")
    print("Using Archetype ECS with in-memory storage\n")
    print("Running 3 scenarios (50 ticks each)...")

    results = await run_experiment(num_ticks=50)
    print_results(results)


if __name__ == "__main__":
    asyncio.run(main())
