# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Fork a Counterfactual: The Butterfly Effect
============================================

Every tick persists as immutable rows, so a counterfactual is a fork plus a
join — not a re-run. One logistic-map processor (a pure column expression)
drives three entities in different dynamical regimes:

    fixed-point  r=2.9     perturbations decay (~0.9x per tick)
    period-2     r=3.2     perturbations are erased (0.16x per cycle)
    chaos        r=3.9999  perturbations double every tick

We run the prime world, fork it, nudge every entity by 1e-9 in the fork, run
both branches forward, and diff the two histories tick-by-tick with a single
join over the shared append-only ledger.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/02_fork_counterfactual.py
"""

import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component, StorageConfig

REGIMES = {"fixed-point": 2.9, "period-2": 3.2, "chaos": 3.9999}
NUDGE = 1e-9
PRE_TICKS = 12
POST_TICKS = 36
CHECKPOINTS = (0, 6, 12, 18, 24, 30, 36)


class Node(Component):
    r: float = 0.0
    x: float = 0.5


class LogisticMap(AsyncProcessor):
    components = (Node,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        x = col("node__x")
        return df.with_column("node__x", col("node__r") * x * (1.0 - x))


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="counterfactuals")

    async with ArchetypeRuntime() as runtime:
        # ── 1. PRIME TIMELINE ─────────────────────────────────────────────────
        prime = runtime.world("prime", storage=storage, processors=[LogisticMap()])
        eids = {name: await prime.spawn(Node(r=r)) for name, r in REGIMES.items()}
        await prime.step()  # persist tick 0: raw initial conditions
        await prime.run(steps=PRE_TICKS)

        fork_tick = (await prime.info()).tick  # the fork continues from here
        last = fork_tick - 1  # latest persisted tick
        at_fork = {
            row["entity_id"]: row["node__x"]
            for row in (await prime.query(Node))
            .where(col("tick") == last)
            .select("entity_id", "node__x")
            .to_pylist()
        }
        print(f"1. PRIME TIMELINE: {fork_tick} ticks persisted; forking at tick {last}")

        # ── 2. FORK AND NUDGE ─────────────────────────────────────────────────
        # An update overlays raw given-state at the fork's next persisted tick;
        # dynamics resume one tick later. So fork tick t+1 corresponds to prime
        # tick t, and k=0 below compares x against exactly x + NUDGE.
        fork = await prime.fork("nudged")
        for name, r in REGIMES.items():
            await fork.update(eids[name], Node(r=r, x=at_fork[eids[name]] + NUDGE))
        print(f"2. FORK AND NUDGE: every entity perturbed by {NUDGE:g} in the fork")

        # ── 3. RESUME BOTH BRANCHES ───────────────────────────────────────────
        await prime.run(steps=POST_TICKS)
        await fork.run(steps=POST_TICKS + 1)

        # ── 4. DIFF THE HISTORIES ─────────────────────────────────────────────
        # Both branches are immutable rows keyed by tick — the counterfactual
        # comparison is one join, aligned on tick offset k from the fork point.
        prime_run = (
            (await prime.query(Node))
            .where(col("tick") >= last)
            .select(
                (col("tick") - last).alias("k"),
                col("node__r").alias("r"),
                col("node__x").alias("x_prime"),
            )
        )
        fork_hist = await fork.query(Node)
        fork_run = fork_hist.where(col("tick") >= fork_tick).select(
            (col("tick") - fork_tick).alias("k"),
            col("node__r").alias("r"),
            col("node__x").alias("x_fork"),
        )
        diverged = (
            prime_run.join(fork_run, on=["k", "r"])
            .with_column("delta", (col("x_prime") - col("x_fork")).abs())
            .where(col("k").is_in(list(CHECKPOINTS)))
            .sort(["r", "k"])
        )
        by_regime: dict[float, dict[int, float]] = {}
        for row in diverged.to_pylist():
            by_regime.setdefault(row["r"], {})[row["k"]] = row["delta"]

        print("3. DIVERGENCE |x_prime - x_fork| by tick offset k from the fork point:")
        print(f"{'regime':>14} {'r':>7} |" + "".join(f"{f'k={k}':>10}" for k in CHECKPOINTS))
        for name, r in sorted(REGIMES.items(), key=lambda kv: kv[1]):
            cells = "".join(f"{by_regime[r][k]:>10.1e}" for k in CHECKPOINTS)
            print(f"{name:>14} {r:>7.4f} |{cells}")

        amplified = by_regime[REGIMES["chaos"]][POST_TICKS] / NUDGE
        inherited = fork_hist.where(col("tick") < fork_tick).count_rows()
        print(f"\nchaos amplified the nudge {amplified:.2g}x in {POST_TICKS} ticks;")
        print(
            f"the fork read {inherited} pre-fork rows through lineage — one shared "
            "append-only past, never copied, never overwritten."
        )


if __name__ == "__main__":
    asyncio.run(main())
