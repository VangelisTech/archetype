# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Time Travel and Fork-and-Diff
==============================

Every tick persists as immutable rows — nothing is overwritten. This
example runs a small simulation, rewinds to any past tick by filtering
the `tick` column, then forks a counterfactual branch, diverges it, and
diffs source vs fork at the same tick.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/03_time_travel.py
"""

import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig


class Position(Component):
    x: float = 0.0


class Velocity(Component):
    vx: float = 0.0


class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns({"position__x": col("position__x") + col("velocity__vx")})


def positions_at(df: DataFrame, tick: int) -> dict[int, float]:
    """Extract {entity_id: x} for one tick from the append-only history."""
    rows = df.where(col("tick") == tick).select("entity_id", "position__x").to_pylist()
    return {row["entity_id"]: row["position__x"] for row in rows}


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="time_travel")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("time-travel-demo", storage=storage, processors=[MovementProcessor()])

        # Spawn 3 entities moving at different speeds and run 5 ticks
        walker = await world.spawn(Position(x=0.0), Velocity(vx=1.0))
        runner = await world.spawn(Position(x=0.0), Velocity(vx=2.0))
        sprinter = await world.spawn(Position(x=0.0), Velocity(vx=3.0))
        await world.run(steps=5)

        info = await world.info()
        latest = info.tick - 1  # rows exist for ticks 0..tick-1
        print(f"Ran {info.tick} ticks: walker={walker}, runner={runner}, sprinter={sprinter}\n")

        # ── 1. TIME TRAVEL ────────────────────────────────────────────────────
        # query() returns the FULL history. Rewind by filtering the tick column.

        print("1. TIME TRAVEL")
        history = await world.query(Position)
        for t in (0, 2, latest):
            state = positions_at(history, t)
            label = "latest" if t == latest else f"tick {t}"
            print(
                f"   {label:>7}: walker.x={state[walker]:5.1f}  "
                f"runner.x={state[runner]:5.1f}  sprinter.x={state[sprinter]:5.1f}"
            )

        # ── 2. FORK AND DIFF ──────────────────────────────────────────────────
        # The fork inherits the source's store, continues from its state, and
        # reads pre-fork ticks through lineage. Diverge it: what if the walker
        # had sped up at the fork point?

        print("\n2. FORK AND DIFF")
        fork = await world.fork("counterfactual")
        await fork.update(walker, Velocity(vx=10.0))

        await world.run(steps=3)
        await fork.run(steps=3)

        cmp_tick = (await world.info()).tick - 1
        source_state = positions_at(await world.query(Position), cmp_tick)
        fork_history = await fork.query(Position)
        fork_state = positions_at(fork_history, cmp_tick)

        print(f"   walker.x at tick {cmp_tick}:")
        print(f"     source (vx=1.0):  {source_state[walker]:5.1f}")
        print(f"     fork   (vx=10.0): {fork_state[walker]:5.1f}")
        print(f"     diff:             {fork_state[walker] - source_state[walker]:+5.1f}")

        # The fork still sees its pre-fork history through lineage
        pre_fork = positions_at(fork_history, 0)
        print(f"   fork at pre-fork tick 0: walker.x={pre_fork[walker]:.1f} (inherited history)")


if __name__ == "__main__":
    asyncio.run(main())
