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
from typing import cast

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component, StorageConfig


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
    rows = df.where(col("tick").is_in([tick])).select("entity_id", "position__x").to_pylist()
    return {row["entity_id"]: row["position__x"] for row in rows}


async def run_demo(storage_uri: str) -> dict[str, object]:
    """Prove history, fork divergence, cold discovery, and writable resume."""
    storage = StorageConfig(uri=storage_uri, namespace="time_travel")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("time-travel-demo", storage=storage, processors=[MovementProcessor()])

        # Spawn 3 entities moving at different speeds and run 5 ticks
        walker = await world.spawn(Position(x=0.0), Velocity(vx=1.0))
        runner = await world.spawn(Position(x=0.0), Velocity(vx=2.0))
        sprinter = await world.spawn(Position(x=0.0), Velocity(vx=3.0))
        await world.run(steps=5)

        info = await world.info()
        latest = info.tick - 1  # rows exist for ticks 0..tick-1

        # ── 1. TIME TRAVEL ────────────────────────────────────────────────────
        # query() returns the FULL history. Rewind by filtering the tick column.

        history = await world.query(Position)
        snapshots = {
            tick: [
                positions_at(history, tick)[entity_id] for entity_id in (walker, runner, sprinter)
            ]
            for tick in (0, 2, latest)
        }

        # ── 2. FORK AND DIFF ──────────────────────────────────────────────────
        # The fork inherits the source's store, continues from its state, and
        # reads pre-fork ticks through lineage. Diverge it: what if the walker
        # had sped up at the fork point?

        fork = await world.fork("counterfactual")
        await fork.update(walker, Velocity(vx=10.0))

        await world.run(steps=3)
        await fork.run(steps=3)

        cmp_tick = (await world.info()).tick - 1
        source_state = positions_at(await world.query(Position), cmp_tick)
        fork_history = await fork.query(Position)
        fork_state = positions_at(fork_history, cmp_tick)

        # The fork still sees its pre-fork history through lineage
        pre_fork = positions_at(fork_history, 0)
        world_id = str((await world.info()).world_id)
        fork_world_id = str((await fork.info()).world_id)
        resume_tick = (await world.info()).tick

    # A fresh runtime proves this is durable discovery/resume, not a live alias.
    async with ArchetypeRuntime() as runtime:
        discovered = {str(item.world_id) for item in await runtime.discover(storage)}
        resumed = await runtime.resume(world_id, storage=storage)
        resumed_info = await resumed.info()
        await resumed.add_processor(MovementProcessor())
        resumed_result = await resumed.run(steps=1)
        continued = positions_at(await resumed.query(Position), resume_tick)

    return {
        "world_ids": {"source": world_id, "fork": fork_world_id},
        "history": {str(tick): values for tick, values in snapshots.items()},
        "comparison": {
            "tick": cmp_tick,
            "source_walker": source_state[walker],
            "fork_walker": fork_state[walker],
            "difference": fork_state[walker] - source_state[walker],
        },
        "inherited_tick_zero": pre_fork[walker],
        "cold_resume": {
            "discovered": world_id in discovered,
            "resume_tick": resumed_info.tick,
            "continued_tick": resumed_result.final_tick,
            "continued_walker": continued[walker],
        },
    }


async def main() -> None:
    result = await run_demo("./archetype_data")
    print("1. TIME TRAVEL")
    history = cast(dict[str, list[float]], result["history"])
    for tick, positions in history.items():
        print(
            f"   tick {tick}: walker={positions[0]:.1f}, runner={positions[1]:.1f}, sprinter={positions[2]:.1f}"
        )
    comparison = cast(dict[str, float | int], result["comparison"])
    print("\n2. FORK AND DIFF")
    print(
        f"   walker.x at tick {comparison['tick']}: source={comparison['source_walker']:.1f}, "
        f"fork={comparison['fork_walker']:.1f}, diff={comparison['difference']:+.1f}"
    )
    print(f"   cold resume: {result['cold_resume']}")


if __name__ == "__main__":
    asyncio.run(main())
