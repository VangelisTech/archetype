# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Mutations
================

Demonstrates the trusted actor-free runtime mutation surface: spawn, despawn,
update, add/remove components, add/remove processors, fork, and history reads.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/01_world_mutations.py

The script's own narration is all you see by default. To watch the
machinery underneath (gated ops, world steps, store writes), set:
    ARCHETYPE_LOG=debug uv run python examples/01_world_mutations.py
"""

import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    hp: int = 100
    max_hp: int = 100


class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx"),
                "position__y": col("position__y") + col("velocity__vy"),
            }
        )


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="world_mutations")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("mutations-demo", storage=storage)

        # Runtime scripts are trusted and call the actor-free application facade.
        await world.step()

        # ── 1. SPAWN ─────────────────────────────────────────────────────────

        print("1. SPAWN")
        scout = await world.spawn(Position(x=0.0, y=0.0))
        dummy = await world.spawn(Position(x=10.0, y=10.0))
        await world.step()
        print(f"   runtime spawned: scout={scout}, dummy={dummy}")

        # ── 2. UPDATE + COMPONENT MUTATIONS ───────────────────────────────────

        print("\n2. UPDATE + COMPONENT MUTATIONS")
        await world.update(scout, Position(x=2.0, y=1.0))
        await world.step()

        await world.add_components(scout, Velocity(vx=1.5, vy=0.5), Health(hp=80, max_hp=100))
        await world.step()

        # Query returns full history — filter to latest tick for "current state"
        info = await world.info()
        df = await world.query(Position, Velocity, Health)
        latest = df.where(col("tick") == info.tick - 1).where(col("entity_id") == scout)
        print("   scout after update + add_components:")
        latest.show()

        await world.remove_components(scout, Health)
        await world.despawn(dummy)
        await world.step()

        print("   scout after remove_components (Health removed):")
        df2 = await world.query(Position, Velocity)
        info2 = await world.info()
        df2.where(col("tick") == info2.tick - 1).where(col("entity_id") == scout).show()

        # ── 3. PROCESSOR MUTATIONS ────────────────────────────────────────────

        print("\n3. PROCESSOR MUTATIONS")
        await world.add_processor(MovementProcessor())
        await world.step()
        print("   MovementProcessor installed")

        print("   scout after MovementProcessor:")
        info3 = await world.info()
        df3 = await world.query(Position, Velocity)
        df3.where(col("tick") == info3.tick - 1).where(col("entity_id") == scout).show()
        await world.remove_processor(MovementProcessor)
        print("   MovementProcessor removed")

        # ── 4. FORK ───────────────────────────────────────────────────────────

        print("\n4. FORK")
        branch = await world.fork("branch-a", storage=storage)
        branch_seed = await branch.spawn(Position(x=-5.0, y=0.0), Velocity(vx=0.5, vy=0.0))
        await branch.step()

        source_info = await world.info()
        branch_info = await branch.info()
        print(f"   source tick={source_info.tick}, branch tick={branch_info.tick}")
        print(f"   branch has its own entity: {branch_seed}")

        # ── 5. HISTORY ────────────────────────────────────────────────────────

        print("\n5. HISTORY")
        history = await world.history()
        print(f"   {history.count_rows()} projected audit rows for source world")
        print("   trusted runtime calls do not fabricate authorization events")


if __name__ == "__main__":
    asyncio.run(main())
