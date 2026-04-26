# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Mutations
================

Demonstrates the runtime mutation surface: spawn, despawn, update,
add/remove components, add/remove processors, fork, actor-bound handles,
and audit history.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/01_world_mutations.py
"""

import asyncio

from daft import DataFrame, col
from uuid_utils import uuid7

from archetype import ArchetypeRuntime
from archetype.app.auth.models import ActorCtx
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

        # Activate the world with admin (default actor) before creating aliases
        await world.step()

        viewer = world.as_actor(ActorCtx(id=uuid7(), roles={"viewer"}))
        player = world.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
        admin = world  # default actor is admin

        # ── 1. SPAWN + RBAC ──────────────────────────────────────────────────

        print("1. SPAWN + RBAC")
        scout = await player.spawn(Position(x=0.0, y=0.0))
        dummy = await player.spawn(Position(x=10.0, y=10.0))
        try:
            await viewer.spawn(Position(x=99.0, y=99.0))
            print("   viewer: SPAWN allowed (unexpected)")
        except PermissionError:
            print("   viewer: SPAWN denied ✓")
        await admin.step()
        print(f"   player spawned: scout={scout}, dummy={dummy}")

        # ── 2. UPDATE + COMPONENT MUTATIONS ───────────────────────────────────

        print("\n2. UPDATE + COMPONENT MUTATIONS")
        await player.update(scout, Position(x=2.0, y=1.0))
        await admin.step()

        await admin.add_components(scout, Velocity(vx=1.5, vy=0.5), Health(hp=80, max_hp=100))
        await admin.step()

        # Query returns full history — filter to latest tick for "current state"
        info = await admin.info()
        df = await admin.query(Position, Velocity, Health)
        latest = df.where(col("tick") == info.tick - 1).where(col("entity_id") == scout)
        print("   scout after update + add_components:")
        latest.show()

        await admin.remove_components(scout, Health)
        await player.despawn(dummy)
        await admin.step()

        print("   scout after remove_components (Health removed):")
        df2 = await admin.query(Position, Velocity)
        info2 = await admin.info()
        df2.where(col("tick") == info2.tick - 1).where(col("entity_id") == scout).show()

        # ── 3. PROCESSOR MUTATIONS ────────────────────────────────────────────

        print("\n3. PROCESSOR MUTATIONS")
        try:
            await player.add_processor(MovementProcessor())
            print("   player: ADD_PROCESSOR allowed (unexpected)")
        except PermissionError:
            print("   player: ADD_PROCESSOR denied ✓")

        await admin.add_processor(MovementProcessor())
        await admin.step()

        print("   scout after MovementProcessor:")
        info3 = await admin.info()
        df3 = await admin.query(Position, Velocity)
        df3.where(col("tick") == info3.tick - 1).where(col("entity_id") == scout).show()
        await admin.remove_processor(MovementProcessor)

        # ── 4. FORK ───────────────────────────────────────────────────────────

        print("\n4. FORK")
        branch = await admin.fork("branch-a", storage=storage)
        branch_seed = await branch.spawn(Position(x=-5.0, y=0.0), Velocity(vx=0.5, vy=0.0))
        await branch.step()

        source_info = await admin.info()
        branch_info = await branch.info()
        print(f"   source tick={source_info.tick}, branch tick={branch_info.tick}")
        print(f"   branch has its own entity: {branch_seed}")

        # ── 5. AUDIT HISTORY ──────────────────────────────────────────────────

        print("\n5. AUDIT HISTORY")
        history = await admin.history()
        print(f"   {history.count_rows()} audit rows for source world")
        history.select("command_type", "actor_id").show()


if __name__ == "__main__":
    asyncio.run(main())
