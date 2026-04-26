# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Mutations
================

Demonstrates the brokered runtime mutation surface: spawn, despawn, update,
add/remove components, add/remove processors, fork, actor-bound handles, and
broker audit history.

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

        print("1. SPAWN + RBAC")
        scout = await player.spawn(Position(x=0.0, y=0.0))
        dummy = await player.spawn(Position(x=10.0, y=10.0))
        try:
            await viewer.spawn(Position(x=99.0, y=99.0))
            print("   viewer: SPAWN allowed (unexpected)")
        except PermissionError:
            print("   viewer: SPAWN denied (correct)")
        await admin.step()
        info = await world.info()
        print(f"   Created world: {info.world_id}")
        print(f"   player: spawned scout={scout}, dummy={dummy}")

        print("\n2. UPDATE + COMPONENT MUTATIONS")
        await player.update(scout, Position(x=2.0, y=1.0))
        await admin.step()
        await admin.add_components(
            scout,
            Velocity(vx=1.5, vy=0.5),
            Health(hp=80, max_hp=100),
        )
        await admin.step()

        enriched = (await admin.query(Position, Velocity, Health, entity_ids=[scout])).collect().to_pylist()[0]
        print(
            "   scout after update/add_components:"
            f" pos=({enriched['position__x']}, {enriched['position__y']})"
            f", vel=({enriched['velocity__vx']}, {enriched['velocity__vy']})"
            f", hp={enriched['health__hp']}"
        )

        await admin.remove_components(scout, Health)
        await player.despawn(dummy)
        await admin.step()

        narrowed = (await admin.query(Position, Velocity, entity_ids=[scout])).collect().to_pylist()[0]
        removed = (await admin.query(Position, entity_ids=[dummy])).collect().to_pylist()
        print(
            "   scout after remove_components:"
            f" pos=({narrowed['position__x']}, {narrowed['position__y']})"
            f", vel=({narrowed['velocity__vx']}, {narrowed['velocity__vy']})"
        )
        print(f"   dummy present after despawn: {bool(removed)}")

        print("\n3. PROCESSOR MUTATIONS")
        try:
            await player.add_processor(MovementProcessor())
            print("   player: ADD_PROCESSOR allowed (unexpected)")
        except PermissionError:
            print("   player: ADD_PROCESSOR denied (correct)")

        await admin.add_processor(MovementProcessor())
        await admin.step()
        moved = (await admin.query(Position, Velocity, entity_ids=[scout])).collect().to_pylist()[0]
        print(
            "   scout after MovementProcessor:"
            f" pos=({moved['position__x']}, {moved['position__y']})"
        )
        await admin.remove_processor(MovementProcessor)

        print("\n4. FORK")
        branch = await admin.fork("branch-a", storage=storage)
        branch_seed = await branch.spawn(Position(x=-5.0, y=0.0), Velocity(vx=0.5, vy=0.0))
        await branch.step()

        source_state = (await admin.query(Position, Velocity, entity_ids=[scout])).collect().to_pylist()[0]
        branch_state = (await branch.query(Position, Velocity, entity_ids=[branch_seed])).collect().to_pylist()[0]
        source_info = await admin.info()
        branch_info = await branch.info()
        print(f"   source tick={source_info.tick}, branch tick={branch_info.tick}")
        print(
            "   source scout:"
            f" pos=({source_state['position__x']}, {source_state['position__y']})"
        )
        print(
            "   branch new entity:"
            f" pos=({branch_state['position__x']}, {branch_state['position__y']})"
        )

        print("\n5. COMMAND HISTORY")
        source_history = await admin.history()
        source_rows = source_history.collect().to_pylist()
        for row in source_rows:
            print(f"   source: {row}")
        branch_history = await branch.history()
        branch_rows = branch_history.collect().to_pylist()
        for row in branch_rows:
            print(f"   branch: {row}")


if __name__ == "__main__":
    asyncio.run(main())
