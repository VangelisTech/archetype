# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Time-Travel Queries
====================

Run a simulation for 10 ticks, then query the state at different
points in history. Every tick is preserved — nothing is overwritten.

This example uses the ArchetypeRuntime high-level API with an escape
hatch to the lower-level query service for historical tick reads.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/03_time_travel.py
"""

import asyncio

from archetype import ArchetypeRuntime
from archetype.core.component import Component
from archetype.core.config import StorageConfig


class Marker(Component):
    """Empty component used to track entity count across ticks."""

    label: str = ""


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="time_travel")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("time-travel-demo", storage=storage)

        # Spawn 3 entities and run 5 ticks
        for i in range(3):
            await world.spawn(Marker(label=f"wave-1-{i}"))
        await world.run(steps=5)

        info = await world.info()
        print(f"After 5 ticks: tick={info.tick}, 3 entities spawned")

        # Spawn 2 more entities and run another 5 ticks
        for i in range(2):
            await world.spawn(Marker(label=f"wave-2-{i}"))
        await world.run(steps=5)

        info = await world.info()
        print(f"After 10 ticks: tick={info.tick}\n")

        # Query current state (all 5 entities visible)
        print(f"  Current state (tick {info.tick}):")
        (await world.query(Marker)).show()

        print("\nCommand history:")
        history = await world.history()
        history.select("command_type", "actor_id").show()


if __name__ == "__main__":
    asyncio.run(main())
