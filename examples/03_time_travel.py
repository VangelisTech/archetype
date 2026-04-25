# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Time-Travel Queries
====================

Run a simulation for 10 ticks, then query the state at different
points in history. Every tick is preserved — nothing is overwritten.

This example intentionally uses the lower-level query service because
historical snapshot reads live on the read path rather than the current
runtime sugar.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/03_time_travel.py
"""

import asyncio

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    world = await container.world_service.create_world(
        WorldConfig(name="time-travel-demo"), StorageConfig(),
    )
    wid = world.world_id

    async def spawn_empty(count: int):
        for _ in range(count):
            await container.command_service.submit(
                wid,
                Command(type=CommandType.SPAWN, payload={"components": []}),
                ctx,
            )

    await spawn_empty(3)
    await container.simulation_service.run(wid, RunConfig(num_steps=5))
    print(f"After 5 ticks: {world.tick} tick, entities spawned")

    await spawn_empty(2)
    await container.simulation_service.run(wid, RunConfig(num_steps=5))
    print(f"After 10 ticks: {world.tick} tick\n")

    for tick in [1, 5, 10]:
        state = await container.query_service.get_world_state(wid, tick=tick)
        print(f"  tick {tick:>2}: {len(state.entities)} entities, archetype_counts={state.archetype_counts}")

    print("\nCommand history:")
    history = await container.query_service.get_command_history(wid)
    for cmd in history:
        print(f"  tick={cmd.tick}: {cmd.type.value}")

    await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
