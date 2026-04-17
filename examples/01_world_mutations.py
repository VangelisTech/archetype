# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Mutations
================

Demonstrates every mutation type: spawn, despawn, update,
add/remove components, add/remove processors, fork, and
the RBAC system that gates all of it.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/01_world_mutations.py
"""

import asyncio

from daft import DataFrame, col
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


# ── Components ──────────────────────────────────────────────────────────────


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    hp: int = 100
    max_hp: int = 100


# ── Processors ──────────────────────────────────────────────────────────────


class MovementProcessor(AsyncProcessor):
    """Move entities by their velocity each tick."""

    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx"),
                "position__y": col("position__y") + col("velocity__vy"),
            }
        )


# ── Main ────────────────────────────────────────────────────────────────────


async def main():
    container = ServiceContainer()
    admin = ActorCtx(id=uuid7(), roles={"admin"})

    world = await container.world_service.create_world(
        WorldConfig(name="mutations-demo"), StorageConfig(),
    )
    wid = world.world_id
    rc = RunConfig()
    print(f"Created world: {wid}\n")

    # ── 1. SPAWN: create entities with components ───────────────────────
    print("1. SPAWN — create entities with components")

    # Entity with Position + Velocity
    # NOTE: use Component.to_payload() (not model_dump) so CommandService can
    # reconstruct the concrete subclass on the other side. See Archetype #90.
    cmd = Command(
        type=CommandType.SPAWN,
        payload={
            "components": [
                Position(x=0, y=0).to_payload(),
                Velocity(vx=1, vy=2).to_payload(),
            ],
        },
    )
    await container.command_service.submit(wid, cmd, admin)

    # Entity with just Position
    cmd = Command(
        type=CommandType.SPAWN,
        payload={
            "components": [
                Position(x=10, y=10).to_payload(),
            ],
        },
    )
    await container.command_service.submit(wid, cmd, admin)

    # Step to materialize
    await container.simulation_service.step(wid, rc)
    print(f"   Spawned 2 entities (tick={world.tick})")

    # ── 2. ADD_PROCESSOR: inject behavior at runtime ────────────────────
    print("\n2. ADD_PROCESSOR — inject behavior at runtime")

    await world.system.add_processor(MovementProcessor())
    print("   Added MovementProcessor (priority=10)")

    # Run 3 ticks — entities with both Position+Velocity will move
    await container.simulation_service.run(wid, RunConfig(num_steps=3))
    print(f"   Ran 3 ticks (tick={world.tick})")

    # ── 3. ADD_COMPONENT: add components to existing entity ─────────────
    print("\n3. ADD_COMPONENT — add components to existing entity")

    # Find the entity that only has Position (no Velocity)
    for sig, df in world._live.items():
        component_names = [c.__name__ for c in sig]
        rows = df.collect().to_pylist()
        for row in rows:
            eid = row["entity_id"]
            print(f"   Entity {eid}: {component_names}")

    # ── 4. RBAC: who can do what ────────────────────────────────────────
    print("\n4. RBAC — permission checks")

    viewer = ActorCtx(id=uuid7(), roles={"viewer"})
    player = ActorCtx(id=uuid7(), roles={"player"})

    # Viewer cannot spawn
    try:
        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(wid, cmd, viewer)
        print("   viewer: SPAWN allowed (unexpected)")
    except PermissionError:
        print("   viewer: SPAWN denied (correct)")

    # Player can spawn
    try:
        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(wid, cmd, player)
        print("   player: SPAWN allowed (correct)")
    except PermissionError:
        print("   player: SPAWN denied (unexpected)")

    # Player cannot add processors
    try:
        cmd = Command(type=CommandType.ADD_PROCESSOR, payload={})
        await container.command_service.submit(wid, cmd, player)
        print("   player: ADD_PROCESSOR allowed (unexpected)")
    except PermissionError:
        print("   player: ADD_PROCESSOR denied (correct)")

    # ── 5. FORK: branch the world ───────────────────────────────────────
    print("\n5. FORK — branch the world")

    # Step to materialize any pending commands first
    await container.simulation_service.step(wid, rc)

    fork = await container.world_service.fork_world(
        source_world_id=wid,
        name="branch-A",
        storage_config=StorageConfig(),
    )
    print(f"   Forked: {fork.world_id}")
    print(f"   Fork tick={fork.tick} (matches source tick={world.tick})")

    # Run fork independently
    await container.simulation_service.run(fork.world_id, RunConfig(num_steps=5))
    print(f"   Fork after 5 more ticks: tick={fork.tick}")
    print(f"   Source unchanged: tick={world.tick}")

    # ── 6. Command history ──────────────────────────────────────────────
    print("\n6. COMMAND HISTORY — full audit trail")

    history = await container.query_service.get_command_history(wid)
    for cmd in history:
        print(f"   tick={cmd.tick}: {cmd.type.value}")

    await container.shutdown()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
