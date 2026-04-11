# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Fork for Counterfactuals
=========================

Fork a world three times with different physics parameters,
run each fork, and compare the results.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/fork_counterfactual.py
"""

import asyncio
from dataclasses import dataclass

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@dataclass
class PhysicsConfig:
    gravity: float = 9.8
    drag: float = 0.1


async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    # Create the base world
    base = await container.world_service.create_world(
        WorldConfig(name="base"), StorageConfig(),
    )
    wid = base.world_id

    # Spawn an entity so there's state to fork
    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(wid, cmd, ctx)

    # Step once to materialize the entity
    await container.simulation_service.run(wid, RunConfig(num_steps=1))

    print(f"Base world: {wid}")
    print(f"Base state: tick={base.tick}\n")

    # Fork with different gravity values and run each
    results = {}
    for gravity in [1.0, 9.8, 25.0]:
        fork = await container.world_service.fork_world(
            source_world_id=wid,
            name=f"gravity-{gravity}",
            storage_config=StorageConfig(),
        )
        fork.resources.insert(PhysicsConfig(gravity=gravity))

        await container.simulation_service.run(
            fork.world_id, RunConfig(num_steps=10),
        )

        state = await container.query_service.get_world_state(fork.world_id)
        results[gravity] = {
            "world_id": str(fork.world_id),
            "final_tick": state.tick,
            "entity_count": len(state.entities),
        }
        print(f"gravity={gravity:>5.1f}: tick={state.tick}, entities={len(state.entities)}")

    print(f"\nRan {len(results)} counterfactual branches from the same base state.")
    await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
