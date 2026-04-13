# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Building a Simulation
======================

Full workflow: define components, write processors, create a world,
spawn entities, run it. Agents accumulate experience each tick.
A scorer processor computes a rating from experience and skill.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/simulation_script.py
"""

import asyncio

from daft import DataFrame, col

from archetype.app.container import ServiceContainer
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


# ── Step 1: Define components ───────────────────────────────────────────────


class Agent(Component):
    name: str = ""
    role: str = ""
    skill: float = 1.0
    experience: float = 0.0
    rating: float = 0.0


# ── Step 2: Write processors ───────────────────────────────────────────────


class ExperienceProcessor(AsyncProcessor):
    """Each tick, agents gain experience proportional to their skill."""

    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__experience",
            col("agent__experience") + col("agent__skill") * 2.0,
        )


class RatingProcessor(AsyncProcessor):
    """Compute a rating from experience and skill."""

    components = (Agent,)
    priority = 50

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__rating",
            col("agent__experience") * col("agent__skill") / 10.0,
        )


# ── Step 3-5: Create world, spawn entities, run ────────────────────────────


async def main():
    container = ServiceContainer()

    world = await container.world_service.create_world(
        WorldConfig(name="skill-sim"), StorageConfig(),
    )

    # Register processors
    await world.system.add_processor(ExperienceProcessor())
    await world.system.add_processor(RatingProcessor())
    print("Processors: Experience (priority=10), Rating (priority=50)")

    # Spawn agents with different starting skills
    await world.create_entity([Agent(name="Alice", role="engineer", skill=3.0)])
    await world.create_entity([Agent(name="Bob", role="designer", skill=2.0)])
    await world.create_entity([Agent(name="Charlie", role="manager", skill=1.5)])
    print("Spawned: Alice (skill=3.0), Bob (skill=2.0), Charlie (skill=1.5)\n")

    # Run 10 ticks
    await world.run(RunConfig(num_steps=10))
    print(f"Ran 10 ticks (final tick={world.tick})\n")

    # Print final state
    print("Final state:")
    for _sig, df in world._live.items():
        rows = df.collect().to_pylist()
        for row in rows:
            name = row.get("agent__name", "?")
            exp = row.get("agent__experience", 0)
            rating = row.get("agent__rating", 0)
            skill = row.get("agent__skill", 0)
            print(f"  {name}: skill={skill:.1f}, experience={exp:.0f}, rating={rating:.1f}")

    await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
