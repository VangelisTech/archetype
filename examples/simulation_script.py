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

from archetype import ArchetypeRuntime, AsyncProcessor, Component, StorageConfig

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
    storage = StorageConfig(uri="./archetype_data", namespace="simulation_script")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "skill-sim",
            storage=storage,
            processors=[ExperienceProcessor(), RatingProcessor()],
        )
        print("Processors: Experience (priority=10), Rating (priority=50)")

        await world.spawn(Agent(name="Alice", role="engineer", skill=3.0))
        await world.spawn(Agent(name="Bob", role="designer", skill=2.0))
        await world.spawn(Agent(name="Charlie", role="manager", skill=1.5))
        print("Spawned: Alice (skill=3.0), Bob (skill=2.0), Charlie (skill=1.5)\n")

        # The first tick persists the raw spawn values. Processors apply on
        # subsequent ticks, so run ten processor transformations after that.
        await world.step()
        result = await world.run(steps=10)
        history = await world.query(Agent)
        current = history.where(col("tick") == result.final_tick - 1).sort("entity_id")

        print(f"Ran 10 processor ticks (world tick={result.final_tick})\n")

        print("Final state:")
        rows = (
            current.select(
                "agent__name",
                "agent__skill",
                "agent__experience",
                "agent__rating",
            )
            .collect()
            .to_pylist()
        )
        for row in rows:
            print(
                f"{row['agent__name']}: skill={row['agent__skill']:.1f}, "
                f"experience={row['agent__experience']:.0f}, "
                f"rating={row['agent__rating']:.2f}"
            )

        print("\nState history sample (DataFrame):")
        history.select(
            "tick",
            "entity_id",
            "agent__name",
            "agent__skill",
            "agent__experience",
            "agent__rating",
        ).show()


if __name__ == "__main__":
    asyncio.run(main())
