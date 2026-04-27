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

from daft import DataFrame, col, lit

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig

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


async def collect_agent_snapshot(world) -> DataFrame:
    info = await world.info()
    return (await world.query(Agent)).with_column("history_tick", lit(info.tick))


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

        history_frames: list[DataFrame] = []
        for _ in range(10):
            await world.step()
            history_frames.append(await collect_agent_snapshot(world))

        info = await world.info()
        print(f"Ran {len(history_frames)} ticks (final tick={info.tick})\n")

        print("Final state:")
        rows = sorted(
            (await world.query(Agent)).collect().to_pylist(),
            key=lambda row: row["entity_id"],
        )
        for row in rows:
            name = row.get("agent__name", "?")
            exp = row.get("agent__experience", 0)
            rating = row.get("agent__rating", 0)
            skill = row.get("agent__skill", 0)
            print(f"  {name}: skill={skill:.1f}, experience={exp:.0f}, rating={rating:.1f}")

        history_df = history_frames[0]
        for frame in history_frames[1:]:
            history_df = history_df.concat(frame)

        print("\nState history (DataFrame):")
        history_row_count = len(history_frames) * len(rows)
        history_df.select(
            "history_tick",
            "entity_id",
            "agent__name",
            "agent__skill",
            "agent__experience",
            "agent__rating",
        ).collect().sort([col("history_tick"), col("entity_id")]).show(history_row_count)


if __name__ == "__main__":
    asyncio.run(main())
