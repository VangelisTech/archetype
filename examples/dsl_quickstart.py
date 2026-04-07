# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
DSL Quickstart
===============

The simplest possible archetype simulation.
Three agents gain experience each tick. A second processor computes ratings.

Usage:
    uv run python examples/dsl_quickstart.py
"""

import asyncio

from daft import DataFrame, col

from archetype.core.component import Component
from archetype.dsl import World, behavior


# ── Components ──────────────────────────────────────────────────────────────


class Agent(Component):
    name: str = ""
    skill: float = 1.0
    experience: float = 0.0
    rating: float = 0.0


# ── Behaviors ───────────────────────────────────────────────────────────────


@behavior
class GainExperience:
    requires = [Agent]
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__experience",
            col("agent__experience") + col("agent__skill") * 2.0,
        )


@behavior
class ComputeRating:
    requires = [Agent]
    priority = 50

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__rating",
            col("agent__experience") * col("agent__skill") / 10.0,
        )


# ── Run ─────────────────────────────────────────────────────────────────────


async def main():
    async with World("skill-sim") as world:
        world.add_behavior(GainExperience)
        world.add_behavior(ComputeRating)

        await world.spawn(Agent(name="Alice", skill=3.0))
        await world.spawn(Agent(name="Bob", skill=2.0))
        await world.spawn(Agent(name="Charlie", skill=1.5))

        await world.run(ticks=10)

        print(f"After {world.tick} ticks:\n")
        for agent in world.agents:
            print(f"  {agent.name}: skill={agent.skill}, exp={agent.experience}, rating={agent.rating}")


if __name__ == "__main__":
    asyncio.run(main())
