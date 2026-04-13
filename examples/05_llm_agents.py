# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
LLM-Powered Agent Simulation
=============================

Demonstrates using daft.functions.prompt inside an ECS processor
to give entities autonomous reasoning each tick.

Three agents with different personalities are spawned into a world.
Each tick, a ThinkProcessor calls an LLM for every agent in parallel
via Daft's columnar prompt execution. Agents accumulate a journal
of their thoughts over time.

Requirements:
    export OPENAI_API_KEY=sk-...
    # or configure any provider via daft.set_provider()

Usage:
    uv run python examples/05_llm_agents.py
"""

import asyncio
import json

from daft import DataFrame, col
from daft.functions import prompt
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

# ── Components ──


class Agent(Component):
    name: str = ""
    role: str = ""
    journal: str = "[]"  # JSON list of thoughts


# ── Processors ──


class ThinkProcessor(AsyncProcessor):
    """
    Each tick, every agent entity gets an LLM call.

    Daft executes these in parallel across all entities — the ECS handles
    batching automatically because world state is a DataFrame.
    """

    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        # Build a prompt from each agent's name, role, and journal
        input_col = (
            col("agent__role")
            + "\nYour name is "
            + col("agent__name")
            + ".\nTick: "
            + str(tick)
            + "\nYour journal so far: "
            + col("agent__journal")
            + "\n\nWhat do you think or do next? One sentence."
        )

        # Call LLM for every row in parallel
        thought = prompt(
            input_col,
            system_message=(
                "You are an agent in a simulation. "
                "Respond with a single short thought or action. "
                "Be creative and stay in character."
            ),
            model="gpt-5-mini",
            max_output_tokens=60,
        )

        # Append the new thought to the journal
        # We rebuild the journal JSON by appending the new thought string
        new_journal = (
            col("agent__journal").str.rstrip("]")
            + ', "'
            + thought
            + '"]'
        )
        # Fix the leading comma for empty journals
        new_journal = new_journal.str.replace('[, "', '["')

        return df.with_columns(
            {
                "agent__journal": new_journal,
            }
        )


# ── Main ──


async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    # Create world
    world = await container.world_service.create_world(
        WorldConfig(name="llm-agents"),
        StorageConfig(),
    )
    wid = world.world_id

    # Add the think processor
    await world.system.add_processor(ThinkProcessor())

    # Spawn three agents with different personalities
    agents = [
        ("Ada", "You are a curious scientist who loves discovering patterns."),
        ("Rex", "You are a bold explorer who takes risks and seeks adventure."),
        ("Iris", "You are a thoughtful philosopher who questions everything."),
    ]

    for name, role in agents:
        cmd = Command(
            type=CommandType.SPAWN,
            payload={
                "components": [
                    Agent(name=name, role=role).to_payload(),
                ],
            },
        )
        await container.command_service.submit(wid, cmd, ctx)

    # Run 5 ticks — each tick calls the LLM for all agents
    print(f"Running 5 ticks with {len(agents)} LLM-powered agents...\n")
    result = await container.simulation_service.run(wid, RunConfig(num_steps=5))
    print(f"Completed {result.ticks_completed} ticks\n")

    # Print journals
    for _sig, df in world._live.items():
        rows = df.collect().to_pylist()
        for row in rows:
            name = row.get("agent__name", "?")
            journal_str = row.get("agent__journal", "[]")
            try:
                thoughts = json.loads(journal_str)
            except json.JSONDecodeError:
                thoughts = [journal_str]
            print(f"=== {name} ===")
            for i, thought in enumerate(thoughts):
                print(f"  tick {i}: {thought}")
            print()

    await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
