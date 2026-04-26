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
import os
import sys

from daft import DataFrame, col
from daft.functions import prompt

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig

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
    if not os.getenv("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not set. Skipping LLM agent example.")
        print("Set the key to run: export OPENAI_API_KEY=sk-...")
        sys.exit(0)

    agents = [
        ("Ada", "You are a curious scientist who loves discovering patterns."),
        ("Rex", "You are a bold explorer who takes risks and seeks adventure."),
        ("Iris", "You are a thoughtful philosopher who questions everything."),
    ]
    storage = StorageConfig(uri="./archetype_data", namespace="llm_agents")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("llm-agents", storage=storage, processors=[ThinkProcessor()])

        for name, role in agents:
            await world.spawn(Agent(name=name, role=role))

        print(f"Running 5 ticks with {len(agents)} LLM-powered agents...\n")
        result = await world.run(steps=5)
        print(f"Completed {result.ticks_completed} ticks\n")

        rows = sorted(
            (await world.query(Agent)).collect().to_pylist(),
            key=lambda row: row.get("agent__name", ""),
        )
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


if __name__ == "__main__":
    asyncio.run(main())
