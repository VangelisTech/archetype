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

import daft
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
    journal_json: str = "[]"
    thought_count: int = 0


# ── Processors ──


@daft.func(return_dtype=daft.DataType.string())
def append_thought(journal_json: str, thought: str) -> str:
    """Append model text without letting quotes or escapes corrupt the journal."""

    journal = json.loads(journal_json)
    if not isinstance(journal, list):
        raise ValueError("agent journal must be a JSON list")
    journal.append(thought)
    return json.dumps(journal, ensure_ascii=False)


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
            + col("agent__journal_json")
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

        return df.with_columns(
            {
                "agent__journal_json": append_thought(col("agent__journal_json"), thought),
                "agent__thought_count": col("agent__thought_count") + 1,
            }
        )


# ── Main ──


async def _run_agents(storage_uri: str):
    agents = [
        ("Ada", "You are a curious scientist who loves discovering patterns."),
        ("Rex", "You are a bold explorer who takes risks and seeks adventure."),
        ("Iris", "You are a thoughtful philosopher who questions everything."),
    ]
    storage = StorageConfig(uri=storage_uri, namespace="llm_agents")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("llm-agents", storage=storage, processors=[ThinkProcessor()])

        for name, role in agents:
            await world.spawn(Agent(name=name, role=role))

        await world.step()  # Persist initial agents before model processing.
        result = await world.run(steps=5)
        history = await world.query(Agent)
        rows = sorted(
            history.where(col("tick") == result.final_tick - 1).collect().to_pylist(),
            key=lambda row: row.get("agent__name", ""),
        )
    return result, rows


async def run_demo(storage_uri: str) -> dict[str, object]:
    """Run once and return redaction-safe model-call coverage evidence."""

    result, rows = await _run_agents(storage_uri)
    return {
        "schema": "examples.llm-agent-thought-coverage/v1",
        "ticks_completed": result.ticks_completed,
        "agents": [
            {
                "name": row.get("agent__name", ""),
                "thought_count": row.get("agent__thought_count", 0),
                "journal_entries": len(json.loads(row.get("agent__journal_json", "[]"))),
            }
            for row in rows
        ],
    }


async def main():
    if not os.getenv("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not set. Skipping LLM agent example.")
        print("Set the key to run: export OPENAI_API_KEY=sk-...")
        sys.exit(0)

    print("Running 5 ticks with 3 LLM-powered agents...\n")
    result, rows = await _run_agents("./archetype_data")
    print(f"Completed {result.ticks_completed} ticks\n")

    for row in rows:
        name = row.get("agent__name", "?")
        journal_str = row.get("agent__journal_json", "[]")
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
