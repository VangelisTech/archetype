#!/usr/bin/env python3
"""
Chat Graph Agents: Channel-Aware LLM Context Assembly
======================================================

Demonstrates agents using ChatGraphRegistry as a Resource to:
1. Post messages to named channels
2. Read conversation context from the graph (not the flat inbox)
3. Branch conversations for counterfactual exploration
4. Use active_path() to build clean LLM prompts per channel

Three agents negotiate across two channels:
- #strategy: high-level planning (all agents)
- #negotiation: Alice and Bob haggle over terms

The key difference from messaging_example.py:
- Processors use graph.active_path() for LLM context, not Inbox components
- Messages are threaded, not flat — agents see the conversation tree
- Ephemeral system messages (append_history=False) don't pollute the graph

Run: uv run python examples/chat_graph_agents.py
"""

import asyncio
import json

from archetype.app.broker import CommandBroker
from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources

# ── Components ──


class Agent(Component):
    name: str = ""
    role: str = ""


class Inbox(Component):
    messages: list[str] = []


# ── Processors ──


class ChannelPostProcessor(AsyncProcessor):
    """
    Agents post to channels. Each message becomes a node in the channel's graph.

    This processor demonstrates the write path:
    broker.enqueue(cmd with channel="strategy") → graph auto-appends
    """

    components = (Agent,)
    priority = 10

    async def process(self, df, resources: Resources, tick: int = 0, **kwargs):
        broker = resources.require(CommandBroker)
        world_id = kwargs.get("world_id", "demo")

        rows = df.select("entity_id", "agent__name", "agent__role").collect().to_pylist()

        for row in rows:
            name = row["agent__name"]
            eid = row["entity_id"]

            # All agents post to #strategy
            await broker.enqueue(world_id, Command(
                type=CommandType.MESSAGE,
                tick=tick,
                channel="strategy",
                payload={
                    "sender_id": eid,
                    "sender_name": name,
                    "content": f"[tick {tick}] {name}: Strategy update from my perspective.",
                },
            ))

            # Only Alice and Bob post to #negotiation
            if name in ("Alice", "Bob"):
                content = {
                    "Alice": f"[tick {tick}] Alice: I propose we split {60 - tick * 5}/{40 + tick * 5}.",
                    "Bob": f"[tick {tick}] Bob: Counter-offer. {50}/{50} is the only fair deal.",
                }.get(name, "")

                await broker.enqueue(world_id, Command(
                    type=CommandType.MESSAGE,
                    tick=tick,
                    channel="negotiation",
                    payload={
                        "sender_id": eid,
                        "sender_name": name,
                        "content": content,
                    },
                ))

            # System heartbeat — ephemeral, never lands in graph
            await broker.enqueue(world_id, Command(
                type=CommandType.MESSAGE,
                tick=tick,
                channel="system",
                append_history=False,
                payload={"sender_id": eid, "type": "heartbeat"},
            ))

        return df


class ContextAssemblyProcessor(AsyncProcessor):
    """
    Demonstrates the READ path: using active_path() to build LLM context.

    Instead of reading from a flat Inbox component, this processor reads
    from the channel graph. Each agent gets a prompt built from the
    conversation thread they're participating in.

    This is where ChatGraph replaces the journal/inbox pattern.
    """

    components = (Agent,)
    priority = 20

    async def process(self, df, resources: Resources, tick: int = 0, **kwargs):
        registry = resources.require(ChatGraphRegistry)
        world_id = kwargs.get("world_id", "demo")

        rows = df.select("entity_id", "agent__name").collect().to_pylist()

        for row in rows:
            name = row["agent__name"]

            # Build context from #strategy channel
            strategy_graph = registry.channel(world_id, "strategy")
            strategy_context = strategy_graph.active_path()

            # An LLM call would use this as the conversation history:
            strategy_messages = [
                cmd.payload.get("content", "")
                for cmd in strategy_context
            ]

            if name in ("Alice", "Bob"):
                # Also read the #negotiation channel
                neg_graph = registry.channel(world_id, "negotiation")
                neg_context = neg_graph.active_path()
                neg_messages = [
                    cmd.payload.get("content", "")
                    for cmd in neg_context
                ]
            else:
                neg_messages = []

            # This is where you'd call the LLM:
            #
            #   prompt = f"""You are {name}. {role}
            #
            #   ## #strategy channel:
            #   {chr(10).join(strategy_messages)}
            #
            #   ## #negotiation channel:
            #   {chr(10).join(neg_messages)}
            #
            #   What do you say next?"""
            #
            #   response = await llm.complete(prompt)
            #
            # The key insight: active_path() gives you ONLY the current
            # branch's messages. If someone branched the negotiation to
            # explore a counterfactual, each branch gets its own clean context.

            if tick == 0:
                print(f"  {name}: reading {len(strategy_messages)} msgs from #strategy"
                      f", {len(neg_messages)} msgs from #negotiation")

        return df


# ── Main Demo ──


async def main():
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.aio.async_world import AsyncWorld
    from archetype.core.config import RunConfig, WorldConfig

    import daft
    import pyarrow as pa
    from archetype.core.archetype import Archetype

    # Minimal in-memory infrastructure (same pattern as messaging_example.py)
    class InMemoryQuerier:
        def __init__(self):
            self._store = {}

        async def query_archetype(self, **kwargs):
            sig = kwargs["sig"]
            ticks = kwargs.get("ticks", [0])
            key = (sig, ticks[0] if ticks else 0)
            if key in self._store:
                return self._store[key]
            schema = Archetype.get_archetype_schema(sig)
            return daft.from_arrow(pa.Table.from_batches([], schema=schema))

        def store(self, sig, tick, df):
            self._store[(sig, tick)] = df

    class InMemoryUpdater:
        def __init__(self, querier):
            self._querier = querier

        async def update(self, df, sig, tick, world_id, run_id):
            df = (
                df
                .with_column("tick", daft.lit(tick))
                .with_column("world_id", daft.lit(str(world_id)))
                .with_column("run_id", daft.lit(str(run_id)))
            )
            df_mat = df.collect()
            self._querier.store(sig, tick, df_mat)
            return df_mat

    print("=" * 60)
    print("Chat Graph Agents: Channel-Aware Context Assembly")
    print("=" * 60)

    # Create world
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    world = AsyncWorld(
        world_config=WorldConfig(name="chat-graph-demo"),
        querier=querier,
        updater=updater,
        system=system,
    )

    # Wire resources — this is the key integration point
    registry = ChatGraphRegistry()
    broker = CommandBroker(chat_graphs=registry)

    world.resources.insert(broker)
    world.resources.insert(registry)  # agents can now require(ChatGraphRegistry)

    print(f"\nResources: {world.resources}")

    # Register processors
    await system.add_processor(ChannelPostProcessor())
    await system.add_processor(ContextAssemblyProcessor())

    # Spawn agents
    agents = [
        [Agent(name="Alice", role="Lead negotiator"), Inbox()],
        [Agent(name="Bob", role="Counterparty"), Inbox()],
        [Agent(name="Charlie", role="Observer and strategist"), Inbox()],
    ]
    for components in agents:
        await world.create_entity(components)

    print(f"Spawned {len(agents)} agents: Alice, Bob, Charlie\n")

    # Run 3 ticks
    print("-" * 60)
    print("Running 3 ticks...")
    print("-" * 60)

    run_config = RunConfig(num_steps=3)
    await world.run(run_config, world_id="demo")

    # Show channel state
    print("\n" + "=" * 60)
    print("Channel State After Simulation")
    print("=" * 60)

    for ch_name in registry.channels("demo"):
        graph = registry.channel("demo", ch_name)
        path = graph.active_path()
        print(f"\n#{ch_name} ({graph.size} messages, cursor at depth {len(path)}):")
        for cmd in path:
            content = cmd.payload.get("content", cmd.payload.get("type", "?"))
            if len(content) > 70:
                content = content[:67] + "..."
            print(f"  {content}")

    # Demonstrate branching: what if Alice took a harder line at tick 1?
    print("\n" + "=" * 60)
    print("Counterfactual Branch: Alice takes harder line")
    print("=" * 60)

    neg_graph = registry.channel("demo", "negotiation")

    # Find Alice's tick-1 message to branch from
    all_nodes = list(neg_graph._nodes.values())
    alice_tick1 = None
    for node in all_nodes:
        if (node.cmd.tick == 1
                and node.cmd.payload.get("sender_name") == "Alice"):
            alice_tick1 = node
            break

    if alice_tick1:
        # Branch: alternative Alice response
        counterfactual = Command(
            type=CommandType.MESSAGE,
            tick=1,
            channel="negotiation",
            payload={
                "sender_name": "Alice",
                "content": "[tick 1] Alice: 65/35. Non-negotiable. Walk if you want.",
            },
        )
        neg_graph.branch(alice_tick1.cmd.id, counterfactual, label="hardball")

        print(f"\nBranched from Alice's tick-1 message.")
        print(f"Original path had {len(all_nodes)} nodes.")

        # Show the two paths
        print("\nActive path (hardball branch):")
        for cmd in neg_graph.active_path():
            print(f"  {cmd.payload.get('content', '?')}")

        # Navigate back to original
        original_leaf = neg_graph.leaves()
        for leaf_id in original_leaf:
            leaf_node = neg_graph.get_node(leaf_id)
            if leaf_node and leaf_node.branch_label != "hardball":
                neg_graph.navigate(leaf_id)
                break

        neg_graph.auto_nav_to_leaf()
        print("\nOriginal path (after navigate back):")
        for cmd in neg_graph.active_path():
            print(f"  {cmd.payload.get('content', '?')}")

    # Show that ephemeral messages didn't land in the graph
    system_channels = registry.channels("demo")
    if "system" not in system_channels:
        print("\n'system' channel has no graph — all heartbeats were ephemeral.")
    else:
        sys_graph = registry.channel("demo", "system")
        print(f"\n'system' channel: {sys_graph.size} messages (should be 0)")


if __name__ == "__main__":
    asyncio.run(main())
