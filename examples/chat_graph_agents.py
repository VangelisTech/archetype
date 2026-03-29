#!/usr/bin/env python3
"""
Chat Graph Agents: Processor-Driven Messaging with Channels
============================================================

Demonstrates the decoupled messaging architecture:

1. **Broker** = governance only (RBAC, quotas). Does NOT own messages.
2. **MessageDeliveryProcessor** = routes Outbox → Inbox, updates ChatGraph,
   handles rejections — all inside the ECS tick as an AsyncProcessor.
3. **ChatGraphRegistry** = Resource for conversation structure. Processors
   read active_path() for LLM context per channel/branch.

Three agents negotiate across two channels:
- #strategy: high-level planning (all agents)
- #negotiation: Alice and Bob haggle over terms

Key patterns shown:
- Agents write to Outbox, MessageDeliveryProcessor handles delivery
- ChatGraph is a Resource: processors read/write it directly
- Ephemeral messages (system probes) skip the graph via _append_history flag
- Governance rejections are visible (bad receiver_id → delivery fails)

Run: uv run python examples/chat_graph_agents.py
"""

import asyncio
import json

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.messaging import Inbox, MessageDeliveryProcessor, Outbox
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources

import daft
from daft import DataFrame, col

# ── Components ──


class Agent(Component):
    name: str = ""
    role: str = ""


# ── Processors ──


class NegotiationProcessor(AsyncProcessor):
    """
    Agents decide what to say and write to their Outbox.

    This is where LLM calls would go. For this demo, agents follow
    a scripted negotiation. Uses @daft.func for row-wise message building.

    The one .collect() is for entity_id→name lookups (cross-row context).
    """

    components = (Agent, Outbox)
    priority = 10  # runs AFTER MessageDeliveryProcessor (-100)

    async def process(self, df: DataFrame, resources: Resources, tick: int = 0, **kwargs):
        world_id = str(kwargs.get("world_id", "demo"))
        registry = resources.require(ChatGraphRegistry)

        # Cross-row context: name lookups for targeting
        id_name_rows = df.select("entity_id", "agent__name").collect().to_pylist()
        name_by_id = {r["entity_id"]: r["agent__name"] for r in id_name_rows}
        id_by_name = {v: k for k, v in name_by_id.items()}

        # Read conversation context from the graph (shared, from Resource)
        strategy_depth = len(registry.channel(world_id, "strategy").active_path())
        neg_depth = len(registry.channel(world_id, "negotiation").active_path())

        @daft.func
        def build_messages(entity_id: int, name: str) -> list[str]:
            """Row-wise: each agent builds its outbox messages."""
            msgs: list[str] = []

            # Broadcast to #strategy: send to every other agent
            for other_name, other_id in id_by_name.items():
                if other_id != entity_id:
                    msgs.append(json.dumps({
                        "receiver_id": other_id,
                        "channel": "strategy",
                        "content": f"{name}: Tick {tick} strategy update "
                                   f"(context depth: {strategy_depth}).",
                    }))

            # Alice and Bob negotiate directly
            if name == "Alice" and "Bob" in id_by_name:
                msgs.append(json.dumps({
                    "receiver_id": id_by_name["Bob"],
                    "channel": "negotiation",
                    "content": f"Alice: I propose {60 - tick * 5}/{40 + tick * 5}. "
                               f"(I can see {neg_depth} prior messages in this thread.)",
                }))
            elif name == "Bob" and "Alice" in id_by_name:
                msgs.append(json.dumps({
                    "receiver_id": id_by_name["Alice"],
                    "channel": "negotiation",
                    "content": f"Bob: Counter. 50/50 is the only fair split.",
                }))

            # System heartbeat — ephemeral, won't land in ChatGraph
            for other_id in id_by_name.values():
                if other_id != entity_id:
                    msgs.append(json.dumps({
                        "receiver_id": other_id,
                        "channel": "system",
                        "content": "heartbeat",
                        "_append_history": False,
                    }))

            return msgs

        return df.with_column(
            "outbox__messages",
            build_messages(col("entity_id"), col("agent__name")),
        )


class ContextReportProcessor(AsyncProcessor):
    """
    After delivery, report what each agent sees in their inbox and channels.
    In a real system this would be where you build LLM prompts.
    """

    components = (Agent, Inbox)
    priority = 20  # runs after NegotiationProcessor

    async def process(self, df: DataFrame, resources: Resources, tick: int = 0, **kwargs):
        if tick != 0:
            return df

        world_id = str(kwargs.get("world_id", "demo"))
        registry = resources.require(ChatGraphRegistry)

        rows = df.select("entity_id", "agent__name", "inbox__messages").collect().to_pylist()

        print(f"\n  Tick {tick} context report:")
        for row in rows:
            name = row["agent__name"]
            inbox_count = len(row.get("inbox__messages") or [])

            strategy_depth = len(registry.channel(world_id, "strategy").active_path())
            neg_depth = len(registry.channel(world_id, "negotiation").active_path())

            print(f"    {name}: inbox={inbox_count} msgs, "
                  f"#strategy depth={strategy_depth}, "
                  f"#negotiation depth={neg_depth}")

        return df


# ── Main Demo ──


async def main():
    import pyarrow as pa
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.aio.async_world import AsyncWorld
    from archetype.core.archetype import Archetype
    from archetype.core.config import RunConfig, WorldConfig

    # Minimal in-memory infrastructure
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
    print("Chat Graph Agents: Processor-Driven Messaging")
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

    # Wire resources — ChatGraphRegistry is the key integration
    # No broker in resources: governance is not needed for in-world messaging
    registry = ChatGraphRegistry()
    world.resources.insert(registry)

    print(f"\nResources: {world.resources}")

    # Register processors (priority order determines execution):
    #   -100: MessageDeliveryProcessor  (Outbox → Inbox, graph updates)
    #     10: NegotiationProcessor      (agents decide what to say)
    #     20: ContextReportProcessor    (report what agents see)
    await system.add_processor(MessageDeliveryProcessor())
    await system.add_processor(NegotiationProcessor())
    await system.add_processor(ContextReportProcessor())

    print("Processors registered:")
    for p in sorted(system.processors, key=lambda x: x.priority):
        print(f"  [{p.priority:4d}] {type(p).__name__} → {[c.__name__ for c in p.components]}")

    # Spawn agents with Outbox + Inbox (messaging components)
    agents = [
        [Agent(name="Alice", role="Lead negotiator"), Outbox(), Inbox()],
        [Agent(name="Bob", role="Counterparty"), Outbox(), Inbox()],
        [Agent(name="Charlie", role="Observer"), Outbox(), Inbox()],
    ]
    for components in agents:
        await world.create_entity(components)

    print(f"\nSpawned {len(agents)} agents: Alice, Bob, Charlie")

    # Run 3 ticks
    print("\n" + "-" * 60)
    print("Running 3 ticks...")
    print("-" * 60)

    run_config = RunConfig(num_steps=3)
    await world.run(run_config, world_id="demo")

    # Show channel state
    print("\n" + "=" * 60)
    print("Channel State After Simulation")
    print("=" * 60)

    for ch_name in sorted(registry.channels("demo")):
        graph = registry.channel("demo", ch_name)
        path = graph.active_path()
        print(f"\n#{ch_name} ({graph.size} total nodes, active path depth={len(path)}):")
        for cmd in path[-5:]:  # show last 5 in active path
            content = cmd.payload.get("content", "?")
            if len(content) > 70:
                content = content[:67] + "..."
            print(f"  [tick {cmd.tick}] {content}")

    # Verify ephemeral messages didn't create a channel
    all_channels = registry.channels("demo")
    if "system" not in all_channels:
        print("\n'system' channel empty — heartbeats were ephemeral.")

    # Demonstrate branching
    print("\n" + "=" * 60)
    print("Counterfactual: Branch #negotiation")
    print("=" * 60)

    neg_graph = registry.channel("demo", "negotiation")
    if neg_graph.size > 0:
        # Branch from root with an alternative opening
        root_id = neg_graph._roots[0]
        counterfactual = Command(
            type=CommandType.MESSAGE,
            tick=0,
            channel="negotiation",
            payload={
                "sender_id": -1,
                "content": "Alice: 70/30. Take it or leave it.",
            },
        )
        neg_graph.branch(root_id, counterfactual, label="hardball")

        print(f"\nBranched from root. Now {neg_graph.size} nodes, "
              f"{len(neg_graph.leaves())} leaves.")

        print("\nActive path (hardball branch):")
        for cmd in neg_graph.active_path():
            print(f"  [tick {cmd.tick}] {cmd.payload.get('content', '?')}")

        # Navigate back to original leaf
        for leaf_id in neg_graph.leaves():
            node = neg_graph.get_node(leaf_id)
            if node and node.branch_label != "hardball":
                neg_graph.navigate(leaf_id)
                neg_graph.auto_nav_to_leaf()
                break

        print("\nOriginal path:")
        for cmd in neg_graph.active_path():
            content = cmd.payload.get("content", "?")
            if len(content) > 70:
                content = content[:67] + "..."
            print(f"  [tick {cmd.tick}] {content}")


if __name__ == "__main__":
    asyncio.run(main())
