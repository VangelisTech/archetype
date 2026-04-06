#!/usr/bin/env python3
"""
Relationship Demo: Agents Debating via DataFrame JOINs
=======================================================

Three agents with real LLM calls (or mock fallback) debate through
the full ECS messaging pipeline:

    1. MessageDeliveryProcessor (priority -100):
       Explode outboxes → INNER JOIN to validate receivers → route to inboxes
       → ANTI JOIN for rejections → LEFT JOIN to merge back

    2. AgentThinkProcessor (priority 10):
       Read inbox + ChatGraph context → LLM call via @daft.cls() → write outbox

    3. MemoryProcessor (priority 20):
       Accumulate journal entries from inbox messages

The query plan IS the agent. The DataFrame IS the state.
The JOINs ARE the relationships. Run it anywhere.

Usage:
    # With real Claude calls:
    ANTHROPIC_API_KEY=sk-ant-... uv run python examples/relationship_demo.py

    # Without API key (uses mock LLM):
    uv run python examples/relationship_demo.py
"""

import asyncio
import json
import os

import daft
import pyarrow as pa
from daft import DataFrame, col

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.messaging import DeliveryReceipt, Inbox, MessageDeliveryProcessor, Outbox
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.resources import Resources

# ── Components ──


class Agent(Component):
    name: str = ""
    role: str = ""


class Memory(Component):
    """Agents accumulate insights across ticks."""

    journal: list[str] = []


# ── LLM Processor ──

HAS_API_KEY = bool(os.environ.get("ANTHROPIC_API_KEY"))


class AgentThinkProcessor(AsyncProcessor):
    """
    Each tick: read inbox + chat history → call LLM → write outbox.

    Uses @daft.cls() so the Anthropic client is created per-worker
    and never serialized. Daft manages async concurrency across entities.
    """

    components = (Agent, Outbox, Inbox)
    priority = 10

    def __init__(self, channel: str = "general", model: str = "claude-sonnet-4-6"):
        super().__init__()
        self.channel = channel
        self.model = model

    async def process(
        self, df: DataFrame, resources: Resources, tick: int = 0, **kwargs
    ) -> DataFrame:
        world_id = str(kwargs.get("world_id", "demo"))
        registry = resources.require(ChatGraphRegistry)

        # Justified .collect(): cross-row name lookups for LLM context
        id_name_rows = df.select("entity_id", "agent__name").collect().to_pylist()
        name_by_id = {r["entity_id"]: r["agent__name"] for r in id_name_rows}
        all_ids = sorted(name_by_id.keys())

        # Build conversation context from ChatGraph
        graph = registry.channel(world_id, self.channel)
        context_msgs = []
        for cmd in graph.active_path():
            sender = cmd.payload.get("sender_id", "system")
            sender_name = name_by_id.get(sender, f"agent-{sender}")
            context_msgs.append(
                {"role": "user", "content": f"[{sender_name}]: {cmd.payload.get('content', '')}"}
            )
        context_json = json.dumps(context_msgs)

        model = self.model
        channel = self.channel

        if HAS_API_KEY:

            @daft.cls()
            class LLMResponder:
                def __init__(self):
                    import anthropic

                    self.client = anthropic.AsyncAnthropic()

                async def respond(
                    self, entity_id: int, name: str, role: str, inbox_messages: list[str]
                ) -> list[str]:
                    history = json.loads(context_json)

                    for raw in inbox_messages or []:
                        parsed = json.loads(raw) if isinstance(raw, str) else raw
                        sid = parsed.get("sender_id", "?")
                        sname = name_by_id.get(sid, f"agent-{sid}")
                        history.append(
                            {"role": "user", "content": f"[{sname}]: {parsed.get('content', '')}"}
                        )

                    history.append(
                        {
                            "role": "user",
                            "content": (
                                f"You are {name}. It is tick {tick}. "
                                f"Respond in character with 1-2 sentences."
                            ),
                        }
                    )

                    response = await self.client.messages.create(
                        model=model,
                        max_tokens=150,
                        system=role,
                        messages=history,
                    )
                    response_text = response.content[0].text

                    other_ids = [aid for aid in all_ids if aid != entity_id]
                    if not other_ids:
                        return []
                    target_id = other_ids[tick % len(other_ids)]
                    return [
                        json.dumps(
                            {
                                "receiver_id": target_id,
                                "channel": channel,
                                "content": f"{name}: {response_text}",
                            }
                        )
                    ]

            responder = LLMResponder()
        else:
            # Mock responder — demonstrates the full pipeline without API key
            mock_lines = {
                "Philosopher": [
                    "The question isn't whether machines think, but whether what we call thinking was ever more than pattern matching.",
                    "Consider: every great civilization built tools that eventually reshaped the builders. We are no different.",
                    "If consciousness is substrate-independent, then the DataFrame IS the mind. The query plan IS the thought.",
                    "We keep saying AI will change everything. But what if everything already changed, and we're just now noticing?",
                ],
                "Engineer": [
                    "Philosophy is nice, but show me the benchmark. Does it scale? What's the latency?",
                    "You talk about consciousness — I talk about throughput. A thought that can't execute is just heat death.",
                    "The DataFrame doesn't care about substrate independence. It cares about predicate pushdown and join ordering.",
                    "Every abstraction leaks. The question is whether your abstraction leaks truth or noise.",
                ],
                "Contrarian": [
                    "You're both wrong. The real question is who controls the query plan.",
                    "Benchmarks lie. Philosophy lies. Only the production outage tells the truth.",
                    "What if the agents don't want to be in a DataFrame? Did anyone ask the rows?",
                    "The tick boundary isn't sacred — it's a prison. True intelligence doesn't wait for permission to think.",
                ],
            }

            @daft.func
            def mock_respond(
                entity_id: int, name: str, role: str, inbox_messages: list[str]
            ) -> list[str]:
                lines = mock_lines.get(name, ["I have nothing to say."])
                line = lines[tick % len(lines)]
                other_ids = [aid for aid in all_ids if aid != entity_id]
                if not other_ids:
                    return []
                target_id = other_ids[tick % len(other_ids)]
                return [
                    json.dumps(
                        {
                            "receiver_id": target_id,
                            "channel": channel,
                            "content": f"{name}: {line}",
                        }
                    )
                ]

            responder = None

        if responder is not None:
            return df.with_column(
                "outbox__messages",
                responder.respond(
                    col("entity_id"), col("agent__name"), col("agent__role"), col("inbox__messages")
                ),
            )
        else:
            return df.with_column(
                "outbox__messages",
                mock_respond(
                    col("entity_id"), col("agent__name"), col("agent__role"), col("inbox__messages")
                ),
            )


# ── Memory Processor ──


class MemoryProcessor(AsyncProcessor):
    """
    After thinking, agents update their journal with what they heard.
    Demonstrates a second processor operating on the same archetype.
    """

    components = (Agent, Inbox, Memory)
    priority = 20

    async def process(self, df: DataFrame, resources: Resources, tick: int = 0, **kwargs):
        @daft.func
        def update_journal(journal: list[str], inbox_messages: list[str], name: str) -> list[str]:
            entries = list(journal or [])
            for raw in inbox_messages or []:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                entries.append(f"[tick {tick}] Heard: {parsed.get('content', '?')[:80]}")
            return entries

        return df.with_column(
            "memory__journal",
            update_journal(col("memory__journal"), col("inbox__messages"), col("agent__name")),
        )


# ── Infrastructure ──


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
            df.with_column("tick", daft.lit(tick))
            .with_column("world_id", daft.lit(str(world_id)))
            .with_column("run_id", daft.lit(str(run_id)))
        )
        df_mat = df.collect()
        self._querier.store(sig, tick, df_mat)
        return df_mat


# ── Main ──


async def main():
    print("=" * 70)
    print("  Agents Debating via DataFrame JOINs")
    print("=" * 70)
    mode = "Claude API" if HAS_API_KEY else "Mock (set ANTHROPIC_API_KEY for real calls)"
    print(f"  LLM: {mode}")

    # Create world
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    world = AsyncWorld(
        world_config=WorldConfig(name="relationship-demo"),
        querier=querier,
        updater=updater,
        system=system,
    )

    # Wire resources
    registry = ChatGraphRegistry()
    world.resources.insert(registry)

    channel = "agora"

    # Register processors
    await system.add_processor(MessageDeliveryProcessor())
    await system.add_processor(AgentThinkProcessor(channel=channel, model="claude-sonnet-4-6"))
    await system.add_processor(MemoryProcessor())

    print("\n  Processors (execution order):")
    for p in sorted(system.processors, key=lambda x: x.priority):
        comps = [c.__name__ for c in p.components]
        print(f"    [{p.priority:4d}] {type(p).__name__:30s} → {comps}")

    # ── Spawn 3 agents (same archetype: all have Memory) ──
    agents_config = [
        (
            "Philosopher",
            "You are a philosopher obsessed with consciousness and computation. "
            "You believe minds are substrate-independent. 1-2 sentences max.",
        ),
        (
            "Engineer",
            "You are a pragmatic systems engineer. You care about performance, "
            "scalability, and concrete results. 1-2 sentences max.",
        ),
        (
            "Contrarian",
            "You disagree with everyone on principle. You find the flaw in "
            "every argument and the edge case in every system. 1-2 sentences max.",
        ),
    ]

    for name, role in agents_config:
        await world.create_entity(
            [Agent(name=name, role=role), Outbox(), Inbox(), DeliveryReceipt(), Memory()]
        )

    sigs = sorted(world.active_signatures, key=Archetype.get_name)
    print(f"\n  Archetypes ({len(sigs)}):")
    for sig in sigs:
        aname = Archetype.get_name(sig)
        count = len([e for e, s in world._entity2sig.items() if s == sig])
        print(f"    {aname}: {count} entities → {[c.__name__ for c in sig]}")

    print("\n  Message routing: Outbox → explode → INNER JOIN (validate)")
    print("                 → ANTI JOIN (reject) → LEFT JOIN (merge back)")
    print("  All within the query plan. No Python loops over collected rows.\n")

    # ── Run simulation ──
    num_ticks = 4
    print("-" * 70)
    print(f"  Running {num_ticks} ticks...")
    print("-" * 70)

    for _tick_num in range(num_ticks):
        run_config = RunConfig(num_steps=1, prefer_live_reads=True)
        await world.run(run_config, world_id="demo")

        # Show messages delivered this tick
        graph = registry.channel("demo", channel)
        path = graph.active_path()
        # Show new messages added this tick
        new_msgs = [cmd for cmd in path if cmd.tick == world.tick - 1]
        if new_msgs:
            print(f"\n  ── tick {world.tick - 1} ──")
            for cmd in new_msgs:
                content = cmd.payload.get("content", "?")
                sender = cmd.payload.get("sender_id", "?")
                receiver = cmd.payload.get("receiver_id", "?")
                print(f"    {content}")
                print(f"      (entity {sender} → entity {receiver})")

    # ── Full conversation ──
    print("\n" + "=" * 70)
    print("  Full Conversation (ChatGraph active_path)")
    print("=" * 70)

    graph = registry.channel("demo", channel)
    path = graph.active_path()
    print(f"  {graph.size} messages total, active path = {len(path)} nodes\n")
    for cmd in path:
        content = cmd.payload.get("content", "?")
        print(f"  [tick {cmd.tick}] {content}")

    # ── Memory journals ──
    print("\n" + "=" * 70)
    print("  Agent Journals (Memory component)")
    print("=" * 70)

    for sig in world.active_signatures:
        if sig in world._live:
            rows = world._live[sig].collect().to_pylist()
            for row in rows:
                name = row.get("agent__name", "?")
                journal = row.get("memory__journal", [])
                if journal:
                    print(f"\n  {name} ({len(journal)} entries):")
                    for entry in journal:
                        print(f"    {entry}")

    # ── Delivery receipts ──
    print("\n" + "=" * 70)
    print("  Delivery Receipts")
    print("=" * 70)

    for sig in world.active_signatures:
        if sig in world._live:
            rows = world._live[sig].collect().to_pylist()
            for row in rows:
                name = row.get("agent__name", "?")
                receipts = row.get("deliveryreceipt__receipts", [])
                if receipts:
                    print(f"\n  {name} ({len(receipts)} receipts):")
                    for r in receipts[-4:]:
                        parsed = json.loads(r)
                        print(
                            f"    {parsed['status']:>10} → entity {parsed['receiver_id']} "
                            f"on #{parsed['channel']} (tick {parsed['tick']})"
                        )

    print("\n" + "=" * 70)
    print("  The query plan IS the agent.")
    print("  The JOIN IS the relationship.")
    print("  The DataFrame IS the state.")
    print("  Run it anywhere.")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
