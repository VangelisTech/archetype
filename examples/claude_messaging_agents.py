#!/usr/bin/env python3
"""
Claude-Powered Agent Messaging
===============================

Demonstrates a processor that uses the Anthropic SDK to call Claude,
then writes Claude's responses into the Outbox for delivery to other
agents via MessageDeliveryProcessor.

The flow each tick:
    1. MessageDeliveryProcessor (priority -100): drains Outbox → Inbox
    2. ClaudeThinkProcessor (priority 10): reads Inbox + ChatGraph context,
       calls Claude, writes response to Outbox for next tick's delivery

Each agent has its own system prompt (role). Claude sees the channel's
active_path() as conversation history, giving it full thread context.

Requirements:
    export ANTHROPIC_API_KEY=sk-ant-...
    pip install anthropic

Usage:
    uv run python examples/claude_messaging_agents.py
"""

import asyncio
import json

import anthropic
import daft
from daft import DataFrame, col

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.messaging import Inbox, MessageDeliveryProcessor, Outbox
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources

# ── Components ──


class Agent(Component):
    name: str = ""
    role: str = ""  # system prompt for Claude


# ── Processor ──


class ClaudeThinkProcessor(AsyncProcessor):
    """
    Each tick, every agent calls Claude with channel context, then
    writes a message to the Outbox targeting another agent.

    Claude sees:
    - The agent's role as a system prompt
    - The channel's active_path() as conversation history
    - The agent's inbox (new messages this tick)

    Claude's response becomes an Outbox message to a target agent.
    """

    components = (Agent, Outbox, Inbox)
    priority = 10

    def __init__(self, model: str = "claude-sonnet-4-6", channel: str = "general"):
        super().__init__()
        self.client = anthropic.AsyncAnthropic()
        self.model = model
        self.channel = channel

    async def process(
        self, df: DataFrame, resources: Resources, tick: int = 0, **kwargs
    ) -> DataFrame:
        world_id = str(kwargs.get("world_id", "demo"))
        registry = resources.require(ChatGraphRegistry)

        # Materialize entity data
        rows = df.select(
            "entity_id", "agent__name", "agent__role", "inbox__messages",
        ).collect().to_pylist()

        # Build name/id lookups
        name_by_id = {r["entity_id"]: r["agent__name"] for r in rows}
        id_by_name = {v: k for k, v in name_by_id.items()}
        all_ids = list(name_by_id.keys())

        # Get conversation context from ChatGraph
        graph = registry.channel(world_id, self.channel)
        context_cmds = graph.active_path()

        # Build shared conversation history for Claude
        conversation_history = []
        for cmd in context_cmds:
            sender = cmd.payload.get("sender_id", "system")
            sender_name = name_by_id.get(sender, f"agent-{sender}")
            content = cmd.payload.get("content", "")
            # Alternate roles based on perspective (simplified: all as "user")
            conversation_history.append({
                "role": "user",
                "content": f"[{sender_name}]: {content}",
            })

        # Call Claude for each agent in parallel
        messages_by_entity: dict[int, list[str]] = {}

        async def think_for_agent(row: dict) -> tuple[int, list[str]]:
            eid = row["entity_id"]
            name = row["agent__name"]
            role = row["agent__role"]

            # Add inbox messages to context
            inbox_msgs = row.get("inbox__messages") or []
            agent_history = list(conversation_history)
            for raw in inbox_msgs:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                sender_id = parsed.get("sender_id", "?")
                sender_name = name_by_id.get(sender_id, f"agent-{sender_id}")
                agent_history.append({
                    "role": "user",
                    "content": f"[{sender_name}]: {parsed.get('content', '')}",
                })

            # Add a prompt for this agent to respond
            agent_history.append({
                "role": "user",
                "content": (
                    f"You are {name}. It is tick {tick}. "
                    f"Respond in character with a short message (1-2 sentences) "
                    f"to continue the conversation."
                ),
            })

            # Call Claude
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=150,
                system=role,
                messages=agent_history if agent_history else [
                    {"role": "user", "content": f"You are {name}. Introduce yourself briefly."}
                ],
            )

            response_text = response.content[0].text

            # Pick a target agent (round-robin: send to next agent)
            other_ids = [aid for aid in all_ids if aid != eid]
            if not other_ids:
                return eid, []

            target_id = other_ids[tick % len(other_ids)]

            outgoing = [json.dumps({
                "receiver_id": target_id,
                "channel": self.channel,
                "content": f"{name}: {response_text}",
            })]

            return eid, outgoing

        # Run all Claude calls concurrently
        results = await asyncio.gather(
            *[think_for_agent(row) for row in rows]
        )

        for eid, msgs in results:
            messages_by_entity[eid] = msgs

        # Write to Outbox via batch UDF
        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def write_outbox(entity_ids: daft.Series) -> list:
            return [
                messages_by_entity.get(eid, [])
                for eid in entity_ids.to_pylist()
            ]

        return df.with_column("outbox__messages", write_outbox(col("entity_id")))


# ── Main Demo ──


async def main():
    import pyarrow as pa
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.aio.async_world import AsyncWorld
    from archetype.core.archetype import Archetype
    from archetype.core.config import RunConfig, WorldConfig

    # In-memory infrastructure (same pattern as chat_graph_agents.py)
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
    print("Claude-Powered Agent Messaging")
    print("=" * 60)

    # Create world
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    world = AsyncWorld(
        world_config=WorldConfig(name="claude-messaging"),
        querier=querier,
        updater=updater,
        system=system,
    )

    # Wire Resources
    registry = ChatGraphRegistry()
    world.resources.insert(registry)

    # Register processors
    await system.add_processor(MessageDeliveryProcessor())
    await system.add_processor(ClaudeThinkProcessor(
        model="claude-sonnet-4-6",
        channel="debate",
    ))

    print(f"\nResources: {world.resources}")
    print("Processors:")
    for p in sorted(system.processors, key=lambda x: x.priority):
        print(f"  [{p.priority:4d}] {type(p).__name__}")

    # Spawn two debating agents
    agents = [
        [
            Agent(
                name="Optimist",
                role="You believe AI will be enormously beneficial for humanity. "
                     "You are thoughtful but enthusiastic. Keep responses to 1-2 sentences.",
            ),
            Outbox(), Inbox(),
        ],
        [
            Agent(
                name="Skeptic",
                role="You are cautious about AI risks and push back on hype. "
                     "You ask hard questions. Keep responses to 1-2 sentences.",
            ),
            Outbox(), Inbox(),
        ],
    ]
    for components in agents:
        await world.create_entity(components)

    print(f"\nSpawned {len(agents)} agents: Optimist, Skeptic")

    # Run 4 ticks
    print("\n" + "-" * 60)
    num_ticks = 4
    print(f"Running {num_ticks} ticks (each tick = Claude API call per agent)...")
    print("-" * 60)

    run_config = RunConfig(num_steps=num_ticks)
    await world.run(run_config, world_id="demo")

    # Show conversation
    print("\n" + "=" * 60)
    print("Debate Channel - Full Conversation")
    print("=" * 60)

    graph = registry.channel("demo", "debate")
    path = graph.active_path()
    print(f"\n{graph.size} messages total, active path depth={len(path)}\n")
    for cmd in path:
        content = cmd.payload.get("content", "?")
        print(f"  [tick {cmd.tick}] {content}")


if __name__ == "__main__":
    asyncio.run(main())
