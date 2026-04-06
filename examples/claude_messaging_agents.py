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

    Uses async @daft.func for row-wise LLM calls — Daft manages
    concurrency across entities. No .collect().to_pylist() loop.

    Claude sees:
    - The agent's role as a system prompt
    - The channel's active_path() as conversation history
    - The agent's inbox (new messages this tick)
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

        # Cross-row context: name lookups for conversation history.
        # This is the ONE justified .collect() — we need global entity
        # knowledge for name resolution and round-robin targeting.
        id_name_rows = df.select("entity_id", "agent__name").collect().to_pylist()
        name_by_id = {r["entity_id"]: r["agent__name"] for r in id_name_rows}
        all_ids = sorted(name_by_id.keys())

        # Build shared conversation history from ChatGraph (Resource, not DataFrame)
        graph = registry.channel(world_id, self.channel)
        context_msgs = []
        for cmd in graph.active_path():
            sender = cmd.payload.get("sender_id", "system")
            sender_name = name_by_id.get(sender, f"agent-{sender}")
            context_msgs.append(
                {
                    "role": "user",
                    "content": f"[{sender_name}]: {cmd.payload.get('content', '')}",
                }
            )
        context_json = json.dumps(context_msgs)

        # @daft.cls() for non-serializable state — client created per worker,
        # never captured in a pickle-unfriendly closure.
        model = self.model
        channel = self.channel

        @daft.cls()
        class ClaudeResponder:
            def __init__(self):
                import anthropic

                self.client = anthropic.AsyncAnthropic()

            async def respond(
                self,
                entity_id: int,
                name: str,
                role: str,
                inbox_messages: list[str],
            ) -> list[str]:
                """Row-wise async: each entity calls Claude and produces outbox messages."""
                history = json.loads(context_json)

                # Add this entity's inbox to context
                for raw in inbox_messages or []:
                    parsed = json.loads(raw) if isinstance(raw, str) else raw
                    sid = parsed.get("sender_id", "?")
                    sname = name_by_id.get(sid, f"agent-{sid}")
                    history.append(
                        {
                            "role": "user",
                            "content": f"[{sname}]: {parsed.get('content', '')}",
                        }
                    )

                history.append(
                    {
                        "role": "user",
                        "content": (
                            f"You are {name}. It is tick {tick}. "
                            f"Respond in character with a short message (1-2 sentences) "
                            f"to continue the conversation."
                        ),
                    }
                )

                # Call Claude (async — Daft manages concurrency)
                response = await self.client.messages.create(
                    model=model,
                    max_tokens=150,
                    system=role,
                    messages=history,
                )
                response_text = response.content[0].text

                # Round-robin target
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

        responder = ClaudeResponder()
        return df.with_column(
            "outbox__messages",
            responder.respond(
                col("entity_id"),
                col("agent__name"),
                col("agent__role"),
                col("inbox__messages"),
            ),
        )


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
                df.with_column("tick", daft.lit(tick))
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
    await system.add_processor(
        ClaudeThinkProcessor(
            model="claude-sonnet-4-6",
            channel="debate",
        )
    )

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
            Outbox(),
            Inbox(),
        ],
        [
            Agent(
                name="Skeptic",
                role="You are cautious about AI risks and push back on hype. "
                "You ask hard questions. Keep responses to 1-2 sentences.",
            ),
            Outbox(),
            Inbox(),
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
