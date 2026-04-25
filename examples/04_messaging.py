#!/usr/bin/env python3
"""
Messaging Example: Resources, MESSAGE Commands, and Hooks

Demonstrates the three new features:
1. Resources - Type-safe dependency injection for shared state
2. MESSAGE CommandType - Agent-to-agent communication via broker
3. Hooks - Lifecycle callbacks for observability

Run: uv run python examples/04_messaging.py
"""

import asyncio
import json
from dataclasses import dataclass

import daft
from daft import DataFrame, col

from archetype import ArchetypeRuntime
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.resources import Resources

# =============================================================================
# Components
# =============================================================================


class AgentState(Component):
    """Agent's internal state."""
    name: str = "unnamed"
    mood: str = "neutral"
    energy: float = 100.0


class Inbox(Component):
    """Messages received by this agent (JSON-encoded strings)."""
    messages: list[str] = []


class Outbox(Component):
    """Messages to be sent by this agent (JSON-encoded strings)."""
    messages: list[str] = []


# =============================================================================
# Resource: Simulation Configuration
# =============================================================================


@dataclass
class SimConfig:
    """Environment parameters - available via resources."""
    greeting_boost: float = 10.0  # Energy boost when receiving a greeting
    max_messages_per_tick: int = 5


@dataclass
class BrokerChannel:
    """Shared broker queue key for the demo."""

    key: str = "messaging-demo"


# =============================================================================
# Processors
# =============================================================================


class GreetingProcessor(AsyncProcessor):
    """
    Agents send greetings to each other via the MESSAGE command.
    Demonstrates Resources access for broker and config.
    """
    components = (AgentState, Outbox)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        """Generate greeting messages and enqueue via broker."""
        broker = resources.require(CommandBroker)
        channel = resources.require(BrokerChannel).key
        resources.require(SimConfig)  # validate config exists

        # Collect entities to process
        rows = df.select("entity_id", "agentstate__name").collect().to_pylist()
        
        # Each agent sends a greeting to all others
        for sender in rows:
            for receiver in rows:
                if sender["entity_id"] != receiver["entity_id"]:
                    # Enqueue MESSAGE command via broker
                    cmd = Command(
                        type=CommandType.MESSAGE,
                        tick=tick,
                        payload={
                            "sender_id": sender["entity_id"],
                            "receiver_id": receiver["entity_id"],
                            "content": f"Hello from {sender['agentstate__name']}!",
                        },
                    )
                    await broker.enqueue(channel, cmd)
        
        return df


class MessageRealizationProcessor(AsyncProcessor):
    """
    Realizes MESSAGE commands from the broker into agent Inboxes.
    This runs early (low priority) to populate inboxes before other processors.
    """
    components = (Inbox,)
    priority = -100  # Run early

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        """Drain MESSAGE commands from broker and populate inboxes."""
        broker = resources.require(CommandBroker)
        channel = resources.require(BrokerChannel).key
        resources.require(SimConfig)  # validate config exists

        # Dequeue all pending MESSAGE commands
        cmds = await broker.dequeue(channel, max_items=1000)
        message_cmds = [c for c in cmds if c.type == CommandType.MESSAGE]
        
        if not message_cmds:
            return df
        
        # Group messages by receiver (as JSON strings)
        messages_by_receiver: dict[int, list[str]] = {}
        for cmd in message_cmds:
            receiver_id = cmd.payload["receiver_id"]
            msg = json.dumps({
                "sender_id": cmd.payload["sender_id"],
                "content": cmd.payload["content"],
                "tick": tick,
            })
            messages_by_receiver.setdefault(receiver_id, []).append(msg)
        
        # Update inboxes via batch UDF
        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def update_inbox(entity_ids: daft.Series, current_inboxes: daft.Series) -> list:
            results = []
            for eid, inbox in zip(entity_ids.to_pylist(), current_inboxes.to_pylist(), strict=False):
                inbox = list(inbox) if inbox else []
                new_msgs = messages_by_receiver.get(eid, [])
                results.append(inbox + new_msgs)
            return results
        
        return df.with_column(
            "inbox__messages",
            update_inbox(col("entity_id"), col("inbox__messages"))
        )


class MoodProcessor(AsyncProcessor):
    """
    Updates agent mood and energy based on received messages.
    """
    components = (AgentState, Inbox)
    priority = 20

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        **kwargs,
    ) -> DataFrame:
        """Process inbox messages and update mood."""
        config = resources.require(SimConfig)
        
        @daft.func.batch(return_dtype=daft.DataType.float64())
        def calculate_energy_boost(inboxes: daft.Series) -> list:
            results = []
            for inbox in inboxes.to_pylist():
                inbox = list(inbox) if inbox else []
                # Each message gives a boost
                boost = len(inbox) * config.greeting_boost
                results.append(boost)
            return results
        
        @daft.func.batch(return_dtype=daft.DataType.string())
        def calculate_mood(inboxes: daft.Series) -> list:
            results = []
            for inbox in inboxes.to_pylist():
                inbox = list(inbox) if inbox else []
                if len(inbox) >= 2:
                    results.append("happy")
                elif len(inbox) == 1:
                    results.append("content")
                else:
                    results.append("lonely")
            return results
        
        return (
            df
            .with_column("_boost", calculate_energy_boost(col("inbox__messages")))
            .with_column("agentstate__energy", col("agentstate__energy") + col("_boost"))
            .with_column("agentstate__mood", calculate_mood(col("inbox__messages")))
            .exclude("_boost")
        )


# =============================================================================
# Main Demo
# =============================================================================


async def main():
    print("=" * 60)
    print("Archetype Messaging Demo: Resources + MESSAGE + Hooks")
    print("=" * 60)

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "demo",
            storage=StorageConfig(uri="./archetype_data", namespace="messaging_demo"),
            processors=[
                MessageRealizationProcessor(),
                GreetingProcessor(),
                MoodProcessor(),
            ],
            resources=[
                SimConfig(greeting_boost=15.0),
                BrokerChannel(),
            ],
        )

        async def on_pre_tick(world, tick, **kwargs):
            print(f"\n→ Pre-tick {tick}: Starting processing...")

        async def on_post_tick(world, tick, **kwargs):
            print(f"← Post-tick {tick}: Completed!")
            broker = world.resources.require(CommandBroker)
            channel = world.resources.require(BrokerChannel).key
            pending = await broker.get_pending_count(channel)
            print(f"   Messages pending in broker: {pending}")

        world.add_hook("pre_tick", on_pre_tick)
        world.add_hook("post_tick", on_post_tick)

        print("\n✓ Runtime world staged with resources, hooks, and processors")

        for name in ("Alice", "Bob", "Charlie"):
            await world.spawn(AgentState(name=name), Inbox(), Outbox())

        print("✓ Created 3 agents: Alice, Bob, Charlie")

        print("\n" + "-" * 60)
        print("Running 3 ticks...")
        print("-" * 60)
        await world.run(steps=3)

        print("\n" + "=" * 60)
        print("Final State")
        print("=" * 60)

        final_df = await world.query(AgentState, Inbox)
        final_df.select(
            "entity_id",
            "agentstate__name",
            "agentstate__mood",
            "agentstate__energy",
        ).show()

        rows = final_df.select(
            "entity_id",
            "agentstate__name",
            "inbox__messages",
        ).collect().to_pylist()
        print("\nMessage counts:")
        for row in rows:
            msgs = row.get("inbox__messages") or []
            print(f"  {row['agentstate__name']}: {len(msgs)} messages received")

        print("\n" + "-" * 60)
        print("Broker Message History (last 10)")
        print("-" * 60)
        broker = world.resources.require(CommandBroker)
        channel = world.resources.require(BrokerChannel).key
        history = await broker.get_history(channel, limit=10)
        for cmd in history[-10:]:
            if cmd.type == CommandType.MESSAGE:
                content = cmd.payload["content"]
                if len(content) > 25:
                    content = content[:25] + "..."
                print(
                    f"  tick={cmd.tick}: agent {cmd.payload['sender_id']} "
                    f"→ agent {cmd.payload['receiver_id']}: {content}"
                )


if __name__ == "__main__":
    asyncio.run(main())
