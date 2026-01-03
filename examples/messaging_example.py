#!/usr/bin/env python3
"""
Messaging Example: Resources, MESSAGE Commands, and Hooks

Demonstrates the three new features:
1. Resources - Type-safe dependency injection for shared state
2. MESSAGE CommandType - Agent-to-agent communication via broker
3. Hooks - Lifecycle callbacks for observability

Run: uv run python examples/messaging_example.py
"""

import asyncio
import json
from dataclasses import dataclass

import daft
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.resources import Resources
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType


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
        config = resources.require(SimConfig)
        
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
                    await broker.enqueue("demo_world", cmd)
        
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
        config = resources.require(SimConfig)
        
        # Dequeue all pending MESSAGE commands
        cmds = await broker.dequeue("demo_world", max_items=1000)
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
            for eid, inbox in zip(entity_ids.to_pylist(), current_inboxes.to_pylist()):
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
# Minimal Async Infrastructure (no persistence for this demo)
# =============================================================================


class InMemoryQuerier:
    """Minimal querier that returns empty DataFrames for tick 0, or stored data."""
    
    def __init__(self):
        self._store: dict[tuple, DataFrame] = {}
    
    async def query_archetype(self, **kwargs):
        from archetype.core.archetype import Archetype
        import pyarrow as pa
        sig = kwargs["sig"]
        ticks = kwargs.get("ticks", [0])
        
        # Return stored data if available
        key = (sig, ticks[0] if ticks else 0)
        if key in self._store:
            return self._store[key]
        
        # Return empty DataFrame with correct schema
        schema = Archetype.get_archetype_schema(sig)
        return daft.from_arrow(pa.Table.from_batches([], schema=schema))
    
    def store(self, sig, tick, df):
        """Store a snapshot for later queries."""
        self._store[(sig, tick)] = df


class InMemoryUpdater:
    """Minimal updater that stamps tick and returns the DataFrame."""
    
    def __init__(self, querier: InMemoryQuerier):
        self._querier = querier
    
    async def update(self, df, sig, tick, world_id, run_id):
        # Stamp metadata
        df = (
            df
            .with_column("tick", daft.lit(tick))
            .with_column("world_id", daft.lit(str(world_id)))
            .with_column("run_id", daft.lit(str(run_id)))
        )
        df_mat = df.collect()
        
        # Store for next tick's queries
        self._querier.store(sig, tick, df_mat)
        
        return df_mat


# =============================================================================
# Main Demo
# =============================================================================


async def main():
    print("=" * 60)
    print("Archetype Messaging Demo: Resources + MESSAGE + Hooks")
    print("=" * 60)
    
    # Create world
    world_config = WorldConfig(name="demo")
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    
    world = AsyncWorld(
        world_config=world_config,
        querier=querier,
        updater=updater,
        system=system,
    )
    
    # Setup Resources
    broker = CommandBroker()
    config = SimConfig(greeting_boost=15.0)
    
    world.resources.insert(broker)
    world.resources.insert(config)
    
    print(f"\n✓ Resources registered: {world.resources}")
    
    # Setup Hooks
    async def on_pre_tick(world, tick, **kwargs):
        print(f"\n→ Pre-tick {tick}: Starting processing...")
    
    async def on_post_tick(world, tick, **kwargs):
        print(f"← Post-tick {tick}: Completed!")
        # Show pending message count
        pending = await broker.get_pending_count("demo_world")
        print(f"   Messages pending in broker: {pending}")
    
    world.add_hook("pre_tick", on_pre_tick)
    world.add_hook("post_tick", on_post_tick)
    
    print("✓ Hooks registered: pre_tick, post_tick")
    
    # Register Processors
    await system.add_processor(MessageRealizationProcessor())
    await system.add_processor(GreetingProcessor())
    await system.add_processor(MoodProcessor())
    
    print("✓ Processors registered: MessageRealization, Greeting, Mood")
    
    # Create Agents
    agents = [
        [AgentState(name="Alice", mood="neutral", energy=100.0), Inbox(), Outbox()],
        [AgentState(name="Bob", mood="neutral", energy=100.0), Inbox(), Outbox()],
        [AgentState(name="Charlie", mood="neutral", energy=100.0), Inbox(), Outbox()],
    ]
    
    for components in agents:
        await world.create_entity(components)
    
    print(f"✓ Created {len(agents)} agents: Alice, Bob, Charlie")
    
    # Run simulation
    print("\n" + "-" * 60)
    print("Running 3 ticks...")
    print("-" * 60)
    
    run_config = RunConfig(num_steps=3)
    await world.run(run_config)
    
    # Show final state
    print("\n" + "=" * 60)
    print("Final State")
    print("=" * 60)
    
    for sig, df in world._live.items():
        print(f"\nArchetype: {[c.__name__ for c in sig]}")
        df.select(
            "entity_id",
            "agentstate__name",
            "agentstate__mood",
            "agentstate__energy",
        ).show()
        
        # Show message counts
        rows = df.select("entity_id", "agentstate__name", "inbox__messages").collect().to_pylist()
        print("\n  Message counts:")
        for row in rows:
            msgs = row.get("inbox__messages") or []
            print(f"    {row['agentstate__name']}: {len(msgs)} messages received")
    
    # Show message history
    print("\n" + "-" * 60)
    print("Broker Message History (last 10)")
    print("-" * 60)
    history = await broker.get_history("demo_world", limit=10)
    for cmd in history[-10:]:
        if cmd.type == CommandType.MESSAGE:
            content = cmd.payload['content'][:25] + "..." if len(cmd.payload['content']) > 25 else cmd.payload['content']
            print(f"  tick={cmd.tick}: agent {cmd.payload['sender_id']} → agent {cmd.payload['receiver_id']}: {content}")


if __name__ == "__main__":
    asyncio.run(main())
