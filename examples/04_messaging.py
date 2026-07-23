#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Messaging Example: Resources, Shared State, and Hooks
=====================================================

Demonstrates:
1. Resources - Type-safe dependency injection for shared state
2. Shared mailbox resource - Agent-to-agent communication via a resource processors read/write
3. Hooks - Lifecycle callbacks for observability

Run: uv run python examples/04_messaging.py
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import cast

import daft
from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component, Resources, StorageConfig
from archetype.core.hooks import PostTick, PreTick

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
# Resources
# =============================================================================


@dataclass
class SimConfig:
    """Environment parameters - available via resources."""

    greeting_boost: float = 10.0  # Energy boost when receiving a greeting
    max_messages_per_tick: int = 5


@dataclass
class Mailbox:
    """Shared mailbox resource: processors deposit messages here, then drain them."""

    pending: list[dict] = field(default_factory=list)
    delivered: int = 0


# =============================================================================
# Processors
# =============================================================================


class GreetingProcessor(AsyncProcessor):
    """
    Agents send greetings to each other via the shared Mailbox resource.
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
        """Generate greeting messages and deposit into the shared Mailbox."""
        mailbox = resources.require(Mailbox)
        resources.require(SimConfig)  # validate config exists

        # Collect entities to process
        rows = df.select("entity_id", "agentstate__name").collect().to_pylist()

        # Each agent sends a greeting to all others
        for sender in rows:
            for receiver in rows:
                if sender["entity_id"] != receiver["entity_id"]:
                    mailbox.pending.append(
                        {
                            "sender_id": sender["entity_id"],
                            "receiver_id": receiver["entity_id"],
                            "content": f"Hello from {sender['agentstate__name']}!",
                            "tick": tick,
                        }
                    )

        return df


class MessageRealizationProcessor(AsyncProcessor):
    """
    Realizes messages from the shared Mailbox into agent Inboxes.
    Runs early (low priority) to populate inboxes before other processors.
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
        """Drain Mailbox and populate inboxes."""
        mailbox = resources.require(Mailbox)
        resources.require(SimConfig)  # validate config exists

        if not mailbox.pending:
            return df

        # Drain all pending messages
        messages = mailbox.pending[:]
        mailbox.pending.clear()
        mailbox.delivered += len(messages)

        # Group messages by receiver (as JSON strings)
        messages_by_receiver: dict[int, list[str]] = {}
        for msg in messages:
            receiver_id = msg["receiver_id"]
            encoded = json.dumps(
                {
                    "sender_id": msg["sender_id"],
                    "content": msg["content"],
                    "tick": msg["tick"],
                }
            )
            messages_by_receiver.setdefault(receiver_id, []).append(encoded)

        # Update inboxes via batch UDF
        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def update_inbox(entity_ids: daft.Series, current_inboxes: daft.Series) -> list:
            results = []
            for eid, inbox in zip(
                entity_ids.to_pylist(), current_inboxes.to_pylist(), strict=False
            ):
                inbox = list(inbox) if inbox else []
                new_msgs = messages_by_receiver.get(eid, [])
                results.append(inbox + new_msgs)
            return results

        return df.with_column(
            "inbox__messages", update_inbox(col("entity_id"), col("inbox__messages"))
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
            df.with_column("_boost", calculate_energy_boost(col("inbox__messages")))
            .with_column("agentstate__energy", col("agentstate__energy") + col("_boost"))
            .with_column("agentstate__mood", calculate_mood(col("inbox__messages")))
            .exclude("_boost")
        )


# =============================================================================
# Main Demo
# =============================================================================


async def run_demo(storage_uri: str, *, verbose: bool = False) -> dict[str, object]:
    """Run the mailbox protocol and return exact, normalized semantics."""
    mailbox = Mailbox()
    hook_order: list[str] = []

    async def on_pre_tick(event: PreTick) -> None:
        hook_order.append(f"pre:{event.tick}")
        if verbose:
            print(f"\n-> Pre-tick {event.tick}: Starting processing...")

    async def on_post_tick(event: PostTick) -> None:
        hook_order.append(f"post:{event.tick - 1}")
        if verbose:
            print(f"<- Post-tick {event.tick - 1}: Completed!")
            print(f"   Messages delivered so far: {mailbox.delivered}")
            print(f"   Messages pending in mailbox: {len(mailbox.pending)}")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "demo",
            storage=StorageConfig(uri=storage_uri, namespace="messaging_demo"),
            processors=[
                MessageRealizationProcessor(),
                GreetingProcessor(),
                MoodProcessor(),
            ],
            resources=[
                SimConfig(greeting_boost=15.0),
                mailbox,
            ],
            hooks=[
                (PreTick, on_pre_tick),
                (PostTick, on_post_tick),
            ],
        )

        for name in ("Alice", "Bob", "Charlie"):
            await world.spawn(AgentState(name=name), Inbox(), Outbox())

        result = await world.run(steps=3)

        final_df = await world.query(AgentState, Inbox)
        rows = (
            final_df.where(col("tick") == result.final_tick - 1)
            .select(
                "agentstate__name",
                "agentstate__mood",
                "agentstate__energy",
                "inbox__messages",
            )
            .collect()
            .to_pylist()
        )
        agents = [
            {
                "name": row["agentstate__name"],
                "mood": row["agentstate__mood"],
                "energy": row["agentstate__energy"],
                "messages": len(row.get("inbox__messages") or []),
            }
            for row in sorted(rows, key=lambda item: item["agentstate__name"])
        ]
        return {
            "ticks_completed": result.ticks_completed,
            "agents": agents,
            "messages_delivered": mailbox.delivered,
            "messages_pending": len(mailbox.pending),
            "hook_order": hook_order,
        }


async def main() -> None:
    print("=" * 60)
    print("Archetype Messaging Demo: Resources + Shared Mailbox + Hooks")
    print("=" * 60)
    result = await run_demo("./archetype_data", verbose=True)
    print("\nFinal State")
    agents = cast(list[dict[str, object]], result["agents"])
    for agent in agents:
        print(
            f"  {agent['name']}: mood={agent['mood']}, energy={agent['energy']}, "
            f"messages={agent['messages']}"
        )
    print(f"\nTotal messages delivered: {result['messages_delivered']}")


if __name__ == "__main__":
    asyncio.run(main())
