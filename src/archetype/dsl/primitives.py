# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
DSL Primitives: Broadcast, Inbox, Message Realization

These primitives are implemented as Daft UDFs, keeping everything
in the lazy DAG for proper batching and optimization.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import daft
from daft import col

from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.dsl.core import AgentProxy, World


# =============================================================================
# Inbox Component
# =============================================================================


class Inbox(Component):
    """
    Built-in component for receiving messages.
    Messages stored as JSON string for Arrow compatibility.
    """

    messages_json: str = "[]"


# =============================================================================
# Broadcaster: @daft.cls UDF with broker DI
# =============================================================================


@daft.cls()
class Broadcaster:
    """
    Daft class UDF that broadcasts messages via the CommandBroker.

    This keeps the broadcast operation in the DAG, allowing Daft to
    optimize and batch the operations properly.

    Usage in processor:
        broadcaster = Broadcaster(broker, world_name, all_agent_names, tick)
        df = df.with_column(
            "_broadcast_result",
            broadcaster.send(
                col("entity_id"),
                col("agentperspective__name"),
                col("_response"),
            )
        )
    """

    def __init__(
        self,
        broker: CommandBroker,
        world_name: str,
        all_agent_names: list[str],
        tick: int,
    ):
        self.broker = broker
        self.world_name = world_name
        self.all_agent_names = all_agent_names
        self.tick = tick
        self._loop = None

    def send(
        self,
        sender_id: int,
        sender_name: str,
        content: str,
    ) -> str:
        """
        Broadcast content to all other agents.
        Returns "ok" on success for column result.
        """
        import asyncio

        # Get or create event loop for async broker calls
        try:
            self._loop = asyncio.get_event_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        # Send to all agents except sender
        for receiver_name in self.all_agent_names:
            if receiver_name != sender_name:
                cmd = Command(
                    type=CommandType.MESSAGE,
                    tick=self.tick,
                    payload={
                        "sender_id": sender_id,
                        "sender_name": sender_name,
                        "receiver_name": receiver_name,
                        "content": content or "",
                    },
                )
                # Run async enqueue in sync context
                self._loop.run_until_complete(self.broker.enqueue(self.world_name, cmd))

        return "ok"


# =============================================================================
# Message Realizer: @daft.cls UDF for inbox updates
# =============================================================================


@daft.cls()
class MessageRealizer:
    """
    Daft class UDF that realizes MESSAGE commands into agent inboxes.

    This runs early in the tick to populate inboxes before other processors.

    Usage:
        realizer = MessageRealizer(messages_by_receiver)
        df = df.with_column(
            "inbox__messages_json",
            realizer.realize(col("entity_id"), col("inbox__messages_json"))
        )
    """

    def __init__(self, messages_by_receiver: dict[int, list[dict]]):
        self.messages_by_receiver = messages_by_receiver

    def realize(self, entity_id: int, current_json: str) -> str:
        """Add new messages to inbox, return updated JSON."""
        current = json.loads(current_json) if current_json else []
        new_msgs = self.messages_by_receiver.get(entity_id, [])
        return json.dumps(current + new_msgs)


# =============================================================================
# Async Broadcast Helper (for use outside DAG)
# =============================================================================


async def broadcast(
    content: Any,
    sender: AgentProxy = None,
    metadata: dict[str, Any] = None,
    exclude: list[AgentProxy] = None,
    world: World = None,
) -> None:
    """
    Broadcast a message to all other agents (async version for use in behaviors).

    Note: For DataFrame operations, use the Broadcaster @daft.cls instead.
    This function is for imperative code in behavior.act() methods.
    """
    if world is None:
        raise ValueError("broadcast() requires world parameter")

    if world.broker is None:
        raise ValueError("World has no broker configured")

    all_agents = world.agents
    exclude_ids = {a.entity_id for a in (exclude or [])}

    # Serialize content
    if isinstance(content, dict | list):
        content_str = json.dumps(content)
    else:
        content_str = str(content)

    # Get sender info
    sender_id = sender.entity_id if sender else None
    sender_name = _get_agent_name(sender) if sender else "unknown"

    # Enqueue MESSAGE for each recipient
    for recipient in all_agents:
        if recipient.entity_id in exclude_ids:
            continue

        recipient_name = _get_agent_name(recipient)

        cmd = Command(
            type=CommandType.MESSAGE,
            payload={
                "sender_id": sender_id,
                "sender_name": sender_name,
                "receiver_id": recipient.entity_id,
                "receiver_name": recipient_name,
                "content": content_str,
                **(metadata or {}),
            },
        )

        await world.broker.enqueue(world.name, cmd)


def _get_agent_name(agent: AgentProxy) -> str:
    """Extract name from agent proxy."""
    for attr in ["perspective", "agent_state", "identity", "profile"]:
        if hasattr(agent, attr):
            comp = getattr(agent, attr)
            if hasattr(comp, "name"):
                return comp.name
    return f"agent_{agent.entity_id}"


# =============================================================================
# DataFrame-Native Broadcast (returns expression for DAG)
# =============================================================================


def create_broadcaster(
    broker: CommandBroker,
    world_name: str,
    agent_names: list[str],
    tick: int,
) -> Broadcaster:
    """
    Create a Broadcaster instance for use in DataFrame operations.

    Usage in processor:
        broadcaster = create_broadcaster(broker, "my_world", ["Alice", "Bob"], tick)
        df = df.with_column("_sent", broadcaster.send(
            col("entity_id"),
            col("agentperspective__name"),
            col("_response"),
        ))
    """
    return Broadcaster(broker, world_name, agent_names, tick)


# =============================================================================
# Message Collection Helper
# =============================================================================


@dataclass
class MessageCollection:
    """Collection of messages with rendering helpers."""

    messages: list[dict] = field(default_factory=list)

    def render(self, max_per_sender: int = 2) -> str:
        """Render messages as readable text for LLM context."""
        if not self.messages:
            return ""

        lines = ["Recent messages:"]
        by_sender: dict[str, list[dict]] = {}

        for msg in self.messages:
            sender = msg.get("sender_name", "unknown")
            by_sender.setdefault(sender, []).append(msg)

        for sender, msgs in by_sender.items():
            for msg in msgs[-max_per_sender:]:
                content = msg.get("content", "")[:200]
                lines.append(f"- {sender}: {content}")

        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self.messages)

    def __iter__(self):
        return iter(self.messages)


def inbox_recent(agent: AgentProxy, limit: int = 10) -> MessageCollection:
    """Get recent messages from an agent's inbox."""
    try:
        inbox = agent.inbox
        messages_json = inbox.messages_json
        messages = json.loads(messages_json) if messages_json else []
        return MessageCollection(messages=messages[-limit:])
    except (AttributeError, json.JSONDecodeError):
        return MessageCollection()


# =============================================================================
# JSON Column Helpers (wrapping daft's json functions)
# =============================================================================


def json_column(col_name: str) -> daft.Expression:
    """
    Deserialize a JSON string column to native type.

    Usage:
        df.with_column("history_list", json_column("history_json"))
    """
    return col(col_name).str.json_decode()


def to_json_column(expr: daft.Expression) -> daft.Expression:
    """
    Serialize a column to JSON string.

    Usage:
        df.with_column("history_json", to_json_column(col("history_list")))
    """
    # Daft's json_encode is on struct/list columns
    return expr.to_json()
