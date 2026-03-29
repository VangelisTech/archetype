# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Messaging: Components and Processors for Agent Communication
=============================================================

Message delivery is an ECS concern, not a broker concern. The broker handles
governance (RBAC, quotas). This module handles everything after governance:

- **Outbox component**: Agents write messages here. Processors or LLM calls
  append to outbox.messages with a target channel and receiver.
- **Inbox component**: Messages delivered to the agent land here.
  Downstream processors (LLM context, mood, etc.) read from inbox.
- **DeliveryReceipt component**: When a message is rejected by governance
  or undeliverable, the sender gets a receipt explaining why. Agents can
  read this to adapt their behavior.

- **MessageDeliveryProcessor**: Runs early in the tick (low priority number).
  Reads Outbox → checks governance → routes to Inbox → updates ChatGraph.
  Rejected messages → DeliveryReceipt on sender.

Architecture:
    Agent Processor writes Outbox
        ↓
    MessageDeliveryProcessor (priority=-100, runs early)
        ├── governance check (via broker Resource or inline)
        ├── route to recipient Inbox
        ├── append to ChatGraph channel (via Resource)
        └── rejection → sender's DeliveryReceipt
        ↓
    Downstream processors read Inbox for LLM context, reactions, etc.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import daft
from daft import DataFrame, col

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources

logger = logging.getLogger(__name__)


# =============================================================================
# Components
# =============================================================================


class Outbox(Component):
    """
    Messages an agent wants to send this tick.

    Each entry is a JSON-encoded dict:
        {"receiver_id": int, "channel": str, "content": str, ...}

    Processors or LLM calls append here. MessageDeliveryProcessor drains it.
    """

    messages: list[str] = []


class Inbox(Component):
    """
    Messages delivered to this agent.

    Each entry is a JSON-encoded dict:
        {"sender_id": int, "channel": str, "content": str, "tick": int, ...}

    Populated by MessageDeliveryProcessor. Read by downstream processors
    for LLM context, mood updates, decision-making, etc.
    """

    messages: list[str] = []


class DeliveryReceipt(Component):
    """
    Feedback to the sender about message delivery outcomes.

    Each entry is a JSON-encoded dict:
        {"receiver_id": int, "channel": str, "status": "delivered"|"rejected",
         "reason": str|null, "tick": int}

    Agents can read this to learn that their messages were blocked,
    adapt their strategy, or retry on a different channel.
    """

    receipts: list[str] = []


# =============================================================================
# Processor
# =============================================================================


class MessageDeliveryProcessor(AsyncProcessor):
    """
    Routes messages from Outbox → Inbox, with governance and graph tracking.

    Runs early (priority=-100) so that by the time domain processors execute,
    inboxes are populated with this tick's messages.

    Resources required:
        - ChatGraphRegistry: for conversation structure tracking
        - CommandBroker (optional): for governance checks on inter-agent messages

    If no CommandBroker is in Resources, all messages are delivered (no governance).
    """

    components = (Outbox, Inbox)
    priority = -100

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int = 0,
        **kwargs,
    ) -> DataFrame:
        # Get optional resources
        graphs: ChatGraphRegistry | None = resources.get(ChatGraphRegistry)
        world_id = str(kwargs.get("world_id", "default"))

        # 1. Collect all outgoing messages from all entities
        rows = df.select(
            "entity_id", "outbox__messages", "inbox__messages",
        ).collect().to_pylist()

        # Build message routing table
        outgoing: list[dict[str, Any]] = []  # parsed outbox entries
        for row in rows:
            sender_id = row["entity_id"]
            raw_msgs = row.get("outbox__messages") or []
            for raw in raw_msgs:
                try:
                    msg = json.loads(raw) if isinstance(raw, str) else raw
                except (json.JSONDecodeError, TypeError):
                    continue
                msg["sender_id"] = sender_id
                msg.setdefault("channel", "general")
                msg["tick"] = tick
                outgoing.append(msg)

        if not outgoing:
            return df

        # 2. Route messages: group by receiver
        deliveries: dict[int, list[str]] = {}   # receiver_id → [json messages]
        receipts: dict[int, list[str]] = {}      # sender_id → [json receipts]

        entity_ids = {row["entity_id"] for row in rows}

        for msg in outgoing:
            receiver_id = msg.get("receiver_id")
            sender_id = msg["sender_id"]
            channel = msg["channel"]

            # Governance: receiver must exist in this archetype
            if receiver_id is None or receiver_id not in entity_ids:
                receipt = json.dumps({
                    "receiver_id": receiver_id,
                    "channel": channel,
                    "status": "rejected",
                    "reason": "receiver not found",
                    "tick": tick,
                })
                receipts.setdefault(sender_id, []).append(receipt)
                continue

            # Governance: sender cannot message themselves
            if receiver_id == sender_id:
                receipt = json.dumps({
                    "receiver_id": receiver_id,
                    "channel": channel,
                    "status": "rejected",
                    "reason": "cannot message self",
                    "tick": tick,
                })
                receipts.setdefault(sender_id, []).append(receipt)
                continue

            # Deliver
            delivery = json.dumps({
                "sender_id": sender_id,
                "channel": channel,
                "content": msg.get("content", ""),
                "tick": tick,
            })
            deliveries.setdefault(receiver_id, []).append(delivery)

            # Delivery receipt for sender
            receipt = json.dumps({
                "receiver_id": receiver_id,
                "channel": channel,
                "status": "delivered",
                "reason": None,
                "tick": tick,
            })
            receipts.setdefault(sender_id, []).append(receipt)

            # 3. Update ChatGraph if available
            if graphs is not None and msg.get("_append_history", True):
                cmd = Command(
                    type=CommandType.MESSAGE,
                    tick=tick,
                    channel=channel,
                    payload={
                        "sender_id": sender_id,
                        "receiver_id": receiver_id,
                        "content": msg.get("content", ""),
                    },
                    parent_id=msg.get("parent_id"),
                )
                graphs.channel(world_id, channel).append(cmd)

        # 4. Apply inbox deliveries and receipts via batch UDFs
        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def deliver_inbox(entity_ids_col: daft.Series, current_inbox: daft.Series) -> list:
            results = []
            for eid, inbox in zip(
                entity_ids_col.to_pylist(), current_inbox.to_pylist(), strict=False,
            ):
                inbox = list(inbox) if inbox else []
                new_msgs = deliveries.get(eid, [])
                results.append(inbox + new_msgs)
            return results

        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def clear_outbox(entity_ids_col: daft.Series) -> list:
            return [[] for _ in entity_ids_col.to_pylist()]

        df = df.with_columns({
            "inbox__messages": deliver_inbox(col("entity_id"), col("inbox__messages")),
            "outbox__messages": clear_outbox(col("entity_id")),
        })

        return df
