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

import daft
from daft import DataFrame, col, lit
from daft.datatype import DataType
from daft.functions import try_deserialize

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources
from archetype.core.side_effects import SideEffectCollector

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
    Routes messages from Outbox → Inbox and updates conversation graph state.

    Runs early (priority=-100) so that by the time domain processors execute,
    inboxes are populated with this tick's messages.

    Resources used:
        - ChatGraphRegistry (optional): for conversation structure tracking
        - SideEffectCollector (optional): for deferred graph mutations (tick atomicity)
    """

    components = (Outbox, Inbox, DeliveryReceipt)
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
        collector: SideEffectCollector | None = resources.get(SideEffectCollector)
        world_id = str(kwargs.get("world_id", "default"))

        # --- 1. Explode outbox messages into individual rows ---
        msgs = df.select(
            col("entity_id").alias("sender_id"),
            col("outbox__messages"),
        ).explode(col("outbox__messages"))

        # Drop rows where the exploded value is null (from empty outbox lists)
        msgs = msgs.where(col("outbox__messages").not_null())

        # Early exit: no outbox messages to route, nothing to do
        if msgs.count_rows() == 0:
            return df

        # --- 2. Parse JSON messages into structured fields ---
        msg_struct_type = DataType.struct(
            {
                "receiver_id": DataType.int64(),
                "channel": DataType.string(),
                "content": DataType.string(),
                "_append_history": DataType.bool(),
                "parent_id": DataType.string(),
            }
        )

        msgs = msgs.with_column(
            "_parsed",
            try_deserialize(col("outbox__messages"), format="json", dtype=msg_struct_type),
        )

        # Drop rows where JSON parsing failed (null _parsed)
        msgs = msgs.where(col("_parsed").not_null())

        # Extract fields from the parsed struct, applying defaults
        msgs = msgs.with_columns(
            {
                "receiver_id": col("_parsed")["receiver_id"],
                "channel": col("_parsed")["channel"].fill_null(lit("general")),
                "content": col("_parsed")["content"].fill_null(lit("")),
                "_append_history": col("_parsed")["_append_history"].fill_null(lit(True)),
                "parent_id": col("_parsed")["parent_id"],
            }
        )

        # Drop rows where receiver_id is null (invalid message)
        msgs = msgs.where(col("receiver_id").not_null())

        # Drop the intermediate columns
        msgs = msgs.exclude("outbox__messages", "_parsed")

        # --- 3. Route messages via join: validate receivers exist ---
        entities = df.select(col("entity_id"))

        # Inner join: only messages whose receiver_id matches a valid entity_id
        routed = msgs.join(
            entities,
            left_on="receiver_id",
            right_on="entity_id",
            how="inner",
        )

        # Filter out self-messaging
        routed = routed.where(col("sender_id") != col("receiver_id"))

        # --- 4. Build rejection receipts ---
        # Messages to non-existent receivers (anti-join)
        rejected_no_receiver = msgs.join(
            entities,
            left_on="receiver_id",
            right_on="entity_id",
            how="anti",
        )

        # Messages to self
        msgs_with_valid_receiver = msgs.join(
            entities,
            left_on="receiver_id",
            right_on="entity_id",
            how="inner",
        )
        rejected_self = msgs_with_valid_receiver.where(col("sender_id") == col("receiver_id"))

        # Build rejection receipt JSON strings
        @daft.func
        def make_reject_receipt_no_receiver(receiver_id: int, channel: str) -> str:
            return json.dumps(
                {
                    "receiver_id": receiver_id,
                    "channel": channel,
                    "status": "rejected",
                    "reason": "receiver not found",
                    "tick": tick,
                }
            )

        @daft.func
        def make_reject_receipt_self(receiver_id: int, channel: str) -> str:
            return json.dumps(
                {
                    "receiver_id": receiver_id,
                    "channel": channel,
                    "status": "rejected",
                    "reason": "cannot message self",
                    "tick": tick,
                }
            )

        rejected_receipts_no_receiver = rejected_no_receiver.select(
            col("sender_id"),
            make_reject_receipt_no_receiver(col("receiver_id"), col("channel")).alias(
                "receipt_json"
            ),
        )

        rejected_receipts_self = rejected_self.select(
            col("sender_id"),
            make_reject_receipt_self(col("receiver_id"), col("channel")).alias("receipt_json"),
        )

        # --- 5. Build delivery receipt JSON strings for successful deliveries ---
        @daft.func
        def make_delivery_receipt(receiver_id: int, channel: str) -> str:
            return json.dumps(
                {
                    "receiver_id": receiver_id,
                    "channel": channel,
                    "status": "delivered",
                    "reason": None,
                    "tick": tick,
                }
            )

        delivered_receipts = routed.select(
            col("sender_id"),
            make_delivery_receipt(col("receiver_id"), col("channel")).alias("receipt_json"),
        )

        # Combine all receipts and group by sender
        all_receipts = delivered_receipts.concat(rejected_receipts_no_receiver).concat(
            rejected_receipts_self
        )

        receipt_agg = all_receipts.groupby("sender_id").agg(
            col("receipt_json").list_agg().alias("new_receipts")
        )

        # --- 6. Build delivery JSON strings and aggregate by receiver ---
        @daft.func
        def make_delivery_json(sender_id: int, channel: str, content: str) -> str:
            return json.dumps(
                {
                    "sender_id": sender_id,
                    "channel": channel,
                    "content": content,
                    "tick": tick,
                }
            )

        inbox_rows = routed.select(
            col("receiver_id"),
            make_delivery_json(col("sender_id"), col("channel"), col("content")).alias(
                "delivery_json"
            ),
        )

        inbox_agg = inbox_rows.groupby("receiver_id").agg(
            col("delivery_json").list_agg().alias("new_inbox")
        )

        # --- 7. Update ChatGraph via SideEffectCollector (deferred) ---
        if graphs is not None:
            # Filter to messages that should be appended to history
            routed_for_graph = routed.where(col("_append_history"))
            routed_for_graph = routed_for_graph.select(
                "sender_id", "receiver_id", "channel", "content", "parent_id"
            )

            # Justified .collect(): need cross-row context for ChatGraph Command construction
            graph_rows = routed_for_graph.collect().to_pylist()
            for row in graph_rows:
                parent_id = row.get("parent_id")
                # parent_id is stored as string in the struct; convert if present
                parent_uuid = None
                if parent_id is not None:
                    try:
                        import uuid_utils as uuid

                        parent_uuid = uuid.UUID(parent_id)
                    except (ValueError, TypeError):
                        parent_uuid = None

                cmd = Command(
                    type=CommandType.MESSAGE,
                    tick=tick,
                    channel=row["channel"],
                    payload={
                        "sender_id": row["sender_id"],
                        "receiver_id": row["receiver_id"],
                        "content": row["content"],
                    },
                    parent_id=parent_uuid,
                )
                channel = row["channel"]
                if collector is not None:
                    collector.defer(
                        lambda c=cmd, ch=channel: graphs.channel(world_id, ch).append(c)
                    )
                else:
                    graphs.channel(world_id, channel).append(cmd)

        # --- 8. Merge inbox updates back into the main DataFrame ---
        df = df.join(
            inbox_agg,
            left_on="entity_id",
            right_on="receiver_id",
            how="left",
        )

        @daft.func
        def concat_lists(current: list[str], new: list[str]) -> list[str]:
            return (current or []) + (new or [])

        df = df.with_column(
            "inbox__messages",
            concat_lists(col("inbox__messages"), col("new_inbox")),
        )

        # --- 9. Merge receipts back into the main DataFrame (cumulative) ---
        df = df.join(
            receipt_agg,
            left_on="entity_id",
            right_on="sender_id",
            how="left",
        )

        if "deliveryreceipt__receipts" in df.column_names:
            df = df.with_column(
                "deliveryreceipt__receipts",
                concat_lists(col("deliveryreceipt__receipts"), col("new_receipts")),
            )

        # --- 10. Clear outboxes, drop join artifact columns ---
        @daft.func
        def clear_list(_anchor: str) -> list[str]:
            """Returns an empty list. Takes a dummy column to bind as a row-wise UDF."""
            return []

        # When deliveryreceipt__receipts column doesn't exist (e.g., minimal test schemas),
        # initialize it from new_receipts, defaulting nulls to empty lists.
        if "deliveryreceipt__receipts" not in df.column_names:
            df = df.with_column(
                "deliveryreceipt__receipts",
                concat_lists(
                    clear_list(col("entity_id").cast(DataType.string())),
                    col("new_receipts"),
                ),
            )

        df = df.with_column(
            "outbox__messages",
            clear_list(col("entity_id").cast(DataType.string())),
        )
        # Drop join artifact columns — LEFT JOINs leak right-side keys
        df = df.exclude("new_inbox", "new_receipts", "receiver_id", "sender_id")

        return df
