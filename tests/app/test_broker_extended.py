# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Extended tests for CommandBroker

Tests cover edge cases, ActorCtx RBAC, and additional functionality.
"""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import (
    _daily_tokens,
    _tick_counters,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType


@pytest.fixture
def _reset_quotas():
    """Snapshot + restore guard counters so broker tests don't leak into auth tests."""
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


class TestBrokerEdgeCases:
    @pytest.mark.asyncio
    async def test_dequeue_empty_queue(self):
        broker = CommandBroker()
        messages = await broker.dequeue("empty_world", max_items=10)
        assert messages == []

    @pytest.mark.asyncio
    async def test_get_pending_count_empty(self):
        broker = CommandBroker()
        count = await broker.get_pending_count("nonexistent")
        assert count == 0

    @pytest.mark.asyncio
    async def test_dequeue_respects_max_items(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for i in range(10):
            await broker.enqueue(
                "test",
                Command(type=CommandType.MESSAGE, payload={"i": i}),
                ctx,
            )

        messages = await broker.dequeue("test", max_items=3)
        assert len(messages) == 3

        count = await broker.get_pending_count("test")
        assert count == 7

    @pytest.mark.asyncio
    async def test_command_types(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for cmd_type in [
            CommandType.SPAWN,
            CommandType.DESPAWN,
            CommandType.MESSAGE,
            CommandType.CUSTOM,
        ]:
            cmd = Command(type=cmd_type, payload={"test": True})
            await broker.enqueue("typed", cmd, ctx)

        messages = await broker.dequeue("typed", max_items=10)
        assert len(messages) == 4

        types = {m.type for m in messages}
        assert CommandType.SPAWN in types
        assert CommandType.DESPAWN in types
        assert CommandType.MESSAGE in types
        assert CommandType.CUSTOM in types

    @pytest.mark.asyncio
    async def test_command_with_tick(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        cmd = Command(
            type=CommandType.MESSAGE,
            tick=42,
            payload={"content": "Hello"},
        )
        await broker.enqueue("test", cmd, ctx)

        messages = await broker.dequeue("test", max_items=10)
        assert messages[0].tick == 42

    @pytest.mark.asyncio
    async def test_command_with_priority(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        high = Command(type=CommandType.SPAWN, priority=1, payload={})
        low = Command(type=CommandType.SPAWN, priority=100, payload={})

        await broker.enqueue("test", low, ctx)
        await broker.enqueue("test", high, ctx)

        messages = await broker.dequeue("test", max_items=10)
        assert messages[0].priority == 1
        assert messages[1].priority == 100

    @pytest.mark.asyncio
    async def test_multiple_dequeues_exhaust_queue(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for i in range(5):
            await broker.enqueue(
                "test",
                Command(type=CommandType.MESSAGE, payload={"i": i}),
                ctx,
            )

        batch1 = await broker.dequeue("test", max_items=3)
        batch2 = await broker.dequeue("test", max_items=3)
        batch3 = await broker.dequeue("test", max_items=3)

        assert len(batch1) == 3
        assert len(batch2) == 2
        assert len(batch3) == 0

    @pytest.mark.asyncio
    async def test_command_payload_preserved(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        complex_payload = {
            "nested": {"a": 1, "b": [1, 2, 3]},
            "string": "hello",
            "number": 42.5,
            "bool": True,
            "null": None,
        }

        await broker.enqueue(
            "test",
            Command(type=CommandType.CUSTOM, payload=complex_payload),
            ctx,
        )

        messages = await broker.dequeue("test", max_items=1)
        assert messages[0].payload == complex_payload

    @pytest.mark.asyncio
    async def test_dequeue_due_respects_tick(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        await broker.enqueue("test", Command(type=CommandType.SPAWN, tick=0), ctx)
        await broker.enqueue("test", Command(type=CommandType.SPAWN, tick=1), ctx)
        await broker.enqueue("test", Command(type=CommandType.SPAWN, tick=5), ctx)

        # Only tick <= 1 should be dequeued
        due = await broker.dequeue_due("test", tick=1)
        assert len(due) == 2
        assert all(cmd.tick <= 1 for cmd in due)

        # tick=5 should still be pending
        count = await broker.get_pending_count("test")
        assert count == 1

    @pytest.mark.asyncio
    async def test_ack_removes_from_pending(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        cmd = Command(type=CommandType.SPAWN, payload={})
        await broker.enqueue("test", cmd, ctx)

        due = await broker.dequeue_due("test", tick=0)
        assert len(due) == 1

        await broker.ack([cmd.id])
        # Pending should be 0 now (already removed by dequeue_due)
        count = await broker.get_pending_count("test")
        assert count == 0


class TestBrokerConcurrency:
    @pytest.mark.asyncio
    async def test_multiple_worlds_independent(self):
        broker = CommandBroker()
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for i in range(3):
            await broker.enqueue(
                f"world_{i}",
                Command(type=CommandType.MESSAGE, payload={"world": i}),
                ctx,
            )

        for i in range(3):
            count = await broker.get_pending_count(f"world_{i}")
            assert count == 1

        await broker.dequeue("world_0", max_items=10)

        assert await broker.get_pending_count("world_0") == 0
        assert await broker.get_pending_count("world_1") == 1
        assert await broker.get_pending_count("world_2") == 1


class TestEnqueueBulkQuotaAccounting:
    """Regression tests for enqueue_bulk's all-or-nothing quota guarantee.

    Prior to the fix, guardrail_allow mutated the per-tick counter and
    daily token budget during the validation phase, so a bulk that failed
    RBAC mid-loop would silently burn quota for the earlier commands even
    though zero commands were enqueued.
    """

    @pytest.mark.asyncio
    async def test_partial_rbac_failure_does_not_debit_quota(self, _reset_quotas):
        broker = CommandBroker()
        actor_id = uuid7()
        # player role: can spawn/despawn/update/message/custom; CANNOT add_processor
        ctx = ActorCtx(id=actor_id, roles={"player"})

        bulk = [
            Command(type=CommandType.SPAWN, payload={"components": []}),
            Command(type=CommandType.SPAWN, payload={"components": []}),
            Command(type=CommandType.ADD_PROCESSOR, payload={}),  # forbidden
            Command(type=CommandType.SPAWN, payload={"components": []}),
        ]

        with pytest.raises(PermissionError, match="add_processor"):
            await broker.enqueue_bulk("w1", bulk, ctx)

        # All-or-nothing: no commands enqueued, so no quota debited.
        assert _tick_counters.get(actor_id, 0) == 0
        assert _daily_tokens.get(actor_id, 0) == 0
        assert len(broker._queues.get("w1", [])) == 0
        assert await broker.get_pending_count("w1") == 0

    @pytest.mark.asyncio
    async def test_successful_bulk_debits_exactly_once_per_command(self, _reset_quotas):
        broker = CommandBroker()
        actor_id = uuid7()
        ctx = ActorCtx(id=actor_id, roles={"player"})

        bulk = [
            Command(type=CommandType.SPAWN, payload={"components": []}),
            Command(type=CommandType.SPAWN, payload={"components": []}),
            Command(type=CommandType.SPAWN, payload={"components": []}),
        ]
        await broker.enqueue_bulk("w1", bulk, ctx)

        # 3 SPAWNs × 10 tokens each = 30 tokens.
        assert _tick_counters[actor_id] == 3
        assert _daily_tokens[actor_id] == 30
        assert len(broker._queues["w1"]) == 3

    @pytest.mark.asyncio
    async def test_bulk_detects_projected_tick_quota_before_mutating(self, _reset_quotas):
        """A bulk that would overflow the per-tick quota fails up-front and
        leaves the actor's counters untouched."""
        from archetype.app.auth.guard import MAX_CMDS_PER_TICK

        broker = CommandBroker()
        actor_id = uuid7()
        ctx = ActorCtx(id=actor_id, roles={"admin"})

        # A bulk larger than the per-tick quota must be rejected atomically.
        oversized = [
            Command(type=CommandType.CUSTOM, payload={}) for _ in range(MAX_CMDS_PER_TICK + 1)
        ]

        with pytest.raises(PermissionError, match="per-tick quota"):
            await broker.enqueue_bulk("w1", oversized, ctx)

        assert _tick_counters.get(actor_id, 0) == 0
        assert _daily_tokens.get(actor_id, 0) == 0
        assert len(broker._queues.get("w1", [])) == 0


class TestBrokerWithoutCtx:
    """Test that broker still works when no ActorCtx is provided (backward compat)."""

    @pytest.mark.asyncio
    async def test_enqueue_without_ctx(self):
        broker = CommandBroker()
        cmd = Command(type=CommandType.SPAWN, payload={})
        await broker.enqueue("test", cmd)

        messages = await broker.dequeue("test", max_items=10)
        assert len(messages) == 1
