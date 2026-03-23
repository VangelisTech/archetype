# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Extended tests for CommandBroker

Tests cover edge cases, ActorCtx RBAC, and additional functionality.
"""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType


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


class TestBrokerWithoutCtx:
    """Test that broker still works when no ActorCtx is provided (backward compat)."""

    @pytest.mark.asyncio
    async def test_enqueue_without_ctx(self):
        broker = CommandBroker()
        cmd = Command(type=CommandType.SPAWN, payload={})
        await broker.enqueue("test", cmd)

        messages = await broker.dequeue("test", max_items=10)
        assert len(messages) == 1
