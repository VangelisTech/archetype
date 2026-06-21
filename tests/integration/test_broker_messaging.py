# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests: CommandBroker + Messaging

Tests the command broker and message passing between agents.
"""

import pytest

from archetype.app.gateway.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.core.component import Component

# =============================================================================
# Test Components
# =============================================================================


class Agent(Component):
    name: str = ""


class Mailbox(Component):
    messages_json: str = "[]"
    unread_count: int = 0


# =============================================================================
# Unit Tests: CommandBroker
# =============================================================================


class TestCommandBroker:
    @pytest.mark.asyncio
    async def test_enqueue_dequeue(self):
        broker = CommandBroker()

        cmd = Command(
            type=CommandType.MESSAGE,
            payload={"content": "Hello"},
        )

        await broker.enqueue("test_world", cmd)

        pending = await broker.get_pending_count("test_world")
        assert pending == 1

        messages = await broker.dequeue("test_world", max_items=10)
        assert len(messages) == 1
        assert messages[0].payload["content"] == "Hello"

        pending = await broker.get_pending_count("test_world")
        assert pending == 0

    @pytest.mark.asyncio
    async def test_multiple_worlds_isolated(self):
        broker = CommandBroker()

        await broker.enqueue("world_a", Command(type=CommandType.MESSAGE, payload={"id": "a"}))
        await broker.enqueue("world_b", Command(type=CommandType.MESSAGE, payload={"id": "b"}))

        a_msgs = await broker.dequeue("world_a", max_items=10)
        b_msgs = await broker.dequeue("world_b", max_items=10)

        assert len(a_msgs) == 1
        assert a_msgs[0].payload["id"] == "a"

        assert len(b_msgs) == 1
        assert b_msgs[0].payload["id"] == "b"

    @pytest.mark.asyncio
    async def test_bulk_enqueue(self):
        broker = CommandBroker()

        commands = [Command(type=CommandType.MESSAGE, payload={"idx": i}) for i in range(100)]

        for cmd in commands:
            await broker.enqueue("bulk_test", cmd)

        pending = await broker.get_pending_count("bulk_test")
        assert pending == 100

        # Dequeue in batches
        batch1 = await broker.dequeue("bulk_test", max_items=30)
        batch2 = await broker.dequeue("bulk_test", max_items=30)
        batch3 = await broker.dequeue("bulk_test", max_items=50)

        assert len(batch1) == 30
        assert len(batch2) == 30
        assert len(batch3) == 40  # Remaining


# =============================================================================
# Integration Tests: Command Types
# =============================================================================


class TestCommandTypes:
    @pytest.mark.asyncio
    async def test_spawn_command(self):
        """Test SPAWN command type."""
        broker = CommandBroker()

        cmd = Command(
            type=CommandType.SPAWN,
            payload={
                "components": [
                    {"type": "Agent", "name": "NewAgent"},
                ],
            },
        )

        await broker.enqueue("world", cmd)
        messages = await broker.dequeue("world", max_items=10)

        assert len(messages) == 1
        assert messages[0].type == CommandType.SPAWN

    @pytest.mark.asyncio
    async def test_despawn_command(self):
        """Test DESPAWN command type."""
        broker = CommandBroker()

        cmd = Command(
            type=CommandType.DESPAWN,
            payload={"entity_id": 123},
        )

        await broker.enqueue("world", cmd)
        messages = await broker.dequeue("world", max_items=10)

        assert len(messages) == 1
        assert messages[0].type == CommandType.DESPAWN
        assert messages[0].payload["entity_id"] == 123

    @pytest.mark.asyncio
    async def test_custom_command(self):
        """Test CUSTOM command type for user-defined actions."""
        broker = CommandBroker()

        cmd = Command(
            type=CommandType.CUSTOM,
            payload={
                "action": "trigger_event",
                "event_type": "explosion",
                "position": {"x": 10, "y": 20},
            },
        )

        await broker.enqueue("world", cmd)
        messages = await broker.dequeue("world", max_items=10)

        assert len(messages) == 1
        assert messages[0].type == CommandType.CUSTOM
        assert messages[0].payload["action"] == "trigger_event"
