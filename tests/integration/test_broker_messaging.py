# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests: CommandBroker + Messaging

Tests the command broker and message passing between agents.
"""

import pytest

from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.dsl import Inbox, World

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
# Integration Tests: Messaging via DSL
# =============================================================================


class TestMessaging:
    @pytest.mark.asyncio
    async def test_broadcast_delivers_messages(self, tmp_path):
        """Test that broadcast messages are delivered via broker."""

        async with World("messaging", storage=tmp_path / "data") as world:
            await world.spawn(Agent(name="Alice"), Inbox(messages_json="[]"))
            await world.spawn(Agent(name="Bob"), Inbox(messages_json="[]"))
            await world.run(ticks=1)

            alice = await world.find_one(lambda a: a.agent.name == "Alice")

            # Alice broadcasts
            await world.broadcast(
                content="Hello everyone!",
                sender=alice,
                exclude=[alice],
            )

            # Check broker has message for Bob
            pending = await world.broker.get_pending_count(world.name)
            assert pending == 1

            # Run another tick to realize messages
            await world.run(ticks=1)

    @pytest.mark.asyncio
    async def test_targeted_message(self, tmp_path):
        """Test sending message to specific agent."""

        async with World("targeted", storage=tmp_path / "data") as world:
            await world.spawn(Agent(name="Sender"), Inbox())
            await world.spawn(Agent(name="Target"), Inbox())
            await world.spawn(Agent(name="Bystander"), Inbox())
            await world.run(ticks=1)

            sender = await world.find_one(lambda a: a.agent.name == "Sender")
            target = await world.find_one(lambda a: a.agent.name == "Target")
            _ = await world.find_one(lambda a: a.agent.name == "Bystander")  # Verify exists

            # Send directly via broker
            cmd = Command(
                type=CommandType.MESSAGE,
                payload={
                    "sender_id": sender.entity_id,
                    "sender_name": "Sender",
                    "receiver_id": target.entity_id,
                    "receiver_name": "Target",
                    "content": "Private message",
                },
            )
            await world.broker.enqueue(world.name, cmd)

            # Only one message should be pending
            pending = await world.broker.get_pending_count(world.name)
            assert pending == 1


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
