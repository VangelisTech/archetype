# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for archetype.dsl.primitives

Tests cover:
- Inbox component
- MessageRealizer UDF
- broadcast() async function
- MessageCollection helper
- inbox_recent helper
"""

import json

import pytest

from archetype.app.broker import CommandBroker
from archetype.app.models import CommandType
from archetype.core.component import Component
from archetype.dsl import Inbox, World, behavior, broadcast
from archetype.dsl.primitives import (
    MessageCollection,
    MessageRealizer,
    _get_agent_name,
    inbox_recent,
)


# =============================================================================
# Test Components
# =============================================================================


class Agent(Component):
    name: str = ""


class Profile(Component):
    name: str = ""


# =============================================================================
# Unit Tests: Inbox Component
# =============================================================================


class TestInboxComponent:
    def test_inbox_default_empty(self):
        inbox = Inbox()
        assert inbox.messages_json == "[]"

    def test_inbox_with_messages(self):
        messages = [{"sender": "Alice", "content": "Hello"}]
        inbox = Inbox(messages_json=json.dumps(messages))
        assert json.loads(inbox.messages_json) == messages


# =============================================================================
# Unit Tests: MessageRealizer
# =============================================================================


class TestMessageRealizer:
    def test_realize_adds_messages(self):
        messages_by_receiver = {
            1: [{"sender": "Alice", "content": "Hi"}],
            2: [{"sender": "Bob", "content": "Hello"}],
        }
        realizer = MessageRealizer(messages_by_receiver)

        # Entity 1 receives Alice's message
        result = realizer.realize(1, "[]")
        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["sender"] == "Alice"

    def test_realize_appends_to_existing(self):
        messages_by_receiver = {
            1: [{"sender": "Charlie", "content": "New message"}],
        }
        realizer = MessageRealizer(messages_by_receiver)

        existing = json.dumps([{"sender": "Bob", "content": "Old message"}])
        result = realizer.realize(1, existing)
        parsed = json.loads(result)

        assert len(parsed) == 2
        assert parsed[0]["sender"] == "Bob"
        assert parsed[1]["sender"] == "Charlie"

    def test_realize_no_messages_for_entity(self):
        messages_by_receiver = {
            1: [{"sender": "Alice", "content": "Hi"}],
        }
        realizer = MessageRealizer(messages_by_receiver)

        # Entity 999 has no messages
        result = realizer.realize(999, "[]")
        assert json.loads(result) == []

    def test_realize_handles_empty_current(self):
        messages_by_receiver = {
            1: [{"content": "test"}],
        }
        realizer = MessageRealizer(messages_by_receiver)

        result = realizer.realize(1, "")
        parsed = json.loads(result)
        assert len(parsed) == 1


# =============================================================================
# Unit Tests: MessageCollection
# =============================================================================


class TestMessageCollection:
    def test_empty_collection(self):
        mc = MessageCollection()
        assert len(mc) == 0
        assert mc.render() == ""

    def test_collection_with_messages(self):
        messages = [
            {"sender_name": "Alice", "content": "Hello"},
            {"sender_name": "Bob", "content": "Hi there"},
        ]
        mc = MessageCollection(messages=messages)

        assert len(mc) == 2
        assert list(mc) == messages

    def test_render_groups_by_sender(self):
        messages = [
            {"sender_name": "Alice", "content": "First"},
            {"sender_name": "Alice", "content": "Second"},
            {"sender_name": "Bob", "content": "From Bob"},
        ]
        mc = MessageCollection(messages=messages)

        rendered = mc.render()
        assert "Recent messages:" in rendered
        assert "Alice:" in rendered
        assert "Bob:" in rendered

    def test_render_limits_per_sender(self):
        messages = [
            {"sender_name": "Alice", "content": f"Message {i}"}
            for i in range(5)
        ]
        mc = MessageCollection(messages=messages)

        # Default max_per_sender=2, should only show last 2
        rendered = mc.render(max_per_sender=2)
        # Should have header + 2 messages
        lines = [l for l in rendered.split("\n") if l.strip()]
        assert len(lines) == 3  # "Recent messages:" + 2 messages

    def test_render_truncates_long_content(self):
        long_content = "x" * 500
        messages = [{"sender_name": "Verbose", "content": long_content}]
        mc = MessageCollection(messages=messages)

        rendered = mc.render()
        # Content should be truncated to 200 chars
        assert len(rendered) < 300


# =============================================================================
# Integration Tests: broadcast()
# =============================================================================


class TestBroadcast:
    @pytest.mark.asyncio
    async def test_broadcast_requires_world(self):
        with pytest.raises(ValueError, match="requires world"):
            await broadcast("Hello", world=None)

    @pytest.mark.asyncio
    async def test_broadcast_requires_broker(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            # Temporarily remove broker
            original_broker = world._broker
            world._broker = None

            with pytest.raises(ValueError, match="no broker"):
                await broadcast("Hello", world=world)

            world._broker = original_broker

    @pytest.mark.asyncio
    async def test_broadcast_enqueues_messages(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(Agent(name="Alice"), Inbox())
            await world.spawn(Agent(name="Bob"), Inbox())
            await world.spawn(Agent(name="Charlie"), Inbox())
            await world.run(ticks=1)

            alice = await world.find_one(lambda a: a.agent.name == "Alice")

            # Broadcast from Alice
            await broadcast(
                "Hello everyone!",
                sender=alice,
                exclude=[alice],
                world=world,
            )

            # Should have 2 messages (to Bob and Charlie)
            pending = await world.broker.get_pending_count(world.name)
            assert pending == 2

    @pytest.mark.asyncio
    async def test_broadcast_serializes_dict_content(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(Agent(name="Alice"), Inbox())
            await world.spawn(Agent(name="Bob"), Inbox())
            await world.run(ticks=1)

            alice = await world.find_one(lambda a: a.agent.name == "Alice")

            # Broadcast dict content
            await broadcast(
                {"type": "greeting", "text": "Hello"},
                sender=alice,
                exclude=[alice],
                world=world,
            )

            # Dequeue and verify serialization
            messages = await world.broker.dequeue(world.name, max_items=10)
            assert len(messages) == 1
            content = messages[0].payload["content"]
            parsed = json.loads(content)
            assert parsed["type"] == "greeting"

    @pytest.mark.asyncio
    async def test_broadcast_with_metadata(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(Agent(name="Alice"), Inbox())
            await world.spawn(Agent(name="Bob"), Inbox())
            await world.run(ticks=1)

            alice = await world.find_one(lambda a: a.agent.name == "Alice")

            await broadcast(
                "Hello",
                sender=alice,
                exclude=[alice],
                metadata={"priority": "high"},
                world=world,
            )

            messages = await world.broker.dequeue(world.name, max_items=10)
            assert messages[0].payload["priority"] == "high"


# =============================================================================
# Unit Tests: _get_agent_name helper
# =============================================================================


class TestGetAgentName:
    def test_extracts_from_perspective(self, tmp_path):
        # Create a mock agent proxy
        class MockProxy:
            entity_id = 1

            class perspective:
                name = "TestAgent"

        name = _get_agent_name(MockProxy())
        assert name == "TestAgent"

    def test_falls_back_to_entity_id(self):
        class MockProxy:
            entity_id = 42

        name = _get_agent_name(MockProxy())
        assert name == "agent_42"


# =============================================================================
# Integration Tests: inbox_recent
# =============================================================================


class TestInboxRecent:
    @pytest.mark.asyncio
    async def test_inbox_recent_returns_messages(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            messages = [
                {"sender": "Alice", "content": f"Message {i}"}
                for i in range(5)
            ]
            await world.spawn(
                Agent(name="Bob"),
                Inbox(messages_json=json.dumps(messages)),
            )
            await world.run(ticks=1)

            bob = await world.find_one(lambda a: a.agent.name == "Bob")
            recent = inbox_recent(bob, limit=3)

            assert len(recent) == 3
            # Should be last 3 messages
            assert recent.messages[0]["content"] == "Message 2"

    @pytest.mark.asyncio
    async def test_inbox_recent_handles_no_inbox(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(Agent(name="NoInbox"))
            await world.run(ticks=1)

            agent = await world.find_one(lambda a: a.agent.name == "NoInbox")
            recent = inbox_recent(agent)

            assert len(recent) == 0
