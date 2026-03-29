# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for ChatGraph DAG, channels, append_history toggle, Resources integration,
and MessageDeliveryProcessor.
"""

import json

import pytest

from archetype.app.broker import CommandBroker
from archetype.app.chat_graph import ChatGraph, ChatGraphRegistry
from archetype.app.models import Command, CommandType
from archetype.core.resources import Resources


# =============================================================================
# ChatGraph Unit Tests
# =============================================================================


class TestChatGraphLinear:
    """Test that the graph works like a flat list when you never branch."""

    def test_empty_graph(self):
        g = ChatGraph("test")
        assert g.size == 0
        assert g.cursor is None
        assert g.active_path() == []
        assert g.leaves() == []
        assert g.channel == "general"

    def test_single_append(self):
        g = ChatGraph("test")
        cmd = Command(type=CommandType.MESSAGE, payload={"content": "hello"})
        node = g.append(cmd)

        assert g.size == 1
        assert g.cursor == cmd.id
        assert node.parent_id is None
        assert g.active_path() == [cmd]

    def test_linear_chain(self):
        g = ChatGraph("test")
        cmds = [
            Command(type=CommandType.MESSAGE, payload={"content": f"msg-{i}"})
            for i in range(5)
        ]

        for cmd in cmds:
            g.append(cmd)

        assert g.size == 5
        assert g.cursor == cmds[-1].id
        path = g.active_path()
        assert len(path) == 5
        assert [c.id for c in path] == [c.id for c in cmds]

    def test_cursor_advances_on_append(self):
        g = ChatGraph("test")
        c1 = Command(type=CommandType.MESSAGE, payload={"content": "a"})
        c2 = Command(type=CommandType.MESSAGE, payload={"content": "b"})

        g.append(c1)
        assert g.cursor == c1.id

        g.append(c2)
        assert g.cursor == c2.id

    def test_leaves_in_linear_chain(self):
        g = ChatGraph("test")
        cmds = [Command(type=CommandType.MESSAGE) for _ in range(3)]
        for c in cmds:
            g.append(c)

        leaves = g.leaves()
        assert len(leaves) == 1
        assert leaves[0] == cmds[-1].id


class TestChatGraphBranching:
    """Test branching and navigation."""

    def test_branch_creates_sibling(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE, payload={"content": "root"})
        g.append(root)

        child_a = Command(type=CommandType.MESSAGE, payload={"content": "reply-a"})
        g.append(child_a)

        child_b = Command(type=CommandType.MESSAGE, payload={"content": "reply-b"})
        g.branch(root.id, child_b, label="alternative")

        assert g.size == 3
        root_node = g.get_node(root.id)
        assert len(root_node.children) == 2
        assert child_a.id in root_node.children
        assert child_b.id in root_node.children

    def test_branch_moves_cursor(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        child = Command(type=CommandType.MESSAGE)
        g.append(child)

        alt = Command(type=CommandType.MESSAGE)
        g.branch(root.id, alt)
        assert g.cursor == alt.id

    def test_branch_active_path(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE, payload={"content": "root"})
        g.append(root)

        child_a = Command(type=CommandType.MESSAGE, payload={"content": "a"})
        g.append(child_a)

        child_b = Command(type=CommandType.MESSAGE, payload={"content": "b"})
        g.branch(root.id, child_b)

        path = g.active_path()
        assert len(path) == 2
        assert path[0].id == root.id
        assert path[1].id == child_b.id

    def test_branch_label(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        alt = Command(type=CommandType.MESSAGE)
        g.branch(root.id, alt, label="retry-v2")

        node = g.get_node(alt.id)
        assert node.branch_label == "retry-v2"

    def test_branch_nonexistent_parent_raises(self):
        g = ChatGraph("test")
        import uuid_utils as uuid

        cmd = Command(type=CommandType.MESSAGE)
        with pytest.raises(KeyError):
            g.branch(uuid.uuid7(), cmd)

    def test_multiple_branches_leaves(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        b1 = Command(type=CommandType.MESSAGE)
        b2 = Command(type=CommandType.MESSAGE)
        b3 = Command(type=CommandType.MESSAGE)

        g.branch(root.id, b1)
        g.branch(root.id, b2)
        g.branch(root.id, b3)

        leaves = g.leaves()
        assert len(leaves) == 3
        assert set(leaves) == {b1.id, b2.id, b3.id}


class TestChatGraphNavigation:
    def test_navigate_to_node(self):
        g = ChatGraph("test")
        c1 = Command(type=CommandType.MESSAGE)
        c2 = Command(type=CommandType.MESSAGE)
        c3 = Command(type=CommandType.MESSAGE)
        g.append(c1)
        g.append(c2)
        g.append(c3)

        path = g.navigate(c1.id)
        assert g.cursor == c1.id
        assert path == [c1.id]

    def test_navigate_nonexistent_raises(self):
        import uuid_utils as uuid
        g = ChatGraph("test")
        with pytest.raises(KeyError):
            g.navigate(uuid.uuid7())

    def test_auto_nav_to_leaf(self):
        g = ChatGraph("test")
        c1 = Command(type=CommandType.MESSAGE)
        c2 = Command(type=CommandType.MESSAGE)
        c3 = Command(type=CommandType.MESSAGE)
        g.append(c1)
        g.append(c2)
        g.append(c3)

        g.navigate(c1.id)
        leaf = g.auto_nav_to_leaf()
        assert leaf == c3.id

    def test_auto_nav_follows_rightmost_branch(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        left = Command(type=CommandType.MESSAGE)
        g.branch(root.id, left)

        right = Command(type=CommandType.MESSAGE)
        g.branch(root.id, right)

        right_child = Command(type=CommandType.MESSAGE)
        g.append(right_child)

        g.navigate(root.id)
        leaf = g.auto_nav_to_leaf()
        assert leaf == right_child.id

    def test_auto_nav_empty_graph(self):
        g = ChatGraph("test")
        assert g.auto_nav_to_leaf() is None


class TestChatGraphPruning:
    def test_prune_leaf(self):
        g = ChatGraph("test")
        c1 = Command(type=CommandType.MESSAGE)
        c2 = Command(type=CommandType.MESSAGE)
        g.append(c1)
        g.append(c2)

        removed = g.prune(c2.id)
        assert removed == 1
        assert g.size == 1
        assert g.cursor == c1.id

    def test_prune_subtree(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)
        mid = Command(type=CommandType.MESSAGE)
        g.append(mid)
        leaf1 = Command(type=CommandType.MESSAGE)
        g.append(leaf1)
        leaf2 = Command(type=CommandType.MESSAGE)
        g.branch(mid.id, leaf2)

        removed = g.prune(mid.id)
        assert removed == 3
        assert g.size == 1
        assert g.cursor == root.id

    def test_prune_root(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)
        child = Command(type=CommandType.MESSAGE)
        g.append(child)

        removed = g.prune(root.id)
        assert removed == 2
        assert g.size == 0
        assert g.cursor is None

    def test_prune_nonexistent(self):
        g = ChatGraph("test")
        removed = g.prune(Command(type=CommandType.MESSAGE).id)
        assert removed == 0


class TestChatGraphSerialization:
    def test_to_dict_empty(self):
        g = ChatGraph("test_world", channel="strategy")
        d = g.to_dict()
        assert d["world_id"] == "test_world"
        assert d["channel"] == "strategy"
        assert d["cursor"] is None
        assert d["size"] == 0

    def test_to_dict_with_nodes(self):
        g = ChatGraph("w1")
        c1 = Command(type=CommandType.MESSAGE, payload={"content": "hi"})
        c2 = Command(type=CommandType.MESSAGE, payload={"content": "hello"})
        g.append(c1)
        g.append(c2)

        d = g.to_dict()
        assert d["size"] == 2
        assert d["nodes"][str(c2.id)]["parent_id"] == str(c1.id)


class TestChatGraphExplicitParent:
    def test_explicit_parent_id(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)
        mid = Command(type=CommandType.MESSAGE)
        g.append(mid)

        fork = Command(type=CommandType.MESSAGE, parent_id=root.id)
        g.append(fork)

        root_node = g.get_node(root.id)
        assert fork.id in root_node.children
        assert g.cursor == fork.id
        path = g.active_path()
        assert [c.id for c in path] == [root.id, fork.id]


# =============================================================================
# ChatGraphRegistry Tests — Channel-Aware
# =============================================================================


class TestChatGraphRegistry:
    def test_lazy_create_default_channel(self):
        reg = ChatGraphRegistry()
        g = reg.get("world_1")
        assert g.channel == "general"

    def test_named_channel(self):
        reg = ChatGraphRegistry()
        g = reg.channel("w1", "strategy")
        assert g.channel == "strategy"

    def test_same_instance_same_channel(self):
        reg = ChatGraphRegistry()
        g1 = reg.channel("w1", "strategy")
        g2 = reg.channel("w1", "strategy")
        assert g1 is g2

    def test_different_channels_different_graphs(self):
        reg = ChatGraphRegistry()
        g1 = reg.channel("w1", "strategy")
        g2 = reg.channel("w1", "negotiation")
        assert g1 is not g2

    def test_channels_list(self):
        reg = ChatGraphRegistry()
        reg.channel("w1", "strategy")
        reg.channel("w1", "negotiation")
        reg.channel("w1", "general")
        assert set(reg.channels("w1")) == {"strategy", "negotiation", "general"}

    def test_channels_empty_world(self):
        reg = ChatGraphRegistry()
        assert reg.channels("nonexistent") == []

    def test_remove_single_channel(self):
        reg = ChatGraphRegistry()
        reg.channel("w1", "strategy")
        reg.channel("w1", "negotiation")
        reg.remove("w1", "strategy")
        assert "strategy" not in reg.channels("w1")
        assert "negotiation" in reg.channels("w1")

    def test_remove_all_channels(self):
        reg = ChatGraphRegistry()
        reg.channel("w1", "strategy")
        reg.channel("w1", "negotiation")
        reg.remove("w1")
        assert reg.channels("w1") == []

    def test_get_is_shorthand_for_general(self):
        reg = ChatGraphRegistry()
        g1 = reg.get("w1")
        g2 = reg.channel("w1", "general")
        assert g1 is g2


# =============================================================================
# append_history Toggle Tests (Broker — Governance Only)
# =============================================================================


class TestAppendHistoryToggle:
    @pytest.mark.asyncio
    async def test_persistent_command_in_history(self):
        broker = CommandBroker()
        cmd = Command(type=CommandType.MESSAGE, append_history=True)
        await broker.enqueue("world", cmd)

        history = await broker.get_history("world")
        assert len(history) == 1

    @pytest.mark.asyncio
    async def test_ephemeral_command_not_in_history(self):
        broker = CommandBroker()
        cmd = Command(type=CommandType.MESSAGE, append_history=False)
        await broker.enqueue("world", cmd)

        history = await broker.get_history("world")
        assert len(history) == 0

    @pytest.mark.asyncio
    async def test_ephemeral_still_enqueued(self):
        broker = CommandBroker()
        cmd = Command(type=CommandType.MESSAGE, append_history=False)
        await broker.enqueue("world", cmd)

        pending = await broker.get_pending_count("world")
        assert pending == 1

    @pytest.mark.asyncio
    async def test_mixed_persistent_and_ephemeral(self):
        broker = CommandBroker()
        await broker.enqueue("world", Command(type=CommandType.MESSAGE, append_history=True))
        await broker.enqueue("world", Command(type=CommandType.MESSAGE, append_history=False))

        history = await broker.get_history("world")
        assert len(history) == 1
        assert await broker.get_pending_count("world") == 2

    @pytest.mark.asyncio
    async def test_bulk_enqueue_respects_toggle(self):
        broker = CommandBroker()
        cmds = [
            Command(type=CommandType.MESSAGE, payload={"i": 0}, append_history=True),
            Command(type=CommandType.MESSAGE, payload={"i": 1}, append_history=False),
            Command(type=CommandType.MESSAGE, payload={"i": 2}, append_history=True),
        ]
        await broker.enqueue_bulk("world", cmds)

        history = await broker.get_history("world")
        assert len(history) == 2

    @pytest.mark.asyncio
    async def test_broker_does_not_touch_chat_graph(self):
        """Broker is governance-only — it should NOT write to ChatGraph."""
        registry = ChatGraphRegistry()
        broker = CommandBroker()

        cmd = Command(type=CommandType.MESSAGE, payload={"content": "hello"})
        await broker.enqueue("world", cmd)

        # Graph should be untouched — broker doesn't know about it
        graph = registry.get("world")
        assert graph.size == 0


# =============================================================================
# Resources Integration Tests
# =============================================================================


class TestResourcesIntegration:
    def test_insert_and_require(self):
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)
        assert resources.require(ChatGraphRegistry) is registry

    def test_contains_check(self):
        resources = Resources()
        assert ChatGraphRegistry not in resources
        resources.insert(ChatGraphRegistry())
        assert ChatGraphRegistry in resources

    def test_graph_via_resources_write_and_read(self):
        """Simulate processor pattern: write to graph via Resources, read back."""
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        # Processor writes a message to #strategy
        reg = resources.require(ChatGraphRegistry)
        cmd = Command(
            type=CommandType.MESSAGE,
            channel="strategy",
            payload={"content": "Plan A"},
        )
        reg.channel("world", "strategy").append(cmd)

        # Another processor reads context
        path = reg.channel("world", "strategy").active_path()
        assert len(path) == 1
        assert path[0].payload["content"] == "Plan A"


# =============================================================================
# MessageDeliveryProcessor Unit Tests
# =============================================================================


class TestMessageDeliveryProcessor:
    """Test the processor in isolation using Daft DataFrames."""

    @pytest.fixture
    def resources_with_graph(self):
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)
        return resources, registry

    @pytest.mark.asyncio
    async def test_basic_delivery(self, resources_with_graph):
        """Message from entity 1 → entity 2 lands in entity 2's inbox."""
        import daft as daft_mod

        from archetype.app.messaging import Inbox, MessageDeliveryProcessor, Outbox

        resources, registry = resources_with_graph
        proc = MessageDeliveryProcessor()

        msg = json.dumps({"receiver_id": 2, "channel": "general", "content": "hello"})

        # Build a DataFrame with two entities
        df = daft_mod.from_pydict({
            "entity_id": [1, 2],
            "is_active": [True, True],
            "outbox__messages": [[msg], []],
            "inbox__messages": [[], []],
        })

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()

        # Entity 2 should have the message in inbox
        entity2 = [r for r in rows if r["entity_id"] == 2][0]
        inbox = entity2["inbox__messages"]
        assert len(inbox) == 1
        parsed = json.loads(inbox[0])
        assert parsed["sender_id"] == 1
        assert parsed["content"] == "hello"

        # Entity 1's outbox should be cleared
        entity1 = [r for r in rows if r["entity_id"] == 1][0]
        assert entity1["outbox__messages"] == []

    @pytest.mark.asyncio
    async def test_bad_receiver_rejected(self, resources_with_graph):
        """Message to nonexistent receiver doesn't crash, message is dropped."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources, _ = resources_with_graph
        proc = MessageDeliveryProcessor()

        msg = json.dumps({"receiver_id": 999, "channel": "general", "content": "hello"})

        df = daft_mod.from_pydict({
            "entity_id": [1],
            "is_active": [True],
            "outbox__messages": [[msg]],
            "inbox__messages": [[]],
        })

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()

        # No crash, outbox cleared
        assert rows[0]["outbox__messages"] == []
        # Inbox still empty (message was rejected)
        assert rows[0]["inbox__messages"] == []

    @pytest.mark.asyncio
    async def test_self_message_rejected(self, resources_with_graph):
        """Agent cannot message itself."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources, _ = resources_with_graph
        proc = MessageDeliveryProcessor()

        msg = json.dumps({"receiver_id": 1, "channel": "general", "content": "echo"})

        df = daft_mod.from_pydict({
            "entity_id": [1],
            "is_active": [True],
            "outbox__messages": [[msg]],
            "inbox__messages": [[]],
        })

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()
        assert rows[0]["inbox__messages"] == []

    @pytest.mark.asyncio
    async def test_graph_updated_on_delivery(self, resources_with_graph):
        """Delivered messages should appear in the ChatGraph."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources, registry = resources_with_graph
        proc = MessageDeliveryProcessor()

        msg = json.dumps({"receiver_id": 2, "channel": "strategy", "content": "plan"})

        df = daft_mod.from_pydict({
            "entity_id": [1, 2],
            "is_active": [True, True],
            "outbox__messages": [[msg], []],
            "inbox__messages": [[], []],
        })

        await proc.process(df, resources, tick=0, world_id="test")

        graph = registry.channel("test", "strategy")
        assert graph.size == 1
        path = graph.active_path()
        assert path[0].payload["content"] == "plan"

    @pytest.mark.asyncio
    async def test_ephemeral_message_skips_graph(self, resources_with_graph):
        """Messages with _append_history=False don't land in the graph."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources, registry = resources_with_graph
        proc = MessageDeliveryProcessor()

        msg = json.dumps({
            "receiver_id": 2,
            "channel": "system",
            "content": "heartbeat",
            "_append_history": False,
        })

        df = daft_mod.from_pydict({
            "entity_id": [1, 2],
            "is_active": [True, True],
            "outbox__messages": [[msg], []],
            "inbox__messages": [[], []],
        })

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()

        # Message delivered to inbox (transport still works)
        entity2 = [r for r in rows if r["entity_id"] == 2][0]
        assert len(entity2["inbox__messages"]) == 1

        # But graph should have nothing for "system" channel
        assert "system" not in registry.channels("test")

    @pytest.mark.asyncio
    async def test_multi_channel_routing(self, resources_with_graph):
        """Messages to different channels land in correct graphs."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources, registry = resources_with_graph
        proc = MessageDeliveryProcessor()

        msgs = [
            json.dumps({"receiver_id": 2, "channel": "strategy", "content": "plan"}),
            json.dumps({"receiver_id": 2, "channel": "negotiation", "content": "offer"}),
        ]

        df = daft_mod.from_pydict({
            "entity_id": [1, 2],
            "is_active": [True, True],
            "outbox__messages": [msgs, []],
            "inbox__messages": [[], []],
        })

        await proc.process(df, resources, tick=0, world_id="test")

        assert registry.channel("test", "strategy").size == 1
        assert registry.channel("test", "negotiation").size == 1

    @pytest.mark.asyncio
    async def test_no_graph_resource_still_delivers(self):
        """If ChatGraphRegistry is not in Resources, messages still deliver."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources = Resources()  # no ChatGraphRegistry
        proc = MessageDeliveryProcessor()

        msg = json.dumps({"receiver_id": 2, "channel": "general", "content": "hi"})

        df = daft_mod.from_pydict({
            "entity_id": [1, 2],
            "is_active": [True, True],
            "outbox__messages": [[msg], []],
            "inbox__messages": [[], []],
        })

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()

        entity2 = [r for r in rows if r["entity_id"] == 2][0]
        assert len(entity2["inbox__messages"]) == 1

    @pytest.mark.asyncio
    async def test_empty_outbox_noop(self, resources_with_graph):
        """No messages → no changes."""
        import daft as daft_mod

        from archetype.app.messaging import MessageDeliveryProcessor

        resources, _ = resources_with_graph
        proc = MessageDeliveryProcessor()

        df = daft_mod.from_pydict({
            "entity_id": [1, 2],
            "is_active": [True, True],
            "outbox__messages": [[], []],
            "inbox__messages": [[], []],
        })

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()

        for row in rows:
            assert row["inbox__messages"] == []
