# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for ChatGraph DAG, channels, append_history toggle, and Resources integration.
"""

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

        # Branch from root with an alternative reply
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

        # Cursor is at child_b, path should be root → child_b
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
    """Test cursor navigation and auto-nav."""

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

        # Move cursor back to root
        g.navigate(c1.id)
        assert g.cursor == c1.id

        # Auto-nav should follow rightmost children to the leaf
        leaf = g.auto_nav_to_leaf()
        assert leaf == c3.id
        assert g.cursor == c3.id

    def test_auto_nav_follows_rightmost_branch(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        left = Command(type=CommandType.MESSAGE)
        g.branch(root.id, left)

        right = Command(type=CommandType.MESSAGE)
        g.branch(root.id, right)

        right_child = Command(type=CommandType.MESSAGE)
        g.append(right_child)  # appends to cursor (right)

        # Go back to root and auto-nav
        g.navigate(root.id)
        leaf = g.auto_nav_to_leaf()
        assert leaf == right_child.id

    def test_auto_nav_empty_graph(self):
        g = ChatGraph("test")
        assert g.auto_nav_to_leaf() is None


class TestChatGraphPruning:
    """Test subtree pruning."""

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

        # Prune mid → should remove mid, leaf1, leaf2
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
    """Test to_dict serialization."""

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
        assert len(d["roots"]) == 1
        assert str(c1.id) in d["nodes"]
        assert str(c2.id) in d["nodes"]
        assert d["nodes"][str(c2.id)]["parent_id"] == str(c1.id)


class TestChatGraphBranchesAt:
    def test_branches_at(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        b1 = Command(type=CommandType.MESSAGE)
        b2 = Command(type=CommandType.MESSAGE)
        g.branch(root.id, b1, label="v1")
        g.branch(root.id, b2, label="v2")

        branches = g.branches_at(root.id)
        assert len(branches) == 2
        assert branches[0].cmd.id == b1.id
        assert branches[1].cmd.id == b2.id

    def test_branches_at_nonexistent(self):
        import uuid_utils as uuid

        g = ChatGraph("test")
        assert g.branches_at(uuid.uuid7()) == []


class TestChatGraphExplicitParent:
    """Test that cmd.parent_id overrides cursor for append."""

    def test_explicit_parent_id(self):
        g = ChatGraph("test")
        root = Command(type=CommandType.MESSAGE)
        g.append(root)

        mid = Command(type=CommandType.MESSAGE)
        g.append(mid)

        # Append with explicit parent_id pointing back to root (not cursor=mid)
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
        assert isinstance(g, ChatGraph)
        assert g.world_id == "world_1"
        assert g.channel == "general"

    def test_named_channel(self):
        reg = ChatGraphRegistry()
        g = reg.channel("w1", "strategy")
        assert g.channel == "strategy"
        assert g.world_id == "w1"

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

    def test_different_worlds_isolated(self):
        reg = ChatGraphRegistry()
        g1 = reg.channel("w1", "general")
        g2 = reg.channel("w2", "general")
        assert g1 is not g2

    def test_channels_list(self):
        reg = ChatGraphRegistry()
        reg.channel("w1", "strategy")
        reg.channel("w1", "negotiation")
        reg.channel("w1", "general")
        reg.channel("w2", "general")

        w1_channels = reg.channels("w1")
        assert set(w1_channels) == {"strategy", "negotiation", "general"}

        w2_channels = reg.channels("w2")
        assert w2_channels == ["general"]

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

    def test_list_worlds(self):
        reg = ChatGraphRegistry()
        reg.channel("w1", "general")
        reg.channel("w2", "strategy")
        assert set(reg.list_worlds()) == {"w1", "w2"}

    def test_get_is_shorthand_for_general(self):
        reg = ChatGraphRegistry()
        g1 = reg.get("w1")
        g2 = reg.channel("w1", "general")
        assert g1 is g2


# =============================================================================
# append_history Toggle Tests (Broker Integration)
# =============================================================================


class TestAppendHistoryToggle:
    @pytest.mark.asyncio
    async def test_persistent_command_in_history(self):
        broker = CommandBroker()
        cmd = Command(type=CommandType.MESSAGE, payload={"content": "visible"}, append_history=True)
        await broker.enqueue("world", cmd)

        history = await broker.get_history("world")
        assert len(history) == 1
        assert history[0].id == cmd.id

    @pytest.mark.asyncio
    async def test_ephemeral_command_not_in_history(self):
        broker = CommandBroker()
        cmd = Command(
            type=CommandType.MESSAGE,
            payload={"content": "ephemeral"},
            append_history=False,
        )
        await broker.enqueue("world", cmd)

        history = await broker.get_history("world")
        assert len(history) == 0

    @pytest.mark.asyncio
    async def test_ephemeral_still_enqueued(self):
        """Ephemeral commands are still processed, just not tracked in history."""
        broker = CommandBroker()
        cmd = Command(type=CommandType.MESSAGE, append_history=False)
        await broker.enqueue("world", cmd)

        pending = await broker.get_pending_count("world")
        assert pending == 1

        dequeued = await broker.dequeue("world")
        assert len(dequeued) == 1
        assert dequeued[0].id == cmd.id

    @pytest.mark.asyncio
    async def test_mixed_persistent_and_ephemeral(self):
        broker = CommandBroker()
        persistent = Command(type=CommandType.MESSAGE, payload={"v": 1}, append_history=True)
        ephemeral = Command(type=CommandType.MESSAGE, payload={"v": 2}, append_history=False)

        await broker.enqueue("world", persistent)
        await broker.enqueue("world", ephemeral)

        history = await broker.get_history("world")
        assert len(history) == 1
        assert history[0].payload["v"] == 1

        # Both are in the queue
        pending = await broker.get_pending_count("world")
        assert pending == 2

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
        assert history[0].payload["i"] == 0
        assert history[1].payload["i"] == 2


# =============================================================================
# Broker + ChatGraph + Channel Integration
# =============================================================================


class TestBrokerChatGraphIntegration:
    @pytest.mark.asyncio
    async def test_broker_auto_appends_to_default_channel(self):
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        cmd = Command(type=CommandType.MESSAGE, payload={"content": "hello"})
        await broker.enqueue("world", cmd)

        graph = registry.get("world")
        assert graph.size == 1
        assert graph.cursor == cmd.id

    @pytest.mark.asyncio
    async def test_broker_routes_to_named_channel(self):
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        cmd = Command(type=CommandType.MESSAGE, channel="strategy", payload={"content": "plan"})
        await broker.enqueue("world", cmd)

        # Default channel should be empty
        assert registry.get("world").size == 0

        # Strategy channel should have the message
        strategy = registry.channel("world", "strategy")
        assert strategy.size == 1
        assert strategy.cursor == cmd.id

    @pytest.mark.asyncio
    async def test_broker_ephemeral_skips_graph(self):
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        cmd = Command(type=CommandType.MESSAGE, append_history=False)
        await broker.enqueue("world", cmd)

        graph = registry.get("world")
        assert graph.size == 0

    @pytest.mark.asyncio
    async def test_broker_linear_chain_builds_graph(self):
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        cmds = [
            Command(type=CommandType.MESSAGE, payload={"i": i})
            for i in range(5)
        ]
        for c in cmds:
            await broker.enqueue("world", c)

        graph = registry.get("world")
        assert graph.size == 5
        path = graph.active_path()
        assert len(path) == 5
        assert [p.payload["i"] for p in path] == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_broker_with_explicit_parent(self):
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        root = Command(type=CommandType.MESSAGE, payload={"content": "root"})
        await broker.enqueue("world", root)

        child = Command(type=CommandType.MESSAGE, payload={"content": "child"})
        await broker.enqueue("world", child)

        # Fork from root via parent_id
        fork = Command(type=CommandType.MESSAGE, payload={"content": "fork"}, parent_id=root.id)
        await broker.enqueue("world", fork)

        graph = registry.get("world")
        root_node = graph.get_node(root.id)
        assert len(root_node.children) == 2

        path = graph.active_path()
        assert len(path) == 2
        assert path[0].id == root.id
        assert path[1].id == fork.id

    @pytest.mark.asyncio
    async def test_multi_channel_isolation(self):
        """Messages on different channels build independent graphs."""
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        for i in range(3):
            await broker.enqueue("world", Command(
                type=CommandType.MESSAGE, channel="strategy", payload={"i": i},
            ))
        for i in range(2):
            await broker.enqueue("world", Command(
                type=CommandType.MESSAGE, channel="negotiation", payload={"i": i},
            ))

        assert registry.channel("world", "strategy").size == 3
        assert registry.channel("world", "negotiation").size == 2

        strategy_path = registry.channel("world", "strategy").active_path()
        neg_path = registry.channel("world", "negotiation").active_path()

        assert len(strategy_path) == 3
        assert len(neg_path) == 2

    @pytest.mark.asyncio
    async def test_channels_list_after_enqueue(self):
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)

        await broker.enqueue("world", Command(
            type=CommandType.MESSAGE, channel="strategy",
        ))
        await broker.enqueue("world", Command(
            type=CommandType.MESSAGE, channel="negotiation",
        ))

        channels = registry.channels("world")
        assert set(channels) == {"strategy", "negotiation"}


# =============================================================================
# Resources Integration Tests
# =============================================================================


class TestResourcesIntegration:
    """Test that ChatGraphRegistry works with the Resources DI container."""

    def test_insert_and_require(self):
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        retrieved = resources.require(ChatGraphRegistry)
        assert retrieved is registry

    def test_processor_pattern(self):
        """Simulate what a processor does: require registry, get channel, read path."""
        resources = Resources()
        registry = ChatGraphRegistry()
        broker = CommandBroker(chat_graphs=registry)
        resources.insert(registry)
        resources.insert(broker)

        # Simulate: processor adds a message via broker
        import asyncio
        async def simulate():
            cmd = Command(
                type=CommandType.MESSAGE,
                channel="strategy",
                payload={"content": "Let's plan."},
            )
            await broker.enqueue("world", cmd)

            # Processor reads context from graph
            reg = resources.require(ChatGraphRegistry)
            graph = reg.channel("world", "strategy")
            path = graph.active_path()

            assert len(path) == 1
            assert path[0].payload["content"] == "Let's plan."

        asyncio.run(simulate())

    def test_contains_check(self):
        resources = Resources()
        assert ChatGraphRegistry not in resources

        resources.insert(ChatGraphRegistry())
        assert ChatGraphRegistry in resources
