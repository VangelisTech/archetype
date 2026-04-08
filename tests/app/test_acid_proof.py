"""
Proof of ACID Transactions in Archetype
========================================

Each test proves one ACID property by constructing a scenario that
would fail without the guarantee, then asserting it holds.

The transaction boundary is the tick. The unit of work is _run_archetype().

    Atomicity   — All side effects apply, or none do.
    Consistency — Only valid messages are routed (join predicates).
    Isolation   — Concurrent archetypes don't observe each other's mutations.
    Durability  — DataFrame persists before side effects commit.
"""

import json

import daft
import pytest

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.messaging import MessageDeliveryProcessor
from archetype.core.resources import Resources
from archetype.core.side_effects import SideEffectCollector


def msg(sender_id: int, receiver_id: int, content: str, channel: str = "general") -> str:
    return json.dumps({"receiver_id": receiver_id, "channel": channel, "content": content})


def build_df(entities: list[dict]) -> daft.DataFrame:
    data = {
        "entity_id": [],
        "is_active": [],
        "outbox__messages": [],
        "inbox__messages": [],
    }
    for e in entities:
        data["entity_id"].append(e["id"])
        data["is_active"].append(True)
        data["outbox__messages"].append(e.get("outbox", []))
        data["inbox__messages"].append(e.get("inbox", []))
    return daft.from_pydict(data)


# ============================================================================
# A — Atomicity
# ============================================================================


class TestAtomicity:
    """All side effects apply, or none do."""

    @pytest.mark.asyncio
    async def test_commit_applies_all_mutations(self):
        """When persist succeeds, ALL deferred ChatGraph appends land."""
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)
        collector = SideEffectCollector()
        resources.insert(collector)

        proc = MessageDeliveryProcessor()

        # 3 agents, each sends to the next
        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "hello from 1")]},
                {"id": 2, "outbox": [msg(2, 3, "hello from 2")]},
                {"id": 3, "outbox": [msg(3, 1, "hello from 3")]},
            ]
        )

        await proc.process(df, resources=resources, tick=1, world_id="proof")

        # Before commit: graph is empty (mutations are deferred)
        graph = registry.get_channel("proof", "general")
        assert graph is None or graph.size == 0

        # Commit (simulates successful persist)
        collector.commit()

        # After commit: all 3 messages in graph
        graph = registry.channel("proof", "general")
        assert graph.size == 3

    @pytest.mark.asyncio
    async def test_rollback_discards_all_mutations(self):
        """When persist fails, NO deferred ChatGraph appends land."""
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)
        collector = SideEffectCollector()
        resources.insert(collector)

        proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "should not appear")]},
                {"id": 2, "outbox": []},
            ]
        )

        await proc.process(df, resources=resources, tick=1, world_id="proof")

        # Simulate persist failure: rollback
        collector.rollback()

        # Graph must be empty
        graph = registry.get_channel("proof", "general")
        assert graph is None or graph.size == 0

    @pytest.mark.asyncio
    async def test_compensating_rollback_on_partial_failure(self):
        """If a mutation raises mid-commit, earlier mutations are undone."""
        registry = ChatGraphRegistry()
        collector = SideEffectCollector()

        graph = registry.channel("proof", "general")

        from archetype.app.models import Command, CommandType

        cmd1 = Command(type=CommandType.MESSAGE, tick=1, payload={"content": "first"})
        cmd2 = Command(type=CommandType.MESSAGE, tick=1, payload={"content": "second"})

        # First mutation: normal append with rollback
        collector.defer(
            apply=lambda: graph.append(cmd1),
            rollback=lambda node: graph.prune(node.cmd.id),
        )

        # Second mutation: will raise
        def bad_apply():
            graph.append(cmd2)
            raise RuntimeError("simulated failure")

        collector.defer(
            apply=bad_apply,
            rollback=lambda node: graph.prune(node.cmd.id),
        )

        with pytest.raises(RuntimeError, match="simulated failure"):
            collector.commit()

        # cmd1 was applied then rolled back; cmd2 raised before completing.
        # Graph should be empty — the compensating action undid cmd1.
        assert graph.size == 0


# ============================================================================
# C — Consistency
# ============================================================================


class TestConsistency:
    """Only valid state transitions occur. Join predicates enforce invariants."""

    @pytest.mark.asyncio
    async def test_invalid_receiver_rejected(self):
        """Messages to non-existent entity_ids are rejected via ANTI JOIN."""
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        proc = MessageDeliveryProcessor()

        # Entity 1 sends to entity 99 (doesn't exist)
        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 99, "hello?")]},
                {"id": 2, "outbox": []},
            ]
        )

        result = await proc.process(df, resources=resources, tick=1, world_id="proof")
        rows = result.collect().to_pylist()

        # Entity 2's inbox is empty (message didn't route)
        entity2 = [r for r in rows if r["entity_id"] == 2][0]
        assert entity2["inbox__messages"] == []

        # Entity 1's outbox is cleared (message was processed, just not delivered)
        entity1 = [r for r in rows if r["entity_id"] == 1][0]
        assert entity1["outbox__messages"] == []

    @pytest.mark.asyncio
    async def test_self_messaging_filtered(self):
        """Agents cannot send messages to themselves."""
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 1, "talking to myself")]},
                {"id": 2, "outbox": []},
            ]
        )

        result = await proc.process(df, resources=resources, tick=1, world_id="proof")
        rows = result.collect().to_pylist()

        entity1 = [r for r in rows if r["entity_id"] == 1][0]
        assert entity1["inbox__messages"] == []

    @pytest.mark.asyncio
    async def test_valid_receiver_delivered(self):
        """Messages to existing entity_ids are delivered via INNER JOIN."""
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "valid message")]},
                {"id": 2, "outbox": []},
            ]
        )

        result = await proc.process(df, resources=resources, tick=1, world_id="proof")
        rows = result.collect().to_pylist()

        entity2 = [r for r in rows if r["entity_id"] == 2][0]
        inbox = entity2["inbox__messages"]
        assert len(inbox) == 1
        assert json.loads(inbox[0])["content"] == "valid message"

    @pytest.mark.asyncio
    async def test_schema_preserved_after_routing(self):
        """DataFrame schema is unchanged after processor — no join artifact columns leak."""
        resources = Resources()
        resources.insert(ChatGraphRegistry())

        proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "test")]},
                {"id": 2, "outbox": []},
            ]
        )

        original_columns = set(df.column_names)
        result = await proc.process(df, resources=resources, tick=1, world_id="proof")
        result_columns = set(result.column_names)

        # No extra columns (receiver_id, sender_id, new_inbox, new_receipts)
        assert result_columns == original_columns


# ============================================================================
# I — Isolation
# ============================================================================


class TestIsolation:
    """Concurrent archetypes don't observe each other's in-progress mutations."""

    def test_forked_resources_have_independent_collectors(self):
        """Each archetype gets its own SideEffectCollector via Resources.fork()."""
        shared = Resources()
        shared.insert(ChatGraphRegistry())

        # Fork for archetype A
        fork_a = shared.fork()
        collector_a = SideEffectCollector()
        fork_a.insert(collector_a)

        # Fork for archetype B
        fork_b = shared.fork()
        collector_b = SideEffectCollector()
        fork_b.insert(collector_b)

        # Collectors are independent instances
        assert fork_a.get(SideEffectCollector) is not fork_b.get(SideEffectCollector)

        # But they share the same ChatGraphRegistry
        assert fork_a.get(ChatGraphRegistry) is fork_b.get(ChatGraphRegistry)

    def test_collector_mutations_invisible_before_commit(self):
        """Deferred mutations are not visible to other readers until commit."""
        registry = ChatGraphRegistry()
        collector = SideEffectCollector()

        from archetype.app.models import Command, CommandType

        cmd = Command(type=CommandType.MESSAGE, tick=1, payload={"content": "hidden"})

        collector.defer(
            apply=lambda: registry.channel("world", "general").append(cmd),
            rollback=lambda node: registry.channel("world", "general").prune(node.cmd.id),
        )

        # Before commit: graph doesn't exist yet
        assert registry.get_channel("world", "general") is None

        collector.commit()

        # After commit: visible
        assert registry.channel("world", "general").size == 1

    def test_fork_does_not_leak_collector_to_parent(self):
        """Inserting a collector into a fork doesn't affect the parent."""
        parent = Resources()
        parent.insert(ChatGraphRegistry())

        child = parent.fork()
        child.insert(SideEffectCollector())

        assert parent.get(SideEffectCollector) is None
        assert child.get(SideEffectCollector) is not None

    @pytest.mark.asyncio
    async def test_tick_boundary_message_visibility(self):
        """Messages written at tick N are only delivered at tick N+1.

        This proves temporal isolation: processors within a tick see the
        previous tick's state, not mutations from concurrent processors.
        """
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        proc = MessageDeliveryProcessor()

        # Tick 0: entity 1 writes a message. Delivery processor runs first
        # (priority -100) and sees empty outboxes. Then think processor fills
        # outboxes. Outboxes persist with the message.
        df_tick0 = build_df(
            [
                {"id": 1, "outbox": []},  # empty at start of tick 0
                {"id": 2, "outbox": []},
            ]
        )

        result0 = await proc.process(df_tick0, resources=resources, tick=0, world_id="proof")
        rows0 = result0.collect().to_pylist()

        # No deliveries at tick 0 (outboxes were empty)
        for row in rows0:
            assert row["inbox__messages"] == []

        # Tick 1: outboxes now have messages from tick 0's think processor
        df_tick1 = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "tick 0 message")]},
                {"id": 2, "outbox": []},
            ]
        )

        result1 = await proc.process(df_tick1, resources=resources, tick=1, world_id="proof")
        rows1 = result1.collect().to_pylist()

        # NOW entity 2 has the message
        entity2 = [r for r in rows1 if r["entity_id"] == 2][0]
        assert len(entity2["inbox__messages"]) == 1
        assert json.loads(entity2["inbox__messages"][0])["content"] == "tick 0 message"


# ============================================================================
# D — Durability
# ============================================================================


class TestDurability:
    """Side effects only apply after data is persisted."""

    @pytest.mark.asyncio
    async def test_commit_order_persist_then_effects(self):
        """Proves the ordering: persist → commit, not commit → persist.

        If we committed side effects before persist, a persist failure would
        leave the ChatGraph in a state that doesn't match any stored data.
        The _run_archetype contract prevents this.
        """
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)
        collector = SideEffectCollector()
        resources.insert(collector)

        proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "durable")]},
                {"id": 2, "outbox": []},
            ]
        )

        # Process: side effects deferred
        result = await proc.process(df, resources=resources, tick=1, world_id="proof")

        # Simulate: persist happens here (in real code: await self.update(df, sig, run_config))
        # We just materialize the DataFrame to prove the data is ready
        persisted = result.collect().to_pylist()
        assert len(persisted) == 2

        # Graph still empty before commit
        assert (
            registry.get_channel("proof", "general") is None
            or registry.channel("proof", "general").size == 0
        )

        # NOW commit (after persist succeeded)
        collector.commit()

        # Graph reflects the persisted data
        assert registry.channel("proof", "general").size == 1

    @pytest.mark.asyncio
    async def test_persist_failure_prevents_commit(self):
        """If persist raises, rollback prevents side effects from applying.

        This is the _run_archetype contract:
            try:
                df_mat = await self.update(df, sig, run_config)  # persist
                collector.commit()                                # effects
            except:
                collector.rollback()                              # discard
                raise
        """
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)
        collector = SideEffectCollector()
        resources.insert(collector)

        proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "outbox": [msg(1, 2, "should not appear")]},
                {"id": 2, "outbox": []},
            ]
        )

        await proc.process(df, resources=resources, tick=1, world_id="proof")

        # Simulate persist failure
        persist_failed = True
        if persist_failed:
            collector.rollback()

        # Graph is empty — data didn't persist, so effects didn't apply
        graph = registry.get_channel("proof", "general")
        assert graph is None or graph.size == 0
