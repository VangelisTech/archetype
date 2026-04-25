# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# ...
"""
Tests for Resources, Hooks, and MESSAGE command flow.

These tests validate the new features added to support agent communication:
1. Resources - Type-safe dependency injection
2. Hooks - Lifecycle callbacks
3. MESSAGE CommandType - Agent-to-agent communication via broker
"""

from dataclasses import dataclass

import daft
import pytest
import pytest_asyncio
from daft import DataFrame, col

from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.resources import Resources

# =============================================================================
# Test Components
# =============================================================================


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Inbox(Component):
    messages: list[str] = []


# =============================================================================
# Test Resources
# =============================================================================


@dataclass
class SimConfig:
    """Test configuration resource."""

    value: int = 42
    name: str = "test"


@dataclass
class OtherConfig:
    """Another config for testing multiple resources."""

    enabled: bool = True


# =============================================================================
# Minimal Test Infrastructure
# =============================================================================


class InMemoryQuerier:
    """Minimal querier for tests."""

    def __init__(self):
        self._store: dict = {}

    async def query_archetype(self, **kwargs):
        import pyarrow as pa

        from archetype.core.archetype import Archetype

        sig = kwargs["sig"]
        ticks = kwargs.get("ticks", [0])
        key = (sig, ticks[0] if ticks else 0)

        if key in self._store:
            return self._store[key]

        schema = Archetype.get_archetype_schema(sig)
        return daft.from_arrow(pa.Table.from_batches([], schema=schema))

    def store(self, sig, tick, df):
        self._store[(sig, tick)] = df


class InMemoryUpdater:
    """Minimal updater for tests."""

    def __init__(self, querier: InMemoryQuerier):
        self._querier = querier

    async def update(self, df, sig, tick, world_id, run_id):
        df = (
            df.with_column("tick", daft.lit(tick))
            .with_column("world_id", daft.lit(str(world_id)))
            .with_column("run_id", daft.lit(str(run_id)))
        )
        df_mat = df.collect()
        self._querier.store(sig, tick, df_mat)
        return df_mat


@pytest_asyncio.fixture
async def world():
    """Create a minimal async world for testing."""
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    wcfg = WorldConfig(name="test_world")
    return AsyncWorld(wcfg, querier, updater, system)


# =============================================================================
# Resources Tests
# =============================================================================


class TestResources:
    """Tests for the Resources DI container."""

    def test_insert_and_get(self):
        """Resources can store and retrieve by type."""
        resources = Resources()
        config = SimConfig(value=100, name="custom")

        resources.insert(config)

        retrieved = resources.get(SimConfig)
        assert retrieved is not None
        assert retrieved.value == 100
        assert retrieved.name == "custom"

    def test_require_raises_on_missing(self):
        """require() raises KeyError for missing resources."""
        resources = Resources()

        with pytest.raises(KeyError, match="SimConfig"):
            resources.require(SimConfig)

    def test_require_returns_existing(self):
        """require() returns existing resource."""
        resources = Resources()
        config = SimConfig(value=999)
        resources.insert(config)

        result = resources.require(SimConfig)
        assert result.value == 999

    def test_get_returns_none_on_missing(self):
        """get() returns None for missing resources."""
        resources = Resources()

        result = resources.get(SimConfig)
        assert result is None

    def test_remove_resource(self):
        """Resources can be removed."""
        resources = Resources()
        config = SimConfig()
        resources.insert(config)

        removed = resources.remove(SimConfig)
        assert removed is config
        assert resources.get(SimConfig) is None

    def test_contains_operator(self):
        """Resources supports 'in' operator."""
        resources = Resources()

        assert SimConfig not in resources

        resources.insert(SimConfig())
        assert SimConfig in resources

    def test_multiple_resource_types(self):
        """Multiple resource types can coexist."""
        resources = Resources()
        resources.insert(SimConfig(value=1))
        resources.insert(OtherConfig(enabled=False))

        assert resources.require(SimConfig).value == 1
        assert resources.require(OtherConfig).enabled is False

    def test_overwrite_same_type(self):
        """Inserting same type overwrites previous."""
        resources = Resources()
        resources.insert(SimConfig(value=1))
        resources.insert(SimConfig(value=2))

        assert resources.require(SimConfig).value == 2

    def test_repr(self):
        """Resources has useful repr."""
        resources = Resources()
        resources.insert(SimConfig())
        resources.insert(OtherConfig())

        repr_str = repr(resources)
        assert "SimConfig" in repr_str
        assert "OtherConfig" in repr_str


# =============================================================================
# Hooks Tests
# =============================================================================


class TestHooks:
    """Tests for lifecycle hooks."""

    @pytest.mark.asyncio
    async def test_pre_tick_hook_fires(self, world):
        """pre_tick hook fires before processing."""
        events = []

        async def on_pre_tick(world, tick, **kwargs):
            events.append(("pre_tick", tick))

        world.add_hook("pre_tick", on_pre_tick)
        await world.create_entity([Position(x=1, y=2)])

        rc = RunConfig(num_steps=1)
        await world.run(rc)

        assert ("pre_tick", 0) in events

    @pytest.mark.asyncio
    async def test_post_tick_hook_fires(self, world):
        """post_tick hook fires after processing."""
        events = []

        async def on_post_tick(world, tick, **kwargs):
            events.append(("post_tick", tick))

        world.add_hook("post_tick", on_post_tick)
        await world.create_entity([Position(x=1, y=2)])

        rc = RunConfig(num_steps=1)
        await world.run(rc)

        # post_tick fires with tick=1 (after increment)
        assert ("post_tick", 1) in events

    @pytest.mark.asyncio
    async def test_hook_ordering(self, world):
        """Hooks fire in correct order relative to processing."""
        events = []

        async def on_pre(world, tick, **kwargs):
            events.append(f"pre:{tick}")

        async def on_post(world, tick, **kwargs):
            events.append(f"post:{tick}")

        world.add_hook("pre_tick", on_pre)
        world.add_hook("post_tick", on_post)
        await world.create_entity([Position(x=0, y=0)])

        rc = RunConfig(num_steps=2)
        await world.run(rc)

        # Verify interleaved order
        assert events == ["pre:0", "post:1", "pre:1", "post:2"]

    @pytest.mark.asyncio
    async def test_multiple_hooks_same_event(self, world):
        """Multiple hooks for same event all fire."""
        counts = {"a": 0, "b": 0}

        async def hook_a(**kwargs):
            counts["a"] += 1

        async def hook_b(**kwargs):
            counts["b"] += 1

        world.add_hook("pre_tick", hook_a)
        world.add_hook("pre_tick", hook_b)
        await world.create_entity([Position()])

        rc = RunConfig(num_steps=1)
        await world.run(rc)

        assert counts["a"] == 1
        assert counts["b"] == 1

    @pytest.mark.asyncio
    async def test_remove_hook(self, world):
        """Hooks can be removed."""
        count = [0]

        async def counter(**kwargs):
            count[0] += 1

        world.add_hook("pre_tick", counter)
        await world.create_entity([Position()])

        rc = RunConfig(num_steps=1)
        await world.run(rc)
        assert count[0] == 1

        world.remove_hook("pre_tick", counter)
        await world.run(rc)
        assert count[0] == 1  # Unchanged

    @pytest.mark.asyncio
    async def test_hook_error_logged_not_raised(self, world):
        """Hook errors are logged but don't crash the world."""

        async def bad_hook(**kwargs):
            raise ValueError("intentional error")

        world.add_hook("pre_tick", bad_hook)
        await world.create_entity([Position()])

        rc = RunConfig(num_steps=1)
        # Should not raise
        await world.run(rc)
        assert world.tick == 1

    @pytest.mark.asyncio
    async def test_on_spawn_hook_fires_when_entity_created(self, world):
        """on_spawn hook fires on create_entity with entity_id and components."""
        events: list[dict] = []

        async def on_spawn(**kwargs):
            events.append(kwargs)

        world.add_hook("on_spawn", on_spawn)
        components = [Position(x=1, y=2)]
        eid = await world.create_entity(components)

        assert len(events) == 1
        assert events[0]["entity_id"] == eid
        assert events[0]["world"] is world
        assert events[0]["components"] == components

    @pytest.mark.asyncio
    async def test_on_despawn_hook_fires_when_entity_removed(self, world):
        """on_despawn hook fires on remove_entity with entity_id."""
        events: list[dict] = []

        async def on_despawn(**kwargs):
            events.append(kwargs)

        eid = await world.create_entity([Position(x=1, y=2)])
        world.add_hook("on_despawn", on_despawn)
        await world.remove_entity(eid)

        assert len(events) == 1
        assert events[0]["entity_id"] == eid
        assert events[0]["world"] is world

    @pytest.mark.asyncio
    async def test_on_despawn_hook_skips_unknown_entity(self, world):
        """on_despawn hook does not fire when remove_entity targets an unknown id."""
        events: list[dict] = []

        async def on_despawn(**kwargs):
            events.append(kwargs)

        world.add_hook("on_despawn", on_despawn)
        await world.remove_entity(9999)

        assert events == []


# =============================================================================
# Resources in Processor Tests
# =============================================================================


class ResourceAccessProcessor(AsyncProcessor):
    """Processor that reads from resources."""

    components = (Position,)
    priority = 10

    async def process(self, df: DataFrame, resources: Resources = None, **kwargs) -> DataFrame:
        if resources is None:
            return df

        config = resources.get(SimConfig)
        if config:
            # Add the config value to x coordinate
            return df.with_column("position__x", col("position__x") + config.value)
        return df


class TestResourcesInProcessor:
    """Tests for resources being passed to processors."""

    @pytest.mark.asyncio
    async def test_processor_receives_resources(self, world):
        """Processors can access world resources."""
        # Setup
        world.resources.insert(SimConfig(value=100))
        await world.system.add_processor(ResourceAccessProcessor())
        await world.create_entity([Position(x=0, y=0)])

        # Run
        rc = RunConfig(num_steps=1)
        await world.run(rc)

        # Verify - x should be 0 + 100 = 100
        for sig in set(world._entity2sig.values()):
            df = await world.querier.query_archetype(
                sig=sig,
                world_id=world.world_id,
                run_id=world.run_id,
                ticks=[world.tick - 1],
            )
            rows = df.collect().to_pylist()
            assert len(rows) == 1
            assert rows[0]["position__x"] == 100

    @pytest.mark.asyncio
    async def test_processor_without_resources_works(self, world):
        """Processors work even if they don't use resources."""
        # Don't add any resources
        await world.system.add_processor(ResourceAccessProcessor())
        await world.create_entity([Position(x=5, y=5)])

        # Run - should not crash
        rc = RunConfig(num_steps=1)
        await world.run(rc)

        # x should be unchanged since no config
        for sig in set(world._entity2sig.values()):
            df = await world.querier.query_archetype(
                sig=sig,
                world_id=world.world_id,
                run_id=world.run_id,
                ticks=[world.tick - 1],
            )
            rows = df.collect().to_pylist()
            assert len(rows) == 1
            assert rows[0]["position__x"] == 5


# =============================================================================
# MESSAGE Command Tests
# =============================================================================


class TestMessageCommand:
    """Tests for MESSAGE CommandType."""

    @pytest.mark.asyncio
    async def test_message_command_enqueue_dequeue(self):
        """MESSAGE commands can be enqueued and dequeued."""
        broker = CommandBroker()

        cmd = Command(
            type=CommandType.MESSAGE,
            payload={
                "sender_id": 1,
                "receiver_id": 2,
                "content": "Hello!",
            },
        )
        await broker.enqueue("world1", cmd)

        pending = await broker.get_pending_count("world1")
        assert pending == 1

        dequeued = await broker.dequeue("world1")
        assert len(dequeued) == 1
        assert dequeued[0].type == CommandType.MESSAGE
        assert dequeued[0].payload["content"] == "Hello!"

    @pytest.mark.asyncio
    async def test_message_ordering_by_tick(self):
        """Messages are ordered by tick, then priority."""
        broker = CommandBroker()

        # Enqueue in reverse tick order
        for tick in [2, 0, 1]:
            cmd = Command(
                type=CommandType.MESSAGE,
                tick=tick,
                payload={"tick": tick},
            )
            await broker.enqueue("world1", cmd)

        dequeued = await broker.dequeue("world1", max_items=3)

        # Should come out in tick order: 0, 1, 2
        assert [c.payload["tick"] for c in dequeued] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_broker_debug_logging(self, caplog):
        """Broker debug mode logs operations."""
        import logging

        caplog.set_level(logging.DEBUG)
        broker = CommandBroker(debug=True)

        cmd = Command(type=CommandType.MESSAGE, payload={"test": True})
        await broker.enqueue("world1", cmd)
        await broker.dequeue("world1")

        # Check logs contain broker events
        log_text = caplog.text
        assert "[broker] enqueue" in log_text
        assert "[broker] dequeue" in log_text

    @pytest.mark.asyncio
    async def test_broker_history_tracks_messages(self):
        """Broker maintains history of enqueued commands."""
        broker = CommandBroker()

        for i in range(3):
            cmd = Command(
                type=CommandType.MESSAGE,
                payload={"index": i},
            )
            await broker.enqueue("world1", cmd)

        history = await broker.get_history("world1")
        assert len(history) == 3
        assert [h.payload["index"] for h in history] == [0, 1, 2]


# =============================================================================
# Integration Test: Resources + Hooks + Messaging
# =============================================================================


class MessageSenderProcessor(AsyncProcessor):
    """Sends a message via broker on each tick."""

    components = (Position,)
    priority = 10

    async def process(
        self, df: DataFrame, resources: Resources = None, tick: int = 0, **kwargs
    ) -> DataFrame:
        if resources is None:
            return df

        broker = resources.get(CommandBroker)
        if broker:
            cmd = Command(
                type=CommandType.MESSAGE,
                tick=tick,
                payload={"sender": "processor", "tick": tick},
            )
            await broker.enqueue("integration_world", cmd)

        return df


class TestIntegration:
    """Integration tests combining Resources, Hooks, and Messaging."""

    @pytest.mark.asyncio
    async def test_full_message_flow(self):
        """Complete flow: processor sends message via broker, hook observes."""
        # Setup
        querier = InMemoryQuerier()
        updater = InMemoryUpdater(querier)
        system = AsyncSystem()
        wcfg = WorldConfig(name="integration_world")
        world = AsyncWorld(wcfg, querier, updater, system)

        broker = CommandBroker()
        world.resources.insert(broker)

        # Track hook invocations
        hook_data = []

        async def track_post_tick(world, tick, **kwargs):
            pending = await broker.get_pending_count("integration_world")
            hook_data.append({"tick": tick, "pending": pending})

        world.add_hook("post_tick", track_post_tick)
        await system.add_processor(MessageSenderProcessor())
        await world.create_entity([Position(x=0, y=0)])

        # Run 3 ticks
        rc = RunConfig(num_steps=3)
        await world.run(rc)

        # Verify messages were enqueued each tick
        assert len(hook_data) == 3

        # Messages accumulate since nothing dequeues them
        # After tick 1: 1 pending, after tick 2: 2 pending, after tick 3: 3 pending
        assert hook_data[0]["pending"] == 1
        assert hook_data[1]["pending"] == 2
        assert hook_data[2]["pending"] == 3

        # Verify broker history
        history = await broker.get_history("integration_world")
        assert len(history) == 3
        assert [h.payload["tick"] for h in history] == [0, 1, 2]
