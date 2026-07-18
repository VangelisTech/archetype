# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# ...
"""
Tests for Resources and lifecycle hooks.

These tests validate the new features added to support agent communication:
1. Resources - Type-safe dependency injection
2. Hooks - Lifecycle callbacks
"""

from dataclasses import dataclass

import daft
import pytest
import pytest_asyncio
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.core.hooks import HookEvent, HookRegistry, OnDespawn, OnSpawn, PostTick, PreTick
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
    return AsyncWorld(
        world_id="test",
        name="test_world",
        querier=querier,
        updater=updater,
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )


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
    """Tests for typed lifecycle hooks."""

    def test_hook_events_share_nominal_base_type(self, world):
        """Concrete events inherit from HookEvent rather than a union alias."""
        event = PreTick(world_id=world.world_id, tick=0)

        assert isinstance(event, HookEvent)
        assert issubclass(PreTick, HookEvent)

    @pytest.mark.asyncio
    async def test_pre_tick_hook_fires(self, world):
        """PreTick fires before processing with the current tick."""
        events: list[PreTick] = []

        async def on_pre_tick(event: PreTick) -> None:
            events.append(event)

        world.add_hook(PreTick, on_pre_tick)
        await world.create_entity([Position(x=1, y=2)])

        rc = RunConfig(num_steps=1)
        await world.run(rc)

        assert len(events) == 1
        assert events[0].tick == 0
        assert events[0].world_id == world.world_id

    @pytest.mark.asyncio
    async def test_post_tick_hook_fires(self, world):
        """PostTick fires with the incremented tick and the live results."""
        events: list[PostTick] = []

        async def on_post_tick(event: PostTick) -> None:
            events.append(event)

        world.add_hook(PostTick, on_post_tick)
        await world.create_entity([Position(x=1, y=2)])

        rc = RunConfig(num_steps=1)
        await world.run(rc)

        assert len(events) == 1
        assert events[0].tick == 1
        assert events[0].world_id == world.world_id
        assert isinstance(events[0].results, dict)

    @pytest.mark.asyncio
    async def test_hook_ordering(self, world):
        """Hooks fire in correct order relative to processing."""
        events: list[str] = []

        async def on_pre(event: PreTick) -> None:
            events.append(f"pre:{event.tick}")

        async def on_post(event: PostTick) -> None:
            events.append(f"post:{event.tick}")

        world.add_hook(PreTick, on_pre)
        world.add_hook(PostTick, on_post)
        await world.create_entity([Position(x=0, y=0)])

        rc = RunConfig(num_steps=2)
        await world.run(rc)

        assert events == ["pre:0", "post:1", "pre:1", "post:2"]

    @pytest.mark.asyncio
    async def test_multiple_hooks_same_event(self, world):
        """Multiple hooks for the same event type all fire."""
        counts = {"a": 0, "b": 0}

        async def hook_a(event: PreTick) -> None:
            counts["a"] += 1

        async def hook_b(event: PreTick) -> None:
            counts["b"] += 1

        world.add_hook(PreTick, hook_a)
        world.add_hook(PreTick, hook_b)
        await world.create_entity([Position()])

        rc = RunConfig(num_steps=1)
        await world.run(rc)

        assert counts["a"] == 1
        assert counts["b"] == 1

    @pytest.mark.asyncio
    async def test_remove_hook(self, world):
        """Hooks can be removed via their handle."""
        count = [0]

        async def counter(event: PreTick) -> None:
            count[0] += 1

        handle = world.add_hook(PreTick, counter)
        await world.create_entity([Position()])

        rc = RunConfig(num_steps=1)
        await world.run(rc)
        assert count[0] == 1

        world.remove_hook(handle)
        await world.run(rc)
        assert count[0] == 1  # Unchanged

    @pytest.mark.asyncio
    async def test_remove_hook_is_idempotent(self, world):
        """Removing the same handle twice does not raise."""

        async def noop(event: PreTick) -> None:
            pass

        handle = world.add_hook(PreTick, noop)
        world.remove_hook(handle)
        world.remove_hook(handle)  # no-op second call

    @pytest.mark.asyncio
    async def test_remove_hook_handle_is_world_local(self, world):
        """A handle from one world must not unregister a matching hook in another."""

        other_querier = InMemoryQuerier()
        other_updater = InMemoryUpdater(other_querier)
        other = AsyncWorld(
            world_id="test",
            name="other_world",
            querier=other_querier,
            updater=other_updater,
            system=AsyncSystem(),
            resources=Resources(),
            hooks=HookRegistry(),
        )

        counts = {"one": 0, "two": 0}

        async def first(event: PreTick) -> None:
            counts["one"] += 1

        async def second(event: PreTick) -> None:
            counts["two"] += 1

        first_handle = world.add_hook(PreTick, first)
        second_handle = other.add_hook(PreTick, second)

        assert first_handle != second_handle

        other.remove_hook(first_handle)
        await world.create_entity([Position()])
        await other.create_entity([Position()])
        await world.run(RunConfig(num_steps=1))
        await other.run(RunConfig(num_steps=1))

        assert counts == {"one": 1, "two": 1}

    @pytest.mark.asyncio
    async def test_hook_error_logged_not_raised(self, world):
        """Hook errors are logged but don't crash the tick."""

        async def bad_hook(event: PreTick) -> None:
            raise ValueError("intentional error")

        world.add_hook(PreTick, bad_hook)
        await world.create_entity([Position()])

        rc = RunConfig(num_steps=1)
        await world.run(rc)  # should not raise
        assert world.tick == 1

    @pytest.mark.asyncio
    async def test_on_spawn_hook_fires_when_entity_created(self, world):
        """OnSpawn fires from create_entity with entity_id and components."""
        events: list[OnSpawn] = []

        async def on_spawn(event: OnSpawn) -> None:
            events.append(event)

        world.add_hook(OnSpawn, on_spawn)
        components = [Position(x=1, y=2)]
        eid = await world.create_entity(components)

        assert len(events) == 1
        assert events[0].entity_id == eid
        assert events[0].world_id == world.world_id
        assert events[0].components == components

    @pytest.mark.asyncio
    async def test_on_despawn_hook_fires_when_entity_removed(self, world):
        """OnDespawn fires from remove_entity with entity_id."""
        events: list[OnDespawn] = []

        async def on_despawn(event: OnDespawn) -> None:
            events.append(event)

        eid = await world.create_entity([Position(x=1, y=2)])
        world.add_hook(OnDespawn, on_despawn)
        await world.remove_entity(eid)

        assert len(events) == 1
        assert events[0].entity_id == eid
        assert events[0].world_id == world.world_id

    @pytest.mark.asyncio
    async def test_on_despawn_hook_skips_unknown_entity(self, world):
        """OnDespawn does not fire when remove_entity targets an unknown id."""
        events: list[OnDespawn] = []

        async def on_despawn(event: OnDespawn) -> None:
            events.append(event)

        world.add_hook(OnDespawn, on_despawn)
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

        # Run two steps: the spawn tick persists raw initial conditions, and
        # the processor (with its injected resources) applies on the next tick.
        rc = RunConfig(num_steps=2)
        await world.run(rc)

        # Verify - x should be 0 + 100 = 100
        for sig in set(world.entity2sig.values()):
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
        for sig in set(world.entity2sig.values()):
            df = await world.querier.query_archetype(
                sig=sig,
                world_id=world.world_id,
                run_id=world.run_id,
                ticks=[world.tick - 1],
            )
            rows = df.collect().to_pylist()
            assert len(rows) == 1
            assert rows[0]["position__x"] == 5
