# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for WorldService.fork_world state cloning (archetype#61)."""

from __future__ import annotations

import pytest

from archetype.app.broker import CommandBroker
from archetype.app.container import ServiceContainer
from archetype.app.world_service import WorldService
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: int
    y: int


class Meta(Component):
    label: str


@pytest.mark.asyncio
async def test_fork_preserves_tick_and_entity_state(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="fork_ns")
        source = await container.world_service.create_world(WorldConfig(name="src"), storage)
        assert isinstance(source, AsyncWorld)

        e1 = await source.create_entity([Position(x=1, y=2)])
        e2 = await source.create_entity([Position(x=3, y=4)])

        rc = RunConfig(num_steps=2)
        await source.run(rc)
        source_tick = source.tick

        fork = await container.world_service.fork_world(
            source.world_id, WorldConfig(name="fork-a"), storage
        )
        assert isinstance(fork, AsyncWorld)

        # Fork has a new world_id and identical tick/entity mapping.
        assert fork.world_id != source.world_id
        assert fork.tick == source_tick
        assert fork._entity2sig == source._entity2sig
        assert set(fork._entity2sig) == {e1, e2}

        # Entity counter resumes from where the source left off.
        next_id_source = next(source._entity_counter)
        next_id_fork = next(fork._entity_counter)
        assert next_id_fork == next_id_source
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_fork_state_visible_via_store_reads(tmp_path):
    """Default step reads from the store; fork must have its snapshot persisted."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="fork_ns2")
        source = await container.world_service.create_world(WorldConfig(name="src"), storage)
        assert isinstance(source, AsyncWorld)

        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        await source.create_entity([Position(x=10, y=20)])
        await source.create_entity([Position(x=30, y=40)])

        rc = RunConfig(num_steps=2)
        await source.run(rc)

        fork = await container.world_service.fork_world(
            source.world_id, WorldConfig(name="fork-b"), storage
        )
        assert isinstance(fork, AsyncWorld)

        # Query the store directly under the new world_id for the last source tick.
        last_tick = source.tick - 1
        df = await fork.querier.query_archetype(
            sig=sig,
            world_id=fork.world_id,
            run_id=str(rc.run_id),
            ticks=[last_tick],
        )
        rows = df.collect().to_pylist()
        assert len(rows) == 2
        assert all(r["world_id"] == str(fork.world_id) for r in rows)
        xs = sorted(r["position__x"] for r in rows)
        assert xs == [10, 30]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_fork_diverges_independently_from_source(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="fork_ns3")
        source = await container.world_service.create_world(WorldConfig(name="src"), storage)
        assert isinstance(source, AsyncWorld)

        await source.create_entity([Position(x=1, y=1)])
        rc = RunConfig(num_steps=1)
        await source.run(rc)

        fork = await container.world_service.fork_world(
            source.world_id, WorldConfig(name="fork-c"), storage
        )
        assert isinstance(fork, AsyncWorld)

        # entity_counter was cloned: each world has its own counter, but both
        # start from the same value, so they may yield identical ids. Entities
        # are still distinct because world_id scopes all storage reads/writes.
        source_before = dict(source._entity2sig)
        fork_before = dict(fork._entity2sig)

        # Mutate only the fork.
        fork_new = await fork.create_entity([Position(x=99, y=99)])
        assert fork_new in fork._entity2sig
        assert fork_new not in source_before  # source's view pre-mutation

        # Mutate only the source.
        source_new = await source.create_entity([Position(x=55, y=55)])
        assert source_new in source._entity2sig
        assert source_new not in fork_before  # fork's view pre-mutation

        # Their mutation caches are independent (not shared references).
        assert source._spawn_cache is not fork._spawn_cache
        assert source._despawn_cache is not fork._despawn_cache
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_fork_inherits_processors_not_hooks_or_broker(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="fork_ns4")
        source = await container.world_service.create_world(WorldConfig(name="src"), storage)
        assert isinstance(source, AsyncWorld)

        # Register a processor via a tiny fake to avoid real processor plumbing.
        class _FakeProc:
            priority = 10
            components: tuple = ()

        fake = _FakeProc()
        source.system.processors.append(fake)  # type: ignore[arg-type]

        # Add a hook that should NOT carry over.
        async def _hook(**kwargs):
            pass

        source.add_hook("post_tick", _hook)

        fork = await container.world_service.fork_world(
            source.world_id, WorldConfig(name="fork-d"), storage
        )
        assert isinstance(fork, AsyncWorld)

        # Processors inherited.
        assert fake in fork.system.processors
        # But the system instance is new so adding a processor to the source
        # later does NOT leak into the fork.
        source.system.processors.append(_FakeProc())
        assert len(fork.system.processors) == 1

        # Hooks not inherited.
        assert fork._hooks.get("post_tick", []) == []

        # Broker re-injected by create_world (not copied from source).
        assert CommandBroker in fork.resources
        fork_broker = fork.resources.require(CommandBroker)
        assert fork_broker is container.broker
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_fork_rejects_pending_mutations(tmp_path):
    """fork_world should refuse when spawn/despawn caches have un-materialized data."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="fork_ns5")
        source = await container.world_service.create_world(WorldConfig(name="src"), storage)
        assert isinstance(source, AsyncWorld)

        # Create entity but do NOT step — data is still in _spawn_cache.
        await source.create_entity([Position(x=1, y=1)])

        with pytest.raises(ValueError, match="pending mutations"):
            await container.world_service.fork_world(
                source.world_id, WorldConfig(name="fork-e"), storage
            )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_fork_unknown_source_raises(tmp_path):
    container = ServiceContainer()
    try:
        from uuid_utils import uuid7

        with pytest.raises(KeyError):
            await container.world_service.fork_world(
                uuid7(), WorldConfig(name="x"), StorageConfig()
            )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_fork_rejects_non_async_world(tmp_path):
    """fork_world should reject non-AsyncWorld instances with a TypeError."""

    class _Fake:
        world_id = "not-an-async-world"

    svc = WorldService.__new__(WorldService)
    svc._worlds = {_Fake.world_id: _Fake()}  # type: ignore[attr-defined]

    with pytest.raises(TypeError):
        await svc.fork_world(
            _Fake.world_id,
            WorldConfig(name="x"),
            StorageConfig(),  # type: ignore[arg-type]
        )
