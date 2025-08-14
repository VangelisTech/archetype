import pytest
import pytest_asyncio

import daft

from archetype.core.config import StorageConfig, WorldConfig, RunConfig, CacheConfig
from archetype.core.runtime.storage import StorageContextFactory
from archetype.core.component import Component
from archetype.core.archetype import Archetype
from archetype.core.aio import (
    AsyncStore,
    AsyncQueryManager,
    AsyncUpdateManager,
    AsyncSystem,
    AsyncWorld,
)
from archetype.core.aio.async_cached_store import AsyncCachedStore, MemTable


class Position(Component):
    x: int
    y: int


@pytest_asyncio.fixture
async def world(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    ctx = StorageContextFactory.build(storage)
    store = AsyncStore(ctx)
    querier = AsyncQueryManager(store)
    updater = AsyncUpdateManager(store)
    system = AsyncSystem()
    wcfg = WorldConfig(name="w-edge")
    try:
        yield AsyncWorld(wcfg, querier, updater, system)
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_add_components_before_first_step_handles_empty_live(world):
    ent = await world.create_entity([Position(x=1, y=1)])
    # Add the same component type before any step; _move_entity will not find old row in _live
    await world.add_components(ent, [Position(x=2, y=2)])
    # Stepping should succeed and write one active row
    rc = RunConfig()
    await world.step(rc)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    df = await world.query_archetype(sig, rc, ticks=[0])
    assert df.collect().count_rows() == 1


@pytest.mark.asyncio
async def test_remove_nonexistent_entity_is_noop(world):
    # Removing an unknown entity should not crash
    await world.remove_entity(999999)
    rc = RunConfig()
    await world.step(rc)


@pytest.mark.asyncio
async def test_add_components_nonexistent_entity_is_noop(world):
    # No existing entity id
    await world.add_components(424242, [Position(x=0, y=0)])
    await world.step(RunConfig())


@pytest.mark.asyncio
async def test_add_components_no_change_is_noop(world):
    ent = await world.create_entity([Position(x=1, y=1)])
    # Adding Position again should be a no-op
    await world.add_components(ent, [Position(x=3, y=3)])
    await world.step(RunConfig())


@pytest.mark.asyncio
async def test_remove_components_nonexistent_entity_is_noop(world):
    # Removing from a non-existent entity is a no-op
    await world.remove_components(777777, [Position])
    await world.step(RunConfig())


@pytest.mark.asyncio
async def test_remove_components_no_change_is_noop(world):
    ent = await world.create_entity([Position(x=1, y=1)])
    await world.step(RunConfig())
    # Removing a component type not present should be a no-op
    await world.remove_components(ent, [])
    await world.step(RunConfig())


@pytest.mark.asyncio
async def test_add_and_remove_processor(world):
    from archetype.core.aio.async_processor import AsyncProcessor

    class NoOp(AsyncProcessor):
        components = (Position,)
        priority = 1

        async def process(self, df, **kwargs):
            return df

    proc = NoOp()
    await world.add_processor(proc)
    await world.remove_processor(NoOp)


@pytest.mark.asyncio
async def test_get_components_with_entity_filter(world):
    e1 = await world.create_entity([Position(x=1, y=1)])
    e2 = await world.create_entity([Position(x=2, y=2)])
    await world.step(RunConfig())
    df = await world.get_components([Position], entity_ids=[e2])
    rows = df.collect().to_pylist()
    assert len(rows) == 1 and rows[0]["entity_id"] == e2


@pytest.mark.asyncio
async def test_async_store_empty_append_and_double_shutdown(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="edge")
    ctx = StorageContextFactory.build(storage)
    store = AsyncStore(ctx)
    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        # Ensure table exists
        _ = await store.get_archetype_df(sig, world_id="w", run_id="r")
        # Append empty df should be a no-op
        empty_df = daft.from_pylist([]).collect()
        await store.append(sig, empty_df)
    finally:
        await store.shutdown()
        # second shutdown should be harmless
        await store.shutdown()


@pytest.mark.asyncio
async def test_async_cached_store_flush_sig_no_rows_and_shutdown(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="edge-cache")
    ctx = StorageContextFactory.build(storage)
    inner = AsyncStore(ctx)
    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)

    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        # Install an empty memtable to exercise early return in _background_flush_sig
        cached._mem[sig] = MemTable()  # zero rows
        # Call private flush on empty table; should be a no-op
        await cached._background_flush_sig(sig)
    finally:
        await cached.shutdown()
        await inner.shutdown()


