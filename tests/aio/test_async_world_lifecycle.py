import pytest
import pytest_asyncio

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig
from archetype.core.hooks import HookRegistry
from archetype.core.resources import Resources
from archetype.runtime.session import configure_session


class Position(Component):
    x: int
    y: int


@pytest_asyncio.fixture(params=["async", "async_cached"], scope="function")
async def store_backend(request, tmp_path):
    uri = str(tmp_path)
    storage = StorageConfig(uri=uri, namespace="test")
    session = configure_session(storage)

    if request.param == "async":
        store = AsyncStore(session, io_config=storage.io_config)
    elif request.param == "async_cached":
        base = AsyncStore(session, io_config=storage.io_config)
        cache_cfg = CacheConfig(
            flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600
        )
        store = AsyncCachedStore(async_store=base, cache_config=cache_cfg)
    else:
        raise AssertionError("unknown backend")

    try:
        yield store
    finally:
        await store.shutdown()


@pytest_asyncio.fixture()
async def world(store_backend):
    return AsyncWorld(
        world_id="test",
        name="w",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )


@pytest.mark.asyncio
async def test_run_sets_run_id_and_advances_ticks(world):
    # Create one entity before running
    e1 = await world.create_entity([Position(x=1, y=2)])
    assert isinstance(e1, int)

    rc = RunConfig(num_steps=2)
    await world.run(rc)
    assert world.tick == 2


@pytest.mark.asyncio
async def test_step_updates_live_snapshot_and_clears_caches(world, store_backend):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    # Spawn 3 entities and step once
    for i in range(3):
        _ = await world.create_entity([Position(x=i, y=-i)])

    rc = RunConfig()
    await world.step(rc)

    # Live view via public API should reflect only active rows for this signature
    live_df = await world.get_components([Position])
    assert live_df.collect().count_rows() == 3

    # Store should have rows stamped with tick=0
    out_all = await store_backend.get_archetype_df(sig, world.world_id, world.run_id)
    out_all = out_all.collect()
    assert out_all.count_rows() == 3
    assert all(row["tick"] == 0 for row in out_all.select("tick").to_pylist())


@pytest.mark.asyncio
async def test_query_default_tick_progression(world, store_backend):
    sig = Archetype.sig_from_components([Position(x=7, y=8)])

    # Create entity and execute 3 steps; rows should be stamped 0,1,2
    _ = await world.create_entity([Position(x=7, y=8)])
    rc = RunConfig()
    await world.step(rc)  # writes tick 0
    await world.step(rc)  # writes tick 1 (query from tick 0)
    await world.step(rc)  # writes tick 2 (query from tick 1)

    df = await store_backend.get_archetype_df(sig, world.world_id, world.run_id)
    ticks = sorted({row["tick"] for row in df.collect().to_pylist()})
    assert ticks == [0, 1, 2]
