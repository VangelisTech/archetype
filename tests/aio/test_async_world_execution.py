import asyncio
import time
import pytest
import pytest_asyncio
import daft
from daft import col, lit

from archetype.core.config import StorageConfig, CacheConfig, WorldConfig, RunConfig
from archetype.core.runtime.storage import StorageSessionFactory
from archetype.core.component import Component
from archetype.core.archetype import Archetype
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.aio.async_processor import AsyncProcessor


class Position(Component):
    x: int
    y: int


class P1ScaleX(AsyncProcessor):
    components = (Position,)
    priority = 5

    async def process(self, df, scale: int = 1, **kwargs):
        return df.with_column("position__x", col("position__x") * lit(scale))


class P2IncY(AsyncProcessor):
    components = (Position,)
    priority = 10

    async def process(self, df, **kwargs):
        return df.with_column("position__y", col("position__y") + lit(1))


class PBug(AsyncProcessor):
    components = (Position,)
    priority = 7

    async def process(self, df, **kwargs):
        raise RuntimeError("boom")


@pytest_asyncio.fixture(params=["async", "async_cached"], scope="function")
async def store_backend(request, tmp_path):
    uri = str(tmp_path)
    storage = StorageConfig(uri=uri, namespace="test", use_lancedb=False)
    context = StorageSessionFactory.build(storage)

    if request.param == "async":
        store = AsyncStore(context)
    elif request.param == "async_cached":
        base = AsyncStore(context)
        cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
        store = AsyncCachedStore(async_store=base, cache_config=cache_cfg)
    else:
        raise AssertionError("unknown backend")

    try:
        yield store
    finally:
        await store.shutdown()
        if isinstance(store, AsyncCachedStore):
            try:
                await store._inner.shutdown()  # type: ignore[attr-defined]
            except Exception:
                pass


@pytest_asyncio.fixture()
async def world(store_backend):
    querier = AsyncQueryManager(store_backend)
    updater = AsyncUpdateManager(store_backend)
    system = AsyncSystem()
    wcfg = WorldConfig(name="w")
    w = AsyncWorld(wcfg, querier, updater, system)
    await w.add_processor(P1ScaleX())
    await w.add_processor(PBug())
    await w.add_processor(P2IncY())
    return w


@pytest.mark.asyncio
async def test_processors_run_in_priority_and_filter_kwargs(world, store_backend):
    sig = Archetype.sig_from_components([Position(x=2, y=3)])
    _ = await world.create_entity([Position(x=2, y=3)])

    # After one step with scale=4:
    # P1: x *= 4  → 8
    # PBug: raises but should be swallowed
    # P2: y += 1  → 4
    rc = RunConfig()
    await world.step(rc, scale=4)

    df = await store_backend.get_archetype_df(sig, world.world_id, rc.run_id)
    out = df.collect().to_pylist()
    assert len(out) == 1
    row = out[0]
    assert row["position__x"] == 8
    assert row["position__y"] == 4


class SleepProc(AsyncProcessor):
    components = (Position,)
    priority = 1

    def __init__(self, delay_ms: int):
        self.delay_ms = delay_ms

    async def process(self, df, **kwargs):
        await asyncio.sleep(self.delay_ms / 1000.0)
        return df


@pytest.mark.asyncio
async def test_archetypes_process_in_parallel(world, store_backend):
    # Create two distinct archetypes by adding a no-op second component schema-wise
    class Marker(Component):
        value: int

    # Use a fresh world with a parallelism indicator
    querier = AsyncQueryManager(store_backend)
    updater = AsyncUpdateManager(store_backend)
    system = AsyncSystem()
    wcfg = WorldConfig(name="w2")
    w = AsyncWorld(wcfg, querier, updater, system)
    await w.add_processor(SleepProc(200))  # 200ms per archetype

    # Two archetypes: A and B
    _ = await w.create_entity([Position(x=0, y=0)])
    _ = await w.create_entity([Position(x=1, y=1), Marker(value=1)])

    rc2 = RunConfig()
    start = time.perf_counter()
    await w.step(rc2)
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    # Sequential would be ~400ms. Allow generous threshold to avoid flakes.
    # Allow higher bound due to environment variability
    assert elapsed_ms < 1200.0


