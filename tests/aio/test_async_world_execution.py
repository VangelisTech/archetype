import asyncio
import pytest
import pytest_asyncio
from daft import col, lit

from archetype.core.config import StorageConfig, CacheConfig, WorldConfig, RunConfig
from archetype.core.runtime.storage import StorageContextFactory
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
    context = StorageContextFactory.build(storage)

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

    def __init__(self, delay_ms: int, shared: dict | None = None, start_evt: asyncio.Event | None = None, proceed_evt: asyncio.Event | None = None):
        self.delay_ms = delay_ms
        self._shared = shared
        self._start_evt = start_evt
        self._proceed_evt = proceed_evt

    async def process(self, df, **kwargs):
        if self._shared is not None:
            lock = self._shared.setdefault("lock", asyncio.Lock())
            async with lock:
                self._shared["current"] = self._shared.get("current", 0) + 1
                self._shared["peak"] = max(self._shared.get("peak", 0), self._shared["current"])
        # Signal that this processor has started, and wait until allowed to proceed. This
        # removes time-based nondeterminism and enforces overlap deterministically in tests.
        if self._start_evt is not None:
            self._start_evt.set()
        if self._proceed_evt is not None:
            await self._proceed_evt.wait()
        else:
            await asyncio.sleep(self.delay_ms / 1000.0)
        if self._shared is not None:
            lock = self._shared["lock"]
            async with lock:
                self._shared["current"] -= 1
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
    shared = {"current": 0, "peak": 0, "lock": asyncio.Lock()}
    # Use events to deterministically coordinate overlap across archetypes
    start_evt = asyncio.Event()
    proceed_evt = asyncio.Event()
    await w.add_processor(SleepProc(0, shared, start_evt=start_evt, proceed_evt=proceed_evt))

    # Two archetypes: A and B
    _ = await w.create_entity([Position(x=0, y=0)])
    _ = await w.create_entity([Position(x=1, y=1), Marker(value=1)])

    rc2 = RunConfig()
    # Start the step concurrently so we can wait until at least one processor starts
    step_task = asyncio.create_task(w.step(rc2))
    # Wait for at least one processor to start, then allow all to proceed simultaneously
    await start_evt.wait()
    proceed_evt.set()
    await step_task
    # With two archetypes, processors should overlap at least once
    assert shared["peak"] >= 2


