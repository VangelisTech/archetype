import asyncio
import json
import logging

import pytest
import pytest_asyncio
from daft import col, lit

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_processor import AsyncProcessor
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
    w = AsyncWorld(
        world_id="test",
        name="w",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )
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

    df = await store_backend.get_archetype_df(sig, world.world_id, world.run_id)
    out = df.collect().to_pylist()
    assert len(out) == 1
    row = out[0]
    assert row["position__x"] == 8
    assert row["position__y"] == 4


class SleepProc(AsyncProcessor):
    components = (Position,)
    priority = 1

    def __init__(
        self,
        delay_ms: int,
        shared: dict | None = None,
        start_evt: asyncio.Event | None = None,
        proceed_evt: asyncio.Event | None = None,
    ):
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
    w = AsyncWorld(
        world_id="test",
        name="w2",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )
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


# ---------------------------------------------------------------------------
# Tests: kwargs forwarding and debug propagation through AsyncSystem.execute.
# ---------------------------------------------------------------------------


class CatchAllKwargsProbe(AsyncProcessor):
    """Processor using the documented catch-all signature. Captures the full
    kwargs dict it receives for assertions."""

    components = (Position,)
    priority = 5

    def __init__(self):
        self.received: list[dict] = []

    async def process(self, df, **kwargs):
        # Copy to snapshot what the system handed us, minus non-serializable
        # values we don't care about (resources is a live container).
        self.received.append({k: v for k, v in kwargs.items() if k != "resources"})
        return df


class DebugProbe(AsyncProcessor):
    """Processor with named ``debug`` param to verify debug propagation."""

    components = (Position,)
    priority = 5

    def __init__(self):
        self.received_debug: list = []

    async def process(self, df, debug=None, **kwargs):
        self.received_debug.append(debug)
        return df


@pytest.mark.asyncio
async def test_var_keyword_processor_receives_all_kwargs(store_backend):
    """A processor declared as ``process(self, df, **kwargs)`` must actually
    receive tick, debug, resources, etc. Previously the filter keyed on
    named parameters only and handed var-keyword processors an empty dict."""
    w = AsyncWorld(
        world_id="test",
        name="catchall",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )

    probe = CatchAllKwargsProbe()
    await w.add_processor(probe)
    _ = await w.create_entity([Position(x=1, y=1)])
    await w.run(RunConfig(num_steps=1, debug=True))

    assert len(probe.received) == 1, f"processor ran {len(probe.received)} times"
    kwargs = probe.received[0]
    assert "tick" in kwargs, f"tick missing from **kwargs: {sorted(kwargs)}"
    assert kwargs["tick"] == 0
    assert kwargs.get("debug") is True, f"debug not propagated: {kwargs.get('debug')!r}"


@pytest.mark.asyncio
async def test_run_config_debug_propagates_to_processor(store_backend):
    """RunConfig(debug=True) must reach processors. Previously ``debug`` bound
    to AsyncSystem.execute's named param and was never re-injected into the
    kwargs dict forwarded to processors."""
    w = AsyncWorld(
        world_id="test",
        name="debugprobe",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )

    probe = DebugProbe()
    await w.add_processor(probe)
    _ = await w.create_entity([Position(x=1, y=1)])
    await w.run(RunConfig(num_steps=1, debug=True))

    assert probe.received_debug == [True], (
        f"debug=True from RunConfig was not forwarded: {probe.received_debug}"
    )


@pytest.mark.asyncio
async def test_run_config_debug_logs_step_lifecycle_via_hooks(store_backend, caplog):
    w = AsyncWorld(
        world_id="test",
        name="debughooks",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )

    await w.add_processor(DebugProbe())
    _ = await w.create_entity([Position(x=1, y=1)])

    caplog.set_level(logging.DEBUG, logger="archetype.core.aio.async_world")
    await w.run(RunConfig(num_steps=1, debug=True))
    await w.run(RunConfig(num_steps=1, debug=False))

    payloads = [
        json.loads(record.message.removeprefix("[archetype] "))
        for record in caplog.records
        if record.name == "archetype.core.aio.async_world"
        and record.message.startswith("[archetype] ")
    ]

    assert [payload["event"] for payload in payloads] == [
        "tick_start",
        "archetypes_processing",
        "tick_end",
    ]
    assert payloads[0]["tick"] == 0
    assert payloads[0]["active_entities"] == 1
    assert payloads[0]["spawn_pending"] == 1
    assert payloads[1]["count"] == 1
    assert payloads[2]["tick"] == 1
    assert payloads[2]["live_entities"] == 1


@pytest.mark.asyncio
async def test_closed_signature_processor_still_filters_unknown_kwargs(store_backend):
    """A processor without ``**kwargs`` and without named ``tick``/``debug``
    must still be called successfully — the filter should drop kwargs it
    doesn't accept rather than raising ``TypeError``."""

    class ClosedProc(AsyncProcessor):
        components = (Position,)
        priority = 5

        def __init__(self):
            self.calls = 0

        async def process(self, df):  # no kwargs, no **kwargs
            self.calls += 1
            return df

    w = AsyncWorld(
        world_id="test",
        name="closed",
        querier=AsyncQueryManager(store=store_backend),
        updater=AsyncUpdateManager(store=store_backend),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )

    proc = ClosedProc()
    await w.add_processor(proc)
    _ = await w.create_entity([Position(x=1, y=1)])
    await w.run(RunConfig(num_steps=1, debug=True))

    assert proc.calls == 1
