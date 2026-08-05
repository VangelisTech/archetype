import asyncio

import daft
import pytest
import pytest_asyncio

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_store import AsyncStore
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import AppendReceipt
from archetype.storage.session import configure_session

pytestmark = [
    pytest.mark.contract("storage.cache.concurrent_no_loss"),
    pytest.mark.race,
]


class Position(Component):
    x: int
    y: int


class _BlockingInnerStore:
    def __init__(self, *, fail_first: bool = False):
        self.append_started = asyncio.Event()
        self.release_append = asyncio.Event()
        self.persisted: list[dict] = []
        self.fail_first = fail_first

    async def append(self, sig, df):
        self.append_started.set()
        await self.release_append.wait()
        if self.fail_first:
            self.fail_first = False
            raise OSError("injected append failure")
        rows = df.collect().to_pylist()
        self.persisted.extend(rows)
        return AppendReceipt(table_id=Archetype.get_name(sig), rows=len(rows), durable=True)

    async def flush(self):
        return None

    async def shutdown(self):
        return None


@pytest_asyncio.fixture
async def inner_store(tmp_path):
    from archetype.core.config import StorageConfig

    storage = StorageConfig(uri=str(tmp_path), namespace="test")
    session = configure_session(storage)
    store = AsyncStore(session, io_config=storage.io_config)
    try:
        yield store
    finally:
        await store.shutdown()


def test_default_global_cache_budget_is_one_gibibyte():
    from archetype.core.config import CacheConfig

    assert CacheConfig().global_mb == 1024


@pytest.mark.asyncio
async def test_cached_store_delegates_open_never_create_discovery():
    from archetype.core.config import CacheConfig

    schema = object()
    frame = object()

    class _DiscoveryInnerStore:
        def __init__(self) -> None:
            self.schema_calls = []
            self.frame_calls = []

        async def get_existing_table_schema(self, table_id):
            self.schema_calls.append(table_id)
            return schema

        async def get_existing_table_df(
            self,
            table_id,
            world_id,
            run_id,
            *,
            ticks=None,
            entity_ids=None,
            active_only=False,
        ):
            self.frame_calls.append((table_id, world_id, run_id, ticks, entity_ids, active_only))
            return frame

        async def flush(self):
            return None

        async def shutdown(self):
            return None

    inner = _DiscoveryInnerStore()
    cached = AsyncCachedStore(
        async_store=inner,  # type: ignore[arg-type]
        cache_config=CacheConfig(idle_sec=3600),
    )
    try:
        assert await cached.get_existing_table_schema("table") is schema
        assert (
            await cached.get_existing_table_df(
                "table",
                "world",
                "run",
                ticks=[1, 2],
                entity_ids=[3],
                active_only=True,
            )
            is frame
        )
        assert inner.schema_calls == ["table"]
        assert inner.frame_calls == [("table", "world", "run", [1, 2], [3], True)]
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_flush_detaches_exact_snapshot_without_losing_concurrent_append():
    from archetype.core.config import CacheConfig

    inner = _BlockingInnerStore()
    cached = AsyncCachedStore(
        async_store=inner,
        cache_config=CacheConfig(
            flush_rows=10_000_000,
            flush_mb=10_000,
            global_mb=10_000,
            idle_sec=3600,
        ),
    )
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    def frame(entity_id: int):
        return daft.from_pylist(
            [
                Archetype.to_row_dict(
                    entity_id=entity_id,
                    tick=0,
                    components=[Position(x=entity_id, y=entity_id)],
                    world_id="w_race",
                    run_id="r_race",
                )
            ]
        ).collect()

    try:
        await cached.append(sig, frame(1))
        flush_task = asyncio.create_task(cached.flush())
        await inner.append_started.wait()

        await cached.append(sig, frame(2))
        inner.release_append.set()
        await flush_task

        assert [row["entity_id"] for row in inner.persisted] == [1, 2]
        assert cached.total_cached_bytes == 0
        assert cached._mem[sig].rows == 0
    finally:
        inner.release_append.set()
        await cached.shutdown()


@pytest.mark.asyncio
async def test_flush_waits_for_already_inflight_background_append():
    """A commit drain is a barrier for rows already detached by idle flush."""
    from archetype.core.config import CacheConfig

    inner = _BlockingInnerStore()
    cached = AsyncCachedStore(
        async_store=inner,
        cache_config=CacheConfig(
            flush_rows=10_000_000,
            flush_mb=10_000,
            global_mb=10_000,
            idle_sec=3600,
        ),
    )
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    frame = daft.from_pylist(
        [
            Archetype.to_row_dict(
                entity_id=1,
                tick=0,
                components=[Position(x=1, y=1)],
                world_id="w_inflight",
                run_id="r_inflight",
            )
        ]
    ).collect()

    try:
        await cached.append(sig, frame)
        background = asyncio.create_task(cached._background_flush_sig(sig))
        await inner.append_started.wait()
        assert sig in cached._inflight and cached._mem[sig].rows == 0

        barrier = asyncio.create_task(cached.flush())
        await asyncio.sleep(0)
        assert not barrier.done(), "flush returned before the detached append became durable"

        inner.release_append.set()
        assert await background is True
        await barrier
        assert [row["entity_id"] for row in inner.persisted] == [1]
        assert cached.total_cached_bytes == 0
    finally:
        inner.release_append.set()
        await cached.shutdown()


@pytest.mark.asyncio
async def test_failed_flush_requeues_snapshot_before_newer_rows():
    from archetype.core.config import CacheConfig

    inner = _BlockingInnerStore(fail_first=True)
    inner.release_append.set()
    cached = AsyncCachedStore(
        async_store=inner,
        cache_config=CacheConfig(
            flush_rows=10_000_000,
            flush_mb=10_000,
            global_mb=10_000,
            idle_sec=3600,
        ),
    )
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    def frame(entity_id: int):
        return daft.from_pylist(
            [
                Archetype.to_row_dict(
                    entity_id=entity_id,
                    tick=0,
                    components=[Position(x=entity_id, y=entity_id)],
                    world_id="w_retry",
                    run_id="r_retry",
                )
            ]
        ).collect()

    try:
        await cached.append(sig, frame(1))
        before_failure = cached.total_cached_bytes
        with pytest.raises(OSError, match="injected append failure"):
            await cached.flush()
        await cached.append(sig, frame(2))

        assert cached.total_cached_bytes > before_failure
        await cached.flush()
        assert [row["entity_id"] for row in inner.persisted] == [1, 2]
        assert cached.total_cached_bytes == 0
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_cached_store_type_casting_and_cache_hit(inner_store):
    from uuid_utils import uuid7

    from archetype.core.config import CacheConfig

    inner = inner_store
    cache_cfg = CacheConfig(flush_rows=1_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)
    try:
        sig = Archetype.sig_from_components([Position(x=1, y=2)])
        world_id = uuid7()  # non-string
        run_id = uuid7()  # non-string

        # Ensure inner table exists before any flush attempts
        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)

        rows = [
            Archetype.to_row_dict(
                entity_id=i,
                tick=0,
                components=[Position(x=i, y=i)],
                world_id=str(world_id),
                run_id=str(run_id),
            )
            for i in range(3)
        ]
        df = daft.from_pylist(rows).collect()
        await cached.append(sig, df)

        # Cache path should cast ids and return rows
        out_cached = await cached.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_cached.collect().count_rows() == 3
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_cached_store_global_budget_triggers_flush(inner_store):
    from archetype.core.config import CacheConfig

    inner = inner_store
    # Tiny global budget triggers immediate flush regardless of per-sig thresholds
    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=0, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)
    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world_id = "w"
        run_id = "r"

        # Ensure inner table exists (append path uses get_table)
        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)

        rows = [
            Archetype.to_row_dict(
                entity_id=i,
                tick=0,
                components=[Position(x=i, y=i)],
                world_id=world_id,
                run_id=run_id,
            )
            for i in range(2)
        ]
        df = daft.from_pylist(rows).collect()
        await cached.append(sig, df)

        out_inner = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_inner.collect().count_rows() == 2
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_cached_store_idle_flush(inner_store):
    from archetype.core.config import CacheConfig

    inner = inner_store
    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=0.1)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)
    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world_id = "w_i"
        run_id = "r_i"

        # Ensure inner table exists
        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)

        rows = [
            Archetype.to_row_dict(
                entity_id=1,
                tick=0,
                components=[Position(x=1, y=1)],
                world_id=world_id,
                run_id=run_id,
            )
        ]
        df = daft.from_pylist(rows).collect()
        await cached.append(sig, df)

        # Immediately inner has no data
        assert inner.get_archetype_df is not None
        out_inner_pre = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_inner_pre.collect().count_rows() == 0

        # Wait for idle flusher with polling to avoid flakes
        deadline = asyncio.get_event_loop().time() + 2.0
        flushed = False
        while asyncio.get_event_loop().time() < deadline:
            out_inner_post = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
            if out_inner_post.collect().count_rows() == 1:
                flushed = True
                break
            await asyncio.sleep(0.02)
        assert flushed, "Idle flush did not persist row within 2s"
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_get_archetype_df_unions_memtable_with_disk(inner_store):
    from archetype.core.config import CacheConfig

    inner = inner_store
    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)
    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world_id = "w_union"
        run_id = "r_union"

        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)

        first = daft.from_pylist(
            [
                Archetype.to_row_dict(
                    entity_id=1,
                    tick=0,
                    components=[Position(x=1, y=1)],
                    world_id=world_id,
                    run_id=run_id,
                )
            ]
        ).collect()
        await cached.append(sig, first)
        await cached._background_flush_sig(sig)

        second = daft.from_pylist(
            [
                Archetype.to_row_dict(
                    entity_id=2,
                    tick=1,
                    components=[Position(x=2, y=2)],
                    world_id=world_id,
                    run_id=run_id,
                )
            ]
        ).collect()
        await cached.append(sig, second)

        out = await cached.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        eids = sorted(r["entity_id"] for r in out.collect().to_pylist())
        assert eids == [1, 2]
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_get_archetype_df_filters_memtable_by_run_id(inner_store):
    from archetype.core.config import CacheConfig

    inner = inner_store
    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)
    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world_id = "w_filter"

        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id="r_a")

        df_a = daft.from_pylist(
            [
                Archetype.to_row_dict(
                    entity_id=10,
                    tick=0,
                    components=[Position(x=10, y=10)],
                    world_id=world_id,
                    run_id="r_a",
                )
            ]
        ).collect()
        await cached.append(sig, df_a)
        await cached._background_flush_sig(sig)

        df_b = daft.from_pylist(
            [
                Archetype.to_row_dict(
                    entity_id=20,
                    tick=0,
                    components=[Position(x=20, y=20)],
                    world_id=world_id,
                    run_id="r_b",
                )
            ]
        ).collect()
        await cached.append(sig, df_b)

        out_a = await cached.get_archetype_df(sig, world_id=world_id, run_id="r_a")
        eids_a = sorted(r["entity_id"] for r in out_a.collect().to_pylist())
        assert eids_a == [10]

        out_b = await cached.get_archetype_df(sig, world_id=world_id, run_id="r_b")
        eids_b = sorted(r["entity_id"] for r in out_b.collect().to_pylist())
        assert eids_b == [20]
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_cached_store_concurrent_appends_are_safe(inner_store):
    from archetype.core.config import CacheConfig

    inner = inner_store
    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)
    try:
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world_id = "w_c"
        run_id = "r_c"

        # Ensure inner table exists
        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)

        async def push(n):
            rows = [
                Archetype.to_row_dict(
                    entity_id=i + 1000 * n,
                    tick=0,
                    components=[Position(x=i, y=i)],
                    world_id=world_id,
                    run_id=run_id,
                )
                for i in range(5)
            ]
            df = daft.from_pylist(rows).collect()
            await cached.append(sig, df)

        await asyncio.gather(*(push(k) for k in range(4)))

        # Shutdown flushes pending rows
        await cached.shutdown()
        out = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out.collect().count_rows() == 4 * 5
    finally:
        # idempotent
        await cached.shutdown()
