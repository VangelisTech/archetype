import asyncio

import daft
import pytest
import pytest_asyncio

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_store import AsyncStore
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.runtime.session import configure_session


class Position(Component):
    x: int
    y: int


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
