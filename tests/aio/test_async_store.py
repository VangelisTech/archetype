import daft
import pytest
import pytest_asyncio
import uuid_utils as uuid
from daft import DataFrame

from archetype import StorageConfig
from archetype.core import Archetype, Component
from archetype.core.aio import AsyncCachedStore, AsyncStore
from archetype.core.runtime.storage import StorageContextFactory


class Position(Component):
    x: int
    y: int


class Meta(Component):
    name: str
    tags: list[str]


class Stats(Component):
    score: float
    active: bool


@pytest_asyncio.fixture
async def inner_store(tmp_path):
    storage = StorageConfig(uri=str(tmp_path), namespace="test", use_lancedb=False)
    context = StorageContextFactory.build(storage)
    store = AsyncStore(context)
    try:
        yield store
    finally:
        await store.shutdown()


@pytest_asyncio.fixture
async def cached_store(inner_store):
    from archetype.core.config import CacheConfig

    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner_store, cache_config=cache_cfg)
    try:
        yield cached
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_async_store_append_and_query_filters_types(inner_store):
    store = inner_store

    # Build a row using the canonical helper so schema matches
    sig = Archetype.sig_from_components([Position(x=1, y=2)])
    world_id = uuid.uuid7()  # non-string on purpose
    run_id = uuid.uuid7()  # non-string on purpose

    row = Archetype.to_row_dict(
        entity_id=1,
        tick=0,
        components=[Position(x=1, y=2)],
        world_id=str(world_id),
        run_id=str(run_id),
    )
    df: DataFrame = daft.from_pylist([row]).collect()

    # Touch table so it exists per store semantics
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)
    await store.append(sig, df)

    # Query using UUID objects; AsyncStore casts to str and should match
    out = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)
    out = out.collect()
    assert out.count_rows() == 1


@pytest.mark.asyncio
async def test_async_cached_store_reads_from_cache_then_flush_on_shutdown(inner_store):
    inner = inner_store
    from archetype.core.config import CacheConfig

    cache_cfg = CacheConfig(flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)

    try:
        sig = Archetype.sig_from_components([Position(x=1, y=2)])
        world_id = "w"
        run_id = "r"
        rows = [
            Archetype.to_row_dict(
                entity_id=i,
                tick=0,
                components=[Position(x=i, y=i)],
                world_id=world_id,
                run_id=run_id,
            )
            for i in range(5)
        ]
        df = daft.from_pylist(rows).collect()

        await cached.append(sig, df)

        # Should be visible via cached store
        out_cached = await cached.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_cached.collect().count_rows() == 5

        # Inner store should not see data yet (no flush occurred)
        out_inner_pre = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_inner_pre.collect().count_rows() == 0

        # Explicitly shutdown cached to flush
        await cached.shutdown()

        out_inner_post = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_inner_post.collect().count_rows() == 5
    finally:
        # Ensure idempotent shutdown
        await cached.shutdown()


@pytest.mark.asyncio
async def test_async_cached_store_flush_on_row_threshold(inner_store):
    inner = inner_store
    from archetype.core.config import CacheConfig

    cache_cfg = CacheConfig(flush_rows=1, flush_mb=1, global_mb=1024, idle_sec=3600)
    cached = AsyncCachedStore(async_store=inner, cache_config=cache_cfg)

    try:
        sig = Archetype.sig_from_components([Position(x=1, y=2)])
        world_id = "w2"
        run_id = "r2"
        row = Archetype.to_row_dict(
            entity_id=1, tick=0, components=[Position(x=1, y=2)], world_id=world_id, run_id=run_id
        )
        df = daft.from_pylist([row]).collect()

        # Ensure inner table exists per store semantics
        _ = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        await cached.append(sig, df)

        # Threshold should have forced a flush to inner immediately
        out_inner = await inner.get_archetype_df(sig, world_id=world_id, run_id=run_id)
        assert out_inner.collect().count_rows() == 1
    finally:
        await cached.shutdown()


@pytest.mark.asyncio
async def test_store_with_nested_and_diverse_types(inner_store):
    store = inner_store

    sig = Archetype.sig_from_components(
        [
            Position(x=0, y=0),
            Meta(name="agent", tags=["alpha", "beta"]),
            Stats(score=3.14, active=True),
        ]
    )

    world_id = "worldX"
    run_id = "runY"

    rows = [
        Archetype.to_row_dict(
            entity_id=i,
            tick=i % 3,
            components=[
                Position(x=i, y=-i),
                Meta(name=f"agent-{i}", tags=["alpha", str(i)]),
                Stats(score=0.5 * i, active=(i % 2 == 0)),
            ],
            world_id=world_id,
            run_id=run_id,
        )
        for i in range(10)
    ]

    df = daft.from_pylist(rows).collect()

    # Ensure table exists and append
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)
    await store.append(sig, df)

    # Query a subset and verify schema presence
    out = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)
    out = out.collect()
    assert out.count_rows() == 10
    for col_name in [
        "position__x",
        "position__y",
        "meta__name",
        "meta__tags",
        "stats__score",
        "stats__active",
    ]:
        assert col_name in out.column_names
