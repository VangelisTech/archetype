import pytest
import pytest_asyncio

from archetype.app.storage_service import StorageContextFactory
from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


@pytest_asyncio.fixture(params=["async", "async_cached"], scope="function")
async def store_backend(request, tmp_path):
    uri = str(tmp_path)
    storage = StorageConfig(uri=uri, namespace="test", use_lancedb=False)
    context = StorageContextFactory.build(storage)

    if request.param == "async":
        store = AsyncStore(context)
    elif request.param == "async_cached":
        base = AsyncStore(context)
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
    querier = AsyncQueryManager(store_backend)
    updater = AsyncUpdateManager(store_backend)
    system = AsyncSystem()
    wcfg = WorldConfig(name="w")
    return AsyncWorld(wcfg, querier, updater, system)


@pytest.mark.asyncio
async def test_query_archetype_with_filters_and_projection(world):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    e1 = await world.create_entity([Position(x=1, y=2)])
    _e2 = await world.create_entity([Position(x=3, y=4)])
    rc = RunConfig()
    await world.step(rc)  # tick 0

    # Explicit tick filter
    df_all = await world.query_archetype(sig, rc, ticks=[0], entity_ids=None, components=None)
    assert df_all.collect().count_rows() == 2

    # Entity filter
    df_e1 = await world.query_archetype(sig, rc, ticks=[0], entity_ids=[e1], components=None)
    out = df_e1.collect().to_pylist()
    assert len(out) == 1 and out[0]["entity_id"] == e1

    # Projection
    df_proj = await world.query_archetype(
        sig, rc, ticks=[0], entity_ids=None, components=[Position(x=0, y=0)]
    )
    cols = set(df_proj.collect().column_names)
    from archetype.core.archetype import Archetype as A

    base_cols = set(A.BASE_SCHEMA.names)
    comp_cols = set(Position.get_prefixed_schema().names)
    assert base_cols.issubset(cols) and comp_cols.issubset(cols)


@pytest.mark.asyncio
async def test_get_components_unions_across_sigs_and_filters(world):
    # Create two entities across two signatures that both include Position
    e1 = await world.create_entity([Position(x=1, y=1)])
    e2 = await world.create_entity([Position(x=2, y=2), Velocity(dx=1, dy=1)])
    await world.step(RunConfig())  # materialize

    df = await world.get_components([Position])
    rows = df.collect().to_pylist()
    ids = sorted({r["entity_id"] for r in rows})
    assert ids == [e1, e2]

    # Filter by entity_ids
    df2 = await world.get_components([Position], entity_ids=[e2])
    rows2 = df2.collect().to_pylist()
    assert len(rows2) == 1 and rows2[0]["entity_id"] == e2
