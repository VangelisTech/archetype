import daft
import pytest
import pytest_asyncio

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_lancedb_store import AsyncLancedbStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.runtime.session import configure_session


class Position(Component):
    x: int
    y: int


@pytest_asyncio.fixture(params=["async", "async_cached", "lancedb"], scope="function")
async def store_backend(request, tmp_path):
    uri = str(tmp_path)
    storage = StorageConfig(
        uri=uri,
        namespace="test",
        backend=StorageBackend.LANCEDB if request.param == "lancedb" else StorageBackend.ICEBERG,
    )
    session = configure_session(storage)

    if request.param == "async":
        store = AsyncStore(session, io_config=storage.io_config)
    elif request.param == "async_cached":
        base = AsyncStore(session, io_config=storage.io_config)
        cache_cfg = CacheConfig(
            flush_rows=10_000_000, flush_mb=10_000, global_mb=10_000, idle_sec=3600
        )
        store = AsyncCachedStore(async_store=base, cache_config=cache_cfg)
    elif request.param == "lancedb":
        store = AsyncLancedbStore(uri, storage.namespace)
    else:
        raise AssertionError("unknown backend")

    try:
        yield store
    finally:
        # Graceful shutdown
        await store.shutdown()
        # If cached, also shutdown inner store
        if isinstance(store, AsyncCachedStore):
            try:
                await store._inner.shutdown()  # type: ignore[attr-defined]
            except Exception:
                pass


@pytest.mark.asyncio
async def test_update_stamps_and_query_filters(store_backend):
    store = store_backend
    querier = AsyncQueryManager(store)
    updater = AsyncUpdateManager(store)

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    world_id = "wq"
    run_id = "rq"

    # Ensure table exists
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)

    # Prepare two rows (tick will be stamped by updater)
    rows = [
        {"entity_id": 1, "position__x": 1, "position__y": 1, "is_active": True},
        {"entity_id": 2, "position__x": 2, "position__y": 2, "is_active": True},
    ]
    df_in = daft.from_pylist(rows)

    # Stamp and append for tick=5
    df_out = await updater.update(df_in, sig, tick=5, world_id=world_id, run_id=run_id)
    assert df_out.collect().count_rows() == 2

    # Query: default filters (is_active + world_id/run_id) and tick filter
    df_all = await querier.query_archetype(
        sig, world_id=world_id, ticks=[5], entity_ids=None, components=None, run_id=run_id
    )
    assert df_all.collect().count_rows() == 2

    # Query single entity
    df_e2 = await querier.query_archetype(
        sig, world_id=world_id, ticks=[5], entity_ids=[2], components=None, run_id=run_id
    )
    out = df_e2.collect()
    assert out.count_rows() == 1
    assert out.to_pylist()[0]["entity_id"] == 2

    # Projection to component schema
    df_proj = await querier.query_archetype(
        sig,
        world_id=world_id,
        ticks=[5],
        entity_ids=None,
        components=[Position(x=0, y=0)],
        run_id=run_id,
    )
    cols = set(df_proj.collect().column_names)
    # Projections exclude commit-identity columns (issue #273).
    base_cols = set(Archetype.LEGACY_BASE_SCHEMA.names)
    comp_cols = set(Position.get_prefixed_schema().names)
    assert base_cols.issubset(cols)
    assert comp_cols.issubset(cols)


@pytest.mark.asyncio
async def test_update_zero_rows_no_persist(store_backend):
    store = store_backend
    querier = AsyncQueryManager(store)
    updater = AsyncUpdateManager(store)

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    world_id = "wz"
    run_id = "rz"

    # Ensure table exists
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)

    # Empty df
    empty_df = daft.from_pylist([]).collect()
    await updater.update(empty_df, sig, tick=1, world_id=world_id, run_id=run_id)
    # df_out may be empty-schema; rely on store read to validate no persistence

    out = await querier.query_archetype(
        sig, world_id=world_id, ticks=[1], entity_ids=None, components=None, run_id=run_id
    )
    assert out.collect().count_rows() == 0
