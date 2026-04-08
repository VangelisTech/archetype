import pytest
import pytest_asyncio
from daft import col

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.runtime.storage import StorageContextFactory


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


class Meta(Component):
    name: str


@pytest_asyncio.fixture(params=["async", "async_cached"], scope="function")
async def store_backend(request, tmp_path):
    uri = str(tmp_path)
    storage = StorageConfig(uri=uri, namespace="test", use_lancedb=(request.param == "lancedb"))
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
    return AsyncWorld(wcfg, querier, updater, system)


# ---------------------------------------------------------------------------
# N+1 lifecycle: spawns and despawns take effect the tick AFTER they're queued
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spawn_processed_at_next_tick(world, store_backend):
    """Entity spawned between ticks is NOT processed until the next tick.

    Tick lifecycle (N+1 semantics):
      create_entity() → _spawn_cache
      step (tick 0): processors run on empty df, then mutations applied to _live
      step (tick 1): entity now in _live, processors see and modify it
    """
    from daft import lit

    from archetype.core.aio.async_processor import AsyncProcessor

    class Marker(AsyncProcessor):
        """Stamps y = -1 to prove the processor ran on this entity."""

        components = (Position,)
        priority = 1

        async def process(self, df, **kwargs):
            return df.with_column("position__y", lit(-1))

    await world.add_processor(Marker())

    # Initial y = 99; Marker would set it to -1 if processors touch it
    e1 = await world.create_entity([Position(x=1, y=99)])
    rc = RunConfig()

    # Tick 0: spawn applied post-processors → entity in _live but NOT processed
    await world.step(rc)

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    df0 = await store_backend.get_archetype_df(sig, world.world_id, rc.run_id)
    rows0 = [r for r in df0.collect().to_pylist() if r["entity_id"] == e1 and r["tick"] == 0]

    # Under N+1 semantics: tick 0 should NOT have the entity processed.
    # Either no row at tick 0 (empty df written), or y is still 99 (Marker didn't run).
    for r in rows0:
        assert r["position__y"] == 99, (
            f"Processor ran at tick 0 but shouldn't have (y={r['position__y']})"
        )

    # Tick 1: entity is in _live, Marker sets y = -1
    await world.step(rc)

    df1 = await store_backend.get_archetype_df(sig, world.world_id, rc.run_id)
    rows1 = [r for r in df1.collect().to_pylist() if r["entity_id"] == e1 and r["tick"] == 1]
    assert len(rows1) == 1
    assert rows1[0]["position__y"] == -1  # Marker processor ran at tick 1


@pytest.mark.asyncio
async def test_despawn_applied_after_processors(world, store_backend):
    """Despawn queued between ticks: entity still processed this tick, removed next.

    create_entity + step (tick 0) → entity active
    remove_entity
    step (tick 1) → entity still processed (despawn applied post-processors)
    step (tick 2) → entity no longer in _live
    """
    e1 = await world.create_entity([Position(x=1, y=2)])
    rc = RunConfig()
    await world.step(rc)  # tick 0: spawn applied to _live

    await world.step(rc)  # tick 1: entity processed (now in store)

    await world.remove_entity(e1)
    await world.step(rc)  # tick 2: despawn applied post-processors → _live removes it

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    # After tick 2, entity should have an inactive row
    df = await store_backend.get_archetype_df(sig, world.world_id, rc.run_id)
    all_rows = df.collect().to_pylist()
    e1_rows = sorted([r for r in all_rows if r["entity_id"] == e1], key=lambda r: r["tick"])
    # Last row for e1 should be inactive
    assert e1_rows[-1]["is_active"] is False


@pytest.mark.asyncio
async def test_add_components_preserves_values_post_step(world, store_backend):
    """Adding a component after stepping preserves existing component values."""
    sig_pos_vel = Archetype.add_components(
        Archetype.sig_from_components([Position(x=0, y=0)]), [Velocity]
    )

    e1 = await world.create_entity([Position(x=5, y=6)])
    rc = RunConfig()
    await world.step(rc)  # tick 0: spawn to _live
    await world.step(rc)  # tick 1: entity processed, in store

    await world.add_components(e1, [Velocity(dx=10, dy=20)])
    await world.step(rc)  # tick 2: move applied post-processors
    await world.step(rc)  # tick 3: entity under new sig processed

    df = await store_backend.get_archetype_df(sig_pos_vel, world.world_id, rc.run_id)
    rows = df.collect().to_pylist()
    e1_rows = [r for r in rows if r["entity_id"] == e1]
    assert len(e1_rows) >= 1
    latest = sorted(e1_rows, key=lambda r: r["tick"])[-1]
    assert latest["position__x"] == 5  # preserved from original
    assert latest["velocity__dx"] == 10  # new component


# ---------------------------------------------------------------------------
# Original tests (adjusted for N+1 semantics where needed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_remove_entity_spawns_and_despawns(world, store_backend):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    e1 = await world.create_entity([Position(x=1, y=2)])
    e2 = await world.create_entity([Position(x=3, y=4)])
    assert e1 != e2

    # First step writes spawns
    rc = RunConfig()
    await world.step(rc)
    df_all = await store_backend.get_archetype_df(sig, world.world_id, rc.run_id)
    assert df_all.collect().count_rows() == 2

    # Request despawn of e1; materialized next step
    await world.remove_entity(e1)
    await world.step(rc)

    out = await store_backend.get_archetype_df(sig, world.world_id, rc.run_id)
    py = out.collect().to_pylist()
    # Should contain 4 rows total: For e1 we have spawn@t0, despawn@t1; for e2 spawn@t0 remains active
    assert len(py) == 4
    # Validate latest row for e1 is inactive (explicitly pick max tick)
    e1_rows = [r for r in py if r["entity_id"] == e1]
    last_e1 = sorted(e1_rows, key=lambda r: r["tick"])[-1]
    assert last_e1["is_active"] is False


@pytest.mark.asyncio
async def test_add_components_moves_to_superset_signature(world, store_backend):
    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])
    sig_pos_vel = Archetype.add_components(sig_pos, [Velocity])

    e1 = await world.create_entity([Position(x=5, y=6)])
    rc = RunConfig()
    await world.step(rc)  # materialize spawn under sig_pos at t=0

    # add velocity; move to new signature
    await world.add_components(e1, [Velocity(dx=1, dy=1)])
    await world.step(rc)

    # Verify last row under new signature exists and is_active
    df_new = await store_backend.get_archetype_df(sig_pos_vel, world.world_id, rc.run_id)
    latest = df_new.collect().sort(col("tick"), desc=True).limit(1).to_pylist()[0]
    assert latest["entity_id"] == e1
    assert latest["is_active"] is True

    # Old signature latest row should be inactive for e1 after move
    df_old = await store_backend.get_archetype_df(sig_pos, world.world_id, rc.run_id)
    old_rows = [r for r in df_old.collect().to_pylist() if r["entity_id"] == e1]
    assert len(old_rows) >= 1
    old_latest = sorted(old_rows, key=lambda r: r["tick"])[-1]
    assert old_latest["is_active"] is False


@pytest.mark.asyncio
async def test_remove_components_moves_to_subset_signature(world, store_backend):
    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])
    sig_pos_meta = Archetype.add_components(sig_pos, [Meta])

    e1 = await world.create_entity([Position(x=0, y=0), Meta(name="a")])
    rc = RunConfig()
    await world.step(rc)  # t=0 under sig_pos_meta

    await world.remove_components(e1, [Meta])
    await world.step(rc)

    # Now entity should appear under sig_pos with active latest row
    df_new = await store_backend.get_archetype_df(sig_pos, world.world_id, rc.run_id)
    latest = [r for r in df_new.collect().to_pylist() if r["entity_id"] == e1][-1]
    assert latest["is_active"] is True

    # And old signature should have an inactive marker for e1
    df_old = await store_backend.get_archetype_df(sig_pos_meta, world.world_id, rc.run_id)
    old_rows2 = [r for r in df_old.collect().to_pylist() if r["entity_id"] == e1]
    assert len(old_rows2) >= 1
    latest_old = sorted(old_rows2, key=lambda r: r["tick"])[-1]
    assert latest_old["is_active"] is False
