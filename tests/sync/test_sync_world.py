# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Smoke tests for the synchronous ECS reference implementation."""

from daft import DataFrame
from daft.session import Session

from archetype.core.component import Component
from archetype.core.config import WorldConfig
from archetype.core.sync import (
    QueryManager,
    SyncProcessor,
    SyncStore,
    SyncSystem,
    SyncWorld,
    UpdateManager,
)


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    vx: int
    vy: int


# ---------------------------------------------------------------------------
# SyncSystem tests
# ---------------------------------------------------------------------------


class TestSyncSystem:
    def test_add_and_remove_processor(self):
        system = SyncSystem()
        proc = SyncProcessor()
        system.add_processor(proc)
        assert proc in system.processors
        system.remove_processor(proc)
        assert proc not in system.processors

    def test_execute_calls_processor(self):
        calls = []

        class Tracer(SyncProcessor):
            components = (Position,)
            priority = 1

            def process(self, df: DataFrame, **kwargs) -> DataFrame:
                calls.append("called")
                return df

        system = SyncSystem()
        tracer = Tracer()
        system.add_processor(tracer)

        # Build a minimal archetype signature containing Position
        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])

        import daft
        import pyarrow as pa

        schema = Archetype.get_archetype_schema(sig)
        table = pa.Table.from_pylist(
            [Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "w", "r")],
            schema=schema,
        )
        df = daft.from_arrow(table)

        result = system.execute(df, sig)
        assert len(calls) == 1
        assert isinstance(result, DataFrame)

    def test_execute_skips_non_matching_processor(self):
        calls = []

        class VelocityProc(SyncProcessor):
            components = (Velocity,)
            priority = 1

            def process(self, df: DataFrame, **kwargs) -> DataFrame:
                calls.append("called")
                return df

        system = SyncSystem()
        system.add_processor(VelocityProc())

        # Signature only contains Position, not Velocity
        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])

        import daft
        import pyarrow as pa

        schema = Archetype.get_archetype_schema(sig)
        table = pa.Table.from_pylist(
            [Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "w", "r")],
            schema=schema,
        )
        df = daft.from_arrow(table)

        system.execute(df, sig)
        assert len(calls) == 0

    def test_execute_priority_ordering(self):
        order = []

        class Low(SyncProcessor):
            components = (Position,)
            priority = 20

            def process(self, df, **kw):
                order.append("low")
                return df

        class High(SyncProcessor):
            components = (Position,)
            priority = 1

            def process(self, df, **kw):
                order.append("high")
                return df

        system = SyncSystem()
        system.add_processor(Low())
        system.add_processor(High())

        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])

        import daft
        import pyarrow as pa

        schema = Archetype.get_archetype_schema(sig)
        table = pa.Table.from_pylist(
            [Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "w", "r")],
            schema=schema,
        )
        df = daft.from_arrow(table)

        system.execute(df, sig)
        assert order == ["high", "low"]


# ---------------------------------------------------------------------------
# SyncWorld tests
# ---------------------------------------------------------------------------


def _make_sync_world(tmp_path, name="test"):
    """Helper to construct a SyncWorld with an in-memory Daft session.

    Used by the lightweight tests that never call ``world.step``.
    """
    session = Session()
    uri = str(tmp_path / "sync_store")
    store = SyncStore(uri=uri, session=session)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system = SyncSystem()
    config = WorldConfig(name=name)
    return SyncWorld(world_config=config, querier=querier, updater=updater, system=system)


def _make_sync_world_with_catalog(tmp_path, name="test"):
    """Helper to construct a SyncWorld backed by a real catalog so
    ``world.step`` can actually persist rows."""
    from archetype.core.config import StorageConfig
    from archetype.core.runtime.storage import StorageContextFactory

    cfg = StorageConfig(uri=str(tmp_path / f"{name}_store"), namespace=f"{name}_ns")
    ctx = StorageContextFactory.build(cfg)
    store = SyncStore(uri=ctx.uri, session=ctx.session)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system = SyncSystem()
    return SyncWorld(
        world_config=WorldConfig(name=name), querier=querier, updater=updater, system=system
    )


class TestSyncWorld:
    def test_init_properties(self, tmp_path):
        world = _make_sync_world(tmp_path)
        assert world.tick == 0
        assert world.name == "test"
        assert world.world_id is not None

    def test_create_entity_assigns_id(self, tmp_path):
        world = _make_sync_world(tmp_path)
        e1 = world.create_entity([Position(x=1, y=2)])
        e2 = world.create_entity([Position(x=3, y=4)])
        assert e1 == 1
        assert e2 == 2

    def test_create_entity_populates_spawn_cache(self, tmp_path):
        world = _make_sync_world(tmp_path)
        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world.create_entity([Position(x=1, y=2)])
        assert sig in world._spawn_cache
        assert len(world._spawn_cache[sig]) == 1

    def test_remove_entity_cancels_pending_spawn_in_same_tick(self, tmp_path):
        from archetype.core.archetype import Archetype

        world = _make_sync_world(tmp_path)
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e1 = world.create_entity([Position(x=1, y=2)])

        world.remove_entity(e1)

        assert sig not in world._spawn_cache
        assert e1 not in world._entity2sig
        assert e1 not in world._despawn_cache.get(sig, [])

    def test_remove_entity_schedules_despawn_after_spawn_materialized(self, tmp_path):
        from archetype.core.archetype import Archetype
        from archetype.core.config import RunConfig

        world = _make_sync_world_with_catalog(tmp_path, "despawn_after_step")
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e1 = world.create_entity([Position(x=1, y=2)])
        world.step(RunConfig(num_steps=1))

        world.remove_entity(e1)

        assert sig in world._despawn_cache
        assert e1 in world._despawn_cache[sig]

    def test_remove_nonexistent_entity_is_noop(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.remove_entity(999)  # should not raise
        assert len(world._despawn_cache) == 0

    def test_active_signatures(self, tmp_path):
        world = _make_sync_world(tmp_path)
        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world.create_entity([Position(x=1, y=2)])
        assert sig in world.active_signatures

    def test_clear_caches(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.create_entity([Position(x=1, y=2)])
        assert len(world._spawn_cache) > 0
        world._clear_caches()
        assert len(world._spawn_cache) == 0
        assert len(world._despawn_cache) == 0

    def test_resources_container_accessible(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.resources.insert("hello")
        assert world.resources.get(str) == "hello"

    def test_entity2sig_tracks_signature(self, tmp_path):
        from archetype.core.archetype import Archetype

        world = _make_sync_world(tmp_path)
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e = world.create_entity([Position(x=1, y=2)])
        assert world._entity2sig[e] == sig

    def test_entity_counter_increments(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.create_entity([Position(x=1, y=2)])
        world.create_entity([Position(x=3, y=4)])
        assert world._next_entity_id == 3

    def test_step_after_same_tick_spawn_remove_leaves_no_active_row(self, tmp_path):
        from archetype.core.archetype import Archetype
        from archetype.core.config import RunConfig

        world = _make_sync_world_with_catalog(tmp_path, "cancel_active")
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        rc = RunConfig(num_steps=1)

        eid = world.create_entity([Position(x=1, y=2)])
        world.remove_entity(eid)
        world.step(rc)

        df = world.querier.query_archetype(
            sig=sig,
            run_config=rc,
            ticks=None,
            entity_ids=[eid],
            components=None,
            world_id=str(world.world_id),
        )
        rows = df.to_pylist()
        assert all(not r["is_active"] for r in rows), (
            f"cancelled entity persisted as active: {rows}"
        )

        for s, live_df in world._live.items():
            active_eids = [r["entity_id"] for r in live_df.to_pylist() if r["is_active"]]
            assert eid not in active_eids, f"_live[{s}] kept cancelled entity {eid}"

    def test_step_after_same_tick_spawn_remove_preserves_sibling_entity(self, tmp_path):
        from archetype.core.archetype import Archetype
        from archetype.core.config import RunConfig

        world = _make_sync_world_with_catalog(tmp_path, "cancel_sibling")
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        rc = RunConfig(num_steps=1)

        survivor = world.create_entity([Position(x=10, y=20)])
        cancelled = world.create_entity([Position(x=1, y=2)])
        world.remove_entity(cancelled)
        world.step(rc)

        df = world.querier.query_archetype(
            sig=sig,
            run_config=rc,
            ticks=None,
            entity_ids=None,
            components=None,
            world_id=str(world.world_id),
        )
        rows = df.to_pylist()
        active_ids = sorted(r["entity_id"] for r in rows if r["is_active"])
        assert active_ids == [survivor], (
            f"expected only survivor {survivor} active, got {active_ids}"
        )
