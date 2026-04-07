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
    """Helper to construct a SyncWorld with in-memory Daft session."""
    session = Session()
    uri = str(tmp_path / "sync_store")
    store = SyncStore(uri=uri, session=session)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system = SyncSystem()
    config = WorldConfig(name=name)
    return SyncWorld(world_config=config, querier=querier, updater=updater, system=system)


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

    def test_remove_entity_populates_despawn_cache(self, tmp_path):
        world = _make_sync_world(tmp_path)
        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e1 = world.create_entity([Position(x=1, y=2)])

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
