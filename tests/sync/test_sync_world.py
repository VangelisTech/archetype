# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Smoke tests for the synchronous ECS reference implementation."""

from daft import DataFrame
from daft.session import Session

from archetype.core.component import Component
from archetype.core.config import WorldConfig
from archetype.core.hooks import SyncHookRegistry
from archetype.core.resources import Resources
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
    return SyncWorld(
        world_id="test",
        name=name,
        querier=querier,
        updater=updater,
        system=system,
        resources=Resources(),
        hooks=SyncHookRegistry(),
    )


def _make_sync_world_with_catalog(tmp_path, name="test"):
    """Helper to construct a SyncWorld backed by a real catalog so
    ``world.step`` can actually persist rows."""
    from archetype.app.storage_service import _resolve_uri
    from archetype.core.config import StorageConfig
    from archetype.runtime.session import configure_session

    cfg = StorageConfig(uri=str(tmp_path / f"{name}_store"), namespace=f"{name}_ns")
    session = configure_session(cfg)
    store = SyncStore(uri=_resolve_uri(str(cfg.uri)), session=session)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system = SyncSystem()
    return SyncWorld(
        world_id="test",
        name=name,
        querier=querier,
        updater=updater,
        system=system,
        resources=Resources(),
        hooks=SyncHookRegistry(),
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
        assert sig in world.spawn_cache
        assert len(world.spawn_cache[sig]) == 1

    def test_remove_entity_cancels_pending_spawn_in_same_tick(self, tmp_path):
        from archetype.core.archetype import Archetype

        world = _make_sync_world(tmp_path)
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e1 = world.create_entity([Position(x=1, y=2)])

        world.remove_entity(e1)

        assert sig not in world.spawn_cache
        assert e1 not in world.entity2sig
        assert e1 not in world.despawn_cache.get(sig, [])

    def test_remove_entity_schedules_despawn_after_spawn_materialized(self, tmp_path):
        from archetype.core.archetype import Archetype
        from archetype.core.config import RunConfig

        world = _make_sync_world_with_catalog(tmp_path, "despawn_after_step")
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e1 = world.create_entity([Position(x=1, y=2)])
        world.step(RunConfig(num_steps=1))

        world.remove_entity(e1)

        assert sig in world.despawn_cache
        assert e1 in world.despawn_cache[sig]

    def test_remove_nonexistent_entity_is_noop(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.remove_entity(999)  # should not raise
        assert len(world.despawn_cache) == 0

    def test_active_signatures(self, tmp_path):
        world = _make_sync_world(tmp_path)
        from archetype.core.archetype import Archetype

        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        world.create_entity([Position(x=1, y=2)])
        assert sig in world.active_signatures

    def test_clear_caches(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.create_entity([Position(x=1, y=2)])
        assert len(world.spawn_cache) > 0
        world._clear_caches()
        assert len(world.spawn_cache) == 0
        assert len(world.despawn_cache) == 0

    def test_resources_container_accessible(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.resources.insert("hello")
        assert world.resources.get(str) == "hello"

    def test_entity2sig_tracks_signature(self, tmp_path):
        from archetype.core.archetype import Archetype

        world = _make_sync_world(tmp_path)
        sig = Archetype.sig_from_components([Position(x=0, y=0)])
        e = world.create_entity([Position(x=1, y=2)])
        assert world.entity2sig[e] == sig

    def test_entity_counter_increments(self, tmp_path):
        world = _make_sync_world(tmp_path)
        world.create_entity([Position(x=1, y=2)])
        world.create_entity([Position(x=3, y=4)])
        assert world.next_entity_id == 3

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

        for s in set(world.entity2sig.values()):
            sig_df = world.querier.query_archetype(
                sig=s,
                world_id=str(world.world_id),
                ticks=[world.tick - 1],
                run_id=world.run_id,
            )
            active_eids = [r["entity_id"] for r in sig_df.collect().to_pylist()]
            assert eid not in active_eids, f"sig {s} kept cancelled entity {eid}"

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


# ---------------------------------------------------------------------------
# SyncWorld hook tests
# ---------------------------------------------------------------------------


class TestSyncHooks:
    """Parity tests for the typed hook system on ``SyncWorld``."""

    def test_pre_tick_hook_fires(self, tmp_path):
        from archetype.core.config import RunConfig
        from archetype.core.hooks import PreTick

        world = _make_sync_world_with_catalog(tmp_path, "sync_pre_tick")
        events: list[PreTick] = []

        def on_pre_tick(event: PreTick) -> None:
            events.append(event)

        world.add_hook(PreTick, on_pre_tick)
        world.create_entity([Position(x=1, y=2)])
        world.step(RunConfig(num_steps=1))

        assert len(events) == 1
        assert events[0].tick == 0
        assert events[0].world_id == world.world_id

    def test_post_tick_hook_fires_with_incremented_tick(self, tmp_path):
        from archetype.core.config import RunConfig
        from archetype.core.hooks import PostTick

        world = _make_sync_world_with_catalog(tmp_path, "sync_post_tick")
        events: list[PostTick] = []

        def on_post_tick(event: PostTick) -> None:
            events.append(event)

        world.add_hook(PostTick, on_post_tick)
        world.create_entity([Position(x=1, y=2)])
        world.step(RunConfig(num_steps=1))

        assert len(events) == 1
        assert events[0].tick == 1
        assert isinstance(events[0].results, dict)

    def test_hook_ordering_across_multiple_ticks(self, tmp_path):
        from archetype.core.config import RunConfig
        from archetype.core.hooks import PostTick, PreTick

        world = _make_sync_world_with_catalog(tmp_path, "sync_ordering")
        events: list[str] = []

        world.add_hook(PreTick, lambda e: events.append(f"pre:{e.tick}"))
        world.add_hook(PostTick, lambda e: events.append(f"post:{e.tick}"))

        world.create_entity([Position(x=0, y=0)])
        world.run(RunConfig(num_steps=2))

        assert events == ["pre:0", "post:1", "pre:1", "post:2"]

    def test_on_spawn_hook_fires_from_create_entity(self, tmp_path):
        from archetype.core.hooks import OnSpawn

        world = _make_sync_world(tmp_path)
        events: list[OnSpawn] = []

        world.add_hook(OnSpawn, events.append)
        components = [Position(x=1, y=2)]
        eid = world.create_entity(components)

        assert len(events) == 1
        assert events[0].entity_id == eid
        assert events[0].components == components

    def test_on_spawn_hook_fires_from_spawn_reserved(self, tmp_path):
        from archetype.core.hooks import OnSpawn

        world = _make_sync_world(tmp_path)
        events: list[OnSpawn] = []

        world.add_hook(OnSpawn, events.append)
        world.spawn_reserved(42, [Position(x=3, y=4)])

        assert len(events) == 1
        assert events[0].entity_id == 42
        assert world.next_entity_id == 43

    def test_spawn_reserved_rejects_live_id(self, tmp_path):
        import pytest

        world = _make_sync_world(tmp_path)
        world.create_entity([Position(x=1, y=2)])
        with pytest.raises(ValueError, match="already exists"):
            world.spawn_reserved(1, [Position(x=9, y=9)])

    def test_on_despawn_hook_fires_when_entity_removed(self, tmp_path):
        from archetype.core.hooks import OnDespawn

        world = _make_sync_world(tmp_path)
        events: list[OnDespawn] = []

        eid = world.create_entity([Position(x=1, y=2)])
        world.add_hook(OnDespawn, events.append)
        world.remove_entity(eid)

        assert len(events) == 1
        assert events[0].entity_id == eid

    def test_on_despawn_hook_skips_unknown_entity(self, tmp_path):
        from archetype.core.hooks import OnDespawn

        world = _make_sync_world(tmp_path)
        events: list[OnDespawn] = []

        world.add_hook(OnDespawn, events.append)
        world.remove_entity(9999)

        assert events == []

    def test_on_component_added_fires_on_archetype_widening(self, tmp_path):
        from archetype.core.hooks import OnComponentAdded

        world = _make_sync_world(tmp_path)
        events: list[OnComponentAdded] = []

        eid = world.create_entity([Position(x=1, y=2)])
        world.add_hook(OnComponentAdded, events.append)
        world.add_components(eid, [Velocity(vx=1, vy=1)])

        assert len(events) == 1
        assert events[0].entity_id == eid
        assert [type(c) for c in events[0].components] == [Velocity]

    def test_on_component_added_skips_noop_add(self, tmp_path):
        from archetype.core.hooks import OnComponentAdded

        world = _make_sync_world(tmp_path)
        events: list[OnComponentAdded] = []

        eid = world.create_entity([Position(x=1, y=2), Velocity(vx=0, vy=0)])
        world.add_hook(OnComponentAdded, events.append)
        # Component already present — signature unchanged, no OnComponentAdded.
        world.add_components(eid, [Velocity(vx=9, vy=9)])

        assert events == []

    def test_on_component_removed_fires_on_archetype_narrowing(self, tmp_path):
        from archetype.core.hooks import OnComponentRemoved

        world = _make_sync_world(tmp_path)
        events: list[OnComponentRemoved] = []

        eid = world.create_entity([Position(x=1, y=2), Velocity(vx=1, vy=1)])
        world.add_hook(OnComponentRemoved, events.append)
        world.remove_components(eid, [Velocity])

        assert len(events) == 1
        assert events[0].component_types == [Velocity]

    def test_remove_hook_by_handle(self, tmp_path):
        from archetype.core.hooks import OnSpawn

        world = _make_sync_world(tmp_path)
        events: list[OnSpawn] = []
        handle = world.add_hook(OnSpawn, events.append)

        world.create_entity([Position(x=1, y=2)])
        assert len(events) == 1

        world.remove_hook(handle)
        world.create_entity([Position(x=3, y=4)])
        assert len(events) == 1  # unchanged

    def test_remove_hook_handle_is_world_local(self, tmp_path):
        from archetype.core.config import RunConfig
        from archetype.core.hooks import PreTick

        world_one = _make_sync_world_with_catalog(tmp_path, "sync_world_one")
        world_two = _make_sync_world_with_catalog(tmp_path, "sync_world_two")
        counts = {"one": 0, "two": 0}

        handle_one = world_one.add_hook(
            PreTick, lambda e: counts.__setitem__("one", counts["one"] + 1)
        )
        handle_two = world_two.add_hook(
            PreTick, lambda e: counts.__setitem__("two", counts["two"] + 1)
        )

        assert handle_one != handle_two

        world_two.remove_hook(handle_one)
        world_one.create_entity([Position(x=1, y=2)])
        world_two.create_entity([Position(x=3, y=4)])
        world_one.step(RunConfig(num_steps=1))
        world_two.step(RunConfig(num_steps=1))

        assert counts == {"one": 1, "two": 1}

    def test_hook_error_is_logged_not_raised(self, tmp_path):
        from archetype.core.config import RunConfig
        from archetype.core.hooks import PreTick

        world = _make_sync_world_with_catalog(tmp_path, "sync_hook_err")

        def bad_hook(event: PreTick) -> None:
            raise ValueError("intentional")

        world.add_hook(PreTick, bad_hook)
        world.create_entity([Position(x=1, y=2)])
        world.step(RunConfig(num_steps=1))  # should not raise
        assert world.tick == 1

    def test_multiple_hooks_same_event_all_fire(self, tmp_path):
        from archetype.core.hooks import OnSpawn

        world = _make_sync_world(tmp_path)
        counts = {"a": 0, "b": 0}
        world.add_hook(OnSpawn, lambda e: counts.__setitem__("a", counts["a"] + 1))
        world.add_hook(OnSpawn, lambda e: counts.__setitem__("b", counts["b"] + 1))

        world.create_entity([Position(x=1, y=2)])
        assert counts == {"a": 1, "b": 1}
