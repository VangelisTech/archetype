# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from unittest.mock import Mock

import daft
import pyarrow as pa
import pytest
from daft import DataFrame, col

from archetype.app.storage_service import _resolve_uri
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.runtime.session import configure_session
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
    x: int = 0
    y: int = 0


class Velocity(Component):
    vx: int = 0
    vy: int = 0


def _make_sync_stack(tmp_path, name: str = "sync"):
    cfg = StorageConfig(uri=str(tmp_path / f"{name}_store"), namespace=f"{name}_ns")
    session = configure_session(cfg)
    store = SyncStore(uri=_resolve_uri(str(cfg.uri)), session=session, io_config=cfg.io_config)
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system = SyncSystem()
    world = SyncWorld(
        world_id="test", name=name,
        querier=querier,
        updater=updater,
        system=system,
        resources=Resources(),
        hooks=SyncHookRegistry(),
    )
    return store, querier, updater, system, world


def _df_from_rows(sig, rows: list[dict]) -> DataFrame:
    schema = Archetype.get_archetype_schema(sig)
    return daft.from_arrow(pa.Table.from_pylist(rows, schema=schema))


def _committed_rows(world, sig) -> list[dict]:
    """Read the previous-tick committed rows for a signature from the store."""
    df = world.querier.query_archetype(
        sig=sig,
        world_id=str(world.world_id),
        ticks=[world.tick - 1],
        run_id=world.run_id,
    )
    return df.collect().to_pylist()


def test_sync_store_append_and_filters_world_and_run():
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    source_df = _df_from_rows(
        sig,
        [
            Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "world-a", "run-a"),
            Archetype.to_row_dict(2, 0, [Position(x=9, y=9)], "world-b", "run-b"),
        ],
    )

    class _Table:
        name = "positions"

        def __init__(self):
            self.appended = None

        def read(self):
            return source_df

        def append(self, df):
            self.appended = df

    table = _Table()

    class _Session:
        def create_table_if_not_exists(self, *args, **kwargs):
            return table

        def get_table(self, name):
            assert name == Archetype.get_name(sig)
            return table

    store = SyncStore(uri="unused", session=_Session())
    store.append(sig, source_df)
    rows = store.get_archetype_df(sig, "world-a", "run-a").collect().to_pylist()

    assert table.appended is source_df
    assert len(rows) == 1
    assert rows[0]["entity_id"] == 1
    assert rows[0]["position__x"] == 1


def test_sync_store_passes_io_config_to_iceberg_paths(monkeypatch):
    from daft.io import IOConfig

    class _IcebergTable:
        name = "positions"

        def __init__(self):
            self._inner = object()

        def read(self):
            raise AssertionError("explicit IOConfig reads should use daft.read_iceberg")

        def append(self, df):
            raise AssertionError("explicit IOConfig writes should use DataFrame.write_iceberg")

    class _Session:
        def __init__(self, table):
            self.table = table

        def create_table_if_not_exists(self, *args, **kwargs):
            return self.table

    class _Frame:
        def __init__(self):
            self.column_names = ["world_id"]

        def write_iceberg(self, inner_table, *, mode, io_config=None):
            seen["write_inner"] = inner_table
            seen["write_mode"] = mode
            seen["write_io_config"] = io_config

    table = _IcebergTable()
    io_config = IOConfig()
    seen = {}

    def fake_read_iceberg(inner_table, *, snapshot_id=None, io_config=None):
        seen["read_inner"] = inner_table
        seen["read_snapshot_id"] = snapshot_id
        seen["read_io_config"] = io_config
        return daft.from_pylist([{"world_id": "w", "run_id": "r"}])

    monkeypatch.setattr("archetype.core.sync.store.read_iceberg", fake_read_iceberg)

    store = SyncStore(uri="unused", session=_Session(table), io_config=io_config)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    rows = store.get_archetype_df(sig, "w", "r").collect().to_pylist()
    store.append(sig, _Frame())

    assert len(rows) == 1
    assert seen["read_inner"] is table._inner
    assert seen["read_snapshot_id"] is None
    assert seen["read_io_config"] is io_config
    assert seen["write_inner"] is table._inner
    assert seen["write_mode"] == "append"
    assert seen["write_io_config"] is io_config


def test_sync_store_real_session_roundtrip_filters_world_and_run(tmp_path):
    store, _querier, updater, _system, _world = _make_sync_stack(tmp_path, "store_roundtrip")
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    updater.update(
        _df_from_rows(
            sig, [Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "ignored", "ignored")]
        ),
        sig,
        tick=0,
        world_id="world-a",
        run_id="run-a",
    )

    rows = store.get_archetype_df(sig, "world-a", "run-a").collect().to_pylist()
    assert len(rows) == 1


def test_sync_store_wraps_table_creation_errors(tmp_path, monkeypatch):
    store, _querier, _updater, _system, _world = _make_sync_stack(tmp_path, "store_error")
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    def boom(*args, **kwargs):
        raise ValueError("catalog offline")

    monkeypatch.setattr(store.sess, "create_table_if_not_exists", boom)

    with pytest.raises(RuntimeError, match="catalog offline"):
        store.get_archetype_df(sig, "world-a", "run-a")


def test_sync_store_shutdown_is_noop(tmp_path):
    store, _querier, _updater, _system, _world = _make_sync_stack(tmp_path, "store_shutdown")
    assert store.shutdown() is None


def test_sync_update_manager_appends_stamped_rows_to_store():
    class RecordingStore:
        def __init__(self):
            self.calls = []

        def append(self, sig, df):
            self.calls.append((sig, df))

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    store = RecordingStore()
    updater = UpdateManager(store=store)
    source = _df_from_rows(sig, [Archetype.to_row_dict(5, 0, [Position(x=7, y=8)], "old", "old")])

    stamped = updater.update(source, sig, tick=9, world_id="world-z", run_id="run-z")

    assert len(store.calls) == 1
    called_sig, appended_df = store.calls[0]
    assert called_sig == sig
    assert appended_df is stamped
    assert stamped.collect().to_pylist() == [
        {
            "world_id": "world-z",
            "run_id": "run-z",
            "entity_id": 5,
            "tick": 9,
            "is_active": True,
            "position__x": 7,
            "position__y": 8,
        }
    ]


def test_sync_update_manager_stamps_metadata_and_swallows_store_errors(caplog):
    class ExplodingStore:
        def append(self, sig, df):
            raise RuntimeError("append failed")

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    updater = UpdateManager(store=ExplodingStore())
    df = _df_from_rows(sig, [Archetype.to_row_dict(3, 0, [Position(x=4, y=5)], "old", "old")])

    stamped = updater.update(df, sig, tick=7, world_id="world-z", run_id="run-z")
    rows = stamped.collect().to_pylist()

    assert rows[0]["tick"] == 7
    assert rows[0]["world_id"] == "world-z"
    assert rows[0]["run_id"] == "run-z"
    assert rows[0]["entity_id"] == 3
    assert "append failed" in caplog.text


def test_sync_query_manager_get_archetype_delegates_to_store():
    rows = _df_from_rows(
        Archetype.sig_from_components([Position(x=0, y=0)]),
        [Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "w", "r")],
    )
    store = Mock()
    store.get_archetype_df.return_value = rows
    querier = QueryManager(store=store)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])

    result = querier.get_archetype(sig, "world-a", "run-a")

    assert result is rows
    store.get_archetype_df.assert_called_once_with(sig, "world-a", "run-a")


def test_sync_query_manager_filters_active_ticks_and_entities():
    sig = Archetype.sig_from_components([Position(x=0, y=0), Velocity(vx=0, vy=0)])
    rc = RunConfig(num_steps=1)
    rows = _df_from_rows(
        sig,
        [
            {
                **Archetype.to_row_dict(
                    1,
                    0,
                    [Position(x=1, y=1), Velocity(vx=10, vy=10)],
                    "w",
                    str(rc.run_id),
                ),
                "is_active": True,
            },
            {
                **Archetype.to_row_dict(
                    2,
                    1,
                    [Position(x=2, y=2), Velocity(vx=20, vy=20)],
                    "w",
                    str(rc.run_id),
                ),
                "is_active": True,
            },
            {
                **Archetype.to_row_dict(
                    3,
                    1,
                    [Position(x=3, y=3), Velocity(vx=30, vy=30)],
                    "w",
                    str(rc.run_id),
                ),
                "is_active": False,
            },
        ],
    )

    class _Store:
        def get_archetype_df(self, _sig, _world_id, _run_id):
            return rows

    querier = QueryManager(store=_Store())
    df = querier.query_archetype(
        sig=sig,
        world_id="w",
        ticks=[1],
        entity_ids=[2],
        run_config=rc,
    )
    filtered = df.collect().to_pylist()

    assert filtered == [
        {
            "world_id": "w",
            "run_id": str(rc.run_id),
            "entity_id": 2,
            "tick": 1,
            "is_active": True,
            "position__x": 2,
            "position__y": 2,
            "velocity__vx": 20,
            "velocity__vy": 20,
        }
    ]


def test_sync_query_manager_projects_requested_components():
    sig = Archetype.sig_from_components([Position(x=0, y=0), Velocity(vx=0, vy=0)])
    rc = RunConfig(num_steps=1)
    rows = _df_from_rows(
        sig,
        [
            Archetype.to_row_dict(
                2,
                1,
                [Position(x=2, y=2), Velocity(vx=20, vy=20)],
                "w",
                str(rc.run_id),
            )
        ],
    )

    class _Store:
        def get_archetype_df(self, _sig, _world_id, _run_id):
            return rows

    querier = QueryManager(store=_Store())
    projected = (
        querier.query_archetype(
            sig=sig,
            world_id="w",
            ticks=[1],
            entity_ids=[2],
            components=[Position(x=0, y=0)],
            run_config=rc,
        )
        .collect()
        .to_pylist()
    )

    assert projected == [
        {
            "world_id": "w",
            "run_id": str(rc.run_id),
            "entity_id": 2,
            "tick": 1,
            "is_active": True,
            "position__x": 2,
            "position__y": 2,
        }
    ]


def test_sync_query_manager_debug_path_preserves_results(caplog):
    caplog.set_level(logging.INFO)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    rc = RunConfig(num_steps=1, debug=True)
    rows_df = _df_from_rows(
        sig,
        [Archetype.to_row_dict(1, 0, [Position(x=8, y=9)], "w", str(rc.run_id))],
    )

    class _Store:
        def get_archetype_df(self, _sig, _world_id, _run_id):
            return rows_df

    querier = QueryManager(store=_Store())
    rows = querier.query_archetype(sig=sig, world_id="w", run_config=rc).collect().to_pylist()

    assert len(rows) == 1
    assert rows[0]["position__x"] == 8
    assert "Querying" in caplog.text


def test_sync_world_materialize_mutations_prefers_latest_spawn_for_same_entity(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_spawn_dedupe")
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    base_df = _df_from_rows(sig, [])
    world.spawn_cache[sig] = [
        Archetype.to_row_dict(7, 0, [Position(x=1, y=1)], str(world.world_id), "run-a"),
        Archetype.to_row_dict(7, 0, [Position(x=9, y=9)], str(world.world_id), "run-a"),
    ]

    rows = world._materialize_mutations(base_df, sig, RunConfig(num_steps=1)).collect().to_pylist()

    assert rows == [
        {
            "world_id": str(world.world_id),
            "run_id": "run-a",
            "entity_id": 7,
            "tick": 0,
            "is_active": True,
            "position__x": 9,
            "position__y": 9,
        }
    ]


def test_sync_world_materialize_mutations_marks_despawned_entities_inactive(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_despawn_mark")
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    base_df = _df_from_rows(
        sig,
        [
            {
                **Archetype.to_row_dict(
                    4,
                    0,
                    [Position(x=1, y=2)],
                    str(world.world_id),
                    "run-a",
                ),
                "is_active": True,
            }
        ],
    )
    world.despawn_cache[sig] = [4, 4]

    rows = world._materialize_mutations(base_df, sig, RunConfig(num_steps=1)).collect().to_pylist()

    assert rows == [
        {
            "world_id": str(world.world_id),
            "run_id": "run-a",
            "entity_id": 4,
            "tick": 0,
            "is_active": False,
            "position__x": 1,
            "position__y": 2,
        }
    ]


def test_sync_world_run_materializes_spawns_and_sets_run_id(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_run")
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    entity_id = world.create_entity([Position(x=1, y=2)])
    rc = RunConfig(num_steps=2)

    world.run(rc)

    assert world.tick == 2
    assert world.run_id is not None  # auto-generated at construction
    rows = _committed_rows(world, sig)
    assert rows[0]["entity_id"] == entity_id
    assert rows[0]["position__x"] == 1


def test_sync_world_reads_previous_tick_from_store(tmp_path):
    class IncX(SyncProcessor):
        components = (Position,)
        priority = 1

        def process(self, df: DataFrame, **kwargs) -> DataFrame:
            return df.with_columns({"position__x": col("position__x") + 1})

    _store, _querier, _updater, system, world = _make_sync_stack(tmp_path, "world_live")
    system.add_processor(IncX())
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    world.create_entity([Position(x=0, y=0)])
    rc = RunConfig(num_steps=1)

    world.step(rc)
    world.step(rc)

    rows = _committed_rows(world, sig)
    assert rows[0]["position__x"] == 2


def test_sync_world_add_and_remove_components_migrate_entity(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_components")
    entity_id = world.create_entity([Position(x=1, y=2)])
    rc = RunConfig(num_steps=1)
    world.step(rc)

    world.add_components(entity_id, [Velocity(vx=5, vy=6)])
    world.step(rc)
    sig_with_velocity = Archetype.sig_from_components([Position(x=0, y=0), Velocity(vx=0, vy=0)])
    rows = _committed_rows(world, sig_with_velocity)
    assert world.entity2sig[entity_id] == sig_with_velocity
    assert rows[0]["velocity__vx"] == 5

    world.remove_components(entity_id, [Velocity])
    world.step(rc)
    sig_position = Archetype.sig_from_components([Position(x=0, y=0)])
    rows = _committed_rows(world, sig_position)
    assert world.entity2sig[entity_id] == sig_position
    assert rows[0]["position__x"] == 1


def test_sync_world_add_components_before_first_step_uses_pending_spawn_row(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_pre_step_move")
    entity_id = world.create_entity([Position(x=1, y=2)])

    world.add_components(entity_id, [Velocity(vx=5, vy=6)])
    world.step(RunConfig(num_steps=1))

    sig = Archetype.sig_from_components([Position(x=0, y=0), Velocity(vx=0, vy=0)])
    rows = _committed_rows(world, sig)

    assert len(rows) == 1
    assert rows[0]["entity_id"] == entity_id
    assert rows[0]["position__x"] == 1
    assert rows[0]["velocity__vx"] == 5


def test_sync_world_execute_passes_resources_and_kwargs(tmp_path):
    class ApplyBonus(SyncProcessor):
        components = (Position,)
        priority = 1

        def process(self, df: DataFrame, resources=None, bonus: int = 0, **kwargs) -> DataFrame:
            offset = resources.require(int)
            return df.with_columns({"position__x": col("position__x") + offset + bonus})

    _store, _querier, _updater, system, world = _make_sync_stack(tmp_path, "world_resources")
    system.add_processor(ApplyBonus())
    world.resources.insert(3)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    world.create_entity([Position(x=1, y=1)])

    world.step(RunConfig(num_steps=1), bonus=2)

    rows = _committed_rows(world, sig)
    assert rows[0]["position__x"] == 6


def test_sync_world_query_archetype_uses_world_tick_and_world_id():
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    querier = Mock()
    updater = Mock()
    system = SyncSystem()
    world = SyncWorld(
        world_id="test", name="query-forward",
        querier=querier, updater=updater, system=system,
        resources=Resources(), hooks=SyncHookRegistry(),
    )
    world.tick = 5
    rc = RunConfig(num_steps=1)

    world.query_archetype(sig, run_config=rc)

    querier.query_archetype.assert_called_once_with(
        sig=sig,
        world_id=str(world.world_id),
        ticks=[5],
        entity_ids=None,
        components=None,
        run_config=rc,
        run_id=world.run_id,
    )


def test_sync_world_move_entity_returns_empty_when_source_row_missing(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_missing_move")
    old_sig = Archetype.sig_from_components([Position(x=0, y=0)])
    new_sig = Archetype.sig_from_components([Position(x=0, y=0), Velocity(vx=0, vy=0)])
    world.entity2sig[42] = old_sig

    world.query_archetype = lambda *args, **kwargs: _df_from_rows(old_sig, [])
    row = world._move_entity(42, old_sig, new_sig, [Velocity(vx=1, vy=1)])

    assert row == {}


def test_sync_world_update_uses_world_tick_by_default():
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    df = _df_from_rows(sig, [Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], "w", "r")])
    querier = Mock()
    updater = Mock(return_value=df)
    system = SyncSystem()
    world = SyncWorld(
        world_id="test", name="update-forward",
        querier=querier, updater=updater, system=system,
        resources=Resources(), hooks=SyncHookRegistry(),
    )
    world.tick = 11
    world.run_id = "pinned-run-id"
    rc = RunConfig(num_steps=1)

    result = world.update(df, sig, rc)

    assert result is updater.update.return_value
    updater.update.assert_called_once_with(df, sig, 11, str(world.world_id), "pinned-run-id")


def test_sync_world_add_processor_and_remove_processor_delegate(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_processors")
    proc = SyncProcessor()

    world.add_processor(proc)
    assert proc in world.system.processors

    world.remove_processor(proc)
    assert proc not in world.system.processors


def test_sync_world_despawn_of_last_entity_materializes_removal(tmp_path):
    _store, _querier, _updater, _system, world = _make_sync_stack(tmp_path, "world_despawn")
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    entity_id = world.create_entity([Position(x=1, y=2)])
    rc = RunConfig(num_steps=1)

    world.step(rc)
    world.remove_entity(entity_id)
    world.step(rc)

    assert _committed_rows(world, sig) == []
