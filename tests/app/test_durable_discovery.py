# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable discovery (issue #272, A1-read).

The control catalog makes the existing registries durable: a fresh process
pointed at the same storage identity discovers every world and signature ever
committed there. P0 tests use true subprocess restarts — same-process
container recycling would not prove cold discovery.
"""

import json
import logging
import multiprocessing
import sqlite3
import subprocess
import sys
import textwrap

import pytest
from uuid_utils import uuid7

from archetype.app._catalog import (
    CatalogConflictError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
    catalog_path_for,
    schema_fingerprint,
    storage_fingerprint,
)
from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Score(Component):
    points: float = 0.0


class Flag(Component):
    label: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


# ─────────────────────────────────────────────────────────────────────────────
# Catalog unit behavior
# ─────────────────────────────────────────────────────────────────────────────


def test_catalog_path_is_a_pure_function_of_storage_identity(tmp_path):
    from archetype.core.config import StorageBackend

    local = StorageConfig(uri=str(tmp_path / "s"), namespace="ns")
    assert catalog_path_for(local) == catalog_path_for(local)
    assert catalog_path_for(local).name == f".archetype-catalog-{local.backend.value}.db"
    assert str(tmp_path / "s" / "ns") in str(catalog_path_for(local))

    remote = StorageConfig(uri="s3://bucket/prefix", namespace="ns")
    a, b = catalog_path_for(remote), catalog_path_for(remote)
    assert a == b, "remote catalogs must resolve deterministically"
    assert str(tmp_path) not in str(a), "remote catalog lives in the host-local root"

    other = StorageConfig(uri="s3://bucket/other", namespace="ns")
    assert catalog_path_for(other) != a

    # The backend is part of the storage identity (it selects a different
    # physical store): same uri/namespace, different backend → different
    # catalog, locally and remotely.
    for cfg in (local, remote):
        pairs = {
            catalog_path_for(StorageConfig(uri=cfg.uri, namespace=cfg.namespace, backend=backend))
            for backend in (StorageBackend.LANCEDB, StorageBackend.ICEBERG)
        }
        assert len(pairs) == 2, "backends must never share a catalog"


def test_catalog_identity_normalizes_equivalent_file_uri(tmp_path):
    target = tmp_path / "file store"
    path_config = StorageConfig(uri=str(target), namespace="ns")
    uri_config = StorageConfig(uri=target.as_uri(), namespace="ns")

    assert catalog_path_for(uri_config) == catalog_path_for(path_config)
    assert storage_fingerprint(uri_config) == storage_fingerprint(path_config)


@pytest.mark.asyncio
async def test_failed_catalog_registration_leaves_no_live_world(tmp_path, monkeypatch):
    """Identity is authoritative both ways: a world the catalog cannot
    describe must not survive as a live, mutable world (create or fork)."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        source = await c.world_service.create_world(WorldConfig(name="src"), storage)

        async def _boom(self, record):
            raise CatalogConflictError("injected registration failure")

        monkeypatch.setattr(SqliteControlCatalog, "register_world", _boom)

        with pytest.raises(CatalogConflictError):
            await c.world_service.create_world(WorldConfig(name="orphan"), storage)
        with pytest.raises(CatalogConflictError):
            await c.world_service.fork_world(source.world_id, name="orphan-fork")

        live = {w.name for w in c.world_service.list_worlds()}
        assert live == {"src"}, f"failed registrations must unwind, saw {live}"
        with pytest.raises(KeyError):
            c.world_service.get_world_by_name("orphan")
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_register_world_is_idempotent_and_conflicts_loudly(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "cat.db")
    record = WorldRecord(
        world_id="w1", name="alpha", run_id="r1", parent_world_id=None, status="active", tick_head=0
    )
    await catalog.register_world(record)
    await catalog.register_world(record)  # same identity + content → no-op
    assert (await catalog.get_world("w1")).name == "alpha"

    with pytest.raises(CatalogConflictError):
        await catalog.register_world(
            WorldRecord(
                world_id="w1",
                name="impostor",
                run_id="r1",
                parent_world_id=None,
                status="active",
                tick_head=0,
            )
        )
    await catalog.close()


def _register_world_proc(path: str, result_queue) -> None:
    import asyncio

    from archetype.app._catalog import SqliteControlCatalog, WorldRecord

    async def go():
        catalog = SqliteControlCatalog.__new__(SqliteControlCatalog)
        # Generous busy timeout: eight writers on one file under a loaded
        # test machine can exceed the 5s default (observed flake).
        catalog.__init__(__import__("pathlib").Path(path), busy_timeout_ms=60_000)
        await catalog.register_world(
            WorldRecord(
                world_id="race",
                name="same",
                run_id="r",
                parent_world_id=None,
                status="active",
                tick_head=0,
            )
        )
        await catalog.close()

    try:
        asyncio.run(go())
        result_queue.put("ok")
    except Exception as exc:  # pragma: no cover - failure reporting
        result_queue.put(f"error: {exc}")


def test_concurrent_identical_registration_yields_one_row(tmp_path):
    """Multi-process put-if-absent: the SQLite control plane, not wishful dedup."""
    path = tmp_path / "race.db"
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    procs = [ctx.Process(target=_register_world_proc, args=(str(path), queue)) for _ in range(8)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
    results = [queue.get(timeout=60) for _ in procs]
    assert all(r == "ok" for r in results), results

    rows = sqlite3.connect(path).execute("SELECT COUNT(*) FROM worlds").fetchone()[0]
    assert rows == 1, "eight identical registrations must produce exactly one row"


# ─────────────────────────────────────────────────────────────────────────────
# Service integration
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_destroyed_worlds_stay_discoverable_with_status(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="ephemeral"), storage)
        await c.world_service.destroy_world(world.world_id)

        infos = await c.world_service.discover_worlds(storage)
        assert [str(i.world_id) for i in infos] == [str(world.world_id)]
        record = await c.storage_service.get_control_catalog(storage).get_world(str(world.world_id))
        assert record.status == "destroyed", "append-only: destroy marks, never deletes"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_record_step_remains_advisory_after_destroy(tmp_path, caplog):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="ephemeral"), storage)
        await c.world_service.destroy_world(world.world_id)

        with caplog.at_level(logging.ERROR):
            await c.world_service.record_step(world.world_id)

        assert "catalog run update failed" in caplog.text
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_records_parent_world(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        base = await c.world_service.create_world(WorldConfig(name="base"), storage)
        await c.mutation_service.create_entity(base.world_id, [Score(points=1.0)])
        await c.simulation_service.step(base.world_id, RunConfig())
        fork = await c.world_service.fork_world(base.world_id, name="branch")

        record = await c.storage_service.get_control_catalog(storage).get_world(str(fork.world_id))
        assert record.parent_world_id == str(base.world_id)
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_readonly_open_never_constructs_a_live_world(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="cold"), storage)
        wid = str(world.world_id)
    finally:
        await c.shutdown()

    fresh = ServiceContainer()
    try:
        info = await fresh.world_service.open_world_readonly(storage, wid)
        assert str(info.world_id) == wid
        assert not fresh.world_service.has_world(wid), (
            "read-only open must not register a live world"
        )
        with pytest.raises(KeyError):
            await fresh.world_service.open_world_readonly(storage, str(uuid7()))
    finally:
        await fresh.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# P0: true cold restart
# ─────────────────────────────────────────────────────────────────────────────

_CHILD = textwrap.dedent(
    """
    import asyncio, sys
    from archetype.app.container import ServiceContainer
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig

    class Score(Component):
        points: float = 0.0

    class Flag(Component):
        label: str = ""

    async def main():
        c = ServiceContainer()
        try:
            w = await c.world_service.create_world(
                WorldConfig(name="cold"), StorageConfig(uri=sys.argv[1], namespace="ns")
            )
            # Two archetypes: (Score,) and (Score, Flag) — the subset query
            # must union both, cold.
            await c.mutation_service.create_entity(w.world_id, [Score(points=7.5)])
            await c.mutation_service.create_entity(
                w.world_id, [Score(points=2.5), Flag(label="x")]
            )
            await c.simulation_service.step(w.world_id, RunConfig())
            print(w.world_id)
        finally:
            await c.shutdown()

    asyncio.run(main())
    """
)


@pytest.mark.asyncio
async def test_p0_cold_subset_query_across_process_boundary(tmp_path):
    """The reproduced bug, as a release gate: write in a child process, then a
    fresh process discovers the world and a typed subset query unions rows
    from every archetype containing the requested component."""
    storage = _storage(tmp_path)
    result = subprocess.run(
        [sys.executable, "-c", _CHILD, storage.uri],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    world_id = result.stdout.strip().splitlines()[-1]

    c = ServiceContainer()
    try:
        infos = await c.world_service.discover_worlds(storage)
        assert [str(i.world_id) for i in infos] == [world_id]
        info = await c.world_service.open_world_readonly(storage, world_id)

        signatures = await c.query_service.list_signatures(storage)
        signature_names = {
            tuple(component.__name__ for component in signature) for signature in signatures
        }
        assert {("Score",), ("Flag", "Score")} <= signature_names, (
            "cold signature discovery must use the durable catalog, not the "
            "fresh store's empty process-local registry"
        )

        df = await c.query_service.query_components([Score], world_id, str(info.run_id), storage)
        points = sorted(row["score__points"] for row in df.to_pylist())
        assert points == [2.5, 7.5], "subset query must union both archetypes, cold"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_p0_stale_descriptor_fails_closed(tmp_path):
    """A catalog descriptor whose fingerprint disagrees with the physical
    table must refuse to read — never an empty frame, never a created table."""
    from archetype.app._catalog import CatalogSchemaMismatchError

    storage = _storage(tmp_path)
    result = subprocess.run(
        [sys.executable, "-c", _CHILD, storage.uri],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    world_id = result.stdout.strip().splitlines()[-1]

    # Corrupt one signature fingerprint directly in the catalog.
    conn = sqlite3.connect(catalog_path_for(storage))
    conn.execute("UPDATE signatures SET fingerprint='deadbeef'")
    conn.commit()
    conn.close()

    c = ServiceContainer()
    try:
        info = await c.world_service.open_world_readonly(storage, world_id)
        with pytest.raises(CatalogSchemaMismatchError):
            await c.query_service.query_components([Score], world_id, str(info.run_id), storage)
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_reads_never_create_tables(tmp_path):
    """Open-never-create: the seam raises KeyError for missing tables and a
    failed read leaves the store's physical table list unchanged."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await c.mutation_service.create_entity(world.world_id, [Score(points=1.0)])
        await c.simulation_service.step(world.world_id, RunConfig())

        store = await c.storage_service.get_or_create_store(storage, None)
        before = sorted(await store._list_table_names())

        with pytest.raises(KeyError):
            await store.get_existing_table_schema("a_nonexistent_table")
        with pytest.raises(KeyError):
            await store.get_existing_table_df("a_nonexistent_table", "w", "r")

        assert sorted(await store._list_table_names()) == before
    finally:
        await c.shutdown()


def test_schema_fingerprint_is_order_and_content_sensitive():
    import pyarrow as pa

    a = pa.schema([("x", pa.int64()), ("y", pa.float64())])
    same = pa.schema([("x", pa.int64()), ("y", pa.float64())])
    reordered = pa.schema([("y", pa.float64()), ("x", pa.int64())])
    retyped = pa.schema([("x", pa.int32()), ("y", pa.float64())])

    assert schema_fingerprint(a) == schema_fingerprint(same)
    assert schema_fingerprint(a) != schema_fingerprint(reordered)
    assert schema_fingerprint(a) != schema_fingerprint(retyped)

    # Physical encoding variants are the SAME logical schema: Iceberg
    # round-trips string as large_string and forces nullability.
    s1 = pa.schema([pa.field("n", pa.string(), nullable=False)])
    s2 = pa.schema([pa.field("n", pa.large_string(), nullable=True)])
    assert schema_fingerprint(s1) == schema_fingerprint(s2)


@pytest.mark.asyncio
async def test_signature_record_roundtrip(tmp_path):
    import pyarrow as pa

    from archetype.app._catalog import arrow_schema_descriptor

    catalog = SqliteControlCatalog(tmp_path / "cat.db")
    schema = pa.schema([("score__points", pa.float64())])
    record = SignatureRecord(
        table_id="t1",
        component_names=("Score",),
        schema_json=json.dumps(arrow_schema_descriptor(schema)),
        fingerprint=schema_fingerprint(schema),
    )
    await catalog.register_signature(record)
    await catalog.register_signature(record)  # idempotent
    (loaded,) = await catalog.list_signatures()
    assert loaded == record
    assert loaded.matches(schema)

    with pytest.raises(CatalogConflictError):
        await catalog.register_signature(
            SignatureRecord(
                table_id="t1",
                component_names=("Score",),
                schema_json=record.schema_json,
                fingerprint="different",
            )
        )
    await catalog.close()


@pytest.mark.asyncio
async def test_iceberg_seam_reads_existing_tables_only(tmp_path):
    """The durable-discovery seam holds on the Iceberg backend too:
    open-never-create reads by table id, KeyError for missing tables.

    The seam reads go through a FRESH session and store — nothing shared
    with the writer but the storage config — so this is a true cold read.
    """
    import daft
    import pyarrow as pa

    from archetype.core.aio import AsyncStore
    from archetype.core.archetype import Archetype
    from archetype.core.config import StorageBackend
    from archetype.runtime.session import configure_session

    storage = StorageConfig(uri=str(tmp_path), namespace="ns", backend=StorageBackend.ICEBERG)
    sig = (Score,)
    schema = Archetype.get_archetype_schema(sig)
    table_id = Archetype.get_name(sig)

    writer = AsyncStore(configure_session(storage), io_config=storage.io_config)
    try:
        row = {
            "world_id": "w",
            "run_id": "r",
            "entity_id": 1,
            "tick": 0,
            "is_active": True,
            "commit_token": "",
            "writer_epoch": 0,
            "score__points": 3.0,
        }
        await writer.append(sig, daft.from_arrow(pa.Table.from_pylist([row], schema=schema)))
    finally:
        await writer.shutdown()

    cold = AsyncStore(configure_session(storage), io_config=storage.io_config)
    try:
        physical = await cold.get_existing_table_schema(table_id)
        assert schema_fingerprint(physical) == schema_fingerprint(schema)

        df = await cold.get_existing_table_df(table_id, "w", "r", active_only=True)
        rows = df.to_pylist()
        assert len(rows) == 1 and rows[0]["score__points"] == 3.0

        with pytest.raises(KeyError):
            await cold.get_existing_table_schema("no_such_table")
    finally:
        await cold.shutdown()
