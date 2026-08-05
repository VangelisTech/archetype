# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable discovery (issue #272, A1-read).

The control catalog makes the existing registries durable: a fresh process
pointed at the same storage identity discovers every world and signature ever
committed there. P0 tests use true subprocess restarts — same-process
container recycling would not prove cold discovery.
"""

import gc
import json
import logging
import multiprocessing
import sqlite3
import subprocess
import sys
import textwrap

import pytest
from uuid_utils import uuid7

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.storage.catalog import (
    CatalogConflictError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
    arrow_schema_descriptor,
    catalog_path_for,
    schema_fingerprint,
    storage_fingerprint,
)
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.storage.signatures import match_signature_records
from archetype.world.models import (
    ComponentTypeRef,
    CreateWorld,
    DestroyWorld,
    DiscoverWorlds,
    ForkWorld,
    ListSignatures,
    OpenWorldReadonly,
    QueryComponents,
    Spawn,
    Step,
)
from tests._runtime import build_test_runtime


class Score(Component):
    points: float = 0.0


class Flag(Component):
    label: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


def _resources(tmp_path):
    storage_service = StorageService(
        control_catalog_config=ControlCatalogConfig(catalog_dir=tmp_path / "control")
    )
    return (
        build_test_runtime(tmp_path, storage_service=storage_service),
        storage_service,
    )


def _world_registry(dispatcher):
    return dispatcher._registry.resolve_name("step").handler.args[0]


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
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        source = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="src"),
                storage_config=storage,
            )
        )

        async def _boom(self, record):
            raise CatalogConflictError("injected registration failure")

        monkeypatch.setattr(SqliteControlCatalog, "register_world", _boom)

        with pytest.raises(CatalogConflictError):
            await dispatcher.apply(
                CreateWorld(
                    config=WorldConfig(name="orphan"),
                    storage_config=storage,
                )
            )
        with pytest.raises(CatalogConflictError):
            await dispatcher.apply(
                ForkWorld(
                    source_world_id=source.world_id,
                    name="orphan-fork",
                )
            )

        registry = _world_registry(dispatcher)
        live = {world.name for world in await registry.list_worlds()}
        assert live == {"src"}, f"failed registrations must unwind, saw {live}"
        with pytest.raises(KeyError):
            await registry.world_id_for_name("orphan")
    finally:
        await resources.aclose()
        await storage_service.shutdown()


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
    with pytest.raises(CatalogConflictError):
        await catalog.register_world(
            WorldRecord(
                world_id="w1",
                name="alpha",
                run_id="r1",
                parent_world_id=None,
                status="active",
                tick_head=0,
                writer_mode="cleanup_only",
            )
        )
    await catalog.close()


@pytest.mark.asyncio
async def test_catalog_migrates_legacy_worlds_to_resumable_writer_mode(tmp_path):
    path = tmp_path / "legacy-cat.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE catalog_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO catalog_meta (key, value)
            VALUES ('schema_version', '9');
            CREATE TABLE worlds (
                world_id TEXT PRIMARY KEY,
                name TEXT,
                run_id TEXT,
                parent_world_id TEXT,
                status TEXT NOT NULL,
                tick_head INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO worlds (
                world_id, name, run_id, parent_world_id, status, tick_head
            ) VALUES ('legacy', 'legacy', 'run', NULL, 'active', 3);
            """
        )

    catalog = SqliteControlCatalog(path)
    try:
        record = await catalog.get_world("legacy")
        assert record is not None
        assert record.writer_mode == "resumable"
    finally:
        await catalog.close()

    with sqlite3.connect(path) as connection:
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(worlds)").fetchall()
        }
        version = connection.execute(
            "SELECT value FROM catalog_meta WHERE key='schema_version'"
        ).fetchone()[0]
    assert "writer_mode" in columns
    assert version == "10"


def _register_world_proc(path: str, result_queue) -> None:
    import asyncio

    from archetype.storage.catalog import SqliteControlCatalog, WorldRecord

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
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="ephemeral"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(DestroyWorld(world_id=world.world_id))

        infos = await dispatcher.apply(DiscoverWorlds(storage_config=storage))
        assert [str(i.world_id) for i in infos] == [str(world.world_id)]
        record = await storage_service.get_control_catalog(storage).get_world(str(world.world_id))
        assert record.status == "destroyed", "append-only: destroy marks, never deletes"
    finally:
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_storage_identity_remains_available_after_destroy(tmp_path):
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="ephemeral"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(DestroyWorld(world_id=world.world_id))

        assert await _world_registry(dispatcher).storage_record(str(world.world_id)) == (
            storage,
            None,
        )
    finally:
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_fork_records_parent_world(tmp_path):
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        base = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="base"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=base.world_id,
                components=[Score(points=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=base.world_id, run_config=RunConfig()))
        fork = await dispatcher.apply(ForkWorld(source_world_id=base.world_id, name="branch"))

        record = await storage_service.get_control_catalog(storage).get_world(str(fork.world_id))
        assert record.parent_world_id == str(base.world_id)
    finally:
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_readonly_open_never_constructs_a_live_world(tmp_path):
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="cold"),
                storage_config=storage,
            )
        )
        wid = str(world.world_id)
    finally:
        await resources.aclose()
        await storage_service.shutdown()

    fresh_resources, fresh_storage = _resources(tmp_path)
    fresh_dispatcher = fresh_resources.dispatcher
    try:
        info = await fresh_dispatcher.apply(OpenWorldReadonly(storage_config=storage, world_id=wid))
        assert str(info.world_id) == wid
        assert not await _world_registry(fresh_dispatcher).contains(wid), (
            "read-only open must not register a live world"
        )
        with pytest.raises(KeyError):
            await fresh_dispatcher.apply(
                OpenWorldReadonly(
                    storage_config=storage,
                    world_id=str(uuid7()),
                )
            )
    finally:
        await fresh_resources.aclose()
        await fresh_storage.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# P0: true cold restart
# ─────────────────────────────────────────────────────────────────────────────

_CHILD = textwrap.dedent(
    """
    import asyncio, sys
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig
    from archetype.storage.config import ControlCatalogConfig
    from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
    from archetype.world.models import CreateWorld, Spawn, Step

    class Score(Component):
        points: float = 0.0

    class Flag(Component):
        label: str = ""

    async def main():
        resources = build_runtime_resources(
            RuntimeBootstrapConfig(control_catalog_config=ControlCatalogConfig())
        )
        try:
            dispatcher = resources.dispatcher
            world = await dispatcher.apply(
                CreateWorld(
                    config=WorldConfig(name="cold"),
                    storage_config=StorageConfig(uri=sys.argv[1], namespace="ns"),
                )
            )
            # Two archetypes: (Score,) and (Score, Flag) — the subset query
            # must union both, cold.
            await dispatcher.apply(
                Spawn.from_components(
                    world_id=world.world_id,
                    components=[Score(points=7.5)],
                )
            )
            await dispatcher.apply(
                Spawn.from_components(
                    world_id=world.world_id,
                    components=[Score(points=2.5), Flag(label="x")],
                )
            )
            await dispatcher.apply(
                Step(world_id=world.world_id, run_config=RunConfig())
            )
            print(world.world_id)
        finally:
            await resources.aclose()

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

    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        infos = await dispatcher.apply(DiscoverWorlds(storage_config=storage))
        assert [str(i.world_id) for i in infos] == [world_id]
        info = await dispatcher.apply(OpenWorldReadonly(storage_config=storage, world_id=world_id))

        signatures = await dispatcher.apply(ListSignatures(storage_config=storage))
        signature_names = {
            tuple(component.__name__ for component in signature) for signature in signatures
        }
        assert {("Score",), ("Flag", "Score")} <= signature_names, (
            "cold signature discovery must use the durable catalog, not the "
            "fresh store's empty process-local registry"
        )

        df = await dispatcher.apply(
            QueryComponents(
                components=(ComponentTypeRef.from_type(Score),),
                world_id=world_id,
                run_id=info.run_id,
                storage_config=storage,
            )
        )
        points = sorted(row["score__points"] for row in df.to_pylist())
        assert points == [2.5, 7.5], "subset query must union both archetypes, cold"
    finally:
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_cold_signature_listing_skips_unresolvable_history(tmp_path, caplog):
    """One unrelated stale record cannot poison storage-wide discovery."""
    storage = _storage(tmp_path)
    writer_resources, writer_storage = _resources(tmp_path)
    writer = writer_resources.dispatcher
    try:
        world = await writer.apply(
            CreateWorld(
                config=WorldConfig(name="current"),
                storage_config=storage,
            )
        )
        await writer.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Score(points=1.0)],
            )
        )
        await writer.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        schema = Archetype.get_archetype_schema((Score,))
        catalog = writer_storage.get_control_catalog(storage)
        await catalog.register_signature(
            SignatureRecord(
                table_id="a_removed_history",
                component_names=("RemovedQueryHistoryComponent",),
                schema_json="{}",
                fingerprint="removed",
            )
        )
        await catalog.register_signature(
            SignatureRecord(
                table_id="a_schema_match_wrong_identity",
                component_names=("Score",),
                schema_json=json.dumps(arrow_schema_descriptor(schema)),
                fingerprint=schema_fingerprint(schema),
            )
        )
    finally:
        await writer_resources.aclose()
        await writer_storage.shutdown()

    reader_resources, reader_storage = _resources(tmp_path)
    reader = reader_resources.dispatcher
    try:
        with caplog.at_level(logging.WARNING, logger="archetype.world.query"):
            signatures = await reader.apply(ListSignatures(storage_config=storage))

        names = {tuple(component.__name__ for component in sig) for sig in signatures}
        assert ("Score",) in names
        assert "a_removed_history" in caplog.text
        assert "a_schema_match_wrong_identity" in caplog.text
    finally:
        await reader_resources.aclose()
        await reader_storage.shutdown()


@pytest.mark.asyncio
async def test_warm_signature_listing_preserves_local_class_identity(tmp_path):
    """Catalog ambiguity fills cold gaps but cannot replace an exact local class."""
    storage = _storage(tmp_path)
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    shadow_score_type: type[Component] | None = None
    try:
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="warm"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Score(points=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        shadow_score_type = type(
            "Score",
            (Component,),
            {
                "__module__": "aaa_catalog_shadow",
                "__annotations__": {"points": float},
                "points": 0.0,
            },
        )
        assert Archetype.get_name((shadow_score_type,)) == Archetype.get_name((Score,))

        signatures = await dispatcher.apply(ListSignatures(storage_config=storage))
        score_table_id = Archetype.get_name((Score,))
        by_table_id = {Archetype.get_name(signature): signature for signature in signatures}

        assert by_table_id[score_table_id] == (Score,)
        assert by_table_id[score_table_id] != (shadow_score_type,)
    finally:
        shadow_score_type = None
        gc.collect()
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_warm_signature_listing_survives_catalog_failure(tmp_path, monkeypatch, caplog):
    """Best-effort discovery retains the complete pre-catalog local answer."""
    storage = _storage(tmp_path)
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="warm"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Score(points=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        def _unavailable(_storage_config):
            raise RuntimeError("injected catalog outage")

        monkeypatch.setattr(storage_service, "get_control_catalog", _unavailable)
        with caplog.at_level(logging.ERROR, logger="archetype.world.query"):
            signatures = await dispatcher.apply(ListSignatures(storage_config=storage))

        assert (Score,) in signatures
        assert "control catalog unavailable for durable signature discovery" in caplog.text
    finally:
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_p0_stale_descriptor_fails_closed(tmp_path):
    """A catalog descriptor whose fingerprint disagrees with the physical
    table must refuse to read — never an empty frame, never a created table."""
    from archetype.storage.catalog import CatalogSchemaMismatchError

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

    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        info = await dispatcher.apply(OpenWorldReadonly(storage_config=storage, world_id=world_id))
        with pytest.raises(CatalogSchemaMismatchError):
            await dispatcher.apply(
                QueryComponents(
                    components=(ComponentTypeRef.from_type(Score),),
                    world_id=world_id,
                    run_id=info.run_id,
                    storage_config=storage,
                )
            )
    finally:
        await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_reads_never_create_tables(tmp_path):
    """Open-never-create: the seam raises KeyError for missing tables and a
    failed read leaves the store's physical table list unchanged."""
    resources, storage_service = _resources(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="w"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Score(points=1.0)],
            )
        )
        await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        store = await storage_service.get_or_create_store(storage, None)
        before = sorted(await store._list_table_names())

        with pytest.raises(KeyError):
            await store.get_existing_table_schema("a_nonexistent_table")
        with pytest.raises(KeyError):
            await store.get_existing_table_df("a_nonexistent_table", "w", "r")

        assert sorted(await store._list_table_names()) == before
    finally:
        await resources.aclose()
        await storage_service.shutdown()


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

    from archetype.storage.catalog import arrow_schema_descriptor

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


def test_signature_resolution_requires_durable_table_identity():
    """A normalized schema match cannot redirect a read to a new table id."""
    schema = Archetype.get_archetype_schema((Score,))
    record = SignatureRecord(
        table_id="a_schema_match_wrong_identity",
        component_names=("Score",),
        schema_json=json.dumps(arrow_schema_descriptor(schema)),
        fingerprint=schema_fingerprint(schema),
    )

    resolved, problems = match_signature_records([record])

    assert resolved == {}
    assert "different table identity" in problems[record.table_id]


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
    from archetype.storage.session import configure_session

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
