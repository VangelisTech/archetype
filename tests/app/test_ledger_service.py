# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import json
import multiprocessing
import queue
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from archetype.app.ledger_service import LedgerService
from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig
from archetype.ledger import (
    LedgerIdentity,
    LedgerManifest,
    LedgerRef,
    ManifestConflictError,
    ManifestCorruptionError,
    StorageRefMismatchError,
)
from archetype.ledger.canonical import internal_digest
from archetype.ledger.records import DurableRecord

_PROCESS_COUNT = 8


def _create_ledger_process(database: str, barrier, results) -> None:
    async def create() -> str:
        service = LedgerService(StorageService())
        ref = await service.create_ledger(
            name="race",
            storage_config=StorageConfig(uri=database, namespace="test"),
            world_id="race-world",
            run_id="race-run",
        )
        return ref.model_dump_json()

    try:
        barrier.wait(timeout=60)
        results.put(("ok", asyncio.run(create())))
    except BaseException as exc:
        results.put(("error", type(exc).__name__, str(exc)))


@pytest.mark.asyncio
async def test_generation_zero_create_recover_list_and_manifest(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)

    ref = await ledgers.create_ledger(
        name="lab",
        storage_config=config,
        world_id="world-1",
        run_id="run-1",
    )
    recovered = await ledgers.get_head(ref.identity, storage_config=config)
    manifest = await ledgers.get_manifest(ref, storage_config=config)
    listed = await ledgers.list_ledgers(
        ref.identity.storage,
        storage_config=config,
        name="lab",
    )

    assert recovered == ref
    assert listed == [await ledgers.describe_ledger(ref, storage_config=config)]
    assert manifest.generation == 0
    assert manifest.previous_manifest_digest is None
    assert manifest.committed_through_tick is None
    assert manifest.next_tick == 0
    assert manifest.next_entity_id == 1
    assert manifest.writer_epoch == 0
    assert manifest.signatures == ()
    assert manifest.entity_directory == ()
    assert manifest.lineage == ()
    assert manifest.batches == ()


@pytest.mark.asyncio
async def test_concurrent_identical_creation_replays_and_conflict_fails(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    ledgers = LedgerService(StorageService())

    async def create(name: str):
        return await ledgers.create_ledger(
            name=name,
            storage_config=config,
            world_id="same-world",
            run_id="same-run",
        )

    refs = await asyncio.gather(*(create("same") for _ in range(8)))
    assert len(set(ref.model_dump_json() for ref in refs)) == 1

    with pytest.raises(ManifestConflictError):
        await create("different")


@pytest.mark.asyncio
async def test_list_filters_latest_ledgers_by_name(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    first = await ledgers.create_ledger(
        name="wanted", storage_config=config, world_id="w-1", run_id="r-1"
    )
    await ledgers.create_ledger(name="other", storage_config=config, world_id="w-2", run_id="r-2")

    all_infos = await ledgers.list_ledgers(first.identity.storage, storage_config=config)
    filtered = await ledgers.list_ledgers(
        first.identity.storage, storage_config=config, name="wanted"
    )
    missing = await ledgers.list_ledgers(
        first.identity.storage, storage_config=config, name="missing"
    )

    assert len(all_infos) == 2
    assert [info.ref for info in filtered] == [first]
    assert missing == []


@pytest.mark.asyncio
async def test_storage_mismatch_fails_before_catalog_read(tmp_path: Path) -> None:
    one = StorageConfig(uri=tmp_path / "one", namespace="test")
    two = StorageConfig(uri=tmp_path / "two", namespace="test")
    ledgers = LedgerService(StorageService())
    ref = await ledgers.create_ledger(name=None, storage_config=one, world_id="world", run_id="run")

    with pytest.raises(StorageRefMismatchError):
        await ledgers.get_head(ref.identity, storage_config=two)
    with pytest.raises(StorageRefMismatchError):
        await ledgers.get_manifest(ref, storage_config=two)
    with pytest.raises(StorageRefMismatchError):
        await ledgers.list_ledgers(ref.identity.storage, storage_config=two)


@pytest.mark.asyncio
async def test_corrupt_catalog_record_fails_as_manifest_corruption(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    ref = await ledgers.create_ledger(
        name="lab", storage_config=config, world_id="world", run_id="run"
    )
    catalog = await storage.get_or_create_atomic_record_store(config)
    with sqlite3.connect(catalog.database_path) as connection:
        connection.execute("UPDATE durable_records SET payload_json = '{}' WHERE revision = 0")

    with pytest.raises(ManifestCorruptionError):
        await ledgers.get_head(ref.identity, storage_config=config)
    with pytest.raises(ManifestCorruptionError):
        await ledgers.create_ledger(
            name="lab", storage_config=config, world_id="world", run_id="run"
        )


@pytest.mark.asyncio
async def test_broken_manifest_predecessor_chain_fails_closed(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    genesis_ref = await ledgers.create_ledger(
        name="lab", storage_config=config, world_id="world", run_id="run"
    )
    catalog = await storage.get_or_create_atomic_record_store(config)
    key = ledgers._identity_key(genesis_ref.identity)
    genesis_record = await catalog.get(
        kind="ledger-manifest",
        scope=genesis_ref.identity.storage.storage_id,
        key=key,
        revision=0,
    )
    assert genesis_record is not None
    manifest = LedgerManifest.create(
        identity=genesis_ref.identity,
        name="lab",
        generation=1,
        previous_manifest_digest="sha256:" + "f" * 64,
        commit_id=internal_digest("test-commit", {"generation": 1}),
        committed_through_tick=None,
        next_tick=0,
        next_entity_id=1,
        signatures=(),
        entity_directory=(),
        lineage=(),
        batches=(),
        writer_epoch=0,
        execution_contract_digest=None,
        committed_at_ms=2,
    )
    broken = DurableRecord.create(
        kind="ledger-manifest",
        scope=genesis_ref.identity.storage.storage_id,
        key=key,
        revision=1,
        previous_digest=genesis_record.content_digest,
        payload=ledgers._manifest_payload(manifest),
        committed_at_ms=manifest.committed_at_ms,
    )
    await catalog.compare_and_swap(
        broken,
        expected_revision=0,
        expected_digest=genesis_record.content_digest,
    )
    broken_ref = LedgerRef(
        identity=manifest.identity,
        manifest_digest=manifest.manifest_digest,
        manifest_generation=1,
        committed_through_tick=None,
        next_tick=0,
    )

    with pytest.raises(ManifestCorruptionError, match="predecessor digest"):
        await ledgers.get_head(genesis_ref.identity, storage_config=config)
    with pytest.raises(ManifestCorruptionError, match="predecessor digest"):
        await ledgers.get_manifest(broken_ref, storage_config=config)


def test_ledger_head_survives_true_process_restart_and_different_cwd(tmp_path: Path) -> None:
    database = tmp_path / "db"
    first_cwd = tmp_path / "first-cwd"
    second_cwd = tmp_path / "second-cwd"
    first_cwd.mkdir()
    second_cwd.mkdir()
    create_code = f"""
import asyncio
from archetype.app.ledger_service import LedgerService
from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig

async def main():
    service = LedgerService(StorageService())
    ref = await service.create_ledger(
        name='restart',
        storage_config=StorageConfig(uri={str(database)!r}, namespace='test'),
        world_id='restart-world',
        run_id='restart-run',
    )
    print(ref.model_dump_json())

asyncio.run(main())
"""
    created = subprocess.run(
        [sys.executable, "-c", create_code],
        cwd=first_cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    created_ref = json.loads(created.stdout.strip().splitlines()[-1])

    recover_code = f"""
import asyncio
from archetype.app.ledger_service import LedgerService
from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig
from archetype.ledger import LedgerIdentity

async def main():
    storage = StorageService()
    service = LedgerService(storage)
    config = StorageConfig(uri={database.as_uri()!r}, namespace='test')
    identity = LedgerIdentity(
        storage=storage.storage_ref(config),
        world_id='restart-world',
        run_id='restart-run',
    )
    ref = await service.get_head(identity, storage_config=config)
    infos = await service.list_ledgers(identity.storage, storage_config=config)
    print(ref.model_dump_json())
    print(len(infos))

asyncio.run(main())
"""
    recovered = subprocess.run(
        [sys.executable, "-c", recover_code],
        cwd=second_cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    lines = recovered.stdout.strip().splitlines()
    assert json.loads(lines[-2]) == created_ref
    assert lines[-1] == "1"


def test_eight_process_first_create_has_one_manifest_and_one_ref(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(_PROCESS_COUNT)
    results = context.Queue()
    database = tmp_path / "db"
    processes = [
        context.Process(
            target=_create_ledger_process,
            args=(str(database), barrier, results),
        )
        for _ in range(_PROCESS_COUNT)
    ]
    for process in processes:
        process.start()

    received = []
    try:
        for _ in processes:
            received.append(results.get(timeout=90))
        for process in processes:
            process.join(timeout=30)
        assert all(process.exitcode == 0 for process in processes)
    except (AssertionError, queue.Empty):
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join(timeout=5)
        raise
    finally:
        results.close()
        results.join_thread()

    assert all(result[0] == "ok" for result in received), received
    assert len({result[1] for result in received}) == 1
    catalog = database / "test" / ".archetype" / "catalog-v1.sqlite3"
    with sqlite3.connect(catalog) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM durable_records WHERE kind = 'ledger-manifest'"
            ).fetchone()[0]
            == 1
        )


def test_identity_rejects_blank_ids_instead_of_minting_replacements(tmp_path: Path) -> None:
    storage = StorageService.storage_ref(StorageConfig(uri=tmp_path, namespace="test"))
    with pytest.raises(ValueError):
        LedgerIdentity(storage=storage, world_id="", run_id="run")
