# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local Iceberg contracts for physical table migration primitives."""

from __future__ import annotations

import daft
import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitStateUnknownException

from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.catalog import CatalogConflictError
from archetype.storage.service import StorageService
from archetype.storage.session import configure_session
from archetype.storage.transfer import table_evidence


def _storage(tmp_path, name: str, namespace: str = "migration") -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / name),
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
    )


def _service(storage: StorageConfig) -> StorageService:
    return StorageService(session=configure_session(storage))


@pytest.mark.asyncio
async def test_namespace_enumeration_includes_unknown_and_empty_tables_only_in_namespace(
    tmp_path,
) -> None:
    storage = _storage(tmp_path, "source")
    service = _service(storage)
    try:
        await service.append_table(
            storage,
            "unknown_application_table",
            daft.from_pydict({"event_id": ["e1"]}),
        )
        store = await service.get_or_create_store(storage)
        catalog = store.session.current_catalog()
        assert catalog is not None
        catalog.create_table(
            f"{storage.namespace}.empty_table",
            source=daft.from_pydict({"value": [1]}).limit(0).schema(),
        )
        catalog.create_namespace("other")
        catalog.create_table(
            "other.foreign_table",
            source=daft.from_pydict({"value": [1]}).schema(),
        )

        assert await service.list_table_names(storage) == (
            "empty_table",
            "unknown_application_table",
        )
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_capture_export_and_import_preserve_full_snapshot_and_resume_exactly(
    tmp_path,
) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    payload = pa.Table.from_pylist(
        [
            {"event_id": "duplicate", "values": [1, None, 3]},
            {"event_id": "other", "values": []},
            {"event_id": "duplicate", "values": [1, None, 3]},
        ],
        schema=pa.schema(
            [
                pa.field("event_id", pa.string()),
                pa.field("values", pa.list_(pa.int64())),
            ]
        ),
    )
    try:
        await source.append_table(source_config, "events", daft.from_arrow(payload))
        frozen = await source.capture_table_snapshot(source_config, "events")
        exported = await source.export_table_snapshot(source_config, frozen)

        first = await destination.import_table_snapshot(destination_config, frozen, exported)
        second = await destination.import_table_snapshot(destination_config, frozen, exported)
        observed = await destination.capture_table_snapshot(destination_config, "events")

        assert first == second
        assert first.source_snapshot_id == frozen.snapshot_id
        assert first.destination_snapshot_id == observed.snapshot_id
        assert observed.row_count == frozen.row_count == 3
        assert observed.schema_fingerprint == frozen.schema_fingerprint
        assert observed.content_digest == frozen.content_digest
        assert (
            len((await destination.export_table_snapshot(destination_config, observed)).to_pylist())
            == 3
        )
    finally:
        await source.shutdown()
        await destination.shutdown()


def test_logical_table_evidence_rejects_top_level_nullability_drift() -> None:
    nullable = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64())],
        schema=pa.schema([pa.field("value", pa.int64(), nullable=True)]),
    )
    required = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64())],
        schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]),
    )
    expected = table_evidence("events", 1, nullable)
    observed = table_evidence("events", 2, required)

    # The legacy family fingerprint intentionally omits top-level nullability;
    # migration's complete evidence must still distinguish the schemas.
    assert expected.schema_fingerprint == observed.schema_fingerprint
    assert expected.content_digest == observed.content_digest
    assert not StorageService._same_logical_table(expected, observed)


@pytest.mark.asyncio
async def test_export_remains_pinned_to_planned_source_snapshot_after_later_append(
    tmp_path,
) -> None:
    storage = _storage(tmp_path, "source")
    service = _service(storage)
    try:
        await service.append_table(storage, "events", daft.from_pydict({"event_id": ["first"]}))
        planned = await service.capture_table_snapshot(storage, "events")
        await service.append_table(storage, "events", daft.from_pydict({"event_id": ["later"]}))

        exported = await service.export_table_snapshot(storage, planned)
        current = await service.capture_table_snapshot(storage, "events")

        assert exported.to_pylist() == [{"event_id": "first"}]
        assert current.snapshot_id != planned.snapshot_id
        assert current.row_count == 2
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_import_accepts_separate_exact_destination_evidence_for_relocation(tmp_path) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        await source.append_table(
            source_config,
            "artifact_files",
            daft.from_pydict(
                {
                    "artifact_id": ["a1", "a2"],
                    "object_uri": ["file:///source/a", "file:///source/b"],
                }
            ),
        )
        source_evidence = await source.capture_table_snapshot(source_config, "artifact_files")
        exported = await source.export_table_snapshot(source_config, source_evidence)
        relocated = exported.set_column(
            exported.schema.get_field_index("object_uri"),
            "object_uri",
            pa.array(["file:///destination/a", "file:///destination/b"]),
        )
        destination_evidence = table_evidence(
            "artifact_files",
            source_evidence.snapshot_id,
            relocated,
        )

        receipt = await destination.import_table_snapshot(
            destination_config,
            source_evidence,
            relocated,
            destination_evidence=destination_evidence,
        )
        observed = await destination.capture_table_snapshot(destination_config, "artifact_files")

        assert receipt.source_content_digest == source_evidence.content_digest
        assert receipt.destination_content_digest == destination_evidence.content_digest
        assert receipt.source_content_digest != receipt.destination_content_digest
        assert observed.content_digest == destination_evidence.content_digest
        assert (await destination.export_table_snapshot(destination_config, observed)).column(
            "object_uri"
        ).to_pylist() == [
            "file:///destination/a",
            "file:///destination/b",
        ]
    finally:
        await source.shutdown()
        await destination.shutdown()


@pytest.mark.asyncio
async def test_import_rejects_changed_payload_without_destination_evidence_before_mutation(
    tmp_path,
) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        await source.append_table(source_config, "events", daft.from_pydict({"value": [1]}))
        evidence = await source.capture_table_snapshot(source_config, "events")
        changed = pa.table({"value": pa.array([2], type=pa.int64())})

        with pytest.raises(CatalogConflictError, match="payload table"):
            await destination.import_table_snapshot(destination_config, evidence, changed)

        assert await destination.list_table_names(destination_config) == ()
    finally:
        await source.shutdown()
        await destination.shutdown()


@pytest.mark.asyncio
async def test_import_rejects_populated_destination_conflict_without_merge(tmp_path) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        await source.append_table(source_config, "events", daft.from_pydict({"value": [1]}))
        expected = await source.capture_table_snapshot(source_config, "events")
        payload = await source.export_table_snapshot(source_config, expected)
        await destination.append_table(
            destination_config,
            "events",
            daft.from_pydict({"value": [99]}),
        )

        with pytest.raises(CatalogConflictError, match="already contains different content"):
            await destination.import_table_snapshot(destination_config, expected, payload)

        rows = await destination.read_table(destination_config, "events")
        assert rows.to_pylist() == [{"value": 99}]
    finally:
        await source.shutdown()
        await destination.shutdown()


@pytest.mark.asyncio
async def test_historical_lookup_recovers_import_evidence_after_later_destination_tick(
    tmp_path,
) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        await source.append_table(source_config, "events", daft.from_pydict({"value": [1]}))
        planned = await source.capture_table_snapshot(source_config, "events")
        payload = await source.export_table_snapshot(source_config, planned)
        imported = await destination.import_table_snapshot(
            destination_config,
            planned,
            payload,
        )
        await destination.append_table(
            destination_config,
            "events",
            daft.from_pydict({"value": [2]}),
        )

        recovered = await destination.find_table_snapshot(destination_config, planned)
        current = await destination.capture_table_snapshot(destination_config, "events")

        assert recovered is not None
        assert recovered.snapshot_id == imported.destination_snapshot_id
        assert recovered.content_digest == planned.content_digest
        assert current.row_count == 2
        assert current.content_digest != planned.content_digest
    finally:
        await source.shutdown()
        await destination.shutdown()


async def _precreate_destination_table(
    service: StorageService,
    storage: StorageConfig,
    name: str,
    payload: pa.Table,
):
    store = await service.get_or_create_store(storage)
    return service._ensure_table(store, name, daft.from_arrow(payload).schema())


@pytest.mark.asyncio
async def test_import_reconciles_commit_that_landed_before_unknown_response(
    tmp_path,
    monkeypatch,
) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        await source.append_table(source_config, "events", daft.from_pydict({"value": [1, 2]}))
        expected = await source.capture_table_snapshot(source_config, "events")
        payload = await source.export_table_snapshot(source_config, expected)
        table = await _precreate_destination_table(
            destination, destination_config, "events", payload
        )
        original_commit = table._inner.catalog.commit_table

        def commit_then_lose_response(*args, **kwargs):
            original_commit(*args, **kwargs)
            raise CommitStateUnknownException("induced lost commit response")

        monkeypatch.setattr(table._inner.catalog, "commit_table", commit_then_lose_response)

        receipt = await destination.import_table_snapshot(destination_config, expected, payload)
        retried = await destination.import_table_snapshot(destination_config, expected, payload)
        native = table._inner.refresh()

        assert receipt == retried
        assert len(native.snapshots()) == 1
        assert (await destination.read_table(destination_config, "events")).to_pylist() == [
            {"value": 1},
            {"value": 2},
        ]
    finally:
        await source.shutdown()
        await destination.shutdown()


@pytest.mark.asyncio
async def test_import_does_not_replay_unproven_unknown_commit(tmp_path, monkeypatch) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        await source.append_table(source_config, "events", daft.from_pydict({"value": [1]}))
        expected = await source.capture_table_snapshot(source_config, "events")
        payload = await source.export_table_snapshot(source_config, expected)
        table = await _precreate_destination_table(
            destination, destination_config, "events", payload
        )
        original_commit = table._inner.catalog.commit_table
        attempts = 0

        def unknown_before_commit(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            raise CommitStateUnknownException("induced unknown pre-commit state")

        monkeypatch.setattr(table._inner.catalog, "commit_table", unknown_before_commit)
        with pytest.raises(CommitStateUnknownException, match="unknown pre-commit"):
            await destination.import_table_snapshot(destination_config, expected, payload)
        assert attempts == 1
        assert table._inner.refresh().current_snapshot() is None

        monkeypatch.setattr(table._inner.catalog, "commit_table", original_commit)
        receipt = await destination.import_table_snapshot(destination_config, expected, payload)

        assert receipt.row_count == 1
        assert len(table._inner.refresh().snapshots()) == 1
    finally:
        await source.shutdown()
        await destination.shutdown()


@pytest.mark.asyncio
async def test_empty_table_is_enumerated_and_imported_without_inventing_a_snapshot(
    tmp_path,
) -> None:
    source_config = _storage(tmp_path, "source")
    destination_config = _storage(tmp_path, "destination")
    source = _service(source_config)
    destination = _service(destination_config)
    try:
        empty = pa.table({"value": pa.array([], type=pa.int64())})
        await _precreate_destination_table(source, source_config, "empty", empty)
        expected = await source.capture_table_snapshot(source_config, "empty")
        payload = await source.export_table_snapshot(source_config, expected)

        receipt = await destination.import_table_snapshot(destination_config, expected, payload)
        observed = await destination.capture_table_snapshot(destination_config, "empty")

        assert expected.snapshot_id is None
        assert receipt.destination_snapshot_id is None
        assert observed.snapshot_id is None
        assert observed.row_count == 0
        assert await destination.list_table_names(destination_config) == ("empty",)
    finally:
        await source.shutdown()
        await destination.shutdown()
