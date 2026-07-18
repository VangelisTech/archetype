# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed Iceberg artifact-table contracts."""

import asyncio
import hashlib
from pathlib import Path
from uuid import UUID

import daft
import pyarrow as pa
import pytest

import archetype.app.artifacts.table_service as artifact_table_service_module
from archetype import ArchetypeRuntime
from archetype.app.artifacts.models import ARTIFACT_ENVELOPE_COLUMNS
from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig


@daft.func(return_dtype=daft.DataType.string())
def _read_text(file: daft.File) -> str:
    with file.open() as stream:
        return stream.read().decode("utf-8")


class TextFacts:
    table_name = "documents"

    def process(self, files):
        return files.with_column("text", _read_text(daft.col("file")))


@daft.func(return_dtype=daft.DataType.string())
def _fail_if_executed(_file: daft.File) -> str:
    raise AssertionError("known logical artifacts must be removed before processor execution")


class MustNotRun:
    table_name = "documents"

    def process(self, files):
        return files.with_column("text", _fail_if_executed(daft.col("file")))


class RewritesIdentity:
    table_name = "documents"

    def process(self, files):
        return files.with_columns(
            {
                "source_uri": daft.lit("sensor://incorrect/1"),
                "text": _read_text(daft.col("file")),
            }
        )


_PIPELINE_EXECUTIONS = 0
_FILE_HASH_EXECUTIONS = 0


@daft.func(return_dtype=daft.DataType.int64())
def _count_pipeline_execution(value: int) -> int:
    global _PIPELINE_EXECUTIONS
    _PIPELINE_EXECUTIONS += 1
    return value


@daft.func(return_dtype=daft.DataType.string())
def _count_file_hash(file: daft.File) -> str:
    global _FILE_HASH_EXECUTIONS
    _FILE_HASH_EXECUTIONS += 1
    digest = hashlib.sha256()
    with file.open() as stream:
        while chunk := stream.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _storage(tmp_path: Path, *, backend: StorageBackend = StorageBackend.ICEBERG):
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=backend,
    )


async def _world(container: ServiceContainer, storage: StorageConfig, name: str = "w"):
    return await container.world_service.create_world(WorldConfig(name=name), storage)


@pytest.mark.asyncio
async def test_file_processor_writes_standard_envelope_in_world_catalog(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        first = tmp_path / "first.txt"
        second = tmp_path / "second.txt"
        first.write_text("same content")
        second.write_text("same content")

        receipt = await container.artifact_table_service.ingest_files(
            str(world.world_id),
            [first, second],
            TextFacts(),
        )

        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert receipt.rows_written == 2
        assert receipt.table_id == "artifacts__documents"
        assert iceberg.has_table(receipt.table_id)

        rows = (
            await container.artifact_table_service.read_artifacts(str(world.world_id), "documents")
        ).to_pylist()
        assert len(rows) == 2
        assert set(ARTIFACT_ENVELOPE_COLUMNS).issubset(rows[0])
        assert "file" not in rows[0]
        assert "observed_at" not in rows[0]
        assert {row["text"] for row in rows} == {"same content"}
        assert len({row["source_uri"] for row in rows}) == 2
        assert len({row["content_hash"] for row in rows}) == 1
        assert all(UUID(row["artifact_id"]).version == 7 for row in rows)
        assert {row["world_id"] for row in rows} == {str(world.world_id)}
        assert {row["run_id"] for row in rows} == {str(world.run_id)}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_same_uri_and_content_is_noop_before_processor(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "reading.txt"
        source.write_text("21.5 C")

        first = await container.artifact_table_service.ingest_files(
            str(world.world_id), source, TextFacts()
        )
        retry = await container.artifact_table_service.ingest_files(
            str(world.world_id), source, MustNotRun()
        )

        assert first.rows_written == 1
        assert first.sources_matched == 1
        assert retry.sources_matched == 1
        assert retry.duplicate
        assert retry.snapshot_id == first.snapshot_id
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_empty_file_match_is_not_reported_as_duplicate(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)

        receipt = await container.artifact_table_service.ingest_files(
            str(world.world_id), tmp_path / "missing" / "*.txt", TextFacts()
        )

        assert receipt.sources_matched == 0
        assert receipt.rows_written == 0
        assert receipt.duplicate is False
        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert not iceberg.has_table("artifacts__documents")
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_file_identity_pipeline_executes_once_per_call(tmp_path, monkeypatch):
    global _FILE_HASH_EXECUTIONS
    _FILE_HASH_EXECUTIONS = 0
    monkeypatch.setattr(artifact_table_service_module, "_file_content_hash", _count_file_hash)
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "reading.txt"
        source.write_text("21.5 C")

        await container.artifact_table_service.ingest_files(
            str(world.world_id), source, TextFacts()
        )
        assert _FILE_HASH_EXECUTIONS == 1

        await container.artifact_table_service.ingest_files(
            str(world.world_id), source, MustNotRun()
        )
        assert _FILE_HASH_EXECUTIONS == 2
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_same_uri_with_changed_content_creates_new_artifact(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "mutable.txt"
        source.write_text("v1")
        await container.artifact_table_service.ingest_files(
            str(world.world_id), source, TextFacts()
        )

        source.write_text("v2")
        changed = await container.artifact_table_service.ingest_files(
            str(world.world_id), source, TextFacts()
        )

        assert changed.rows_written == 1
        rows = (
            await container.artifact_table_service.read_artifacts(str(world.world_id), "documents")
        ).to_pylist()
        assert {row["text"] for row in rows} == {"v1", "v2"}
        assert len({row["source_uri"] for row in rows}) == 1
        assert len({row["content_hash"] for row in rows}) == 2
        assert len({row["artifact_id"] for row in rows}) == 2
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_direct_pipeline_serializes_duplicate_writes(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        digest = hashlib.sha256(b"same").hexdigest()
        artifacts = daft.from_pydict(
            {
                "source_uri": ["sensor://a/1"],
                "content_hash": [digest],
                "temperature": [21.5],
            }
        )

        receipts = await asyncio.gather(
            container.artifact_table_service.write_artifacts(
                str(world.world_id), "temperatures", artifacts
            ),
            container.artifact_table_service.write_artifacts(
                str(world.world_id), "temperatures", artifacts
            ),
        )

        assert sorted(receipt.rows_written for receipt in receipts) == [0, 1]
        assert all(receipt.sources_matched is None for receipt in receipts)
        receipts_by_rows = {receipt.rows_written: receipt for receipt in receipts}
        assert receipts_by_rows[0].duplicate is None
        assert receipts_by_rows[1].duplicate is False
        rows = await container.artifact_table_service.read_artifacts(
            str(world.world_id), "temperatures"
        )
        assert rows.count_rows() == 1
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_zero_row_direct_write_does_not_create_or_lock_artifact_table(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        empty = daft.from_arrow(
            pa.table(
                {
                    "source_uri": pa.array([], type=pa.string()),
                    "content_hash": pa.array([], type=pa.string()),
                    "value": pa.array([], type=pa.string()),
                }
            )
        )

        receipt = await container.artifact_table_service.write_artifacts(
            str(world.world_id), "periodic", empty
        )

        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert receipt.rows_written == 0
        assert receipt.snapshot_id is None
        assert receipt.duplicate is None
        assert not iceberg.has_table("artifacts__periodic")
        with pytest.raises(KeyError, match="does not exist"):
            await container.artifact_table_service.read_artifacts(str(world.world_id), "periodic")

        nonempty = daft.from_pydict(
            {
                "source_uri": ["sensor://periodic/1"],
                "content_hash": [hashlib.sha256(b"periodic").hexdigest()],
                "value": [1],
            }
        )
        written = await container.artifact_table_service.write_artifacts(
            str(world.world_id), "periodic", nonempty
        )
        assert written.rows_written == 1
        rows = await container.artifact_table_service.read_artifacts(
            str(world.world_id), "periodic"
        )
        assert rows.select("value").to_pylist() == [{"value": 1}]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_direct_pipeline_preserves_string_file_payload(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        artifacts = daft.from_pydict(
            {
                "source_uri": ["sensor://file/1"],
                "content_hash": [hashlib.sha256(b"file").hexdigest()],
                "file": ["reading.csv"],
                "value": [1],
            }
        )

        await container.artifact_table_service.write_artifacts(
            str(world.world_id), "file_payload", artifacts
        )

        rows = await container.artifact_table_service.read_artifacts(
            str(world.world_id), "file_payload"
        )
        assert rows.to_pylist()[0]["file"] == "reading.csv"
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_direct_pipeline_executes_once_at_iceberg_boundary(tmp_path):
    global _PIPELINE_EXECUTIONS
    _PIPELINE_EXECUTIONS = 0
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        digest = hashlib.sha256(b"once").hexdigest()
        artifacts = daft.from_pydict(
            {
                "source_uri": ["sensor://once/1", "sensor://once/2"],
                "content_hash": [digest, digest],
                "value": [1, 2],
            }
        ).with_column("value", _count_pipeline_execution(daft.col("value")))

        receipt = await container.artifact_table_service.write_artifacts(
            str(world.world_id), "counted", artifacts
        )

        assert receipt.rows_written == 2
        assert _PIPELINE_EXECUTIONS == 2
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_artifact_keys_isolate_worlds_in_shared_table(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        first = await _world(container, storage, "first")
        second = await _world(container, storage, "second")
        digest = hashlib.sha256(b"shared").hexdigest()
        artifacts = daft.from_pydict(
            {
                "source_uri": ["sensor://shared/1"],
                "content_hash": [digest],
                "value": [1],
            }
        )

        first_receipt = await container.artifact_table_service.write_artifacts(
            str(first.world_id), "shared", artifacts
        )
        second_receipt = await container.artifact_table_service.write_artifacts(
            str(second.world_id), "shared", artifacts
        )

        assert first_receipt.rows_written == second_receipt.rows_written == 1
        first_rows = await container.artifact_table_service.read_artifacts(
            str(first.world_id), "shared"
        )
        second_rows = await container.artifact_table_service.read_artifacts(
            str(second.world_id), "shared"
        )
        assert {row["world_id"] for row in first_rows.to_pylist()} == {str(first.world_id)}
        assert {row["world_id"] for row in second_rows.to_pylist()} == {str(second.world_id)}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_typed_artifact_visibility_is_fork_local(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        parent = await _world(container, storage, "parent")
        digest = hashlib.sha256(b"shared").hexdigest()
        artifacts = daft.from_pydict(
            {
                "source_uri": ["sensor://shared/1"],
                "content_hash": [digest],
                "value": [1],
            }
        )
        await container.artifact_table_service.write_artifacts(
            str(parent.world_id), "fork_local", artifacts
        )
        await container.simulation_service.step(parent.world_id, RunConfig())
        fork = await container.world_service.fork_world(parent.world_id, name="fork")

        inherited = await container.artifact_table_service.read_artifacts(
            str(fork.world_id), "fork_local"
        )
        assert fork.lineage
        assert inherited.count_rows() == 0

        receipt = await container.artifact_table_service.write_artifacts(
            str(fork.world_id), "fork_local", artifacts
        )
        assert receipt.rows_written == 1
        fork_rows = await container.artifact_table_service.read_artifacts(
            str(fork.world_id), "fork_local"
        )
        assert {row["world_id"] for row in fork_rows.to_pylist()} == {str(fork.world_id)}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_artifact_table_accepts_compatible_schema_in_different_column_order(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        digest = hashlib.sha256(b"compatible").hexdigest()
        first = daft.from_pydict(
            {
                "source_uri": ["sensor://ordered/1"],
                "content_hash": [digest],
                "label": ["first"],
                "value": [1],
            }
        )
        reordered = daft.from_pydict(
            {
                "content_hash": [digest],
                "value": [2],
                "source_uri": ["sensor://ordered/2"],
                "label": ["second"],
            }
        )

        await container.artifact_table_service.write_artifacts(
            str(world.world_id), "ordered", first
        )
        receipt = await container.artifact_table_service.write_artifacts(
            str(world.world_id), "ordered", reordered
        )

        assert receipt.rows_written == 1
        rows = await container.artifact_table_service.read_artifacts(str(world.world_id), "ordered")
        assert {(row["label"], row["value"]) for row in rows.to_pylist()} == {
            ("first", 1),
            ("second", 2),
        }
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_artifact_table_rejects_schema_drift_and_duplicate_keys(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        digest = hashlib.sha256(b"x").hexdigest()
        valid = daft.from_pydict(
            {
                "source_uri": ["sensor://a/1"],
                "content_hash": [digest],
                "value": [1],
            }
        )
        await container.artifact_table_service.write_artifacts(str(world.world_id), "typed", valid)

        drifted = daft.from_pydict(
            {
                "source_uri": ["sensor://a/2"],
                "content_hash": [digest],
                "value": ["one"],
            }
        )
        with pytest.raises(ValueError, match="different typed schema"):
            await container.artifact_table_service.write_artifacts(
                str(world.world_id), "typed", drifted
            )

        duplicated = daft.from_pydict(
            {
                "source_uri": ["sensor://b/1", "sensor://b/1"],
                "content_hash": [digest, digest],
                "value": [1, 2],
            }
        )
        with pytest.raises(ValueError, match="exactly one row"):
            await container.artifact_table_service.write_artifacts(
                str(world.world_id), "duplicated", duplicated
            )
        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert not iceberg.has_table("artifacts__duplicated")
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_file_processor_cannot_rewrite_source_identity(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "source.txt"
        source.write_text("truth")

        with pytest.raises(ValueError, match="preserve source_uri and content_hash"):
            await container.artifact_table_service.ingest_files(
                str(world.world_id), source, RewritesIdentity()
            )
        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert not iceberg.has_table("artifacts__documents")
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_runtime_exposes_file_pipeline_and_read_surfaces(tmp_path):
    storage = _storage(tmp_path)
    source = tmp_path / "runtime.txt"
    source.write_text("runtime file")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("runtime", storage=storage)
        file_receipt = await world.ingest_files(source, TextFacts())
        assert file_receipt.rows_written == 1
        assert (await world.artifacts("documents")).to_pylist()[0]["text"] == "runtime file"

        digest = hashlib.sha256(b"pipeline").hexdigest()
        pipeline = daft.from_pydict(
            {
                "source_uri": ["sensor://pipeline/1"],
                "content_hash": [digest],
                "reading": [7.0],
            }
        )
        assert (await world.write_artifacts("readings", pipeline)).rows_written == 1


@pytest.mark.asyncio
async def test_typed_artifact_tables_require_iceberg(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path, backend=StorageBackend.LANCEDB)
        world = await _world(container, storage)
        artifacts = daft.from_pydict(
            {
                "source_uri": ["sensor://a/1"],
                "content_hash": [hashlib.sha256(b"x").hexdigest()],
                "value": [1],
            }
        )

        with pytest.raises(ValueError, match="StorageBackend.ICEBERG"):
            await container.artifact_table_service.write_artifacts(
                str(world.world_id), "artifacts", artifacts
            )
    finally:
        await container.shutdown()
