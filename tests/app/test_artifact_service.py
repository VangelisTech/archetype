# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ArtifactService contracts: extraction, idempotency, recovery, and cold reads."""

import hashlib
import tarfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import daft
import pytest
from uuid_utils import uuid7

from archetype import ArchetypeRuntime, Component
from archetype.app._catalog import (
    ArtifactPublicationConflictError,
    artifact_publication_key,
)
from archetype.app.artifact_service import (
    _ARTIFACT_INDEX_TABLE,
    CheckpointArtifactSourceResolver,
)
from archetype.app.artifacts import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
)
from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, WorldConfig

pytestmark = pytest.mark.asyncio


class _ArtifactProbe(Component):
    value: int = 0


def _request(
    world,
    source: Path,
    *,
    idempotency_key: str = "publication-1",
    logical_path: str = "results/result.json",
) -> ArtifactBundleRequest:
    return ArtifactBundleRequest(
        world_id=str(world.world_id),
        run_id=str(world.run_id),
        entity_id=7,
        tick=3,
        attempt_id="attempt-1",
        idempotency_key=idempotency_key,
        checkpoint_ref="test-checkpoint://snapshot-1",
        checkpoint_provider="test",
        checkpoint_restorable=True,
        accepted=True,
        retention="run",
        artifacts=(
            ArtifactCandidate(
                source_ref=str(source),
                logical_path=logical_path,
                kind="result",
            ),
        ),
    )


def _local_uri_path(uri: str) -> Path:
    parsed = urlparse(uri)
    assert parsed.scheme == "file"
    return Path(unquote(parsed.path))


async def test_publish_is_idempotent_and_queryable_after_cold_restart(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    world_storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text('{"passed":true}\n')
    operator = ActorCtx(id=uuid7(), roles={"operator"})
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})

    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(
            WorldConfig(name="artifact-world"), world_storage
        )
        request = _request(world, source)
        first = await container.command_service.publish_artifacts(
            operator, world.world_id, request, storage_config=world_storage
        )
        assert first.status == "indexed"
        assert not first.duplicate
        assert len(first.records) == 3  # payload + provider checkpoint + bundle manifest

        payload = next(record for record in first.records if record.kind == "result")
        assert payload.content_hash == hashlib.sha256(source.read_bytes()).hexdigest()
        assert payload.mime_type == "application/json"
        assert payload.size_bytes == source.stat().st_size
        assert _local_uri_path(payload.object_uri).read_bytes() == source.read_bytes()

        before = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        duplicate = await container.command_service.publish_artifacts(
            operator, world.world_id, request, storage_config=world_storage
        )
        after = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        assert duplicate.duplicate
        assert duplicate.records == first.records
        assert after == before

        queried = await container.command_service.query_artifacts(
            viewer, world.world_id, str(world.run_id), attempt_id="attempt-1"
        )
        queried_rows = queried.collect().to_pylist()
        assert len(queried_rows) == 3
        assert {row["artifact_id"] for row in queried_rows} == {
            record.artifact_id for record in first.records
        }

        # A lost lease can replay an already-committed Iceberg append. The
        # physical rows are at-least-once, while the service's lazy read path
        # guarantees one logical row per immutable record.
        iceberg = await container.storage_service.get_iceberg_context(artifact_config.index_storage)
        table = iceberg.get_table(_ARTIFACT_INDEX_TABLE)
        await iceberg.append_counted(
            table,
            daft.from_pylist([record.model_dump(mode="python") for record in first.records]),
        )
        deduplicated = await container.command_service.query_artifacts(
            viewer, world.world_id, str(world.run_id), attempt_id="attempt-1"
        )
        assert len(deduplicated.collect().to_pylist()) == 3
        world_id, run_id = str(world.world_id), str(world.run_id)
        await container.command_service.destroy_world(operator, world.world_id)
    finally:
        await container.shutdown()

    fresh = ServiceContainer(artifact_store_config=artifact_config)
    try:
        cold = await fresh.command_service.query_artifacts(viewer, world_id, run_id)
        assert len(cold.collect().to_pylist()) == 3
    finally:
        await fresh.shutdown()


async def test_same_idempotency_key_with_different_request_conflicts(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        await container.artifact_service.publish(_request(world, source), storage_config=storage)
        with pytest.raises(ArtifactPublicationConflictError):
            await container.artifact_service.publish(
                _request(world, source, logical_path="different.json"),
                storage_config=storage,
            )
    finally:
        await container.shutdown()


async def test_uploaded_phase_reconciles_without_uploading_again(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("durable evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        real_index = container.artifact_service._index_records

        async def fail_index(_records):
            raise RuntimeError("index temporarily unavailable")

        monkeypatch.setattr(container.artifact_service, "_index_records", fail_index)
        with pytest.raises(RuntimeError, match="index temporarily unavailable"):
            await container.artifact_service.publish(request, storage_config=storage)

        files_before = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        key = artifact_publication_key(request.world_id, request.run_id, request.idempotency_key)
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "UPLOADED"
        assert publication.records_json != "[]"

        monkeypatch.setattr(container.artifact_service, "_index_records", real_index)
        result = await container.artifact_service.reconcile(
            request.world_id, storage_config=storage
        )
        assert result.indexed == 1
        files_after = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        assert files_after == files_before
        indexed = await container.artifact_service.query(request.world_id, request.run_id)
        assert len(indexed.collect().to_pylist()) == 3
    finally:
        await container.shutdown()


async def test_pending_retry_reuses_objects_and_original_lifecycle_clock(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("claim-time-stable")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        catalog = container.storage_service.get_control_catalog(storage)
        real_record = catalog.record_artifact_uploads
        calls = 0

        async def crash_before_upload_metadata(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("crash before UPLOADED transition")
            return await real_record(*args, **kwargs)

        monkeypatch.setattr(catalog, "record_artifact_uploads", crash_before_upload_metadata)
        with pytest.raises(RuntimeError, match="before UPLOADED"):
            await container.artifact_service.publish(request, storage_config=storage)

        files_before = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        receipt = await container.artifact_service.publish(request, storage_config=storage)
        files_after = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )

        assert receipt.status == "indexed"
        assert files_after == files_before
        assert len({record.created_at_ms for record in receipt.records}) == 1
    finally:
        await container.shutdown()


async def test_indexed_rows_recover_when_catalog_completion_crashes(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("indexed once")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        catalog = container.storage_service.get_control_catalog(storage)
        real_complete = catalog.complete_artifact_publication
        calls = 0

        async def crash_after_index(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("crash after Iceberg commit")
            return await real_complete(*args, **kwargs)

        monkeypatch.setattr(catalog, "complete_artifact_publication", crash_after_index)
        with pytest.raises(RuntimeError, match="crash after Iceberg commit"):
            await container.artifact_service.publish(request, storage_config=storage)
        physical = await container.artifact_service.query(request.world_id, request.run_id)
        assert len(physical.collect().to_pylist()) == 3

        receipt = await container.artifact_service.publish(request, storage_config=storage)
        assert receipt.status == "indexed"
        physical = await container.artifact_service.query(request.world_id, request.run_id)
        assert len(physical.collect().to_pylist()) == 3
    finally:
        await container.shutdown()


async def test_command_gate_allows_viewer_reads_but_not_publication(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        viewer = ActorCtx(id=uuid7(), roles={"viewer"})
        with pytest.raises(GuardrailError):
            await container.command_service.publish_artifacts(
                viewer,
                world.world_id,
                _request(world, source),
                storage_config=storage,
            )
        empty = await container.command_service.query_artifacts(
            viewer, world.world_id, str(world.run_id)
        )
        assert empty.collect().to_pylist() == []
    finally:
        await container.shutdown()


async def test_apple_rootfs_resolver_extracts_file_and_directory(tmp_path):
    tree = tmp_path / "tree"
    (tree / "workspace/repo/.context").mkdir(parents=True)
    (tree / "workspace/repo/result.json").write_text('{"ok":true}')
    (tree / "workspace/repo/.context/findings.md").write_text("finding")
    archive = tmp_path / "rootfs.tar"
    with tarfile.open(archive, "w") as output:
        output.add(tree / "workspace", arcname="workspace")

    prefix = f"apple-container-rootfs://{archive}#"
    resolver = CheckpointArtifactSourceResolver()
    values = await resolver.materialize(
        (
            ArtifactCandidate(
                source_ref=f"{prefix}/workspace/repo/result.json",
                logical_path="result.json",
                kind="result",
            ),
            ArtifactCandidate(
                source_ref=f"{prefix}/workspace/repo/.context",
                logical_path="context",
                kind="context",
                recursive=True,
            ),
        ),
        tmp_path / "extracted",
    )
    assert {value.logical_path for value in values} == {
        "result.json",
        "context/findings.md",
    }
    assert {value.path.read_text() for value in values} == {'{"ok":true}', "finding"}


async def test_runtime_world_exposes_publish_query_and_reconcile(tmp_path):
    source = tmp_path / "runtime.txt"
    source.write_text("runtime artifact")
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")

    async with ArchetypeRuntime(artifact_store=artifact_config) as runtime:
        world = runtime.world("artifact-runtime", storage=storage)
        entity_id = await world.spawn(_ArtifactProbe(value=1))
        info = await world.info()
        request = ArtifactBundleRequest(
            world_id=str(info.world_id),
            run_id=str(info.run_id),
            entity_id=entity_id,
            tick=0,
            attempt_id="runtime-attempt",
            idempotency_key="runtime-bundle",
            checkpoint_ref="test-checkpoint://runtime",
            checkpoint_provider="test",
            artifacts=(
                ArtifactCandidate(
                    source_ref=str(source),
                    logical_path="runtime.txt",
                ),
            ),
        )
        receipt = await world.publish_artifacts(request)
        assert receipt.status == "indexed"
        rows = (await world.artifacts(attempt_id="runtime-attempt")).collect().to_pylist()
        assert len(rows) == 3
        reconciled = await world.reconcile_artifacts()
        assert reconciled.examined == 0
