# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ArtifactBundleService contracts: extraction, idempotency, recovery, and cold reads."""

import hashlib
import json
import tarfile
import time
from dataclasses import replace
from pathlib import Path
from urllib.parse import unquote, urlparse

import daft
import pytest

from archetype import ArchetypeRuntime, Component
from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
)
from archetype.app.artifacts.bundle_service import (
    _ARTIFACT_INDEX_TABLE,
    CheckpointArtifactSourceResolver,
)
from archetype.app.container import ServiceContainer
from archetype.app.redaction import (
    RedactionPolicyConfig,
    RedactionService,
    SecretQuarantineError,
)
from archetype.app.storage.catalog import (
    ArtifactPublicationConflictError,
    artifact_publication_key,
)
from archetype.core.config import StorageConfig, WorldConfig

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("artifacts.bundle.publication_replay"),
]


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
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(
            WorldConfig(name="artifact-world"), world_storage
        )
        request = _request(world, source)
        empty = await container.artifact_bundle_service.query(request.world_id, request.run_id)
        assert empty.collect().to_pylist() == []
        first = await container.application.publish_artifact_bundle(
            request, storage_config=world_storage
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
        duplicate = await container.application.publish_artifact_bundle(
            request, storage_config=world_storage
        )
        after = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        assert duplicate.duplicate
        assert duplicate.records == first.records
        assert after == before

        queried = await container.application.query_artifact_bundles(
            world.world_id, str(world.run_id), attempt_id="attempt-1"
        )
        queried_rows = queried.collect().to_pylist()
        assert len(queried_rows) == 3
        assert {row["artifact_id"] for row in queried_rows} == {
            record.artifact_id for record in first.records
        }
        results_only = await container.application.query_artifact_bundles(
            world.world_id, str(world.run_id), kinds=["result"]
        )
        assert [row["kind"] for row in results_only.collect().to_pylist()] == ["result"]

        # A lost lease can replay an already-committed Iceberg append. The
        # physical rows are at-least-once, while the service's lazy read path
        # guarantees one logical row per immutable record.
        iceberg = await container.storage_service.get_iceberg_context(artifact_config.index_storage)
        table = iceberg.get_table(_ARTIFACT_INDEX_TABLE)
        await iceberg.append_counted(
            table,
            daft.from_pylist([record.model_dump(mode="python") for record in first.records]),
        )
        deduplicated = await container.application.query_artifact_bundles(
            world.world_id, str(world.run_id), attempt_id="attempt-1"
        )
        assert len(deduplicated.collect().to_pylist()) == 3
        world_id, run_id = str(world.world_id), str(world.run_id)
        await container.application.destroy_world(world.world_id)
    finally:
        await container.shutdown()

    fresh = ServiceContainer(artifact_store_config=artifact_config)
    try:
        cold = await fresh.application.query_artifact_bundles(world_id, run_id)
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
        await container.artifact_bundle_service.publish(
            _request(world, source), storage_config=storage
        )
        with pytest.raises(ArtifactPublicationConflictError):
            await container.artifact_bundle_service.publish(
                _request(world, source, logical_path="different.json"),
                storage_config=storage,
            )
    finally:
        await container.shutdown()


async def test_uploaded_phase_reconciles_without_uploading_again(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 30.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("durable evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        real_index = container.artifact_bundle_service._index_records

        async def fail_index(_records):
            raise RuntimeError("index temporarily unavailable")

        monkeypatch.setattr(container.artifact_bundle_service, "_index_records", fail_index)
        with pytest.raises(RuntimeError, match="index temporarily unavailable"):
            await container.artifact_bundle_service.publish(request, storage_config=storage)

        files_before = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        key = artifact_publication_key(request.world_id, request.run_id, request.idempotency_key)
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "UPLOADED"
        assert publication.records_json != "[]"
        await catalog.fail_artifact_publication(
            request.world_id,
            publication.publication_key,
            publication.claimant,
            "make uploaded row immediately due",
            retry_at=0.0,
        )

        # This is a real durable corrupt row, not a list_due test double. The
        # reconciler must first acquire it, then persist a diagnostic/backoff
        # without preventing the valid publication from recovering.
        _, corrupt = await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id="corrupt-attempt",
            idempotency_key="corrupt-publication",
            request_digest="corrupt-request-digest",
            request_json="{not-json",
            claimant="corrupt-seed",
            retry_until_ms=int(time.time() * 1000) + 60_000,
            lease_seconds=0.0,
        )

        monkeypatch.setattr(container.artifact_bundle_service, "_index_records", real_index)
        reconcile_started = time.time()
        result = await container.artifact_bundle_service.reconcile(
            request.world_id, storage_config=storage
        )
        assert result.examined == 2
        assert result.indexed == 1
        assert result.failed == 1
        corrupt_after = await catalog.get_artifact_publication(
            request.world_id, corrupt.publication_key
        )
        assert corrupt_after is not None
        assert corrupt_after.claimant.startswith("artifact-reconciler-")
        assert "ValidationError" in corrupt_after.last_error
        assert corrupt_after.lease_expires_at >= reconcile_started + 29.0
        files_after = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        assert files_after == files_before
        indexed = await container.artifact_bundle_service.query(request.world_id, request.run_id)
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
            await container.artifact_bundle_service.publish(request, storage_config=storage)

        files_before = sorted(
            path for path in Path(artifact_config.object_uri).rglob("*") if path.is_file()
        )
        receipt = await container.artifact_bundle_service.publish(request, storage_config=storage)
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
            await container.artifact_bundle_service.publish(request, storage_config=storage)
        physical = await container.artifact_bundle_service.query(request.world_id, request.run_id)
        assert len(physical.collect().to_pylist()) == 3

        receipt = await container.artifact_bundle_service.publish(request, storage_config=storage)
        assert receipt.status == "indexed"
        physical = await container.artifact_bundle_service.query(request.world_id, request.run_id)
        assert len(physical.collect().to_pylist()) == 3
    finally:
        await container.shutdown()


async def test_bundle_publication_fails_closed_when_not_configured(tmp_path):
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("evidence")
    container = ServiceContainer()
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        with pytest.raises(RuntimeError, match="artifact publication is not configured"):
            await container.application.publish_artifact_bundle(
                _request(world, source), storage_config=storage
            )
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
        receipt = await world.publish_artifact_bundle(request)
        assert receipt.status == "indexed"
        rows = (await world.artifact_bundles(attempt_id="runtime-attempt")).collect().to_pylist()
        assert len(rows) == 3
        reconciled = await world.reconcile_artifact_bundles()
        assert reconciled.examined == 0
        with pytest.raises(ValueError, match="limit must be at least 1"):
            await world.reconcile_artifact_bundles(limit=0)


async def test_resolver_rejects_unsupported_and_unsafe_sources(tmp_path):
    resolver = CheckpointArtifactSourceResolver()
    unsupported = ArtifactCandidate(
        source_ref="s3://bucket/result.json", logical_path="result.json"
    )
    with pytest.raises(ValueError, match="no artifact source resolver"):
        await resolver.materialize((unsupported,), tmp_path / "unsupported")

    with pytest.raises(ValueError, match="Apple Container artifact refs require"):
        await resolver.materialize(
            (
                ArtifactCandidate(
                    source_ref="apple-container-rootfs://missing-fragment",
                    logical_path="result.json",
                ),
            ),
            tmp_path / "malformed",
        )

    missing_archive = tmp_path / "missing.tar"
    with pytest.raises(FileNotFoundError, match="checkpoint does not exist"):
        await resolver.materialize(
            (
                ArtifactCandidate(
                    source_ref=f"apple-container-rootfs://{missing_archive}#/result.json",
                    logical_path="result.json",
                ),
            ),
            tmp_path / "missing",
        )

    archive = tmp_path / "rootfs.tar"
    with tarfile.open(archive, "w"):
        pass
    with pytest.raises(ValueError, match="unsafe checkpoint member path"):
        await resolver.materialize(
            (
                ArtifactCandidate(
                    source_ref=f"apple-container-rootfs://{archive}#/../secret",
                    logical_path="secret",
                ),
            ),
            tmp_path / "unsafe",
        )


@pytest.mark.parametrize(
    ("source_kind", "recursive", "required", "message"),
    [
        ("missing", False, True, "required artifact source does not exist"),
        ("directory", False, True, "directory but recursive=False"),
        ("empty_directory", True, True, "required artifact directory is empty"),
        ("file", True, True, "file but recursive=True"),
    ],
)
async def test_resolver_enforces_direct_source_shape(
    tmp_path, source_kind, recursive, required, message
):
    source = tmp_path / source_kind
    if source_kind in {"directory", "empty_directory"}:
        source.mkdir()
    elif source_kind == "file":
        source.write_text("value")
    candidate = ArtifactCandidate(
        source_ref=str(source),
        logical_path="artifact",
        recursive=recursive,
        required=required,
    )
    with pytest.raises((FileNotFoundError, IsADirectoryError, NotADirectoryError), match=message):
        await CheckpointArtifactSourceResolver().materialize((candidate,), tmp_path / "output")


async def test_resolver_skips_optional_sources_and_rejects_collisions(tmp_path):
    resolver = CheckpointArtifactSourceResolver()
    optional = ArtifactCandidate(
        source_ref=str(tmp_path / "absent"),
        logical_path="optional",
        required=False,
    )
    assert await resolver.materialize((optional,), tmp_path / "optional-output") == []

    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("first")
    second.write_text("second")
    with pytest.raises(ValueError, match="multiple artifact sources resolve"):
        await resolver.materialize(
            (
                ArtifactCandidate(source_ref=str(first), logical_path="same.txt"),
                ArtifactCandidate(source_ref=str(second), logical_path="same.txt"),
            ),
            tmp_path / "collision-output",
        )


async def test_resolver_reports_required_member_missing_from_checkpoint(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "present.txt").write_text("present")
    archive = tmp_path / "rootfs.tar"
    with tarfile.open(archive, "w") as output:
        output.add(tree, arcname="workspace")

    with pytest.raises(FileNotFoundError, match="absent.txt.*is absent from checkpoint"):
        await CheckpointArtifactSourceResolver().materialize(
            (
                ArtifactCandidate(
                    source_ref=(f"apple-container-rootfs://{archive}#/workspace/absent.txt"),
                    logical_path="absent.txt",
                ),
            ),
            tmp_path / "extracted",
        )


async def test_reconcile_distinguishes_unowned_and_failure_recording_errors(
    tmp_path, monkeypatch, caplog
):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        catalog = container.storage_service.get_control_catalog(storage)
        valid = _request(world, source, idempotency_key="acquire-fails")
        _, valid_row = await catalog.acquire_artifact_publication(
            world_id=valid.world_id,
            run_id=valid.run_id,
            attempt_id=valid.attempt_id,
            idempotency_key=valid.idempotency_key,
            request_digest=valid.digest(),
            request_json=valid.canonical_json(),
            claimant="seed-valid",
            retry_until_ms=int(time.time() * 1000) + 60_000,
            lease_seconds=0.0,
        )
        _, corrupt_row = await catalog.acquire_artifact_publication(
            world_id=valid.world_id,
            run_id=valid.run_id,
            attempt_id="corrupt-attempt",
            idempotency_key="failure-recording-fails",
            request_digest="corrupt",
            request_json="{not-json",
            claimant="seed-corrupt",
            retry_until_ms=int(time.time() * 1000) + 60_000,
            lease_seconds=0.0,
        )
        real_acquire = catalog.acquire_artifact_publication

        async def selective_acquire(**kwargs):
            if kwargs["idempotency_key"] == valid.idempotency_key:
                raise RuntimeError("acquire transport unavailable")
            return await real_acquire(**kwargs)

        async def fail_recording(*args, **kwargs):
            raise RuntimeError("failure catalog unavailable")

        monkeypatch.setattr(catalog, "acquire_artifact_publication", selective_acquire)
        monkeypatch.setattr(catalog, "fail_artifact_publication", fail_recording)
        caplog.set_level("ERROR", logger="archetype.app.artifacts.bundle_service")

        result = await container.artifact_bundle_service.reconcile(
            valid.world_id, storage_config=storage
        )
        assert result.examined == 2 and result.failed == 2
        assert "failed before lease acquisition" in caplog.text
        assert "failed to record retry state" in caplog.text

        valid_after = await catalog.get_artifact_publication(
            valid.world_id, valid_row.publication_key
        )
        corrupt_after = await catalog.get_artifact_publication(
            valid.world_id, corrupt_row.publication_key
        )
        assert valid_after is not None and valid_after.claimant == "seed-valid"
        assert corrupt_after is not None
        assert corrupt_after.claimant.startswith("artifact-reconciler-")
        assert corrupt_after.last_error == ""
    finally:
        await container.shutdown()


async def test_reconcile_expires_pending_publication_after_retry_window(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, idempotency_key="expired-pending")
        catalog = container.storage_service.get_control_catalog(storage)
        _, publication = await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=request.digest(),
            request_json=request.canonical_json(),
            claimant="seed",
            retry_until_ms=1,
            lease_seconds=0.0,
        )
        result = await container.artifact_bundle_service.reconcile(
            request.world_id, storage_config=storage
        )
        assert result.expired == 1 and result.failed == 0
        expired = await catalog.get_artifact_publication(
            request.world_id, publication.publication_key
        )
        assert expired is not None and expired.status == "EXPIRED"
    finally:
        await container.shutdown()


async def test_durable_request_identity_and_digest_are_authenticated(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        catalog = container.storage_service.get_control_catalog(storage)
        _, publication = await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=request.digest(),
            request_json=request.canonical_json(),
            claimant="seed",
            retry_until_ms=int(time.time() * 1000) + 60_000,
        )
        with pytest.raises(ValueError, match="identity does not match"):
            container.artifact_bundle_service._request_from_publication(
                replace(publication, attempt_id="tampered")
            )
        with pytest.raises(ValueError, match="digest does not match"):
            container.artifact_bundle_service._request_from_publication(
                replace(publication, request_digest="tampered")
            )
    finally:
        await container.shutdown()


async def test_secret_metadata_is_rejected_before_the_durable_claim(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    secret = "signed-credential-" + "X" * 32
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source).model_copy(
            update={"checkpoint_ref": "https://provider.invalid/checkpoint?" + "token=" + secret}
        )
        with pytest.raises(SecretQuarantineError) as error:
            await container.artifact_bundle_service.publish(request, storage_config=storage)
        assert secret not in str(error.value)

        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        assert await catalog.get_artifact_publication(request.world_id, key) is None
    finally:
        await container.shutdown()


async def test_credential_source_path_is_rejected_before_the_durable_claim(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / ".codex" / "auth.json"
    source.parent.mkdir()
    source.write_text("otherwise-unrecognized-credential")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, logical_path="innocent-result.json")
        with pytest.raises(SecretQuarantineError, match="credential-file-path"):
            await container.artifact_bundle_service.publish(request, storage_config=storage)

        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        assert await catalog.get_artifact_publication(request.world_id, key) is None
    finally:
        await container.shutdown()


async def test_caller_cannot_select_a_different_redaction_policy(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source).model_copy(
            update={"redaction_policy_id": "archetype-secret-redaction-v0:retired"}
        )
        with pytest.raises(ValueError, match="does not match the active policy"):
            await container.artifact_bundle_service.publish(request, storage_config=storage)
        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        assert await catalog.get_artifact_publication(request.world_id, key) is None
    finally:
        await container.shutdown()


async def test_text_secrets_are_redacted_before_hash_upload_manifest_and_index(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    secret = "codex-refresh-" + "Y" * 32
    source = tmp_path / "session.jsonl"
    source.write_text(f'{{"refresh_token":"{secret}","status":"complete"}}\n')
    original = source.read_bytes()
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, logical_path="sessions/session.jsonl")
        receipt = await container.artifact_bundle_service.publish(
            request,
            storage_config=storage,
        )

        assert source.read_bytes() == original
        payload = next(record for record in receipt.records if record.kind == "result")
        uploaded = _local_uri_path(payload.object_uri).read_bytes()
        assert secret.encode() not in uploaded
        assert b"<redacted:sensitive-assignment>" in uploaded
        assert payload.content_hash == hashlib.sha256(uploaded).hexdigest()

        manifest_record = next(
            record for record in receipt.records if record.kind == "bundle_manifest"
        )
        manifest_bytes = _local_uri_path(manifest_record.object_uri).read_bytes()
        manifest = json.loads(manifest_bytes)
        assert secret.encode() not in manifest_bytes
        assert manifest["redaction"]["policy_id"] == container.redaction_service.policy_id
        assert manifest["redaction"]["status"] == "redacted"
        assert manifest["redaction"]["redaction_count"] == 1
        assert manifest["redaction"]["rule_ids"] == ["sensitive-assignment"]

        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None
        assert secret not in publication.request_json
        durable_request = ArtifactBundleRequest.model_validate_json(publication.request_json)
        assert durable_request.redaction_policy_id == container.redaction_service.policy_id
        assert secret not in publication.records_json
    finally:
        await container.shutdown()


async def test_binary_secret_quarantine_has_no_object_or_index_visibility(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    secret = "ghp_" + "Z" * 36
    source = tmp_path / "opaque.bin"
    source.write_bytes(b"\x00binary" + secret.encode())
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, logical_path="opaque.bin")
        with pytest.raises(SecretQuarantineError) as error:
            await container.artifact_bundle_service.publish(request, storage_config=storage)
        assert secret not in str(error.value)

        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "PENDING"
        assert "github-token" in publication.last_error
        assert secret not in publication.last_error
        object_root = Path(artifact_config.object_uri)
        assert not object_root.exists() or not any(
            path.is_file() for path in object_root.rglob("*")
        )
        indexed = await container.artifact_bundle_service.query(
            request.world_id,
            request.run_id,
        )
        assert indexed.collect().to_pylist() == []
    finally:
        await container.shutdown()


async def test_retry_catalog_redacts_untrusted_failure_diagnostics(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("safe input")
    secret = "sk-ant-api03-" + "U" * 32
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)

        async def fail_with_untrusted_detail(*_args, **_kwargs):
            raise RuntimeError(f"provider failed with {secret}")

        monkeypatch.setattr(
            container.artifact_bundle_service,
            "_upload_bundle",
            fail_with_untrusted_detail,
        )
        with pytest.raises(RuntimeError, match="provider failed"):
            await container.artifact_bundle_service.publish(request, storage_config=storage)

        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "PENDING"
        assert secret not in publication.last_error
        assert "<redacted:anthropic-api-key>" in publication.last_error
    finally:
        await container.shutdown()


async def test_retry_diagnostic_fails_safe_when_the_scanner_itself_errors(
    tmp_path,
    monkeypatch,
):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifacts")
    )
    secret = "sk-ant-api03-" + "V" * 32

    def fail_scanner(*_args, **_kwargs):
        raise RuntimeError(f"scanner failed while handling {secret}")

    monkeypatch.setattr(container.redaction_service, "redact_text", fail_scanner)
    try:
        detail = container.artifact_bundle_service._safe_failure_detail(
            RuntimeError(f"provider returned {secret}")
        )
        assert detail == "RuntimeError: failure detail unavailable"
        assert secret not in detail
    finally:
        await container.shutdown()


async def test_upload_uses_the_controlled_snapshot_when_source_mutates_after_scan(
    tmp_path, monkeypatch
):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("approved-before-scan")
    redaction = RedactionService()
    real_sanitize = redaction.sanitize_file

    def sanitize_then_mutate(*args, **kwargs):
        result = real_sanitize(*args, **kwargs)
        source.write_text("mutated-after-scan")
        return result

    monkeypatch.setattr(redaction, "sanitize_file", sanitize_then_mutate)
    container = ServiceContainer(
        artifact_store_config=artifact_config,
        redaction_service=redaction,
    )
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        receipt = await container.artifact_bundle_service.publish(
            _request(world, source),
            storage_config=storage,
        )
        payload = next(record for record in receipt.records if record.kind == "result")
        assert source.read_text() == "mutated-after-scan"
        assert _local_uri_path(payload.object_uri).read_text() == "approved-before-scan"
    finally:
        await container.shutdown()


async def test_indexed_replay_remains_idempotent_across_scanner_upgrade(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("stable evidence")
    first = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await first.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        original = await first.artifact_bundle_service.publish(request, storage_config=storage)
    finally:
        await first.shutdown()

    upgraded = ServiceContainer(
        artifact_store_config=artifact_config,
        redaction_service=RedactionService(RedactionPolicyConfig(max_archive_members=9)),
    )
    try:
        duplicate = await upgraded.artifact_bundle_service.publish(
            request,
            storage_config=storage,
        )
        assert duplicate.duplicate
        assert duplicate.records == original.records
    finally:
        await upgraded.shutdown()


async def test_uploaded_recovery_does_not_require_retired_scanner(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("already sanitized")
    first = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await first.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)

        async def fail_index(_records):
            raise RuntimeError("index unavailable during old policy")

        monkeypatch.setattr(first.artifact_bundle_service, "_index_records", fail_index)
        with pytest.raises(RuntimeError, match="old policy"):
            await first.artifact_bundle_service.publish(request, storage_config=storage)
    finally:
        await first.shutdown()

    upgraded = ServiceContainer(
        artifact_store_config=artifact_config,
        redaction_service=RedactionService(RedactionPolicyConfig(max_archive_members=9)),
    )
    try:
        recovered = await upgraded.artifact_bundle_service.publish(
            request,
            storage_config=storage,
        )
        assert recovered.status == "indexed"
        assert not recovered.duplicate
    finally:
        await upgraded.shutdown()


async def test_pending_recovery_requires_its_bound_scanner_policy(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    secret = "ghp_" + "P" * 36
    source = tmp_path / "opaque.bin"
    source.write_bytes(b"\x00" + secret.encode())
    first = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await first.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, logical_path="opaque.bin")
        with pytest.raises(SecretQuarantineError):
            await first.artifact_bundle_service.publish(request, storage_config=storage)
    finally:
        await first.shutdown()

    upgraded = ServiceContainer(
        artifact_store_config=artifact_config,
        redaction_service=RedactionService(RedactionPolicyConfig(max_archive_members=9)),
    )
    try:
        with pytest.raises(ValueError, match="unavailable redaction policy") as error:
            await upgraded.artifact_bundle_service.publish(request, storage_config=storage)
        assert secret not in str(error.value)
    finally:
        await upgraded.shutdown()
