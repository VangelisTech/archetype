# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ArtifactBundleService contracts: extraction, idempotency, recovery, and cold reads."""

import hashlib
import json
import shutil
import tarfile
import time
from dataclasses import replace
from pathlib import Path
from urllib.parse import unquote, urlparse

import daft
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from archetype import ArchetypeRuntime, Component, _obs
from archetype.app.artifacts import bundle_service as bundle_service_module
from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactPublicationStatus,
    ArtifactStoreConfig,
    MaterializedArtifact,
)
from archetype.app.artifacts.bundle_service import (
    _ARTIFACT_INDEX_TABLE,
    ArtifactBundleService,
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


class _LegacyProviderResolver:
    """The supported provider contract from before bounded preflight existed."""

    def __init__(self, source: Path) -> None:
        self.source = source
        self.calls = 0

    async def materialize(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        _destination: Path,
    ) -> list[MaterializedArtifact]:
        self.calls += 1
        return [
            MaterializedArtifact(
                path=self.source,
                source_ref=candidate.source_ref,
                logical_path=candidate.logical_path,
                kind=candidate.kind,
            )
            for candidate in candidates
        ]


class _BoundedProviderResolver(_LegacyProviderResolver):
    def __init__(self, source: Path) -> None:
        super().__init__(source)
        self.limits: tuple[int, int] | None = None

    async def materialize(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
    ) -> list[MaterializedArtifact]:
        raise AssertionError("bounded providers must use materialize_bounded")

    async def materialize_bounded(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
        *,
        max_artifact_bytes: int,
        max_bundle_bytes: int,
    ) -> list[MaterializedArtifact]:
        self.limits = (max_artifact_bytes, max_bundle_bytes)
        return await super().materialize(candidates, destination)


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


async def test_span_attributes_use_only_canonical_safe_coordinates(tmp_path):
    request = ArtifactBundleRequest(
        world_id="019bf5a0-4f24-7000-8000-000000000001",
        run_id="019bf5a0-4f24-7000-8000-000000000002",
        entity_id=7,
        tick=3,
        attempt_id="raw-attempt-must-not-be-exported",
        idempotency_key="raw-idempotency-key-must-not-be-exported",
        checkpoint_ref="test-checkpoint://snapshot-1",
        checkpoint_provider="test",
        artifacts=(
            ArtifactCandidate(
                source_ref=str(tmp_path / "result.json"),
                logical_path="results/result.json",
                kind="result",
            ),
        ),
    )

    assert ArtifactBundleService._span_attributes(request) == {
        "archetype.world.id": request.world_id,
        "archetype.run.id": request.run_id,
        "archetype.entity.id": 7,
        "archetype.tick": 3,
    }


async def test_publication_spans_emit_only_canonical_safe_coordinates(tmp_path, monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(_obs, "_tracer", provider.get_tracer("archetype"))

    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    world_storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text('{"passed":true}\n')
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(
            WorldConfig(name="artifact-span-world"), world_storage
        )
        request = _request(
            world,
            source,
            idempotency_key="raw-idempotency-must-not-be-exported",
        ).model_copy(update={"attempt_id": "raw-attempt-must-not-be-exported"})

        receipt = await container.artifact_bundle_service.publish(
            request,
            storage_config=world_storage,
        )
    finally:
        await container.shutdown()

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert set(spans) == {"artifact.publish", "artifact.upload", "artifact.index"}
    coordinates = {
        "archetype.world.id": request.world_id,
        "archetype.run.id": request.run_id,
        "archetype.entity.id": request.entity_id,
        "archetype.tick": request.tick,
    }
    assert dict(spans["artifact.publish"].attributes or {}) == coordinates
    assert dict(spans["artifact.upload"].attributes or {}) == {
        **coordinates,
        "archetype.artifact.bundle.digest": receipt.bundle_id,
    }
    assert dict(spans["artifact.index"].attributes or {}) == {
        **coordinates,
        "archetype.artifact.bundle.digest": receipt.bundle_id,
        "archetype.artifact.count": len(receipt.records),
    }
    exported = repr([span.attributes for span in spans.values()])
    assert request.attempt_id not in exported
    assert request.idempotency_key not in exported


async def test_prepare_binds_and_authenticates_request_without_any_io(tmp_path):
    class _ForbiddenIO:
        def __getattr__(self, name):
            raise AssertionError(f"prepare attempted forbidden I/O through {name}")

    object_root = tmp_path / "objects-that-must-not-exist"
    config = ArtifactStoreConfig.local(tmp_path / "artifact-config").model_copy(
        update={"object_uri": object_root}
    )
    service = ArtifactBundleService(
        _ForbiddenIO(),
        _ForbiddenIO(),
        config,
        _ForbiddenIO(),
        redaction_service=RedactionService(),
    )
    request = ArtifactBundleRequest(
        world_id="world-1",
        run_id="run-1",
        entity_id=7,
        tick=3,
        attempt_id="attempt-1",
        idempotency_key="publication-1",
        checkpoint_ref="test-checkpoint://snapshot-1",
        checkpoint_provider="test",
        artifacts=(
            ArtifactCandidate(
                source_ref="forbidden-source://snapshot/result.json",
                logical_path="result.json",
            ),
        ),
    )

    prepared = service.prepare(request)

    bound = ArtifactBundleRequest.model_validate_json(prepared.request_json)
    assert bound.redaction_policy_id == service._redaction_service.policy_id
    assert prepared.request_digest == bound.request_digest()
    assert prepared.producer_digest == request.producer_digest()
    assert prepared.publication_key == artifact_publication_key(
        request.world_id,
        request.run_id,
        request.idempotency_key,
    )
    assert not object_root.exists()

    tampered = prepared.model_copy(update={"producer_digest": "f" * 64})
    with pytest.raises(ValueError, match="producer_digest does not authenticate"):
        await service.publish_prepared(tampered)
    assert not object_root.exists()


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
        prepared = container.artifact_bundle_service.prepare(request)
        assert first.bundle_id == prepared.publication_key
        assert first.request_digest == prepared.request_digest
        assert first.producer_digest == prepared.producer_digest
        assert first.redaction_policy_id == prepared.redaction_policy_id
        assert first.manifest_uri
        assert first.index_snapshot_id > 0
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


@pytest.mark.parametrize(
    ("raw_snapshot_id", "error"),
    [
        pytest.param(True, "non-integer snapshot identity", id="bool"),
        pytest.param(1.0, "non-integer snapshot identity", id="float"),
        pytest.param("1", "non-integer snapshot identity", id="numeric-string"),
        pytest.param(None, "non-integer snapshot identity", id="none"),
        pytest.param(0, "outside the positive signed 64-bit range", id="zero"),
        pytest.param(
            1 << 63,
            "outside the positive signed 64-bit range",
            id="above-signed-64",
        ),
    ],
)
async def test_raw_iceberg_snapshot_identity_must_be_exact_before_catalog_indexing(
    tmp_path,
    monkeypatch,
    raw_snapshot_id,
    error,
):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text('{"passed":true}\n')
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        iceberg = await container.storage_service.get_iceberg_context(artifact_config.index_storage)
        monkeypatch.setattr(
            type(iceberg),
            "current_snapshot_id",
            lambda _self, _table: raw_snapshot_id,
        )

        with pytest.raises(ValueError, match=error):
            await container.artifact_bundle_service.publish(request, storage_config=storage)

        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(
            request.world_id,
            artifact_publication_key(
                request.world_id,
                request.run_id,
                request.idempotency_key,
            ),
        )
        assert publication is not None
        assert publication.status == "UPLOADED"
        assert publication.index_snapshot_id == 0
        assert publication.completed_at is None
    finally:
        await container.shutdown()


async def test_raw_iceberg_snapshot_identity_accepts_positive_signed_64_max(
    tmp_path,
    monkeypatch,
):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text('{"passed":true}\n')
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        iceberg = await container.storage_service.get_iceberg_context(artifact_config.index_storage)
        max_snapshot_id = (1 << 63) - 1
        monkeypatch.setattr(
            type(iceberg),
            "current_snapshot_id",
            lambda _self, _table: max_snapshot_id,
        )

        receipt = await container.artifact_bundle_service.publish(
            request,
            storage_config=storage,
        )

        assert receipt.status is ArtifactPublicationStatus.INDEXED
        assert receipt.index_snapshot_id == max_snapshot_id
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(
            request.world_id,
            artifact_publication_key(
                request.world_id,
                request.run_id,
                request.idempotency_key,
            ),
        )
        assert publication is not None
        assert publication.status == "INDEXED"
        assert publication.index_snapshot_id == max_snapshot_id
    finally:
        await container.shutdown()


async def test_recursive_candidate_records_remain_bound_to_declared_children(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "context"
    (source / "nested").mkdir(parents=True)
    (source / "one.txt").write_text("one")
    (source / "nested" / "two.txt").write_text("two")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source).model_copy(
            update={
                "artifacts": (
                    ArtifactCandidate(
                        source_ref=str(source),
                        logical_path="context",
                        kind="context",
                        recursive=True,
                    ),
                )
            }
        )

        receipt = await container.artifact_bundle_service.publish(
            request,
            storage_config=storage,
        )

        portable = {
            record.logical_path: record for record in receipt.records if record.kind == "context"
        }
        assert set(portable) == {"context/one.txt", "context/nested/two.txt"}
        assert portable["context/one.txt"].source_ref == f"{source}/one.txt"
        assert portable["context/nested/two.txt"].source_ref == f"{source}/nested/two.txt"
    finally:
        await container.shutdown()


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

        original = container.artifact_bundle_service.prepare(_request(world, source))
        changed = container.artifact_bundle_service.prepare(
            _request(world, source, logical_path="different.json")
        )
        assert changed.publication_key == original.publication_key
        assert changed.request_digest != original.request_digest
        assert changed.producer_digest != original.producer_digest
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


async def test_uploaded_recovery_rejects_records_from_another_request(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("durable evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        other = await container.artifact_bundle_service.publish(
            _request(world, source, idempotency_key="other-publication"),
            storage_config=storage,
        )
        target = _request(world, source, idempotency_key="target-publication")

        async def fail_index(_records):
            raise RuntimeError("leave target uploaded")

        monkeypatch.setattr(container.artifact_bundle_service, "_index_records", fail_index)
        with pytest.raises(RuntimeError, match="leave target uploaded"):
            await container.artifact_bundle_service.publish(target, storage_config=storage)

        catalog = container.storage_service.get_control_catalog(storage)
        key = artifact_publication_key(
            target.world_id,
            target.run_id,
            target.idempotency_key,
        )
        publication = await catalog.get_artifact_publication(target.world_id, key)
        assert publication is not None and publication.status == "UPLOADED"
        corrupt = replace(
            publication,
            records_json=json.dumps(
                [record.model_dump(mode="json") for record in other.records],
                sort_keys=True,
                separators=(",", ":"),
            ),
            manifest_uri=other.manifest_uri,
        )
        durable_request = container.artifact_bundle_service._request_from_publication(
            corrupt,
            require_policy=False,
        )

        with pytest.raises(ValueError, match="does not match its durable request"):
            await container.artifact_bundle_service._resume(
                durable_request,
                corrupt,
                corrupt.claimant,
                catalog,
            )
    finally:
        await container.shutdown()


async def test_misbound_resolver_records_leave_publication_pending(tmp_path):
    class _MisboundResolver:
        async def materialize(self, candidates, _destination):
            candidate = candidates[0]
            return [
                MaterializedArtifact(
                    path=source,
                    source_ref=str(source) + ".different",
                    logical_path=candidate.logical_path,
                    kind=candidate.kind,
                )
            ]

    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("durable evidence")
    container = ServiceContainer(
        artifact_store_config=artifact_config,
        artifact_source_resolver=_MisboundResolver(),
    )
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, idempotency_key="misbound-resolver")

        with pytest.raises(ValueError, match="does not match exactly one declared candidate"):
            await container.artifact_bundle_service.publish(request, storage_config=storage)

        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(
            request.world_id,
            artifact_publication_key(
                request.world_id,
                request.run_id,
                request.idempotency_key,
            ),
        )
        assert publication is not None
        assert publication.status == "PENDING"
        assert publication.records_json == "[]"
        assert publication.manifest_uri == ""
    finally:
        await container.shutdown()


async def test_indexed_receipt_rejects_manifest_uri_mismatch(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("durable evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        await container.artifact_bundle_service.publish(request, storage_config=storage)
        catalog = container.storage_service.get_control_catalog(storage)
        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "INDEXED"

        with pytest.raises(ValueError, match="manifest object_uri does not match"):
            container.artifact_bundle_service._receipt(
                replace(publication, manifest_uri="file:///different/manifest.json"),
                duplicate=True,
            )
    finally:
        await container.shutdown()


async def test_indexed_receipt_rejects_object_uri_outside_content_address(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("durable evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        await container.artifact_bundle_service.publish(request, storage_config=storage)
        catalog = container.storage_service.get_control_catalog(storage)
        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "INDEXED"
        records = json.loads(publication.records_json)
        manifest_uri = next(
            record["object_uri"] for record in records if record["kind"] == "bundle_manifest"
        )
        payload = next(record for record in records if record["kind"] == "result")
        payload["object_uri"] = manifest_uri
        corrupt = replace(
            publication,
            records_json=json.dumps(records, sort_keys=True, separators=(",", ":")),
        )

        with pytest.raises(ValueError, match="outside its content-addressed folder"):
            container.artifact_bundle_service._receipt(corrupt, duplicate=True)
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


async def test_partial_payload_upload_is_verified_and_replaced_before_index(
    tmp_path,
    monkeypatch,
):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_bytes(b"durable payload that must survive a transport crash")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        service = container.artifact_bundle_service
        real_upload_files = service._upload_files
        partial_path: Path | None = None

        def crash_during_payload_upload(rows):
            nonlocal partial_path
            assert len(rows) == 1
            destination = _local_uri_path(rows[0]["destination"])
            destination.parent.mkdir(parents=True, exist_ok=True)
            partial_path = destination
            partial_path.write_bytes(source.read_bytes()[:7])
            raise RuntimeError("transport crashed during payload upload")

        monkeypatch.setattr(service, "_upload_files", crash_during_payload_upload)
        with pytest.raises(RuntimeError, match="during payload upload"):
            await service.publish(request, storage_config=storage)

        assert partial_path is not None and partial_path.read_bytes() == source.read_bytes()[:7]
        partial_destination = partial_path
        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        catalog = container.storage_service.get_control_catalog(storage)
        publication = await catalog.get_artifact_publication(request.world_id, key)
        assert publication is not None and publication.status == "PENDING"
        assert (await service.query(request.world_id, request.run_id)).collect().to_pylist() == []

        real_existing_object = service._existing_object
        rejected_partial = False

        def observe_existing_object(folder, *, content_hash, size_bytes):
            nonlocal rejected_partial
            result = real_existing_object(
                folder,
                content_hash=content_hash,
                size_bytes=size_bytes,
            )
            if _local_uri_path(folder) == partial_destination:
                rejected_partial = True
                assert result == ""
            return result

        monkeypatch.setattr(service, "_existing_object", observe_existing_object)
        monkeypatch.setattr(service, "_upload_files", real_upload_files)
        receipt = await service.publish(request, storage_config=storage)

        payload = next(record for record in receipt.records if record.kind == "result")
        indexed_bytes = _local_uri_path(payload.object_uri).read_bytes()
        assert rejected_partial
        assert receipt.status == "indexed"
        assert indexed_bytes == source.read_bytes()
        assert len(indexed_bytes) == payload.size_bytes
        assert hashlib.sha256(indexed_bytes).hexdigest() == payload.content_hash
    finally:
        await container.shutdown()


async def test_corrupt_manifest_retry_reuploads_manifest_before_index(tmp_path, monkeypatch):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("manifest integrity must be checked")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        service = container.artifact_bundle_service
        catalog = container.storage_service.get_control_catalog(storage)
        real_record_uploads = catalog.record_artifact_uploads
        record_calls = 0

        async def crash_before_uploaded_transition(*args, **kwargs):
            nonlocal record_calls
            record_calls += 1
            if record_calls == 1:
                raise RuntimeError("crash after manifest upload")
            return await real_record_uploads(*args, **kwargs)

        monkeypatch.setattr(
            catalog,
            "record_artifact_uploads",
            crash_before_uploaded_transition,
        )
        with pytest.raises(RuntimeError, match="after manifest upload"):
            await service.publish(request, storage_config=storage)

        key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        manifest_path: Path | None = None
        for candidate in Path(artifact_config.object_uri).rglob("*"):
            if not candidate.is_file():
                continue
            try:
                value = json.loads(candidate.read_bytes())
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if value.get("schema_version") == 1 and value.get("bundle_id") == key:
                manifest_path = candidate
                break
        assert manifest_path is not None
        manifest_destination = manifest_path.parent
        original_manifest = manifest_path.read_bytes()
        corrupted_manifest = bytearray(original_manifest)
        corrupted_manifest[len(corrupted_manifest) // 2] ^= 1
        manifest_path.write_bytes(corrupted_manifest)

        real_existing_object = service._existing_object
        rejected_manifest = False

        def observe_existing_object(folder, *, content_hash, size_bytes):
            nonlocal rejected_manifest
            result = real_existing_object(
                folder,
                content_hash=content_hash,
                size_bytes=size_bytes,
            )
            if _local_uri_path(folder) == manifest_destination:
                rejected_manifest = True
                assert result == ""
            return result

        def forbid_payload_reupload(_rows):
            raise AssertionError("valid payload object should be reused")

        real_upload_bytes = service._upload_bytes
        manifest_uploads = 0

        def count_manifest_upload(value, destination):
            nonlocal manifest_uploads
            manifest_uploads += 1
            return real_upload_bytes(value, destination)

        monkeypatch.setattr(service, "_existing_object", observe_existing_object)
        monkeypatch.setattr(service, "_upload_files", forbid_payload_reupload)
        monkeypatch.setattr(service, "_upload_bytes", count_manifest_upload)
        receipt = await service.publish(request, storage_config=storage)

        manifest = next(record for record in receipt.records if record.kind == "bundle_manifest")
        indexed_bytes = _local_uri_path(manifest.object_uri).read_bytes()
        assert rejected_manifest
        assert manifest_uploads == 1
        assert indexed_bytes == original_manifest
        assert len(indexed_bytes) == manifest.size_bytes
        assert hashlib.sha256(indexed_bytes).hexdigest() == manifest.content_hash
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


async def test_legacy_provider_resolver_contract_remains_supported(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "provider-result.txt"
    source.write_text("legacy provider evidence")
    resolver = _LegacyProviderResolver(source)
    container = ServiceContainer(
        artifact_store_config=artifact_config,
        artifact_source_resolver=resolver,
    )
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        receipt = await container.artifact_bundle_service.publish(
            _request(world, source), storage_config=storage
        )
        assert receipt.status == "indexed"
        assert resolver.calls == 1
    finally:
        await container.shutdown()


async def test_legacy_provider_resolver_still_obeys_post_materialization_limits(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"max_artifact_bytes": 3, "max_bundle_bytes": 4}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "provider-result.bin"
    source.write_bytes(b"1234")
    resolver = _LegacyProviderResolver(source)
    container = ServiceContainer(
        artifact_store_config=artifact_config,
        artifact_source_resolver=resolver,
    )
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        with pytest.raises(ValueError, match="result.json.*4 bytes; limit is 3"):
            await container.artifact_bundle_service.publish(
                _request(world, source), storage_config=storage
            )
        assert resolver.calls == 1
    finally:
        await container.shutdown()


async def test_bounded_provider_receives_configured_materialization_limits(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"max_artifact_bytes": 4, "max_bundle_bytes": 8}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "provider-result.bin"
    source.write_bytes(b"1234")
    resolver = _BoundedProviderResolver(source)
    container = ServiceContainer(
        artifact_store_config=artifact_config,
        artifact_source_resolver=resolver,
    )
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        receipt = await container.artifact_bundle_service.publish(
            _request(world, source), storage_config=storage
        )
        assert receipt.status == "indexed"
        assert resolver.calls == 1
        assert resolver.limits == (4, 8)
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


async def test_apple_rootfs_resolver_rejects_member_before_copying(tmp_path, monkeypatch):
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "oversized.bin").write_bytes(b"12345")
    archive = tmp_path / "rootfs.tar"
    with tarfile.open(archive, "w") as output:
        output.add(tree / "oversized.bin", arcname="workspace/oversized.bin")

    copied = False

    def unexpected_copy(*args, **kwargs):
        nonlocal copied
        copied = True
        raise AssertionError("oversized checkpoint member must not be copied")

    monkeypatch.setattr(
        "archetype.app.artifacts.bundle_service.shutil.copyfileobj", unexpected_copy
    )
    candidate = ArtifactCandidate(
        source_ref=f"apple-container-rootfs://{archive}#/workspace/oversized.bin",
        logical_path="oversized.bin",
    )
    with pytest.raises(ValueError, match="oversized.bin.*5 bytes; limit is 4"):
        await CheckpointArtifactSourceResolver().materialize_bounded(
            (candidate,),
            tmp_path / "extracted",
            max_artifact_bytes=4,
            max_bundle_bytes=8,
        )
    assert not copied


async def test_apple_rootfs_resolver_bounds_recursive_cumulative_copy(tmp_path, monkeypatch):
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "first.bin").write_bytes(b"123")
    (tree / "second.bin").write_bytes(b"456")
    archive = tmp_path / "rootfs.tar"
    with tarfile.open(archive, "w") as output:
        output.add(tree, arcname="workspace")

    real_copy = shutil.copyfileobj
    copies = 0

    def counted_copy(*args, **kwargs):
        nonlocal copies
        copies += 1
        return real_copy(*args, **kwargs)

    monkeypatch.setattr("archetype.app.artifacts.bundle_service.shutil.copyfileobj", counted_copy)
    candidate = ArtifactCandidate(
        source_ref=f"apple-container-rootfs://{archive}#/workspace",
        logical_path="workspace",
        recursive=True,
    )
    with pytest.raises(ValueError, match="bundle would be at least 6 bytes; limit is 5"):
        await CheckpointArtifactSourceResolver().materialize_bounded(
            (candidate,),
            tmp_path / "extracted",
            max_artifact_bytes=3,
            max_bundle_bytes=5,
        )
    assert copies == 1


@pytest.mark.parametrize(
    ("max_artifact_bytes", "max_bundle_bytes", "message"),
    [
        (0, 1, "max_artifact_bytes must be positive"),
        (2, 1, "max_bundle_bytes must be >= max_artifact_bytes"),
    ],
)
async def test_artifact_resolver_rejects_invalid_materialization_limits(
    tmp_path, max_artifact_bytes, max_bundle_bytes, message
):
    with pytest.raises(ValueError, match=message):
        await CheckpointArtifactSourceResolver().materialize_bounded(
            (),
            tmp_path / "extracted",
            max_artifact_bytes=max_artifact_bytes,
            max_bundle_bytes=max_bundle_bytes,
        )


async def test_direct_artifact_limits_are_checked_before_archive_extraction(tmp_path):
    oversized = tmp_path / "oversized.bin"
    oversized.write_bytes(b"1234")
    oversized_candidate = ArtifactCandidate(
        source_ref=str(oversized),
        logical_path="oversized.bin",
    )
    resolver = CheckpointArtifactSourceResolver()
    with pytest.raises(ValueError, match="oversized.bin.*4 bytes; limit is 3"):
        await resolver.materialize_bounded(
            (oversized_candidate,),
            tmp_path / "oversized-output",
            max_artifact_bytes=3,
            max_bundle_bytes=5,
        )

    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"123")
    second.write_bytes(b"456")
    with pytest.raises(ValueError, match="bundle is at least 6 bytes; limit is 5"):
        await resolver.materialize_bounded(
            (
                ArtifactCandidate(source_ref=str(first), logical_path="first.bin"),
                ArtifactCandidate(source_ref=str(second), logical_path="second.bin"),
            ),
            tmp_path / "bundle-output",
            max_artifact_bytes=3,
            max_bundle_bytes=5,
        )


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


async def test_reconcile_counts_expiry_crossing_inside_resume(tmp_path, monkeypatch):
    class _Clock:
        def __init__(self, start: float) -> None:
            self.values = iter((start + 1.0, start + 1.0, start + 200.0))

        def time(self) -> float:
            return next(self.values)

    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, idempotency_key="expires-inside-resume")
        prepared = container.artifact_bundle_service.prepare(request)
        catalog = container.storage_service.get_control_catalog(storage)
        started_at = time.time()
        await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant="seed",
            retry_until_ms=int((started_at + 150.0) * 1000),
            lease_seconds=0.0,
        )
        monkeypatch.setattr(bundle_service_module, "time", _Clock(started_at))

        result = await container.artifact_bundle_service.reconcile(
            request.world_id,
            storage_config=storage,
        )

        assert result.examined == 1
        assert result.expired == 1
        assert result.indexed == 0
        assert result.failed == 0
        publication = await catalog.get_artifact_publication(
            request.world_id,
            prepared.publication_key,
        )
        assert publication is not None and publication.status == "EXPIRED"
    finally:
        await container.shutdown()


async def test_duplicate_expired_publication_returns_typed_receipt(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, idempotency_key="expired-duplicate")
        prepared = container.artifact_bundle_service.prepare(request)
        catalog = container.storage_service.get_control_catalog(storage)
        _, publication = await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant="seed",
            retry_until_ms=int(time.time() * 1000) + 60_000,
        )
        await catalog.expire_artifact_publication(
            request.world_id,
            publication.publication_key,
            "seed",
            "checkpoint expired",
        )

        receipt = await container.artifact_bundle_service.publish_prepared(
            prepared,
            storage_config=storage,
        )

        assert receipt.status is ArtifactPublicationStatus.EXPIRED
        assert receipt.duplicate
        assert receipt.bundle_id == prepared.publication_key
        assert receipt.request_digest == prepared.request_digest
        assert receipt.producer_digest == prepared.producer_digest
        assert receipt.redaction_policy_id == prepared.redaction_policy_id
        assert receipt.records == ()
    finally:
        await container.shutdown()


async def test_expiry_during_resume_returns_typed_receipt(tmp_path):
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.json"
    source.write_text("{}")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source, idempotency_key="expires-during-resume")
        prepared = container.artifact_bundle_service.prepare(request)
        catalog = container.storage_service.get_control_catalog(storage)
        await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant="seed",
            retry_until_ms=1,
            lease_seconds=0.0,
        )

        receipt = await container.artifact_bundle_service.publish_prepared(
            prepared,
            storage_config=storage,
        )

        assert receipt.status is ArtifactPublicationStatus.EXPIRED
        assert not receipt.duplicate
        expired = await catalog.get_artifact_publication(
            request.world_id,
            prepared.publication_key,
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


async def test_reconcile_rechecks_changed_secret_bearing_object_root_before_io(tmp_path):
    safe_config = ArtifactStoreConfig.local(tmp_path / "artifacts").model_copy(
        update={"retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    source = tmp_path / "result.txt"
    source.write_text("safe durable recovery input")
    first = ServiceContainer(artifact_store_config=safe_config)
    try:
        world = await first.world_service.create_world(WorldConfig(name="w"), storage)
        request = _request(world, source)
        prepared = first.artifact_bundle_service.prepare(request)
        catalog = first.storage_service.get_control_catalog(storage)
        outcome, publication = await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant="seed-before-config-drift",
            retry_until_ms=int(time.time() * 1000) + 60_000,
            lease_seconds=0.0,
        )
        assert outcome == "acquired" and publication.status == "PENDING"
        world_id = request.world_id
        run_id = request.run_id
        publication_key = publication.publication_key
    finally:
        await first.shutdown()

    secret = "ghp_" + "R" * 36
    unsafe_root = tmp_path / secret
    drifted_config = safe_config.model_copy(update={"object_uri": unsafe_root})
    cold = ServiceContainer(artifact_store_config=drifted_config)
    try:
        result = await cold.artifact_bundle_service.reconcile(
            world_id,
            storage_config=storage,
        )

        assert result.examined == 1
        assert result.indexed == 0
        assert result.failed == 1
        catalog = cold.storage_service.get_control_catalog(storage)
        failed = await catalog.get_artifact_publication(world_id, publication_key)
        assert failed is not None and failed.status == "PENDING"
        assert "github-token" in failed.last_error
        assert secret not in failed.last_error
        assert not unsafe_root.exists()
        indexed = await cold.artifact_bundle_service.query(world_id, run_id)
        assert indexed.collect().to_pylist() == []
    finally:
        await cold.shutdown()


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
