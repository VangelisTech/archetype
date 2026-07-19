# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validation contracts for portable artifact requests and index rows."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactIndexRecord,
    ArtifactPublicationStatus,
    ArtifactPublishReceipt,
    ArtifactStoreConfig,
    PreparedArtifactBundleRequest,
)
from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.storage.catalog import artifact_publication_key
from archetype.core.config import StorageConfig

pytestmark = pytest.mark.contract("artifacts.bundle.publication_replay")


def _request_data() -> dict:
    return {
        "world_id": "world-1",
        "run_id": "run-1",
        "tick": 0,
        "attempt_id": "attempt-1",
        "idempotency_key": "bundle-1",
        "checkpoint_ref": "checkpoint://one",
        "checkpoint_provider": "test",
        "artifacts": (ArtifactCandidate(source_ref="result.json", logical_path="result.json"),),
    }


def _index_data() -> dict:
    return {
        "artifact_id": "a" * 64,
        "bundle_id": "b" * 64,
        "world_id": "world-1",
        "run_id": "run-1",
        "entity_id": 7,
        "tick": 0,
        "attempt_id": "attempt-1",
        "idempotency_key": "bundle-1",
        "kind": "result",
        "logical_path": "result.json",
        "source_ref": "file:///result.json",
        "object_uri": "s3://bucket/result.json",
        "storage_kind": "object",
        "content_hash": "c" * 64,
        "size_bytes": 2,
        "mime_type": "application/json",
        "checkpoint_provider": "test",
        "checkpoint_ref": "checkpoint://one",
        "restorable": False,
        "accepted": True,
        "retention": "run",
        "created_at_ms": 10,
        "expires_at_ms": 20,
    }


@pytest.mark.parametrize("field", ["source_ref", "kind"])
def test_candidate_rejects_blank_identity(field):
    values = {"source_ref": "result.json", "logical_path": "result.json", "kind": "result"}
    values[field] = "  "
    with pytest.raises(ValidationError, match="must not be empty"):
        ArtifactCandidate(**values)


@pytest.mark.parametrize("logical_path", ["", "../secret", "a/../../secret"])
def test_candidate_rejects_unsafe_logical_path(logical_path):
    with pytest.raises(ValidationError, match="portable relative path"):
        ArtifactCandidate(source_ref="result.json", logical_path=logical_path)


def test_candidate_normalizes_platform_separators():
    candidate = ArtifactCandidate(source_ref="result.json", logical_path=r"folder\result.json")
    assert candidate.logical_path == "folder/result.json"


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"artifacts": ()}, "requires at least one candidate"),
        (
            {
                "artifacts": (
                    ArtifactCandidate(source_ref="one", logical_path="same"),
                    ArtifactCandidate(source_ref="two", logical_path="same"),
                )
            },
            "logical paths must be unique",
        ),
        (
            {"checkpoint_created_at_ms": 20, "checkpoint_expires_at_ms": 10},
            "expiration must be after checkpoint creation",
        ),
    ],
)
def test_bundle_request_rejects_ambiguous_or_invalid_lifecycle(update, message):
    values = _request_data()
    values.update(update)
    with pytest.raises(ValidationError, match=message):
        ArtifactBundleRequest(**values)


def test_bundle_request_identity_is_canonical_across_artifact_order():
    values = _request_data()
    values["artifacts"] = (
        ArtifactCandidate(source_ref="b", logical_path="b", kind="two"),
        ArtifactCandidate(source_ref="a", logical_path="a", kind="one"),
    )
    forward = ArtifactBundleRequest(**values)
    values["artifacts"] = tuple(reversed(values["artifacts"]))
    reverse = ArtifactBundleRequest(**values)
    assert forward.canonical_json() == reverse.canonical_json()
    assert forward.digest() == reverse.digest()


def test_bundle_request_policy_identity_is_canonical_and_bound_at_service_time():
    request = ArtifactBundleRequest(**_request_data())
    assert request.redaction_policy_id == ""
    bound = request.model_copy(
        update={"redaction_policy_id": "archetype-secret-redaction-v1:" + "a" * 64}
    )
    assert bound.redaction_policy_id in bound.canonical_json()
    assert bound.digest() == request.digest()
    assert bound.producer_digest() == request.producer_digest()
    assert bound.request_digest() != request.request_digest()


def test_publication_status_is_a_string_preserving_typed_enum():
    assert ArtifactPublicationStatus.INDEXED == "indexed"
    assert ArtifactPublicationStatus("uploaded") is ArtifactPublicationStatus.UPLOADED


def test_publish_receipt_requires_exact_status_bound_signed_64_bit_snapshot():
    values = {
        "bundle_id": "b" * 64,
        "world_id": "world-1",
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "status": ArtifactPublicationStatus.INDEXED,
        "manifest_uri": "s3://artifacts/manifest.json",
        "index_snapshot_id": MAX_ICEBERG_SNAPSHOT_ID,
        "request_digest": "c" * 64,
        "producer_digest": "d" * 64,
        "redaction_policy_id": "policy-v1",
    }
    assert ArtifactPublishReceipt(**values).index_snapshot_id == MAX_ICEBERG_SNAPSHOT_ID
    for invalid in (MAX_ICEBERG_SNAPSHOT_ID + 1, 1.5, True):
        with pytest.raises(ValidationError, match="index_snapshot_id|snapshot"):
            ArtifactPublishReceipt(**{**values, "index_snapshot_id": invalid})

    expired = ArtifactPublishReceipt(
        **{
            **values,
            "status": ArtifactPublicationStatus.EXPIRED,
            "manifest_uri": "",
            "index_snapshot_id": 0,
        }
    )
    assert expired.index_snapshot_id == 0


def test_prepared_request_authenticates_exact_policy_producer_and_publication_identity():
    request = ArtifactBundleRequest(**_request_data()).model_copy(
        update={"redaction_policy_id": "policy:v1"}
    )
    prepared = PreparedArtifactBundleRequest(
        request_json=request.canonical_json(),
        request_digest=request.request_digest(),
        publication_key=artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        ),
        producer_digest=request.producer_digest(),
        redaction_policy_id=request.redaction_policy_id,
    )

    assert prepared.request_digest != prepared.producer_digest
    with pytest.raises(ValidationError, match="request_digest does not authenticate"):
        PreparedArtifactBundleRequest(
            **prepared.model_dump(exclude={"request_digest"}),
            request_digest="b" * 64,
        )
    with pytest.raises(ValidationError, match="publication_key does not match"):
        PreparedArtifactBundleRequest(
            **prepared.model_dump(exclude={"publication_key"}),
            publication_key="a" * 64,
        )


def test_bundle_request_rejects_blank_identity():
    values = _request_data()
    values["checkpoint_provider"] = "  "
    with pytest.raises(ValidationError, match="must not be empty"):
        ArtifactBundleRequest(**values)


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"artifact_id": "bad"}, "artifact_id must be"),
        ({"bundle_id": "bad"}, "bundle_id must be"),
        ({"expires_at_ms": 10}, "expiration must be after creation"),
        ({"content_hash": "bad"}, "portable objects require"),
        ({"size_bytes": -1}, "non-negative size"),
        ({"restorable": True}, "evidence, not provider checkpoints"),
        (
            {
                "storage_kind": "provider_checkpoint",
                "content_hash": "c" * 64,
                "size_bytes": -1,
            },
            "empty content hash",
        ),
    ],
)
def test_index_record_rejects_integrity_violations(update, message):
    values = _index_data()
    values.update(update)
    with pytest.raises(ValidationError, match=message):
        ArtifactIndexRecord(**values)


def test_provider_checkpoint_row_uses_provider_native_size_contract():
    values = _index_data()
    values.update(
        storage_kind="provider_checkpoint",
        content_hash="",
        size_bytes=-1,
        restorable=True,
    )
    assert ArtifactIndexRecord(**values).restorable


def test_index_record_rejects_blank_identity():
    values = _index_data()
    values["object_uri"] = "  "
    with pytest.raises(ValidationError, match="must not be empty"):
        ArtifactIndexRecord(**values)


def test_store_config_requires_iceberg_and_consistent_size_limits(tmp_path):
    with pytest.raises(ValidationError, match="must use StorageBackend.ICEBERG"):
        ArtifactStoreConfig(object_uri=tmp_path, index_storage=StorageConfig())

    local = ArtifactStoreConfig.local(tmp_path)
    with pytest.raises(ValidationError, match="max_bundle_bytes must be"):
        ArtifactStoreConfig(
            object_uri=tmp_path,
            index_storage=local.index_storage,
            max_artifact_bytes=2,
            max_bundle_bytes=1,
        )


def test_store_config_retention_policy(tmp_path: Path):
    config = ArtifactStoreConfig.local(tmp_path).model_copy(
        update={"attempt_retention_seconds": 3, "run_retention_seconds": 5}
    )
    assert config.retention_seconds("attempt") == 3
    assert config.retention_seconds("run") == 5
    assert config.retention_seconds("durable") == 0
