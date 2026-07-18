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
    ArtifactStoreConfig,
)
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
