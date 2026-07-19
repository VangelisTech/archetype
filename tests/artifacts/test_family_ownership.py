# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Artifacts family ownership regressions (issue #558).

The #558 relocation is a pure move: `ArtifactMeta`/`AssetRef` and the
reusable artifact/bundle value contracts left `archetype.app.artifacts`
for the top-level `archetype.artifacts` family without changing any
serialized field, default, validator, or digest. These tests pin that
contract: byte-identical digest vectors, unchanged Arrow/Pydantic schemas,
single class identity behind the supported root exports, a one-way
app -> family dependency, and app-owned publication-key authority.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pyarrow as pa

import archetype
from archetype.artifacts import bundles, contracts
from archetype.artifacts.components import ArtifactMeta, AssetRef
from archetype.artifacts.contracts import (
    ARTIFACT_ENVELOPE_COLUMNS,
    ARTIFACT_ID_COLUMN,
    ARTIFACT_KEY_COLUMNS,
    artifact_payload_digest,
    artifact_table_id,
    digest_bytes,
    digest_file,
)
from archetype.core.component import Component

_SRC = Path(__file__).resolve().parents[2] / "src" / "archetype"
_FAMILY_DIR = _SRC / "artifacts"
_APP_ARTIFACTS_DIR = _SRC / "app" / "artifacts"


def test_artifact_component_arrow_schemas_are_unchanged() -> None:
    meta_schema = ArtifactMeta.get_prefixed_schema()
    assert [(field.name, field.type) for field in meta_schema] == [
        ("artifactmeta__producer", pa.string()),
        ("artifactmeta__external_id", pa.string()),
        ("artifactmeta__payload_digest", pa.string()),
        ("artifactmeta__commit_id", pa.string()),
    ]
    ref_schema = AssetRef.get_prefixed_schema()
    assert [(field.name, field.type) for field in ref_schema] == [
        ("assetref__digest", pa.string()),
        ("assetref__uri", pa.string()),
        ("assetref__media_type", pa.string()),
        ("assetref__size_bytes", pa.int64()),
        ("assetref__created_at_ms", pa.int64()),
    ]


def test_artifact_component_pydantic_defaults_are_unchanged() -> None:
    meta_defaults = {name: info.default for name, info in ArtifactMeta.model_fields.items()}
    assert meta_defaults == {
        "producer": "",
        "external_id": "",
        "payload_digest": "",
        "commit_id": "",
    }
    ref_defaults = {name: info.default for name, info in AssetRef.model_fields.items()}
    assert ref_defaults == {
        "digest": "",
        "uri": "",
        "media_type": "",
        "size_bytes": 0,
        "created_at_ms": 0,
    }


def test_envelope_vocabulary_is_unchanged() -> None:
    assert ARTIFACT_ID_COLUMN == "artifact_id"
    assert ARTIFACT_KEY_COLUMNS == ("world_id", "run_id", "source_uri", "content_hash")
    assert ARTIFACT_ENVELOPE_COLUMNS == (
        "artifact_id",
        "world_id",
        "run_id",
        "source_uri",
        "content_hash",
    )
    assert artifact_table_id("events") == "artifacts__events"


def test_digest_vectors_are_byte_for_byte_unchanged(tmp_path: Path) -> None:
    """Vectors recorded from archetype.app.artifacts before the move."""
    meta = ArtifactMeta(producer="p-1", external_id="ext-1", payload_digest="", commit_id="c-1")
    ref = AssetRef(
        digest="d" * 8,
        uri="file:///tmp/a.bin",
        media_type="application/octet-stream",
        size_bytes=42,
        created_at_ms=1700000000000,
    )
    assert artifact_payload_digest([meta, ref]) == (
        "e08fbf301e60f5e4273e6f4c856efa77b8d3993b23c5946ffe62cd5fe72ff0fb"
    )
    # Order-invariance is part of the identity contract.
    assert artifact_payload_digest([ref, meta]) == artifact_payload_digest([meta, ref])
    assert artifact_payload_digest([]) == (
        "7b91f677fa7b1d50dd1e8c96721a08008113c71d5e052c4108808fdce3c5652a"
    )

    evidence = b"archetype-artifact-evidence"
    vector = "d7fed7fa7436d6e6b0cfeea819b506044d822b9ea3b691cf4b6c2787439e7f5b"
    assert digest_bytes(evidence) == vector
    on_disk = tmp_path / "evidence.bin"
    on_disk.write_bytes(evidence)
    assert digest_file(on_disk) == vector


def test_bundle_request_digest_vectors_are_byte_for_byte_unchanged() -> None:
    """Vectors recorded from archetype.app.artifacts.bundle_models before the move."""
    request = bundles.ArtifactBundleRequest(
        world_id="world-1",
        run_id="run-1",
        entity_id=7,
        tick=3,
        attempt_id="attempt-1",
        idempotency_key="key-1",
        redaction_policy_id="policy-1",
        checkpoint_ref="ckpt://one",
        checkpoint_provider="provider-1",
        checkpoint_restorable=True,
        checkpoint_created_at_ms=1000,
        checkpoint_expires_at_ms=2000,
        accepted=False,
        retention="run",
        artifact_expires_at_ms=3000,
        artifacts=(
            bundles.ArtifactCandidate(
                source_ref="file:///tmp/b.txt", logical_path="b.txt", kind="artifact"
            ),
            bundles.ArtifactCandidate(
                source_ref="file:///tmp/a.txt",
                logical_path="a.txt",
                kind="log",
                recursive=False,
                required=False,
            ),
        ),
    )
    assert request.producer_digest() == (
        "5ada07fd1ab9a7508da47474cebab4bb6c69dce01c7cd3a1f1b74d6e3b6507de"
    )
    assert request.digest() == request.producer_digest()
    assert request.request_digest() == (
        "e6f2b32245da5124229f9efe1ede0d70a76a891d39228544aa5cd609f9b1ebb6"
    )


def test_snapshot_bound_matches_the_application_limit() -> None:
    from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID

    assert bundles._MAX_ICEBERG_SNAPSHOT_ID == MAX_ICEBERG_SNAPSHOT_ID


def test_supported_root_exports_resolve_to_the_single_moved_definitions() -> None:
    assert archetype.ArtifactReceipt is contracts.ArtifactReceipt
    assert archetype.ArtifactWriteReceipt is contracts.ArtifactWriteReceipt
    assert archetype.ArtifactProcessor is contracts.ArtifactProcessor
    assert archetype.ArtifactBundleRequest is bundles.ArtifactBundleRequest
    assert archetype.ArtifactCandidate is bundles.ArtifactCandidate
    assert archetype.ArtifactIndexRecord is bundles.ArtifactIndexRecord
    assert archetype.ArtifactPublishReceipt is bundles.ArtifactPublishReceipt
    assert archetype.ArtifactReconcileResult is bundles.ArtifactReconcileResult
    assert archetype.ArtifactSourceResolver is bundles.ArtifactSourceResolver
    assert archetype.BoundedArtifactSourceResolver is bundles.BoundedArtifactSourceResolver
    assert archetype.ArtifactStoreConfig is bundles.ArtifactStoreConfig
    assert archetype.MaterializedArtifact is bundles.MaterializedArtifact
    assert ArtifactMeta.__module__ == "archetype.artifacts.components"
    assert AssetRef.__module__ == "archetype.artifacts.components"
    assert contracts.ArtifactReceipt.__module__ == "archetype.artifacts.contracts"
    assert bundles.ArtifactBundleRequest.__module__ == "archetype.artifacts.bundles"


def test_no_duplicate_artifact_component_exists() -> None:
    """`get_type_by_name` raises when two Component subclasses share a name."""
    import archetype.app.artifacts.service  # noqa: F401 — load the app side too
    import archetype.app.artifacts.table_service  # noqa: F401

    assert Component.get_type_by_name("ArtifactMeta") is ArtifactMeta
    assert Component.get_type_by_name("AssetRef") is AssetRef
    assert not (_APP_ARTIFACTS_DIR / "models.py").exists()


def test_publication_key_authority_remains_app_owned() -> None:
    """The prepared request stays behind the catalog's publication-key check."""
    from archetype.app.artifacts.bundle_models import PreparedArtifactBundleRequest

    assert PreparedArtifactBundleRequest.__module__ == "archetype.app.artifacts.bundle_models"
    assert not hasattr(archetype.artifacts, "PreparedArtifactBundleRequest")
    family_sources = "".join(
        path.read_text(encoding="utf-8") for path in sorted(_FAMILY_DIR.rglob("*.py"))
    )
    assert "def artifact_publication_key" not in family_sources


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return modules


def test_app_artifacts_imports_the_family_and_never_the_reverse() -> None:
    forbidden_prefixes = ("archetype.app", "archetype.runtime", "archetype.api", "archetype.cli")
    for path in sorted(_FAMILY_DIR.rglob("*.py")):
        outward = {
            module for module in _imported_modules(path) if module.startswith(forbidden_prefixes)
        }
        assert not outward, f"{path} imports outward packages: {sorted(outward)}"

    app_imports: set[str] = set()
    for path in sorted(_APP_ARTIFACTS_DIR.rglob("*.py")):
        app_imports.update(_imported_modules(path))
    assert any(module.startswith("archetype.artifacts") for module in app_imports), (
        "app artifacts no longer consumes the top-level family contracts"
    )
