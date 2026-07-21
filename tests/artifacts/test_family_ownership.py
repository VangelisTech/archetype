# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""File-artifact family ownership and public-contract regressions."""

from __future__ import annotations

import ast
from pathlib import Path

from uuid_utils import uuid7

import archetype
from archetype.artifacts import ArtifactContext, ArtifactRef, ArtifactSource, ArtifactStoreConfig

_SRC = Path(__file__).resolve().parents[2] / "src" / "archetype"


def test_root_exports_are_the_family_contracts() -> None:
    assert archetype.ArtifactRef is ArtifactRef
    assert archetype.ArtifactContext is ArtifactContext
    assert archetype.ArtifactSource is ArtifactSource
    assert archetype.ArtifactStoreConfig is ArtifactStoreConfig


def test_context_identity_is_uuidv7_and_task_is_required() -> None:
    import pytest

    context = ArtifactContext(task="Explain the submitted evidence")
    assert uuid7_timestamp_ms(context.context_id) > 0
    with pytest.raises(ValueError, match="task must not be empty"):
        ArtifactContext(task=" ")


def test_artifact_ref_derives_time_from_uuidv7() -> None:
    artifact_id = str(uuid7())
    reference = ArtifactRef(
        artifact_id=artifact_id,
        logical_path="results/report.json",
        uri="file:///objects/report.json",
        sha256="a" * 64,
        xxhash3_64="b" * 16,
        media_type="application/json",
        size_bytes=42,
    )
    assert reference.artifact_id == artifact_id
    assert int(reference.ingested_at.timestamp() * 1000) == uuid7_timestamp_ms(artifact_id)


def uuid7_timestamp_ms(value: str) -> int:
    from uuid_utils import UUID

    return UUID(value).timestamp


def test_source_rejects_nonportable_logical_paths() -> None:
    import pytest

    with pytest.raises(ValueError, match="portable relative path"):
        ArtifactSource(source_uri="result.txt", logical_path="../result.txt")


def test_artifact_contracts_reject_ambiguous_identity_and_unbounded_batches() -> None:
    import pytest

    with pytest.raises(ValueError, match="source_uri"):
        ArtifactSource(source_uri=" ")
    assert ArtifactSource(source_uri="result.txt", logical_root="").logical_root == ""
    with pytest.raises(ValueError, match="recursive sources"):
        ArtifactSource(source_uri="results", recursive=True, logical_path="result.txt")

    valid = {
        "artifact_id": str(uuid7()),
        "logical_path": "results/report.json",
        "uri": "file:///objects/report.json",
        "sha256": "a" * 64,
        "xxhash3_64": "b" * 16,
        "media_type": "application/json",
        "size_bytes": 42,
    }
    with pytest.raises(ValueError, match="UUIDv7"):
        ArtifactRef(**(valid | {"artifact_id": "00000000-0000-4000-8000-000000000000"}))
    with pytest.raises(ValueError, match="must not be empty"):
        ArtifactRef(**(valid | {"uri": " "}))
    with pytest.raises(ValueError, match="SHA-256"):
        ArtifactRef(**(valid | {"sha256": "not-a-digest"}))
    with pytest.raises(ValueError, match="XXH3-64"):
        ArtifactRef(**(valid | {"xxhash3_64": "not-a-digest"}))

    context_id = str(uuid7())
    assert ArtifactContext(task="Analyze", context_id=context_id).context_id == context_id
    with pytest.raises(ValueError, match="UUIDv7"):
        ArtifactContext(
            task="Analyze",
            context_id="00000000-0000-4000-8000-000000000000",
        )
    with pytest.raises(ValueError, match="must be >="):
        ArtifactStoreConfig(max_artifact_bytes=2, max_ingestion_bytes=1)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return modules


def test_artifact_family_never_imports_application_layers() -> None:
    forbidden = ("archetype.app", "archetype.runtime", "archetype.api", "archetype.cli")
    for path in sorted((_SRC / "artifacts").rglob("*.py")):
        outward = [module for module in _imports(path) if module.startswith(forbidden)]
        assert not outward, f"{path} imports outward packages: {outward}"
