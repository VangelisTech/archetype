# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local object-integrity contracts for Artifact storage migration."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pytest
import xxhash
from uuid_utils import uuid7

from archetype.artifacts.migration import (
    ArtifactIntegrityError,
    ArtifactObjectConflictError,
    capture_artifact_inventory,
    relocate_artifact_objects,
    relocate_artifact_table,
)
from archetype.storage.transfer import table_evidence


def _artifact_table(paths: tuple[Path, ...], payload: bytes) -> pa.Table:
    count = len(paths)
    digest = hashlib.sha256(payload).hexdigest()
    fast = xxhash.xxh3_64_hexdigest(payload)
    return pa.table(
        {
            "artifact_id": [str(uuid7()) for _ in paths],
            "ingested_at": list(range(count)),
            "world_id": ["world-a"] * count,
            "run_id": ["run-a"] * count,
            "tick": [4] * count,
            "source_uri": [f"source://occurrence/{index}" for index in range(count)],
            "logical_path": [f"evidence/{index}.bin" for index in range(count)],
            "object_uri": [path.resolve().as_uri() for path in paths],
            "size_bytes": [len(payload)] * count,
            "mime_type": ["application/octet-stream"] * count,
            "media_family": ["binary"] * count,
            "sha256": [digest] * count,
            "xxhash3_64": [fast] * count,
        }
    )


def _destination_path(root: Path, payload: bytes) -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    return root / "objects" / "sha256" / digest[:2] / digest


def test_inventory_verifies_every_uri_and_relocation_preserves_occurrences(
    tmp_path: Path,
) -> None:
    payload = b"one object, two durable Artifact occurrences"
    sources = (tmp_path / "source-a.bin", tmp_path / "source-b.bin")
    for source in sources:
        source.write_bytes(payload)
    table = _artifact_table(sources, payload)
    destination_root = tmp_path / "destination-artifacts"

    inventory = capture_artifact_inventory(table)
    assert inventory.occurrence_count == 2
    assert inventory.distinct_content_count == 1
    assert inventory.total_verified_bytes == len(payload)
    assert len(inventory.contents[0].object_uri_fingerprints) == 2

    planned_table = relocate_artifact_table(table, destination_root)
    assert not destination_root.exists(), "planning must not write destination objects"
    source_evidence = table_evidence("artifact_files", 41, table)
    result = relocate_artifact_objects(
        table,
        inventory,
        destination_root,
        source_evidence=source_evidence,
    )

    destination = _destination_path(destination_root, payload)
    assert destination.read_bytes() == payload
    assert result.relocated_table.equals(planned_table)
    assert result.destination_evidence == table_evidence("artifact_files", 41, planned_table)
    assert result.receipt.copied_content_count == 1
    assert result.receipt.reused_content_count == 0
    for name in table.column_names:
        if name != "object_uri":
            assert result.relocated_table.column(name).equals(table.column(name))
    assert result.relocated_table.column("object_uri").to_pylist() == [
        destination.resolve().as_uri(),
        destination.resolve().as_uri(),
    ]


def test_exact_destination_object_is_rehashed_and_reused(tmp_path: Path) -> None:
    payload = b"already copied exact bytes"
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    table = _artifact_table((source,), payload)
    inventory = capture_artifact_inventory(table)
    destination_root = tmp_path / "destination"
    destination = _destination_path(destination_root, payload)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(payload)

    result = relocate_artifact_objects(
        table,
        inventory,
        destination_root,
        source_evidence=table_evidence("artifact_files", 7, table),
    )

    assert destination.read_bytes() == payload
    assert result.receipt.copied_content_count == 0
    assert result.receipt.reused_content_count == 1


def test_conflicting_destination_object_is_never_overwritten(tmp_path: Path) -> None:
    payload = b"expected bytes"
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    table = _artifact_table((source,), payload)
    inventory = capture_artifact_inventory(table)
    destination_root = tmp_path / "destination"
    destination = _destination_path(destination_root, payload)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"conflicting bytes")

    with pytest.raises(ArtifactObjectConflictError, match="disagrees"):
        relocate_artifact_objects(
            table,
            inventory,
            destination_root,
            source_evidence=table_evidence("artifact_files", 8, table),
        )

    assert destination.read_bytes() == b"conflicting bytes"


@pytest.mark.parametrize("destination_kind", ["symlink", "directory"])
def test_destination_object_must_be_a_regular_file_under_destination_authority(
    tmp_path: Path,
    destination_kind: str,
) -> None:
    payload = b"valid bytes behind an invalid destination entry"
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    table = _artifact_table((source,), payload)
    inventory = capture_artifact_inventory(table)
    destination_root = tmp_path / "destination"
    destination = _destination_path(destination_root.resolve(), payload)
    destination.parent.mkdir(parents=True)
    if destination_kind == "symlink":
        destination.symlink_to(source)
    else:
        destination.mkdir()

    planned = relocate_artifact_table(table, destination_root)
    planned_uri = str(planned.column("object_uri")[0].as_py())
    assert planned_uri == destination.as_uri()
    assert Path(planned_uri.removeprefix("file://")).is_relative_to(destination_root.resolve())

    with pytest.raises(ArtifactObjectConflictError, match="not a regular file"):
        relocate_artifact_objects(
            table,
            inventory,
            destination_root,
            source_evidence=table_evidence("artifact_files", 9, table),
        )


def test_source_drift_fails_before_destination_write_and_redacts_uri(
    tmp_path: Path,
) -> None:
    payload = b"planned bytes"
    source = tmp_path / "secret-bearing-source.bin"
    source.write_bytes(payload)
    table = _artifact_table((source,), payload)
    inventory = capture_artifact_inventory(table)
    source.write_bytes(b"changed after planning")
    destination_root = tmp_path / "destination"

    with pytest.raises(ArtifactIntegrityError) as captured:
        relocate_artifact_objects(
            table,
            inventory,
            destination_root,
            source_evidence=table_evidence("artifact_files", 9, table),
        )

    assert str(source) not in str(captured.value)
    assert not destination_root.exists()


def test_source_table_evidence_drift_fails_before_object_reads(tmp_path: Path) -> None:
    payload = b"source table evidence"
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    table = _artifact_table((source,), payload)
    inventory = capture_artifact_inventory(table)
    changed = table.set_column(
        table.schema.get_field_index("logical_path"),
        "logical_path",
        pa.array(["changed.bin"]),
    )
    source.unlink()

    with pytest.raises(ArtifactIntegrityError, match="changed after migration planning"):
        relocate_artifact_objects(
            changed,
            inventory,
            tmp_path / "destination",
            source_evidence=table_evidence("artifact_files", 10, table),
        )


def test_inventory_is_bound_to_the_configured_source_authority(tmp_path: Path) -> None:
    payload = b"authority-bound object"
    source_root = tmp_path / "source-authority"
    canonical = _destination_path(source_root, payload)
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(payload)

    inventory = capture_artifact_inventory(
        _artifact_table((canonical,), payload),
        source_object_root=source_root,
    )
    assert inventory.distinct_content_count == 1

    outside = tmp_path / "same-bytes-outside-authority.bin"
    outside.write_bytes(payload)
    with pytest.raises(ArtifactIntegrityError, match="configured source authority"):
        capture_artifact_inventory(
            _artifact_table((outside,), payload),
            source_object_root=source_root,
        )

    canonical_table = _artifact_table((canonical,), payload)
    canonical.unlink()
    canonical.symlink_to(outside)
    with pytest.raises(ArtifactIntegrityError, match="not a regular file"):
        capture_artifact_inventory(
            canonical_table,
            source_object_root=source_root,
        )


def test_empty_artifact_table_has_empty_inventory_and_pure_relocation(
    tmp_path: Path,
) -> None:
    table = pa.table(
        {
            "artifact_id": pa.array([], type=pa.string()),
            "object_uri": pa.array([], type=pa.string()),
            "sha256": pa.array([], type=pa.string()),
            "xxhash3_64": pa.array([], type=pa.string()),
            "size_bytes": pa.array([], type=pa.int64()),
        }
    )

    inventory = capture_artifact_inventory(table)
    relocated = relocate_artifact_table(table, tmp_path / "destination")

    assert inventory.is_empty
    assert inventory.distinct_content_count == 0
    assert relocated.equals(table)
    assert not (tmp_path / "destination").exists()
