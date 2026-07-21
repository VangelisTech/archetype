# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Content-integrity contracts for the file-ingestion persistence boundary."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from archetype.ingestion import FileIngestionPipeline


def _local_pipeline(root: Path, *, max_artifact_bytes: int = 1024) -> FileIngestionPipeline:
    return FileIngestionPipeline(
        object_uri=root.resolve().as_uri(),
        local_object_root=str(root),
        max_artifact_bytes=max_artifact_bytes,
    )


def _object_path(root: Path, payload: bytes) -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    return root / "objects" / "sha256" / digest[:2] / digest


def test_local_persistence_copies_verified_bytes_to_the_content_address(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    payload = b"verified artifact bytes"
    source.write_bytes(payload)
    pipeline = _local_pipeline(store)

    discovered = pipeline.scan(str(source)).collect()
    (stored,) = pipeline.persist(discovered).to_pylist()

    destination = _object_path(store, payload)
    assert destination.read_bytes() == payload
    assert stored["object_uri"] == destination.resolve().as_uri()


def test_local_persistence_rejects_same_size_source_mutation_even_when_object_exists(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    discovered_payload = b"old"
    source.write_bytes(discovered_payload)
    pipeline = _local_pipeline(store)
    discovered = pipeline.scan(str(source)).collect()

    pipeline.persist(discovered).collect()
    destination = _object_path(store, discovered_payload)
    source.write_bytes(b"NEW")

    with pytest.raises(Exception, match="source changed after discovery"):
        pipeline.persist(discovered).collect()

    assert destination.read_bytes() == discovered_payload


def test_local_persistence_rejects_source_size_change(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    discovered_payload = b"short"
    source.write_bytes(discovered_payload)
    pipeline = _local_pipeline(store)
    discovered = pipeline.scan(str(source)).collect()
    source.write_bytes(b"longer, but still within the configured limit")

    with pytest.raises(Exception, match="source changed after discovery"):
        pipeline.persist(discovered).collect()

    assert not _object_path(store, discovered_payload).exists()


def test_local_persistence_enforces_limit_against_bytes_actually_read(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    discovered_payload = b"small"
    source.write_bytes(discovered_payload)
    pipeline = _local_pipeline(store, max_artifact_bytes=8)
    discovered = pipeline.scan(str(source)).collect()
    source.write_bytes(b"nine-byte")

    with pytest.raises(Exception, match="limit exceeded while persisting"):
        pipeline.persist(discovered).collect()

    assert not _object_path(store, discovered_payload).exists()


def test_upload_persistence_validates_bytes_before_upload(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    upload_root = tmp_path / "upload"
    source.write_bytes(b"old")
    pipeline = FileIngestionPipeline(object_uri=upload_root.resolve().as_uri())
    discovered = pipeline.scan(str(source)).collect()
    source.write_bytes(b"NEW")

    with pytest.raises(Exception, match="source changed after discovery"):
        pipeline.persist(discovered).collect()

    assert not upload_root.exists()
