# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Content-integrity contracts for the file-ingestion persistence boundary."""

from __future__ import annotations

import hashlib
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
import xxhash
from daft.file.file import BUFFER_COPY

import archetype.ingestion.pipeline as pipeline_module
from archetype.ingestion import FileIngestionPipeline
from archetype.ingestion.pipeline import _persist_local_file


class _TrackingStream(BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if size < 0 or size > BUFFER_COPY:
            raise AssertionError(f"unbounded persistence read: {size}")
        return super().read(size)


class _TrackingFile:
    def __init__(self, payload: bytes) -> None:
        self.stream = _TrackingStream(payload)
        self.open_count = 0
        self.buffer_sizes: list[int | None] = []

    def open(self, *, buffer_size: int | None = None) -> _TrackingStream:
        self.open_count += 1
        self.buffer_sizes.append(buffer_size)
        return self.stream


def _local_pipeline(root: Path) -> FileIngestionPipeline:
    return FileIngestionPipeline(
        object_uri=root.resolve().as_uri(),
        local_object_root=str(root),
    )


def _object_path(root: Path, payload: bytes) -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    return root / "objects" / "sha256" / digest[:2] / digest


def test_local_persistence_copies_and_identifies_one_stream(tmp_path: Path) -> None:
    store = tmp_path / "store"
    payload = b"verified artifact bytes"
    file = _TrackingFile(payload)

    stored = _persist_local_file(cast(Any, file), str(store))

    destination = _object_path(store, payload)
    assert file.open_count == 1
    assert file.buffer_sizes == [BUFFER_COPY]
    assert file.stream.read_sizes == [BUFFER_COPY, BUFFER_COPY]
    assert destination.read_bytes() == payload
    assert stored == {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "xxhash3_64": xxhash.xxh3_64_hexdigest(payload),
        "size_bytes": len(payload),
        "object_uri": destination.resolve().as_uri(),
    }


def test_local_persistence_accepts_file_larger_than_copy_buffer(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    payload = b"a" * (BUFFER_COPY * 2 + 17)
    source.write_bytes(payload)
    pipeline = _local_pipeline(store)

    discovered = pipeline.scan(str(source)).collect()
    (stored,) = pipeline.persist(discovered).to_pylist()

    destination = _object_path(store, payload)
    assert destination.read_bytes() == payload
    assert stored["size_bytes"] == len(payload)
    assert stored["sha256"] == hashlib.sha256(payload).hexdigest()
    assert stored["xxhash3_64"] == xxhash.xxh3_64_hexdigest(payload)


def test_local_persistence_addresses_bytes_read_at_persist_time(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    source.write_bytes(b"old")
    pipeline = _local_pipeline(store)
    discovered = pipeline.scan(str(source)).collect()

    persisted_payload = b"NEW"
    source.write_bytes(persisted_payload)
    (stored,) = pipeline.persist(discovered).to_pylist()

    destination = _object_path(store, persisted_payload)
    assert destination.read_bytes() == persisted_payload
    assert stored["object_uri"] == destination.resolve().as_uri()
    assert stored["sha256"] == hashlib.sha256(persisted_payload).hexdigest()
    assert not _object_path(store, b"old").exists()


def test_local_persistence_atomically_repairs_existing_content_address(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    store = tmp_path / "store"
    payload = b"correct bytes"
    source.write_bytes(payload)
    destination = _object_path(store, payload)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"corrupt")

    (stored,) = (
        _local_pipeline(store)
        .persist(_local_pipeline(store).scan(str(source)).collect())
        .to_pylist()
    )

    assert destination.read_bytes() == payload
    assert stored["object_uri"] == destination.resolve().as_uri()


def test_local_persistence_cleans_staging_file_when_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = tmp_path / "store"
    file = _TrackingFile(b"bytes")

    def fail_replace(_source: str, _destination: Path) -> None:
        raise OSError("publish failed")

    monkeypatch.setattr(pipeline_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="publish failed"):
        _persist_local_file(cast(Any, file), str(store))

    staging = store / "objects" / ".staging"
    assert not [path for path in staging.iterdir() if path.is_file()]


def test_binary_upload_addresses_bytes_read_at_persist_time(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    upload_root = tmp_path / "upload"
    source.write_bytes(b"old")
    pipeline = FileIngestionPipeline(object_uri=upload_root.resolve().as_uri())
    discovered = pipeline.scan(str(source)).collect()

    persisted_payload = b"NEW"
    source.write_bytes(persisted_payload)
    (stored,) = pipeline.persist(discovered).to_pylist()

    destination = _object_path(upload_root, persisted_payload)
    assert destination.read_bytes() == persisted_payload
    assert stored["object_uri"] == destination.resolve().as_uri()
    assert stored["sha256"] == hashlib.sha256(persisted_payload).hexdigest()
