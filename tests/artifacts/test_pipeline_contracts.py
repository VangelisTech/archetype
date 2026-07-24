# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned artifact pipeline persistence contracts."""

from __future__ import annotations

import hashlib
import sys
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import xxhash
from daft.file.file import BUFFER_COPY
from uuid_utils import UUID, uuid7


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


def test_family_pipeline_copies_and_identifies_one_stream(tmp_path: Path) -> None:
    from archetype.artifacts.pipeline import _persist_local_file

    payload = b"one-pass artifact content"
    file = _TrackingFile(payload)
    root = tmp_path / "objects"

    stored = _persist_local_file(cast(Any, file), str(root))

    digest = hashlib.sha256(payload).hexdigest()
    destination = root / "objects" / "sha256" / digest[:2] / digest
    assert file.open_count == 1
    assert file.buffer_sizes == [BUFFER_COPY]
    assert file.stream.read_sizes == [BUFFER_COPY, BUFFER_COPY]
    assert destination.read_bytes() == payload
    assert stored == {
        "sha256": digest,
        "xxhash3_64": xxhash.xxh3_64_hexdigest(payload),
        "size_bytes": len(payload),
        "object_uri": destination.resolve().as_uri(),
    }


def test_family_pipeline_owns_scanners_without_legacy_imports() -> None:
    from archetype.artifacts.pipeline import (
        FileIngestionPipeline,
        ingestion_time_for,
        media_family_for,
    )
    from archetype.artifacts.scanners import (
        hash_file,
        scan_diff_metadata,
        scan_text_metadata,
    )

    assert FileIngestionPipeline.__module__ == "archetype.artifacts.pipeline"
    assert hash_file.__module__ == "archetype.artifacts.scanners"

    pipeline_source = Path(sys.modules[FileIngestionPipeline.__module__].__file__).read_text(
        encoding="utf-8"
    )
    assert "archetype.ingestion" not in pipeline_source

    payload = b"one-pass artifact content"
    assert hash_file(BytesIO(payload)) == {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "xxhash3_64": xxhash.xxh3_64_hexdigest(payload),
        "size_bytes": len(payload),
    }
    assert media_family_for("application/octet-stream", "change.patch") == "text"
    assert media_family_for("application/octet-stream", "opaque.bin") == "binary"
    assert scan_text_metadata(BytesIO(b"# Evidence\n"), "brief.md") == {
        "text_kind": "markdown",
        "language": "markdown",
        "line_count": 1,
        "utf8": True,
    }
    assert scan_diff_metadata(
        BytesIO(b"diff --git a/a.py b/a.py\n--- a/a.py\n+++ b/a.py\n@@ -1 +1 @@\n-old\n+new\n")
    ) == {
        "format": "git",
        "file_count": 1,
        "hunk_count": 1,
        "additions": 1,
        "deletions": 1,
        "binary_file_count": 0,
    }
    artifact_id = str(uuid7())
    assert ingestion_time_for(artifact_id) == datetime.fromtimestamp(
        UUID(artifact_id).timestamp / 1000,
        tz=UTC,
    )
