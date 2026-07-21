# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Direct contracts for pure metadata scanners used inside Daft UDFs."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from io import BytesIO

import pytest
import xxhash
from uuid_utils import UUID, uuid7

from archetype.ingestion import (
    FileIngestionPipeline,
    hash_file,
    ingestion_time_for,
    media_family_for,
    scan_diff_metadata,
    scan_text_metadata,
)


def test_file_classification_and_hashing_are_deterministic() -> None:
    expected_families = [
        ("application/pdf", "paper.pdf", "pdf"),
        ("image/png", "image.bin", "image"),
        ("audio/wav", "audio.bin", "audio"),
        ("video/mp4", "video.bin", "video"),
        ("text/markdown", "brief.md", "text"),
        ("application/json", "context.json", "text"),
        ("application/x-ndjson", "context.jsonl", "text"),
        ("application/xml", "context.xml", "text"),
        ("application/octet-stream", "change.patch", "text"),
        ("application/octet-stream", "context.unknown", "binary"),
    ]
    assert [
        media_family_for(mime_type, logical_path)
        for mime_type, logical_path, _expected in expected_families
    ] == [_expected for _mime, _path, _expected in expected_families]

    payload = b"one-pass artifact content"
    assert hash_file(BytesIO(payload)) == {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "xxhash3_64": xxhash.xxh3_64_hexdigest(payload),
        "size_bytes": len(payload),
    }


def test_pipeline_uses_daft_file_mime_type_directly(tmp_path) -> None:
    patch = tmp_path / "change.patch"
    patch.write_text("+line\n")

    row = (
        FileIngestionPipeline().scan(str(patch)).select("mime_type", "media_family").to_pylist()[0]
    )

    assert row == {"mime_type": "application/octet-stream", "media_family": "text"}


def test_ingestion_time_requires_uuidv7() -> None:
    artifact_id = str(uuid7())
    assert ingestion_time_for(artifact_id) == datetime.fromtimestamp(
        UUID(artifact_id).timestamp / 1000,
        tz=UTC,
    )
    with pytest.raises(ValueError, match="UUIDv7"):
        ingestion_time_for("00000000-0000-4000-8000-000000000000")


@pytest.mark.parametrize(
    ("logical_path", "payload", "kind", "language", "lines", "utf8"),
    [
        ("brief.md", b"# Brief\n", "markdown", "markdown", 1, True),
        ("change.patch", b"+change", "diff", "diff", 1, True),
        ("events.jsonl", b"{}\n{}\n", "structured_text", "jsonl", 2, True),
        ("pipeline.py", b"return 1", "source_code", "python", 1, True),
        ("notes.txt", b"plain", "plain_text", "text", 1, True),
        ("invalid.txt", b"bad:\xff\n", "plain_text", "text", 1, False),
        ("partial.txt", b"partial:\xe2\x82", "plain_text", "text", 1, False),
    ],
)
def test_text_metadata_scans_shape_and_encoding(
    logical_path: str,
    payload: bytes,
    kind: str,
    language: str,
    lines: int,
    utf8: bool,
) -> None:
    assert scan_text_metadata(BytesIO(payload), logical_path) == {
        "text_kind": kind,
        "language": language,
        "line_count": lines,
        "utf8": utf8,
    }


def test_diff_metadata_handles_git_unified_and_binary_patches() -> None:
    git_patch = (
        b"diff --git a/a.py b/a.py\n"
        b"--- a/a.py\n"
        b"+++ b/a.py\n"
        b"@@ -1 +1 @@\n"
        b"-old\n"
        b"+new\n"
        b"GIT binary patch"
    )
    assert scan_diff_metadata(BytesIO(git_patch)) == {
        "format": "git",
        "file_count": 1,
        "hunk_count": 1,
        "additions": 1,
        "deletions": 1,
        "binary_file_count": 1,
    }

    unified_patch = (
        b"--- before.txt\n"
        b"+++ after.txt\n"
        b"@@ -0,0 +1 @@\n"
        b"+content\n"
        b"Binary files before.bin and after.bin differ\n"
    )
    assert scan_diff_metadata(BytesIO(unified_patch)) == {
        "format": "unified",
        "file_count": 1,
        "hunk_count": 1,
        "additions": 1,
        "deletions": 0,
        "binary_file_count": 1,
    }
