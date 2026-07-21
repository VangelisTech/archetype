# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Lazy file discovery, identity, classification, and integrity transforms."""

from __future__ import annotations

import hashlib
import mimetypes
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import BinaryIO, cast
from urllib.parse import unquote, urlparse

import daft
import xxhash
from daft import DataFrame, DataType, col
from daft.functions import file_path, uuid
from daft.io import IOConfig
from uuid_utils import UUID

ARTIFACT_FILES = "artifact_files"
_COPY_BUFFER = 1 << 20
_MIME_OVERRIDES = {
    ".diff": "text/x-diff",
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".patch": "text/x-diff",
}
_HASHES = DataType.struct(
    {
        "sha256": DataType.string(),
        "xxhash3_64": DataType.string(),
        "size_bytes": DataType.int64(),
    }
)


def _uri_path(value: str) -> PurePosixPath:
    parsed = urlparse(value)
    raw = unquote(parsed.path) if parsed.scheme else value
    return PurePosixPath(raw.replace("\\", "/"))


def logical_path_for(
    source_uri: str,
    *,
    source_root: str = "",
    logical_root: str = "",
    logical_path: str = "",
) -> str:
    """Return a portable occurrence path independent of source/object location."""

    if logical_path:
        relative = PurePosixPath(logical_path)
    else:
        source = _uri_path(source_uri)
        root = _uri_path(source_root) if source_root else source.parent
        try:
            relative = source.relative_to(root)
        except ValueError:
            relative = PurePosixPath(source.name)
    result = PurePosixPath(logical_root) / relative
    normalized = result.as_posix().strip("/")
    if not normalized or result.is_absolute() or ".." in result.parts:
        raise ValueError("logical paths must be non-empty portable relative paths")
    return normalized


@daft.func(return_dtype=DataType.string())
def _logical_path(
    source_uri: str,
    source_root: str,
    logical_root: str,
    logical_path: str,
) -> str:
    return logical_path_for(
        source_uri,
        source_root=source_root,
        logical_root=logical_root,
        logical_path=logical_path,
    )


@daft.func(return_dtype=DataType.string())
def _mime_type(file: daft.File, source_uri: str) -> str:
    override = _mime_override(source_uri)
    if override is not None:
        return override
    return detect_mime_type(source_uri, file.mime_type())


def _mime_override(source_uri: str) -> str | None:
    return _MIME_OVERRIDES.get(_uri_path(source_uri).suffix.lower())


def detect_mime_type(source_uri: str, detected: str) -> str:
    """Resolve stable extension overrides and a scanner-provided MIME type."""

    override = _mime_override(source_uri)
    if override is not None:
        return override
    if detected != "application/octet-stream":
        return detected
    return mimetypes.guess_type(source_uri)[0] or detected


@daft.func(return_dtype=DataType.string())
def _media_family(mime_type: str) -> str:
    return media_family_for(mime_type)


def media_family_for(mime_type: str) -> str:
    """Map a MIME type into one specialized artifact-index family."""

    if mime_type == "application/pdf":
        return "pdf"
    family = mime_type.partition("/")[0]
    if family in {"image", "audio", "video", "text"}:
        return family
    if mime_type in {"application/json", "application/x-ndjson", "application/xml"}:
        return "text"
    return "binary"


@daft.func(return_dtype=DataType.timestamp("us", timezone="UTC"))
def _uuid7_timestamp(artifact_id: str) -> datetime:
    return ingestion_time_for(artifact_id)


def ingestion_time_for(artifact_id: str) -> datetime:
    """Derive the UTC ingestion time encoded by a UUIDv7 artifact ID."""

    value = UUID(artifact_id)
    if value.version != 7:
        raise ValueError("artifact_id must be UUIDv7")
    return datetime.fromtimestamp(value.timestamp / 1000, tz=UTC)


@daft.func(return_dtype=_HASHES)
def _file_hashes(file: daft.File) -> dict[str, str | int]:
    """Compute both integrity hashes and size during one streaming read."""

    with file.open(buffer_size=_COPY_BUFFER) as stream:
        return hash_file(cast(BinaryIO, stream))


def hash_file(stream: BinaryIO) -> dict[str, str | int]:
    """Compute SHA-256, XXH3-64, and size in one pass over a binary stream."""

    sha256 = hashlib.sha256()
    fast = xxhash.xxh3_64()
    size = 0
    while chunk := stream.read(_COPY_BUFFER):
        sha256.update(chunk)
        fast.update(chunk)
        size += len(chunk)
    return {
        "sha256": sha256.hexdigest(),
        "xxhash3_64": fast.hexdigest(),
        "size_bytes": size,
    }


def scan_files(
    path: str | list[str],
    *,
    source_root: str = "",
    logical_root: str = "",
    logical_path: str = "",
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Build one lazy row per file, with stable identity and up-front metadata."""

    files = daft.from_files(path, io_config=io_config)
    files = files.with_column("artifact_id", uuid(version="v7").cast(DataType.string()))
    files = files.with_column("source_uri", file_path(col("file")))
    files = files.with_column(
        "logical_path",
        _logical_path(
            col("source_uri"),
            daft.lit(source_root),
            daft.lit(logical_root),
            daft.lit(logical_path),
        ),
    )
    files = files.with_column("ingested_at", _uuid7_timestamp(col("artifact_id")))
    files = files.with_column("mime_type", _mime_type(col("file"), col("source_uri")))
    files = files.with_column("media_family", _media_family(col("mime_type")))
    files = files.with_column("_hashes", _file_hashes(col("file")))
    return files.select(
        "file",
        "artifact_id",
        "ingested_at",
        "source_uri",
        "logical_path",
        "mime_type",
        "media_family",
        col("_hashes").unnest(),
    )


def common_index(files: DataFrame) -> DataFrame:
    """Project durable common-index columns after the app adds ``object_uri``."""

    return files.select(
        "artifact_id",
        "ingested_at",
        "tick",
        "source_uri",
        "logical_path",
        "object_uri",
        "size_bytes",
        "mime_type",
        "media_family",
        "sha256",
        "xxhash3_64",
    )
