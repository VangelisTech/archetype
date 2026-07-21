# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""One readable lazy Daft graph for file discovery, storage, and metadata."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast
from urllib.parse import unquote, urlparse

import daft
from daft import DataFrame, DataType, col, lit
from daft.functions import (
    audio_file,
    audio_metadata,
    file_path,
    image_file,
    image_file_metadata,
    upload,
    uuid,
    video_file,
    video_metadata,
)
from daft.functions import (
    file as daft_file,
)
from daft.functions import (
    format as daft_format,
)
from daft.io import IOConfig
from uuid_utils import UUID

from archetype.ingestion.scanners import (
    hash_file,
    scan_diff_metadata,
    scan_pdf_metadata,
    scan_text_metadata,
)

ARTIFACT_FILES = "artifact_files"
ARTIFACT_IMAGES = "artifact_images"
ARTIFACT_AUDIO = "artifact_audio"
ARTIFACT_VIDEO = "artifact_video"
ARTIFACT_PDF = "artifact_pdf"
ARTIFACT_TEXT = "artifact_text"
ARTIFACT_DIFF = "artifact_diff"

_COPY_BUFFER = 1 << 20
_TEXT_SUFFIXES = {
    ".c",
    ".cc",
    ".cpp",
    ".css",
    ".diff",
    ".go",
    ".h",
    ".hpp",
    ".html",
    ".java",
    ".js",
    ".json",
    ".jsonl",
    ".jsx",
    ".md",
    ".mdx",
    ".ndjson",
    ".patch",
    ".py",
    ".rs",
    ".sh",
    ".sql",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}
_HASHES = DataType.struct(
    {
        "sha256": DataType.string(),
        "xxhash3_64": DataType.string(),
        "size_bytes": DataType.int64(),
    }
)
_PDF_METADATA = DataType.struct(
    {
        "page_count": DataType.int64(),
        "encrypted": DataType.bool(),
        "title": DataType.string(),
        "author": DataType.string(),
    }
)
_TEXT_METADATA = DataType.struct(
    {
        "text_kind": DataType.string(),
        "language": DataType.string(),
        "line_count": DataType.int64(),
        "utf8": DataType.bool(),
    }
)
_DIFF_METADATA = DataType.struct(
    {
        "format": DataType.string(),
        "file_count": DataType.int64(),
        "hunk_count": DataType.int64(),
        "additions": DataType.int64(),
        "deletions": DataType.int64(),
        "binary_file_count": DataType.int64(),
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
    """Return a portable occurrence path independent of physical location."""

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


def ingestion_time_for(artifact_id: str) -> datetime:
    """Derive the UTC ingestion time encoded by a UUIDv7 artifact ID."""

    value = UUID(artifact_id)
    if value.version != 7:
        raise ValueError("artifact_id must be UUIDv7")
    return datetime.fromtimestamp(value.timestamp / 1000, tz=UTC)


def media_family_for(mime_type: str, logical_path: str = "") -> str:
    """Route built-in Daft MIME output to one specialized metadata branch."""

    if mime_type == "application/pdf":
        return "pdf"
    family = mime_type.partition("/")[0]
    if family in {"image", "audio", "video", "text"}:
        return family
    if mime_type in {"application/json", "application/x-ndjson", "application/xml"}:
        return "text"
    if PurePosixPath(logical_path).suffix.lower() in _TEXT_SUFFIXES:
        return "text"
    return "binary"


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
def _mime_type(file: daft.File) -> str:
    """Use Daft's built-in content/extension MIME classification directly."""

    return file.mime_type()


@daft.func(return_dtype=DataType.string())
def _media_family(mime_type: str, logical_path: str) -> str:
    return media_family_for(mime_type, logical_path)


@daft.func(return_dtype=DataType.timestamp("us", timezone="UTC"))
def _uuid7_timestamp(artifact_id: str) -> datetime:
    return ingestion_time_for(artifact_id)


@daft.func(return_dtype=_HASHES)
def _file_hashes(file: daft.File) -> dict[str, str | int]:
    with file.open(buffer_size=_COPY_BUFFER) as stream:
        return hash_file(cast(BinaryIO, stream))


@daft.func(return_dtype=DataType.string())
def _copy_local_object(
    file: daft.File,
    sha256: str,
    size_bytes: int,
    root: str,
    max_artifact_bytes: int,
) -> str:
    if size_bytes > max_artifact_bytes:
        raise ValueError(
            f"artifact is {size_bytes} bytes; per-artifact limit is {max_artifact_bytes}"
        )
    destination = Path(root) / "objects" / "sha256" / sha256[:2] / sha256
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file() and destination.stat().st_size == size_bytes:
        digest = hashlib.sha256()
        with destination.open("rb") as existing:
            while chunk := existing.read(_COPY_BUFFER):
                digest.update(chunk)
        if digest.hexdigest() == sha256:
            return destination.resolve().as_uri()

    descriptor, temporary = tempfile.mkstemp(prefix=f".{sha256}-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as target, file.open(buffer_size=_COPY_BUFFER) as source:
            shutil.copyfileobj(source, target, length=_COPY_BUFFER)
        os.replace(temporary, destination)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return destination.resolve().as_uri()


@daft.func(return_dtype=DataType.binary())
def _read_bounded(file: daft.File, size_bytes: int, max_artifact_bytes: int) -> bytes:
    if size_bytes > max_artifact_bytes:
        raise ValueError(
            f"artifact is {size_bytes} bytes; per-artifact limit is {max_artifact_bytes}"
        )
    with file.open(buffer_size=_COPY_BUFFER) as stream:
        return stream.read()


@daft.func(return_dtype=_PDF_METADATA)
def _pdf_metadata(file: daft.File) -> dict[str, Any]:
    # PdfReader performs many seeks. One bounded read avoids hundreds of
    # object-store range requests for a single metadata scan.
    with file.open() as stream:
        return scan_pdf_metadata(stream.read())


@daft.func(return_dtype=_TEXT_METADATA)
def _text_metadata(file: daft.File, logical_path: str) -> dict[str, str | int | bool]:
    with file.open() as stream:
        return scan_text_metadata(cast(BinaryIO, stream), logical_path)


@daft.func(return_dtype=DataType.bool())
def _is_diff(logical_path: str) -> bool:
    return logical_path.lower().endswith((".diff", ".patch"))


@daft.func(return_dtype=_DIFF_METADATA)
def _diff_metadata(file: daft.File) -> dict[str, str | int]:
    with file.open() as stream:
        return scan_diff_metadata(cast(BinaryIO, stream))


class FileIngestionPipeline:
    """Compose the complete lazy file occurrence and metadata graph.

    The application configures this object once per artifact submission. The
    pipeline knows files and Daft only: it has no world, run, catalog, lock, or
    publication state.
    """

    def __init__(
        self,
        *,
        io_config: IOConfig | None = None,
        object_uri: str = "",
        local_object_root: str | None = None,
        max_artifact_bytes: int = 256 * 1024 * 1024,
        max_connections: int = 32,
    ) -> None:
        self.io_config = io_config
        self.object_uri = object_uri.rstrip("/")
        self.local_object_root = local_object_root
        self.max_artifact_bytes = max_artifact_bytes
        self.max_connections = max_connections

    def scan(
        self,
        path: str | list[str],
        *,
        source_root: str = "",
        logical_root: str = "",
        logical_path: str = "",
    ) -> DataFrame:
        """Build one lazy row per file with identity and common metadata."""

        files = daft.from_files(path, io_config=self.io_config)
        files = files.with_column("artifact_id", uuid(version="v7").cast(DataType.string()))
        files = files.with_column("source_uri", file_path(col("file")))
        files = files.with_column(
            "logical_path",
            _logical_path(
                col("source_uri"),
                lit(source_root),
                lit(logical_root),
                lit(logical_path),
            ),
        )
        files = files.with_column("ingested_at", _uuid7_timestamp(col("artifact_id")))
        files = files.with_column("mime_type", _mime_type(col("file")))
        files = files.with_column(
            "media_family",
            _media_family(col("mime_type"), col("logical_path")),
        )
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

    def persist(self, files: DataFrame) -> DataFrame:
        """Copy submitted bytes to the configured content-addressed namespace."""

        if not self.object_uri:
            raise ValueError("file ingestion persistence requires an object_uri")
        if self.local_object_root is not None:
            return files.with_column(
                "object_uri",
                _copy_local_object(
                    col("file"),
                    col("sha256"),
                    col("size_bytes"),
                    lit(self.local_object_root),
                    lit(self.max_artifact_bytes),
                ),
            )

        folders = files.with_column(
            "_object_folder",
            daft_format(
                f"{self.object_uri}/objects/sha256/{{}}/{{}}",
                col("sha256").left(2),
                col("sha256"),
            ),
        )
        folders = folders.with_column(
            "_object_bytes",
            _read_bounded(
                col("file"),
                col("size_bytes"),
                lit(self.max_artifact_bytes),
            ),
        )
        return folders.with_column(
            "object_uri",
            upload(
                col("_object_bytes"),
                col("_object_folder"),
                max_connections=self.max_connections,
                io_config=self.io_config,
            ),
        ).exclude("_object_folder", "_object_bytes")

    def reopen(self, files: DataFrame) -> DataFrame:
        """Bind specialized scanners to the immutable stored object."""

        return files.with_column(
            "file",
            daft_file(col("object_uri"), io_config=self.io_config),
        )

    def specialized_indexes(
        self,
        files: DataFrame,
        *,
        media_families: set[str],
        include_diff: bool,
    ) -> tuple[tuple[str, DataFrame], ...]:
        """Build only the typed index branches present in this submission."""

        indexes: list[tuple[str, DataFrame]] = []
        if "audio" in media_families:
            indexes.append((ARTIFACT_AUDIO, self.audio_index(files)))
        if "image" in media_families:
            indexes.append((ARTIFACT_IMAGES, self.image_index(files)))
        if "pdf" in media_families:
            indexes.append((ARTIFACT_PDF, self.pdf_index(files)))
        if "text" in media_families:
            indexes.append((ARTIFACT_TEXT, self.text_index(files)))
        if "video" in media_families:
            indexes.append((ARTIFACT_VIDEO, self.video_index(files)))
        if include_diff:
            indexes.append((ARTIFACT_DIFF, self.diff_index(files)))
        return tuple(indexes)

    @staticmethod
    def image_index(files: DataFrame) -> DataFrame:
        images = files.where(
            files["media_family"] == "image"  # ty: ignore[invalid-argument-type]
        )
        images = images.with_column("_image", image_file(col("file")))
        images = images.with_column("_metadata", image_file_metadata(col("_image")))
        return images.select("artifact_id", col("_metadata").unnest())

    @staticmethod
    def audio_index(files: DataFrame) -> DataFrame:
        audio = files.where(
            files["media_family"] == "audio"  # ty: ignore[invalid-argument-type]
        )
        audio = audio.with_column("_audio", audio_file(col("file")))
        audio = audio.with_column("_metadata", audio_metadata(col("_audio")))
        audio = audio.select("artifact_id", col("_metadata").unnest())
        return audio.with_column("duration_seconds", col("frames") / col("sample_rate"))

    @staticmethod
    def video_index(files: DataFrame) -> DataFrame:
        video = files.where(
            files["media_family"] == "video"  # ty: ignore[invalid-argument-type]
        )
        video = video.with_column("_video", video_file(col("file")))
        video = video.with_column("_metadata", video_metadata(col("_video")))
        video = video.select("artifact_id", col("_metadata").unnest())
        return video.with_column("duration_seconds", col("frame_count") / col("fps"))

    @staticmethod
    def pdf_index(files: DataFrame) -> DataFrame:
        pdfs = files.where(
            files["media_family"] == "pdf"  # ty: ignore[invalid-argument-type]
        )
        pdfs = pdfs.with_column("_metadata", _pdf_metadata(col("file")))
        return pdfs.select("artifact_id", col("_metadata").unnest())

    @staticmethod
    def text_index(files: DataFrame) -> DataFrame:
        text = files.where(
            files["media_family"] == "text"  # ty: ignore[invalid-argument-type]
        )
        text = text.with_column(
            "_metadata",
            _text_metadata(col("file"), col("logical_path")),
        )
        return text.select("artifact_id", col("_metadata").unnest())

    @staticmethod
    def diff_index(files: DataFrame) -> DataFrame:
        diffs = files.where(_is_diff(col("logical_path")))
        diffs = diffs.with_column("_metadata", _diff_metadata(col("file")))
        return diffs.select("artifact_id", col("_metadata").unnest())

    @staticmethod
    def common_index(files: DataFrame) -> DataFrame:
        """Project the common visibility root after all typed branches."""

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
