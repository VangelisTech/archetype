# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""One readable lazy Daft graph for artifact discovery, storage, and metadata."""

from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import UTC, datetime
from glob import has_magic
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast
from urllib.parse import unquote, urlsplit

import daft
import xxhash
from daft import DataFrame, DataType, col, lit
from daft.file.file import BUFFER_COPY
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

from archetype.artifacts.models import ArtifactSource
from archetype.artifacts.scanners import (
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
_PERSISTED_OBJECT = DataType.struct(
    {
        "sha256": DataType.string(),
        "xxhash3_64": DataType.string(),
        "size_bytes": DataType.int64(),
        "object_uri": DataType.string(),
    }
)
_MATERIALIZED_OBJECT = DataType.struct(
    {
        "sha256": DataType.string(),
        "xxhash3_64": DataType.string(),
        "size_bytes": DataType.int64(),
        "object_bytes": DataType.binary(),
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
    file: daft.File,
    logical_path: str,
) -> str:
    """Apply portable workflow naming after Daft resolves the physical file."""

    value = logical_path or unquote(file.name)
    normalized = value.strip().replace("\\", "/").strip("/")
    path = PurePosixPath(normalized)
    if not normalized or path.is_absolute() or ".." in path.parts:
        raise ValueError("logical paths must be non-empty portable relative paths")
    return path.as_posix()


@daft.func(return_dtype=DataType.bool())
def _file_exists(file: daft.File) -> bool:
    """Keep exact-source existence checks inside the Daft discovery graph."""

    return file.exists()


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


def _copy_and_hash(file: daft.File, target: BinaryIO) -> dict[str, str | int]:
    """Copy one file while deriving both hashes and size from those same bytes."""

    sha256 = hashlib.sha256()
    fast = xxhash.xxh3_64()
    size = 0
    with file.open(buffer_size=BUFFER_COPY) as source:
        while chunk := source.read(BUFFER_COPY):
            sha256.update(chunk)
            fast.update(chunk)
            size += len(chunk)
            target.write(chunk)
    return {
        "sha256": sha256.hexdigest(),
        "xxhash3_64": fast.hexdigest(),
        "size_bytes": size,
    }


def _persist_local_file(
    file: daft.File,
    root: str,
) -> dict[str, str | int]:
    """Stage, identify, and atomically publish one local object in one read."""

    staging = Path(root) / "objects" / ".staging"
    staging.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="artifact-",
            dir=staging,
            delete=False,
        ) as target:
            temporary = target.name
            identity = _copy_and_hash(file, cast(BinaryIO, target))

        sha256 = str(identity["sha256"])
        destination = Path(root) / "objects" / "sha256" / sha256[:2] / sha256
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, destination)
        temporary = None
        return identity | {"object_uri": destination.resolve().as_uri()}
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


@daft.func(return_dtype=_PERSISTED_OBJECT)
def _persist_local_object(file: daft.File, root: str) -> dict[str, str | int]:
    return _persist_local_file(file, root)


@daft.func(return_dtype=_MATERIALIZED_OBJECT)
def _materialize_remote_object(file: daft.File) -> dict[str, str | int | bytes]:
    """Read once for Daft 0.7's Binary-only upload expression.

    Daft 0.7.19 has no public streaming File-to-File write API. Keep this
    limitation explicit until its writable File and multipart APIs land.
    """

    target = BytesIO()
    identity = _copy_and_hash(file, target)
    return identity | {"object_bytes": target.getvalue()}


@daft.func(return_dtype=_PDF_METADATA)
def _pdf_metadata(file: daft.File) -> dict[str, Any]:
    # PdfReader performs many seeks. One explicit in-memory materialization
    # avoids hundreds of object-store range requests for one metadata scan.
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

    The handler configures this object once per artifact submission. The
    pipeline knows files and Daft only: it has no world, run, catalog, lock, or
    publication state.
    """

    def __init__(
        self,
        *,
        io_config: IOConfig | None = None,
        object_uri: str = "",
        local_object_root: str | None = None,
        max_connections: int = 32,
    ) -> None:
        self.io_config = io_config
        self.object_uri = object_uri.rstrip("/")
        self.local_object_root = local_object_root
        self.max_connections = max_connections

    def scan(
        self,
        path: str | list[str],
        *,
        pattern: bool = False,
        logical_path: str = "",
    ) -> DataFrame:
        """Build one lazy row per file with identity and common metadata."""

        if pattern:
            discovered = daft.from_glob_path(path, io_config=self.io_config)
            files = discovered.select(
                daft_file(col("path"), io_config=self.io_config).alias("file")
            )
        else:
            paths = [path] if isinstance(path, str) else path
            files = daft.from_pydict({"source_uri": paths}).with_column(
                "file",
                daft_file(col("source_uri"), io_config=self.io_config),
            )
            files = files.where(_file_exists(col("file"))).select("file")

        files = files.with_column("artifact_id", uuid(version="v7").cast(DataType.string()))
        files = files.with_column("source_uri", file_path(col("file")))
        files = files.with_column(
            "logical_path",
            _logical_path(col("file"), lit(logical_path)),
        )
        files = files.with_column("ingested_at", _uuid7_timestamp(col("artifact_id")))
        files = files.with_column("mime_type", _mime_type(col("file")))
        files = files.with_column(
            "media_family",
            _media_family(col("mime_type"), col("logical_path")),
        )
        return files.select(
            "file",
            "artifact_id",
            "ingested_at",
            "source_uri",
            "logical_path",
            "mime_type",
            "media_family",
        )

    def persist(self, files: DataFrame) -> DataFrame:
        """Copy submitted bytes to the configured content-addressed namespace."""

        if not self.object_uri:
            raise ValueError("file ingestion persistence requires an object_uri")
        if self.local_object_root is not None:
            return (
                files.with_column(
                    "_persisted_object",
                    _persist_local_object(col("file"), lit(self.local_object_root)),
                )
                .select("*", col("_persisted_object").unnest())
                .exclude("_persisted_object")
            )

        materialized = (
            files.with_column(
                "_materialized_object",
                _materialize_remote_object(col("file")),
            )
            .select("*", col("_materialized_object").unnest())
            .exclude("_materialized_object")
        )
        addressed = materialized.with_column(
            "_object_uri",
            daft_format(
                f"{self.object_uri}/objects/sha256/{{}}/{{}}",
                col("sha256").left(2),
                col("sha256"),
            ),
        )
        return addressed.with_column(
            "object_uri",
            upload(
                col("object_bytes"),
                col("_object_uri"),
                max_connections=self.max_connections,
                io_config=self.io_config,
            ),
        ).exclude("_object_uri", "object_bytes")

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


def _is_pattern(source_uri: str) -> bool:
    """Classify only URI path wildcards; signed-query ``?`` stays exact."""

    parsed = urlsplit(source_uri)
    return has_magic(parsed.path if parsed.scheme else source_uri)


def scan_sources(
    sources: tuple[ArtifactSource, ...],
    pipeline: FileIngestionPipeline,
) -> DataFrame:
    """Compose declared sources into one uniformly typed lazy scan."""

    # Daft 0.7's glob scan has no micro-partition when every pattern matches
    # zero files, so materializing that otherwise valid empty graph fails
    # before family-level required-source validation can run. A typed,
    # zero-row exact scan is the concat identity and keeps discovery lazy.
    frames = [pipeline.scan([], pattern=False).with_column("_source_index", lit(-1))]
    for index, source in enumerate(sources):
        frame = pipeline.scan(
            source.source_uri,
            pattern=_is_pattern(source.source_uri),
            logical_path=source.logical_path,
        ).with_column("_source_index", lit(index))
        frames.append(frame)
    return daft.concat(frames)


__all__ = [
    "ARTIFACT_AUDIO",
    "ARTIFACT_DIFF",
    "ARTIFACT_FILES",
    "ARTIFACT_IMAGES",
    "ARTIFACT_PDF",
    "ARTIFACT_TEXT",
    "ARTIFACT_VIDEO",
    "FileIngestionPipeline",
    "ingestion_time_for",
    "media_family_for",
    "scan_sources",
]
