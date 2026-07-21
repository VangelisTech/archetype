# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application execution boundary for file discovery and object persistence."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from glob import has_magic
from pathlib import Path

import daft
from daft import DataFrame, DataType, col, lit
from daft.functions import format as daft_format
from daft.functions import upload

from archetype._storage_uri import local_storage_path
from archetype.artifacts.contracts import ArtifactSource, ArtifactStoreConfig
from archetype.ingestion.files import scan_files

_COPY_BUFFER = 1 << 20


@dataclass(frozen=True)
class SourcePlan:
    """Resolved discovery coordinates for one declared source."""

    index: int
    source: ArtifactSource
    path: str
    source_root: str


def _local_source_value(source_uri: str) -> Path | None:
    return local_storage_path(source_uri)


def plan_source(index: int, source: ArtifactSource) -> SourcePlan:
    """Resolve local directory semantics without performing content I/O."""

    local = _local_source_value(source.source_uri)
    if local is None:
        path = source.source_uri.rstrip("/") + "/**/*" if source.recursive else source.source_uri
        return SourcePlan(index, source, path, source.source_uri)

    if not has_magic(source.source_uri) and local.exists():
        if local.is_dir():
            if not source.recursive:
                raise IsADirectoryError(
                    f"artifact source is a directory but recursive=False: {local}"
                )
            return SourcePlan(index, source, str(local / "**/*"), str(local))
        if source.recursive:
            raise NotADirectoryError(f"artifact source is a file but recursive=True: {local}")
        return SourcePlan(index, source, str(local), str(local.parent))

    root = source.source_uri
    if has_magic(root):
        wildcard = min(
            position
            for position in (root.find("*"), root.find("?"), root.find("["))
            if position >= 0
        )
        prefix = root[:wildcard]
        root = str(Path(prefix) if prefix.endswith("/") else Path(prefix).parent)
    return SourcePlan(index, source, source.source_uri, root)


def scan_sources(
    sources: tuple[ArtifactSource, ...],
    *,
    io_config,
) -> tuple[DataFrame, tuple[SourcePlan, ...]]:
    """Compose all source scans into one lazy, uniformly typed frame."""

    plans = tuple(plan_source(index, source) for index, source in enumerate(sources))
    frames = []
    for plan in plans:
        frame = scan_files(
            plan.path,
            source_root=plan.source_root,
            logical_root=plan.source.logical_root,
            logical_path=plan.source.logical_path,
            io_config=io_config,
        ).with_column("_source_index", lit(plan.index))
        frames.append(frame)
    return daft.concat(frames), plans


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


def persist_objects(
    files: DataFrame,
    *,
    object_uri: str,
    config: ArtifactStoreConfig,
) -> DataFrame:
    """Copy files to the content-addressed object namespace."""

    local = local_storage_path(object_uri)
    if local is not None:
        return files.with_column(
            "object_uri",
            _copy_local_object(
                col("file"),
                col("sha256"),
                col("size_bytes"),
                lit(str(local)),
                lit(config.max_artifact_bytes),
            ),
        )

    folders = files.with_column(
        "_object_folder",
        daft_format(
            f"{object_uri.rstrip('/')}/objects/sha256/{{}}/{{}}",
            col("sha256").left(2),
            col("sha256"),
        ),
    )
    folders = folders.with_column(
        "_object_bytes",
        _read_bounded(
            col("file"),
            col("size_bytes"),
            lit(config.max_artifact_bytes),
        ),
    )
    return folders.with_column(
        "object_uri",
        upload(
            col("_object_bytes"),
            col("_object_folder"),
            max_connections=config.max_connections,
            io_config=config.io_config,
        ),
    ).exclude("_object_folder", "_object_bytes")
