# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure discovery validation and occurrence projections shared by workflows."""

from typing import Any

from archetype.artifacts.models import ArtifactRef, ArtifactSource


def validate_discovery(columns: dict[str, list[Any]], sources: tuple[ArtifactSource, ...]) -> None:
    source_indexes = [int(value) for value in columns.get("_source_index", [])]
    logical_paths = [str(value) for value in columns.get("logical_path", [])]
    for index, source in enumerate(sources):
        if source.required and index not in source_indexes:
            raise FileNotFoundError(
                f"required artifact source matched no files: {source.source_uri}"
            )
    if len(logical_paths) != len(set(logical_paths)):
        raise ValueError("artifact sources resolve to duplicate logical paths")


def references(values: dict[str, list[Any]]) -> tuple[ArtifactRef, ...]:
    return tuple(
        ArtifactRef(
            artifact_id=str(artifact_id),
            logical_path=str(logical_path),
            uri=str(uri),
            sha256=str(sha256),
            xxhash3_64=str(fast_hash),
            media_type=str(media_type),
            size_bytes=int(size_bytes),
        )
        for artifact_id, logical_path, uri, sha256, fast_hash, media_type, size_bytes in zip(
            values["artifact_id"],
            values["logical_path"],
            values["object_uri"],
            values["sha256"],
            values["xxhash3_64"],
            values["mime_type"],
            values["size_bytes"],
            strict=True,
        )
    )
