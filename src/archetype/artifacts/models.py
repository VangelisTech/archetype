# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models owned by the artifact family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict

from archetype.artifacts.contracts import ArtifactSource
from archetype.core.config import JsonUUID, StorageConfig


class _ArtifactOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class IngestArtifacts(_ArtifactOperation):
    """Ingest declared artifact sources for one world."""

    operation: Literal["ingest_artifacts"] = "ingest_artifacts"
    world_id: str | JsonUUID
    sources: tuple[ArtifactSource, ...]
    storage_config: StorageConfig | None = None


class QueryArtifacts(_ArtifactOperation):
    """Read the common artifact index for one world."""

    operation: Literal["query_artifacts"] = "query_artifacts"
    world_id: str | JsonUUID
    storage_config: StorageConfig | None = None


def summarize_artifact_operation(
    operation: IngestArtifacts | QueryArtifacts,
) -> Mapping[str, Any]:
    """Return bounded routing identity without source or storage contents."""

    return {
        "operation": operation.operation,
        "world_id": str(operation.world_id),
    }


__all__ = [
    "IngestArtifacts",
    "QueryArtifacts",
    "summarize_artifact_operation",
]
