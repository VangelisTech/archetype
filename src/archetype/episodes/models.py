# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models owned by the episodes family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict

from archetype.core.component import Component
from archetype.core.config import JsonUUID, StorageConfig
from archetype.episodes.contracts import ClaudeTranscriptSource, TrajectorySelection
from archetype.evaluation.contracts import TrajectoryGrader


class _EpisodeOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class IngestClaudeTranscript(_EpisodeOperation):
    """Sanitize and ingest one Claude transcript source."""

    operation: Literal["ingest_claude_transcript"] = "ingest_claude_transcript"
    world_id: str | JsonUUID
    source: ClaudeTranscriptSource
    storage_config: StorageConfig | None = None


class QueryTranscriptRows(_EpisodeOperation):
    """Read normalized transcript rows for one world."""

    operation: Literal["query_transcript_rows"] = "query_transcript_rows"
    world_id: str | JsonUUID
    storage_config: StorageConfig | None = None


class QueryTrajectory(_EpisodeOperation):
    """Read one typed trajectory table with optional row filters."""

    operation: Literal["query_trajectory"] = "query_trajectory"
    component: type[Component]
    world_id: str | JsonUUID
    run_id: str | JsonUUID
    storage_config: StorageConfig | None = None
    selection: TrajectorySelection | None = None
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None


class GradeTrajectory(_EpisodeOperation):
    """Query one typed trajectory table and run grader callbacks."""

    operation: Literal["grade_trajectory"] = "grade_trajectory"
    component: type[Component]
    world_id: str | JsonUUID
    run_id: str | JsonUUID
    graders: tuple[TrajectoryGrader, ...]
    storage_config: StorageConfig | None = None
    selection: TrajectorySelection | None = None
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None


def summarize_episode_operation(
    operation: (IngestClaudeTranscript | QueryTranscriptRows | QueryTrajectory | GradeTrajectory),
) -> Mapping[str, Any]:
    """Return bounded routing identity without rows, source paths, or graders."""

    return {
        "operation": operation.operation,
        "world_id": str(operation.world_id),
    }


__all__ = [
    "GradeTrajectory",
    "IngestClaudeTranscript",
    "QueryTrajectory",
    "QueryTranscriptRows",
    "summarize_episode_operation",
]
