# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models for Mission transcript and trajectory evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict

from archetype.core.component import Component
from archetype.core.config import JsonUUID, StorageConfig
from archetype.evaluation.contracts import TrajectoryGrader
from archetype.missions.trajectories.contracts import (
    ClaudeTranscriptSource,
    TrajectorySelection,
)


class _TrajectoryOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class IngestClaudeTranscript(_TrajectoryOperation):
    """Sanitize and ingest one Claude transcript source."""

    operation: Literal["ingest_claude_transcript"] = "ingest_claude_transcript"
    world_id: str | JsonUUID
    source: ClaudeTranscriptSource
    storage_config: StorageConfig | None = None


class QueryTranscriptRows(_TrajectoryOperation):
    """Read normalized transcript rows for one world."""

    operation: Literal["query_transcript_rows"] = "query_transcript_rows"
    world_id: str | JsonUUID
    storage_config: StorageConfig | None = None


class QueryTrajectory(_TrajectoryOperation):
    """Read one typed trajectory table with optional row filters."""

    operation: Literal["query_trajectory"] = "query_trajectory"
    component: type[Component]
    world_id: str | JsonUUID
    run_id: str | JsonUUID
    storage_config: StorageConfig | None = None
    selection: TrajectorySelection | None = None
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None


class GradeTrajectory(_TrajectoryOperation):
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


def summarize_trajectory_operation(
    operation: IngestClaudeTranscript | QueryTranscriptRows | QueryTrajectory | GradeTrajectory,
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
    "summarize_trajectory_operation",
]
