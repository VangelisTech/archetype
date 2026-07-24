# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models owned by the evaluation family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Literal

from daft import DataFrame
from pydantic import BaseModel, ConfigDict

from archetype.core.component import Component
from archetype.core.config import JsonUUID, StorageConfig
from archetype.evaluation.contracts import (
    GraderContract,
    TrajectoryGrader,
)


class _EvaluationOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class RunGraders(_EvaluationOperation):
    """Run ephemeral grader callbacks over one lazy frame."""

    operation: Literal["run_graders"] = "run_graders"
    df: DataFrame
    graders: tuple[TrajectoryGrader, ...]


class Evaluate(_EvaluationOperation):
    """Evaluate and persist a receipt for one pinned world snapshot."""

    operation: Literal["evaluate"] = "evaluate"
    world_id: str | JsonUUID
    components: tuple[type[Component], ...]
    contract: GraderContract
    grader: TrajectoryGrader
    evaluation_id: str
    storage_config: StorageConfig | None = None
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None


def summarize_evaluation_operation(
    operation: _EvaluationOperation,
) -> Mapping[str, Any]:
    """Return bounded routing identity without frames, callbacks, or evidence."""

    summary: dict[str, Any] = {"operation": operation.operation}
    if isinstance(operation, Evaluate):
        summary["world_id"] = str(operation.world_id)
    return summary


__all__ = [
    "Evaluate",
    "RunGraders",
    "summarize_evaluation_operation",
]
