# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models owned by the research family."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict
from uuid_utils import UUID

from archetype.research.contracts import (
    AutoResearchConfig,
    CandidatePreparer,
    Evaluator,
    IterationResult,
)


class _ResearchOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class AutoResearch(_ResearchOperation):
    """Run one resumable autoresearch workflow from a base world."""

    operation: Literal["autoresearch"] = "autoresearch"
    world_id: str | UUID
    config: AutoResearchConfig
    evaluator: Evaluator
    prepare_candidate: CandidatePreparer | None = None
    lab_world_id: str | UUID | None = None
    on_iteration: Callable[[IterationResult], Any] | None = None


def summarize_research_operation(operation: AutoResearch) -> Mapping[str, Any]:
    """Return bounded routing identity without callbacks or candidate evidence."""

    return {
        "operation": operation.operation,
        "world_id": str(operation.world_id),
    }


__all__ = ["AutoResearch", "summarize_research_operation"]
