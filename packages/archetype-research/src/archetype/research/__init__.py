# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Research values, generic ledger state, and resumable optimization workflow."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from archetype.research.models import (
    AutoResearchConfig,
    AutoResearchResult,
    CandidatePreparer,
    Evaluation,
    EvaluationResult,
    Evaluator,
    IterationResult,
    ResearchCandidateContext,
)
from archetype.research.runtime import Research

if TYPE_CHECKING:
    from archetype.research.components import (
        BranchHead,
        Experiment,
        Result,
        Run,
        RunStatus,
    )

__all__ = [
    "AutoResearchConfig",
    "AutoResearchResult",
    "BranchHead",
    "CandidatePreparer",
    "Evaluation",
    "EvaluationResult",
    "Evaluator",
    "Experiment",
    "IterationResult",
    "ResearchCandidateContext",
    "Research",
    "Result",
    "Run",
    "RunStatus",
]

_COMPONENT_EXPORTS = frozenset(
    {
        "BranchHead",
        "Experiment",
        "Result",
        "Run",
        "RunStatus",
    }
)


def __getattr__(name: str) -> Any:
    if name in _COMPONENT_EXPORTS:
        from archetype.research import components

        value = getattr(components, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
