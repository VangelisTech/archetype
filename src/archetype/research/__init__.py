# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reusable research values, ledger state, and runner-state decoding."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from archetype.research.models import (
    AutoResearchConfig,
    AutoResearchResult,
    CandidateContext,
    CandidatePreparer,
    Evaluation,
    EvaluationResult,
    Evaluator,
    IterationResult,
    ResearchCandidateContext,
)

if TYPE_CHECKING:
    from archetype.research.components import (
        BranchHead,
        Experiment,
        Result,
        Run,
        RunStatus,
    )
    from archetype.research.loaders import load_runner_state_db

__all__ = [
    "AutoResearchConfig",
    "AutoResearchResult",
    "BranchHead",
    "CandidateContext",
    "CandidatePreparer",
    "Evaluation",
    "EvaluationResult",
    "Evaluator",
    "Experiment",
    "IterationResult",
    "ResearchCandidateContext",
    "Result",
    "Run",
    "RunStatus",
    "load_runner_state_db",
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
    if name == "load_runner_state_db":
        from archetype.research.loaders import load_runner_state_db

        globals()[name] = load_runner_state_db
        return load_runner_state_db
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
