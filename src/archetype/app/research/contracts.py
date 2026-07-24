# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility exports for research value contracts.

PR-6 deletes this application-owned path after every consumer imports the
family-owned values directly.  Keep these as imports, never duplicate types.
"""

from archetype.research.contracts import (
    AutoResearchConfig,
    AutoResearchResult,
    CandidateContext,
    CandidatePreparer,
    Evaluation,
    EvaluationResult,
    Evaluator,
    IterationResult,
)

__all__ = [
    "AutoResearchConfig",
    "AutoResearchResult",
    "CandidateContext",
    "CandidatePreparer",
    "Evaluation",
    "EvaluationResult",
    "Evaluator",
    "IterationResult",
]
