# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Core eval types: EvalCase, EvalResult, Grader.

Follows the pattern from Anthropic's eval guide:
- EvalCase: what to test (input + expected)
- Grader: how to score (binary, numeric, rubric)
- EvalResult: the outcome (score + metadata)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Tier(StrEnum):
    UNIT = "unit"
    INTEGRATION = "integration"
    END_TO_END = "end_to_end"


class Status(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"


@dataclass
class EvalCase:
    """A single eval case: input, expected output, and metadata."""

    name: str
    input: dict[str, Any]
    expected: Any
    tier: Tier
    tags: list[str] = field(default_factory=list)


@dataclass
class EvalResult:
    """Outcome of running and grading a single eval case."""

    case_name: str
    tier: Tier
    status: Status
    score: float  # 0.0 = fail, 1.0 = pass (or continuous for rubric)
    elapsed_s: float = 0.0
    details: str = ""
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == Status.PASS


# ---------------------------------------------------------------------------
# Graders
# ---------------------------------------------------------------------------


def binary(
    actual: Any, expected: Any, *, case_name: str = "", tier: Tier = Tier.UNIT
) -> EvalResult:
    """Binary grader: 1.0 if actual == expected, 0.0 otherwise."""
    passed = actual == expected
    return EvalResult(
        case_name=case_name,
        tier=tier,
        status=Status.PASS if passed else Status.FAIL,
        score=1.0 if passed else 0.0,
        details=f"expected={expected!r}, actual={actual!r}" if not passed else "",
    )


def numeric_close(
    actual: float,
    expected: float,
    *,
    tolerance: float = 0.01,
    case_name: str = "",
    tier: Tier = Tier.UNIT,
) -> EvalResult:
    """Numeric grader: passes if |actual - expected| <= tolerance."""
    diff = abs(actual - expected)
    passed = diff <= tolerance
    return EvalResult(
        case_name=case_name,
        tier=tier,
        status=Status.PASS if passed else Status.FAIL,
        score=1.0 if passed else max(0.0, 1.0 - diff),
        details="" if passed else f"expected={expected}, actual={actual}, diff={diff}",
    )


def rubric(
    checks: dict[str, bool],
    *,
    case_name: str = "",
    tier: Tier = Tier.END_TO_END,
    threshold: float = 1.0,
) -> EvalResult:
    """Rubric grader: score = fraction of checks that pass.

    Passes if score >= threshold (default: all checks must pass).
    """
    total = len(checks)
    passed_count = sum(1 for v in checks.values() if v)
    score = passed_count / total if total > 0 else 0.0
    failed = {k for k, v in checks.items() if not v}
    return EvalResult(
        case_name=case_name,
        tier=tier,
        status=Status.PASS if score >= threshold else Status.FAIL,
        score=score,
        details=f"failed checks: {failed}" if failed else "",
    )
