# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Code-based graders for agent evals.

Per the Anthropic eval guide, code-based graders are:
- Fast, cheap, objective, reproducible, easy to debug
- Best for deterministic verification of outcomes

These grade the OUTCOME, not the path the agent took.
"""

from __future__ import annotations

from typing import Any

from evals.types import GraderResult


def exact_match(actual: Any, expected: Any, *, name: str = "exact_match") -> GraderResult:
    """Binary grader: passes if actual == expected."""
    passed = actual == expected
    return GraderResult(
        grader_name=name,
        passed=passed,
        score=1.0 if passed else 0.0,
        details="" if passed else f"expected={expected!r}, got={actual!r}",
    )


def state_check(checks: dict[str, bool], *, name: str = "state_check") -> GraderResult:
    """Outcome verification: passes if ALL state checks are True.

    Supports partial credit (score = fraction of checks passing).
    """
    total = len(checks)
    if total == 0:
        return GraderResult(grader_name=name, passed=True, score=1.0)
    passed_count = sum(1 for v in checks.values() if v)
    score = passed_count / total
    failed = {k for k, v in checks.items() if not v}
    return GraderResult(
        grader_name=name,
        passed=len(failed) == 0,
        score=score,
        details=f"failed: {failed}" if failed else "",
    )


def threshold(
    value: float,
    *,
    min_val: float | None = None,
    max_val: float | None = None,
    name: str = "threshold",
) -> GraderResult:
    """Numeric threshold grader: passes if value is within bounds."""
    passed = True
    if min_val is not None and value < min_val:
        passed = False
    if max_val is not None and value > max_val:
        passed = False
    return GraderResult(
        grader_name=name,
        passed=passed,
        score=1.0 if passed else 0.0,
        details="" if passed else f"value={value}, bounds=[{min_val}, {max_val}]",
    )


def raises(exc_type: type[Exception], fn, *args, name: str = "raises", **kwargs) -> GraderResult:
    """Grader that passes if fn(*args) raises the expected exception type."""
    try:
        fn(*args, **kwargs)
        return GraderResult(
            grader_name=name,
            passed=False,
            score=0.0,
            details=f"expected {exc_type.__name__} but no exception raised",
        )
    except exc_type:
        return GraderResult(grader_name=name, passed=True, score=1.0)
    except Exception as e:
        return GraderResult(
            grader_name=name,
            passed=False,
            score=0.0,
            details=f"expected {exc_type.__name__}, got {type(e).__name__}: {e}",
        )
