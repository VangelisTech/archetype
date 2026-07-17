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

    Rejects an empty check set. Otherwise, supports partial credit
    (score = fraction of checks passing).
    """
    if not checks:
        raise ValueError("state_check requires at least one check")

    total = len(checks)
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


def crap_score(
    complexity: float,
    coverage: float,
    *,
    max_crap: float = 30.0,
    max_complexity: float | None = None,
    name: str = "crap_score",
) -> GraderResult:
    """CRAP grader: passes if CRAP ≤ max_crap (and CC ≤ max_complexity, if set).

    CRAP(m) = comp(m)² · (1 − cov(m))³ + comp(m)

    `coverage` is a fraction in [0.0, 1.0]. Score is 1.0 on pass, otherwise
    1 − min(crap / max_crap, 2) / 2 so worse code gets a lower partial score
    (clamped to 0). Per Crap4J the canonical threshold is 30.
    """
    if not 0.0 <= coverage <= 1.0:
        raise ValueError(f"coverage must be in [0.0, 1.0], got {coverage}")
    if complexity < 0:
        raise ValueError(f"complexity must be >= 0, got {complexity}")
    if max_crap <= 0:
        raise ValueError(f"max_crap must be > 0, got {max_crap}")

    crap = complexity * complexity * (1.0 - coverage) ** 3 + complexity
    cc_ok = max_complexity is None or complexity <= max_complexity
    crap_ok = crap <= max_crap
    passed = cc_ok and crap_ok

    if passed:
        score = 1.0
    else:
        score = max(0.0, 1.0 - min(crap / max_crap, 2.0) / 2.0)

    if passed:
        details = ""
    else:
        parts = [f"crap={crap:.2f} (max {max_crap})", f"cc={complexity}"]
        if max_complexity is not None:
            parts[-1] += f" (max {max_complexity})"
        parts.append(f"cov={coverage:.2%}")
        details = ", ".join(parts)

    return GraderResult(grader_name=name, passed=passed, score=score, details=details)


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
