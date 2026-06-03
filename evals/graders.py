# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Code-based graders for agent evals.

Per the Anthropic eval guide, code-based graders are:
- Fast, cheap, objective, reproducible, easy to debug
- Best for deterministic verification of outcomes

These grade the OUTCOME, not the path the agent took.

`llm_judge` is the lone non-deterministic grader. It uses
`daft.functions.prompt` so a single judging pass is one columnar plan
across all (criterion, subject) cases — no per-row HTTP loops, no
custom client wrappers.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import daft
from daft import col
from daft.functions import prompt as _daft_prompt
from pydantic import BaseModel, Field

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


_DEFAULT_JUDGE_SYSTEM = (
    "You are a strict but fair evaluator. For each case you receive a "
    "criterion and the subject text it should be judged against. "
    "Respond with a boolean `passed`, a `score` in [0.0, 1.0], and a "
    "one-sentence `reasoning`. Do not add any other fields."
)


class JudgeVerdict(BaseModel):
    """Structured verdict the judge model returns per case."""

    passed: bool
    score: float = Field(ge=0.0, le=1.0)
    reasoning: str


class JudgeCase(BaseModel):
    """One (criterion, subject) pair to send to the judge."""

    criterion: str
    subject: str


def llm_judge(
    cases: Sequence[JudgeCase | dict[str, str]],
    *,
    model: str = "gpt-5-mini",
    provider: str | None = None,
    system_message: str = _DEFAULT_JUDGE_SYSTEM,
    pass_threshold: float = 0.5,
    name: str = "llm_judge",
    prompt_fn: Callable[..., Any] | None = None,
) -> GraderResult:
    """LLM-as-judge grader powered by `daft.functions.prompt`.

    All cases are judged in a single columnar pass. Returns one
    aggregated `GraderResult`: `passed` iff the mean per-case score is
    >= `pass_threshold` AND every per-case verdict marked `passed=True`;
    `score` is the mean per-case score.

    Pass `prompt_fn` to inject a fake for tests — it must accept the
    same kwargs as `daft.functions.prompt` and return a Daft expression
    yielding a struct column with fields {passed, score, reasoning}.
    """
    if not cases:
        raise ValueError("llm_judge requires at least one case")
    if not 0.0 <= pass_threshold <= 1.0:
        raise ValueError(f"pass_threshold must be in [0.0, 1.0], got {pass_threshold}")

    rows = [c.model_dump() if isinstance(c, JudgeCase) else dict(c) for c in cases]
    for i, row in enumerate(rows):
        if "criterion" not in row or "subject" not in row:
            raise ValueError(f"case {i} missing 'criterion' or 'subject': {row!r}")

    fn = prompt_fn or _daft_prompt
    df = daft.from_pylist(rows)
    message = (
        daft.lit("Criterion:\n")
        + col("criterion")
        + daft.lit("\n\nSubject:\n")
        + col("subject")
    )
    verdict_expr = fn(
        message,
        return_format=JudgeVerdict,
        system_message=system_message,
        provider=provider,
        model=model,
    )
    rows_out = (
        df.with_column("verdict", verdict_expr)
        .select("criterion", "verdict")
        .collect()
        .to_pylist()
    )

    if len(rows_out) != len(rows):
        raise RuntimeError(
            f"judge returned {len(rows_out)} verdicts for {len(rows)} cases"
        )

    per_case: list[tuple[str, dict[str, Any]]] = []
    for case_row, out in zip(rows, rows_out):
        verdict = out["verdict"]
        per_case.append((case_row["criterion"], verdict))

    mean_score = sum(v["score"] for _, v in per_case) / len(per_case)
    all_passed = all(v["passed"] for _, v in per_case)
    passed = all_passed and mean_score >= pass_threshold

    if passed:
        details = ""
    else:
        failures = [
            f"{crit[:40]!r}: score={v['score']:.2f} passed={v['passed']} — {v['reasoning']}"
            for crit, v in per_case
            if not v["passed"] or v["score"] < pass_threshold
        ]
        details = "; ".join(failures)

    return GraderResult(grader_name=name, passed=passed, score=mean_score, details=details)


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
