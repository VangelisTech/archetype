# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for `evals.graders.llm_judge`.

These tests inject a fake `prompt_fn` that returns canned struct rows
so the grader can be exercised without an API key. A live smoke test
against a real provider runs only when `OPENAI_API_KEY` is set, per
the dogfood-as-skipif convention.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest
from daft import DataType, lit
from pydantic import ValidationError

from evals.graders import JudgeCase, JudgeVerdict, llm_judge


_VERDICT_DTYPE = DataType.struct(
    {
        "passed": DataType.bool(),
        "score": DataType.float64(),
        "reasoning": DataType.string(),
    }
)


def _verdict_struct(passed: bool, score: float, reasoning: str):
    """Build a daft struct literal matching JudgeVerdict's shape."""
    return lit(
        {"passed": passed, "score": score, "reasoning": reasoning},
    ).cast(_VERDICT_DTYPE)


def _fake_prompt_fn(verdicts: list[dict[str, Any]]) -> Callable[..., Any]:
    """Return a prompt_fn that emits one canned verdict per case, in order.

    The fake ignores its `messages` argument and returns the i-th verdict
    for the i-th row. Implemented via a Daft `when/otherwise` ladder keyed
    on `criterion` so the result is a real columnar expression — no UDF.
    """

    def fake(messages, *, return_format=None, **kwargs):  # noqa: ANN001
        del messages, return_format, kwargs
        # Build a deterministic row index column inside the plan by
        # cycling through verdicts in registration order. We rely on the
        # caller building one row per case and `criterion` being unique.
        from daft import col
        from daft.functions import when

        if not verdicts:
            raise AssertionError("fake prompt_fn called with no verdicts queued")

        def expr_for(i: int):
            v = verdicts[i]
            return _verdict_struct(v["passed"], v["score"], v["reasoning"])

        # Match by criterion text; tests must use distinct criteria.
        criteria = [v.get("_criterion") for v in verdicts]
        if any(c is None for c in criteria):
            raise AssertionError("fake verdicts must each carry _criterion")

        chain = expr_for(len(verdicts) - 1)
        for i in range(len(verdicts) - 2, -1, -1):
            chain = when(col("criterion") == criteria[i], expr_for(i)).otherwise(chain)
        return chain

    return fake


# ---------------------------------------------------------------------------
# Verdict / case validation
# ---------------------------------------------------------------------------


def test_judge_verdict_rejects_score_above_one() -> None:
    with pytest.raises(ValidationError):
        JudgeVerdict(passed=True, score=1.5, reasoning="too high")


def test_judge_verdict_rejects_score_below_zero() -> None:
    with pytest.raises(ValidationError):
        JudgeVerdict(passed=False, score=-0.1, reasoning="too low")


def test_llm_judge_rejects_empty_cases() -> None:
    with pytest.raises(ValueError, match="at least one case"):
        llm_judge([], prompt_fn=_fake_prompt_fn([]))


def test_llm_judge_rejects_threshold_outside_unit_interval() -> None:
    with pytest.raises(ValueError, match="pass_threshold"):
        llm_judge(
            [JudgeCase(criterion="c", subject="s")],
            pass_threshold=1.5,
            prompt_fn=_fake_prompt_fn([]),
        )


def test_llm_judge_rejects_dict_case_missing_keys() -> None:
    with pytest.raises(ValueError, match="missing"):
        llm_judge(
            [{"criterion": "only criterion"}],  # type: ignore[list-item]
            prompt_fn=_fake_prompt_fn([]),
        )


# ---------------------------------------------------------------------------
# Aggregation behaviour with injected verdicts
# ---------------------------------------------------------------------------


def test_llm_judge_passes_when_all_cases_pass_and_mean_meets_threshold() -> None:
    cases = [
        JudgeCase(criterion="C1", subject="good text"),
        JudgeCase(criterion="C2", subject="also good"),
    ]
    verdicts = [
        {"_criterion": "C1", "passed": True, "score": 0.9, "reasoning": "solid"},
        {"_criterion": "C2", "passed": True, "score": 0.8, "reasoning": "also solid"},
    ]
    result = llm_judge(cases, pass_threshold=0.7, prompt_fn=_fake_prompt_fn(verdicts))

    assert result.passed is True
    assert result.score == pytest.approx(0.85)
    assert result.details == ""
    assert result.grader_name == "llm_judge"


def test_llm_judge_fails_when_any_case_fails_even_if_mean_high() -> None:
    cases = [
        JudgeCase(criterion="C1", subject="good"),
        JudgeCase(criterion="C2", subject="terrible"),
    ]
    verdicts = [
        {"_criterion": "C1", "passed": True, "score": 1.0, "reasoning": "great"},
        {"_criterion": "C2", "passed": False, "score": 0.6, "reasoning": "missed point"},
    ]
    result = llm_judge(cases, pass_threshold=0.5, prompt_fn=_fake_prompt_fn(verdicts))

    assert result.passed is False
    assert result.score == pytest.approx(0.8)
    assert "missed point" in result.details


def test_llm_judge_fails_when_mean_below_threshold() -> None:
    cases = [
        JudgeCase(criterion="C1", subject="meh"),
        JudgeCase(criterion="C2", subject="meh too"),
    ]
    verdicts = [
        {"_criterion": "C1", "passed": True, "score": 0.4, "reasoning": "weak pass"},
        {"_criterion": "C2", "passed": True, "score": 0.3, "reasoning": "weak pass"},
    ]
    result = llm_judge(cases, pass_threshold=0.5, prompt_fn=_fake_prompt_fn(verdicts))

    assert result.passed is False
    assert result.score == pytest.approx(0.35)


def test_llm_judge_accepts_dict_cases() -> None:
    cases = [
        {"criterion": "C1", "subject": "ok"},
        {"criterion": "C2", "subject": "ok2"},
    ]
    verdicts = [
        {"_criterion": "C1", "passed": True, "score": 1.0, "reasoning": "fine"},
        {"_criterion": "C2", "passed": True, "score": 1.0, "reasoning": "fine"},
    ]
    result = llm_judge(cases, prompt_fn=_fake_prompt_fn(verdicts))

    assert result.passed is True
    assert result.score == pytest.approx(1.0)


def test_llm_judge_forwards_model_and_provider_to_prompt_fn() -> None:
    seen: dict[str, Any] = {}

    def capturing_fn(messages, *, return_format=None, **kwargs):  # noqa: ANN001
        del messages, return_format
        seen.update(kwargs)
        return _verdict_struct(True, 1.0, "ok")

    llm_judge(
        [JudgeCase(criterion="only", subject="s")],
        model="claude-haiku-4-5-20251001",
        provider="anthropic",
        prompt_fn=capturing_fn,
    )
    assert seen["model"] == "claude-haiku-4-5-20251001"
    assert seen["provider"] == "anthropic"


# ---------------------------------------------------------------------------
# Live smoke test — only when OPENAI_API_KEY is set
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set — skipping live llm_judge smoke test",
)
def test_llm_judge_live_smoke() -> None:
    """Sanity check against a real provider: clear-pass case must pass."""
    result = llm_judge(
        [
            JudgeCase(
                criterion="The response is the literal string 'hello'.",
                subject="hello",
            ),
        ],
        model="gpt-5-mini",
        pass_threshold=0.5,
    )
    assert result.passed is True
    assert result.score >= 0.5
