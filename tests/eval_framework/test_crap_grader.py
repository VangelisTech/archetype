# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the CRAP code-quality grader."""

from __future__ import annotations

import pytest

from evals.graders import crap_score


def test_clean_code_passes() -> None:
    r = crap_score(complexity=4, coverage=0.95)
    assert r.passed
    assert r.score == 1.0
    assert r.details == ""


def test_borderline_passes_under_default_threshold() -> None:
    # cc=10, cov=0.80 → CRAP = 100·0.008 + 10 = 10.8 ≤ 30
    r = crap_score(complexity=10, coverage=0.80)
    assert r.passed


def test_high_complexity_low_coverage_fails() -> None:
    # cc=20, cov=0.20 → CRAP = 400·0.512 + 20 = 224.8
    r = crap_score(complexity=20, coverage=0.20)
    assert not r.passed
    assert r.score == 0.0
    assert "crap=224.80" in r.details
    assert "20.00%" in r.details


def test_complexity_cap_overrides_full_coverage() -> None:
    # cc=15, cov=1.0 → CRAP = 15 (passes max_crap), but cc > max_complexity
    r = crap_score(complexity=15, coverage=1.0, max_complexity=10)
    assert not r.passed
    assert "max 10" in r.details


def test_perfect_coverage_collapses_crap_to_complexity() -> None:
    r = crap_score(complexity=29, coverage=1.0)
    assert r.passed  # CRAP == 29 ≤ 30
    r2 = crap_score(complexity=31, coverage=1.0)
    assert not r2.passed  # CRAP == 31 > 30


def test_zero_coverage_amplifies_complexity() -> None:
    # cc=6, cov=0 → CRAP = 36 + 6 = 42 → fails default threshold
    r = crap_score(complexity=6, coverage=0.0)
    assert not r.passed


def test_invalid_coverage_raises() -> None:
    with pytest.raises(ValueError, match=r"\[0\.0, 1\.0\]"):
        crap_score(complexity=1, coverage=1.5)
    with pytest.raises(ValueError, match=r"\[0\.0, 1\.0\]"):
        crap_score(complexity=1, coverage=-0.1)


def test_invalid_complexity_raises() -> None:
    with pytest.raises(ValueError, match=">= 0"):
        crap_score(complexity=-1, coverage=0.5)


def test_invalid_max_crap_raises() -> None:
    with pytest.raises(ValueError, match="max_crap must be > 0"):
        crap_score(complexity=1, coverage=0.5, max_crap=0)
    with pytest.raises(ValueError, match="max_crap must be > 0"):
        crap_score(complexity=1, coverage=0.5, max_crap=-1)


def test_partial_score_between_thresholds() -> None:
    # cc=12, cov=0.5 → CRAP = 144·0.125 + 12 = 30 → passes (== threshold)
    # bump complexity slightly to force failure with non-zero partial credit
    r = crap_score(complexity=13, coverage=0.5)
    assert not r.passed
    assert 0.0 < r.score < 1.0


def test_custom_name_propagates() -> None:
    r = crap_score(complexity=1, coverage=1.0, name="my_grader")
    assert r.grader_name == "my_grader"
