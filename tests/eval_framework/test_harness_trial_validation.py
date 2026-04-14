# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for evals.harness / evals.run trial count validation
and pass-rate reporting semantics (issue #144)."""

from __future__ import annotations

import subprocess
import sys

import pytest

from evals.harness import EvalHarness
from evals.types import GraderResult, TaskResult, TrialResult


def _trial(passed: bool, idx: int = 0) -> TrialResult:
    return TrialResult(trial_idx=idx, passed=passed, score=1.0 if passed else 0.0)


def test_harness_init_rejects_zero_trials() -> None:
    with pytest.raises(ValueError, match=">= 1"):
        EvalHarness(trials=0)


def test_harness_init_rejects_negative_trials() -> None:
    with pytest.raises(ValueError, match=">= 1"):
        EvalHarness(trials=-1)


def test_harness_init_accepts_one_trial() -> None:
    harness = EvalHarness(trials=1)
    assert harness.trials == 1


def test_pass_at_k_reports_fraction_for_mixed_trials() -> None:
    result = TaskResult(task_id="mixed", suite="regression")
    result.trials = [_trial(True, 0), _trial(False, 1), _trial(False, 2)]
    assert result.pass_at_k == pytest.approx(1 / 3)
    assert result.pass_pow_k == 0.0
    assert result.all_passed is False


def test_pass_at_k_is_one_when_every_trial_passes() -> None:
    result = TaskResult(task_id="all_pass", suite="regression")
    result.trials = [_trial(True, 0), _trial(True, 1)]
    assert result.pass_at_k == 1.0
    assert result.pass_pow_k == 1.0


def test_pass_at_k_is_zero_when_no_trials_pass() -> None:
    result = TaskResult(task_id="all_fail", suite="regression")
    result.trials = [_trial(False, 0), _trial(False, 1)]
    assert result.pass_at_k == 0.0
    assert result.pass_pow_k == 0.0


def test_pass_pow_k_is_zero_when_any_trial_fails() -> None:
    result = TaskResult(task_id="flaky", suite="regression")
    result.trials = [_trial(True, 0), _trial(True, 1), _trial(False, 2)]
    assert result.pass_pow_k == 0.0


def test_to_dict_round_trips_pass_rate_for_mixed_trials() -> None:
    result = TaskResult(task_id="mixed_dict", suite="regression")
    result.trials = [_trial(True, 0), _trial(False, 1), _trial(False, 2), _trial(True, 3)]
    payload = result.to_dict()
    assert payload["pass_at_k"] == pytest.approx(0.5)
    assert payload["pass_pow_k"] == 0.0
    assert payload["all_passed"] is False


def test_harness_run_records_grader_outcomes_across_trials() -> None:
    harness = EvalHarness(trials=2)
    calls = {"n": 0}

    def task() -> list[GraderResult]:
        calls["n"] += 1
        passed = calls["n"] == 1
        return [GraderResult(grader_name="g", passed=passed, score=1.0 if passed else 0.0)]

    harness.add("flaky_task", suite="regression", fn=task)
    [result] = harness.run()
    assert result.k == 2
    assert result.pass_at_k == pytest.approx(0.5)
    assert result.pass_pow_k == 0.0


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "evals.run", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_run_cli_rejects_zero_trials() -> None:
    proc = _run_cli("--suite", "regression", "--trials", "0")
    assert proc.returncode != 0
    assert "must be >= 1" in proc.stderr


def test_run_cli_rejects_negative_trials() -> None:
    proc = _run_cli("--suite", "regression", "--trials", "-1")
    assert proc.returncode != 0
    assert "must be >= 1" in proc.stderr


def test_run_cli_rejects_non_integer_trials() -> None:
    proc = _run_cli("--trials", "abc")
    assert proc.returncode != 0
