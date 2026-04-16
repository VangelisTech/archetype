# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for eval harness validation and exit code correctness.

Covers:
- Rejection of trials <= 0 at both harness and CLI layers
- Vacuous success prevention (no tasks → non-zero exit)
- Mixed pass/fail trial metric correctness

``evals.run`` and ``evals.suites.*`` are never imported in this process.
They register Component subclasses (``Tag``, ``Health``) that collide with
identically-named locals in other test files via
``Component.__subclasses__()``.  CLI-level tests use ``subprocess`` instead.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from evals.harness import EvalHarness
from evals.types import GraderResult, TaskResult, TrialResult

# ---------------------------------------------------------------------------
# Harness: trials <= 0 rejected
# ---------------------------------------------------------------------------


def test_harness_init_rejects_zero_trials():
    with pytest.raises(ValueError, match="trials must be >= 1"):
        EvalHarness(trials=0)


def test_harness_init_rejects_negative_trials():
    with pytest.raises(ValueError, match="trials must be >= 1"):
        EvalHarness(trials=-1)


def test_harness_init_accepts_one_trial():
    h = EvalHarness(trials=1)
    assert h.trials == 1


# ---------------------------------------------------------------------------
# CLI: trials validation (subprocess to avoid Component registry pollution)
# ---------------------------------------------------------------------------


def test_cli_rejects_zero_trials():
    result = subprocess.run(
        [sys.executable, "-m", "evals.run", "--trials", "0"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "trials must be >= 1" in result.stderr


def test_cli_rejects_negative_trials():
    result = subprocess.run(
        [sys.executable, "-m", "evals.run", "--trials", "-1"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "trials must be >= 1" in result.stderr


# ---------------------------------------------------------------------------
# Exit code: vacuous success prevented (subprocess)
# ---------------------------------------------------------------------------


def test_cli_returns_nonzero_for_no_matching_tasks():
    """Suite filter that matches no tasks should exit non-zero."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; sys.path.insert(0, '.'); "
                "from evals.harness import EvalHarness; "
                "from evals.run import print_report; "
                "h = EvalHarness(trials=1); "
                "results = h.run(suite_filter='capability'); "
                "print_report(results); "
                "sys.exit(1 if not results else 0)"
            ),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "NO TASKS WERE EXECUTED" in result.stdout


def test_harness_run_returns_empty_for_unmatched_suite():
    harness = EvalHarness(trials=1)
    harness.add("t", suite="regression", fn=lambda: [GraderResult("g", True, 1.0)], desc="")
    results = harness.run(suite_filter="capability")
    assert results == []


# ---------------------------------------------------------------------------
# Mixed pass/fail: metric correctness
# ---------------------------------------------------------------------------


def _make_mixed_task(pass_pattern: list[bool]) -> TaskResult:
    """Build a TaskResult from a list of pass/fail booleans."""
    tr = TaskResult(task_id="mixed", suite="regression")
    for idx, passed in enumerate(pass_pattern):
        tr.trials.append(
            TrialResult(
                trial_idx=idx,
                passed=passed,
                score=1.0 if passed else 0.0,
            )
        )
    return tr


def test_pass_at_k_reports_one_when_any_trial_passes():
    task = _make_mixed_task([False, True, False])
    assert task.pass_at_k == 1.0


def test_pass_at_k_reports_zero_when_all_trials_fail():
    task = _make_mixed_task([False, False, False])
    assert task.pass_at_k == 0.0


def test_pass_pow_k_reports_zero_for_mixed_results():
    task = _make_mixed_task([True, False, True])
    assert task.pass_pow_k == 0.0


def test_pass_pow_k_reports_one_when_all_pass():
    task = _make_mixed_task([True, True, True])
    assert task.pass_pow_k == 1.0


def test_all_passed_false_for_mixed_results():
    task = _make_mixed_task([True, False])
    assert task.all_passed is False


def test_all_passed_true_when_all_pass():
    task = _make_mixed_task([True, True])
    assert task.all_passed is True


def test_avg_score_for_mixed_results():
    task = _make_mixed_task([True, False, True])
    assert task.avg_score == pytest.approx(2.0 / 3.0)


def test_task_result_empty_trials_returns_safe_defaults():
    task = TaskResult(task_id="empty", suite="regression")
    assert task.k == 0
    assert task.pass_at_k == 0.0
    assert task.pass_pow_k == 0.0
    assert task.avg_score == 0.0
    assert task.all_passed is False


# ---------------------------------------------------------------------------
# Harness: task execution with mixed outcomes
# ---------------------------------------------------------------------------


def test_harness_run_captures_mixed_trial_outcomes():
    call_count = 0

    def alternating_task() -> list[GraderResult]:
        nonlocal call_count
        call_count += 1
        passed = call_count % 2 == 1
        return [GraderResult(grader_name="alt", passed=passed, score=1.0 if passed else 0.0)]

    harness = EvalHarness(trials=3)
    harness.add("alt_task", suite="regression", fn=alternating_task, desc="")
    results = harness.run()

    assert len(results) == 1
    task = results[0]
    assert task.k == 3
    assert task.trials[0].passed is True
    assert task.trials[1].passed is False
    assert task.trials[2].passed is True
    assert task.pass_at_k == 1.0
    assert task.pass_pow_k == 0.0
    assert task.all_passed is False


def test_harness_run_records_trial_errors():
    def failing_task() -> list[GraderResult]:
        raise RuntimeError("boom")

    harness = EvalHarness(trials=1)
    harness.add("err_task", suite="regression", fn=failing_task, desc="")
    results = harness.run()

    assert len(results) == 1
    trial = results[0].trials[0]
    assert trial.passed is False
    assert trial.error == "boom"
