# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for evals.harness / evals.run trial count validation
and pass-rate reporting semantics (issue #144)."""

from __future__ import annotations

import logging
import subprocess
import sys

import pytest

from evals.graders import state_check
from evals.harness import EvalHarness
from evals.run import _configure_eval_logging, _ExpectedEvalNoiseFilter, build_harness
from evals.run import main as run_main
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


def test_pass_rate_reports_fraction_for_mixed_trials() -> None:
    result = TaskResult(task_id="mixed", suite="regression")
    result.trials = [_trial(True, 0), _trial(False, 1), _trial(False, 2)]
    assert result.pass_rate == pytest.approx(1 / 3)
    assert result.all_passed is False


def test_pass_rate_is_one_when_every_trial_passes() -> None:
    result = TaskResult(task_id="all_pass", suite="regression")
    result.trials = [_trial(True, 0), _trial(True, 1)]
    assert result.pass_rate == 1.0
    assert result.all_passed is True


def test_pass_rate_is_zero_when_no_trials_pass() -> None:
    result = TaskResult(task_id="all_fail", suite="regression")
    result.trials = [_trial(False, 0), _trial(False, 1)]
    assert result.pass_rate == 0.0


def test_to_dict_uses_literal_trial_metric_names() -> None:
    result = TaskResult(task_id="mixed_dict", suite="regression")
    result.trials = [_trial(True, 0), _trial(False, 1), _trial(False, 2), _trial(True, 3)]
    payload = result.to_dict()
    assert payload["trial_count"] == 4
    assert payload["pass_rate"] == pytest.approx(0.5)
    assert payload["all_passed"] is False
    assert "k" not in payload
    assert "pass_at_k" not in payload
    assert "pass_pow_k" not in payload


def test_harness_run_records_grader_outcomes_across_trials() -> None:
    harness = EvalHarness(trials=2)
    calls = {"n": 0}

    def task() -> list[GraderResult]:
        calls["n"] += 1
        passed = calls["n"] == 1
        return [GraderResult(grader_name="g", passed=passed, score=1.0 if passed else 0.0)]

    harness.add("flaky_task", suite="regression", fn=task)
    [result] = harness.run()
    assert result.trial_count == 2
    assert result.pass_rate == pytest.approx(0.5)
    assert result.all_passed is False


def test_harness_run_fails_trial_without_grader_evidence() -> None:
    harness = EvalHarness()
    harness.add("empty_task", suite="regression", fn=lambda: [])

    [result] = harness.run()
    [trial] = result.trials

    assert trial.passed is False
    assert trial.score == 0.0
    assert trial.grader_results == []
    assert trial.error == "task 'empty_task' produced no grader evidence"
    assert result.pass_rate == 0.0
    assert result.all_passed is False


def test_registered_tasks_expose_an_immutable_inventory() -> None:
    harness = EvalHarness()

    def task() -> list[GraderResult]:
        return []

    harness.add("visible", suite="regression", fn=task, desc="listed task")

    assert harness.registered_tasks == (("visible", "regression", task, "listed task"),)


def test_harness_run_preserves_multi_grader_aggregation() -> None:
    grader_results = [
        GraderResult(grader_name="passing", passed=True, score=0.25),
        GraderResult(grader_name="failing", passed=False, score=0.75),
    ]
    harness = EvalHarness()
    harness.add("multi_grader", suite="regression", fn=lambda: grader_results)

    [result] = harness.run()
    [trial] = result.trials

    assert trial.passed is False
    assert trial.score == pytest.approx(0.5)
    assert trial.grader_results == grader_results
    assert trial.error is None


def test_state_check_rejects_empty_evidence() -> None:
    with pytest.raises(ValueError, match="requires at least one check"):
        state_check({})


def test_state_check_preserves_partial_credit_for_nonempty_checks() -> None:
    result = state_check({"materialized": True, "persisted": False})

    assert result.passed is False
    assert result.score == pytest.approx(0.5)
    assert result.details == "failed: {'persisted'}"


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


def test_run_cli_lists_the_live_registry_without_executing_tasks() -> None:
    proc = _run_cli("--list", "--suite", "capability")

    expected = [
        task_id
        for task_id, suite, _fn, _desc in build_harness().registered_tasks
        if suite == "capability"
    ]
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.startswith("suite\ttask\tdescription\n")
    assert "REPOSITORY CHECK RESULTS" not in proc.stdout
    assert expected
    assert all(f"capability\t{task_id}\t" in proc.stdout for task_id in expected)


def test_run_cli_fails_required_task_without_grader_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    harness = EvalHarness()
    harness.add("empty_task", suite="regression", fn=lambda: [])
    monkeypatch.setattr("evals.run.build_harness", lambda trials=1: harness)
    monkeypatch.setattr(sys, "argv", ["evals.run", "--suite", "regression"])

    assert run_main() == 1
    report = capsys.readouterr().out
    assert "[FAIL] empty_task" in report
    assert "error: task 'empty_task' produced no grader evidence" in report


def test_run_cli_fails_capability_task_with_missed_grader(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    harness = EvalHarness()
    harness.add(
        "architectural_invariant",
        suite="capability",
        fn=lambda: [
            GraderResult(
                grader_name="observable_outcome",
                passed=False,
                score=0.0,
                details="invariant did not hold",
            )
        ],
    )
    monkeypatch.setattr("evals.run.build_harness", lambda trials=1: harness)
    monkeypatch.setattr(sys, "argv", ["evals.run", "--suite", "capability"])

    assert run_main() == 1
    report = capsys.readouterr().out
    assert "[FAIL] architectural_invariant" in report
    assert "invariant did not hold" in report


def test_regression_cli_reports_poison_outcomes_without_library_tracebacks() -> None:
    proc = _run_cli("--suite", "regression")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stderr == ""
    assert "Traceback" not in proc.stdout
    assert "[PASS] poison_in_batch" in proc.stdout
    assert "[PASS] despawn_nonexistent" in proc.stdout


def _apply_failure_record(error: Exception) -> logging.LogRecord:
    return logging.LogRecord(
        name="archetype.app.command_service",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Failed to apply command command-id (spawn)",
        args=(),
        exc_info=(type(error), error, None),
    )


def test_eval_log_filter_quiets_reserved_spawn_replay() -> None:
    error = ValueError(
        "Entity 77 is already registered. "
        "Use update_entity to change component values on a live entity."
    )

    assert not _ExpectedEvalNoiseFilter().filter(_apply_failure_record(error))


def test_eval_log_filter_keeps_unexpected_apply_failures_visible() -> None:
    error = RuntimeError("unexpected storage failure")

    assert _ExpectedEvalNoiseFilter().filter(_apply_failure_record(error))


def test_eval_logging_honors_handler_inherited_from_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package_logger = logging.getLogger("archetype")
    root_logger = logging.getLogger()
    root_handler = logging.NullHandler()
    monkeypatch.setattr(package_logger, "handlers", [])
    monkeypatch.setattr(package_logger, "propagate", True)
    monkeypatch.setattr(root_logger, "handlers", [root_handler])

    _configure_eval_logging()

    assert package_logger.handlers == []
    assert package_logger.propagate is True
