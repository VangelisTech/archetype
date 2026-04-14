# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Eval harness: runs tasks, manages trials, grades outcomes, aggregates results.

Each task is a callable that returns a list of GraderResults (the outcome
of applying all graders to the task's output).  The harness runs each task
k times (trials), collects GraderResults per trial, and produces TaskResults
with pass@k / pass^k metrics.

Usage:
    harness = EvalHarness(trials=3)
    harness.add("my_task", suite="regression", fn=my_task_fn, desc="...")
    results = harness.run()
"""

from __future__ import annotations

import time
from collections.abc import Callable

from evals.types import GraderResult, TaskResult, TrialResult

# A task function returns the grader results for one trial.
TaskFn = Callable[[], list[GraderResult]]


class EvalHarness:
    """Evaluation harness that runs tasks across multiple trials."""

    def __init__(self, trials: int = 1):
        if trials < 1:
            raise ValueError(f"trials must be >= 1, got {trials}")
        self.trials = trials
        self._tasks: list[tuple[str, str, TaskFn, str]] = []  # (id, suite, fn, desc)

    def add(self, task_id: str, *, suite: str, fn: TaskFn, desc: str = "") -> None:
        """Register a task to be run."""
        self._tasks.append((task_id, suite, fn, desc))

    def run(self, *, suite_filter: str | None = None) -> list[TaskResult]:
        """Run all registered tasks and return aggregated results."""
        results = []

        for task_id, suite, fn, desc in self._tasks:
            if suite_filter and suite != suite_filter:
                continue

            task_result = TaskResult(task_id=task_id, suite=suite, desc=desc)

            for trial_idx in range(self.trials):
                t0 = time.perf_counter()
                try:
                    grader_results = fn()
                    elapsed = time.perf_counter() - t0

                    all_passed = all(g.passed for g in grader_results)
                    avg_score = (
                        sum(g.score for g in grader_results) / len(grader_results)
                        if grader_results
                        else 0.0
                    )

                    task_result.trials.append(
                        TrialResult(
                            trial_idx=trial_idx,
                            passed=all_passed,
                            score=avg_score,
                            elapsed_s=elapsed,
                            grader_results=grader_results,
                        )
                    )
                except Exception as exc:
                    elapsed = time.perf_counter() - t0
                    task_result.trials.append(
                        TrialResult(
                            trial_idx=trial_idx,
                            passed=False,
                            score=0.0,
                            elapsed_s=elapsed,
                            error=str(exc),
                        )
                    )

            results.append(task_result)

        return results
