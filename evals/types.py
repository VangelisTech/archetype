# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Result records and statistics for Archetype's repository verification runner.

Vocabulary matches the article:
- Task: a single test with inputs, expected outcomes, and graders
- Trial: one attempt at a task (run multiple for non-determinism)
- Grader: scoring logic applied to the outcome of a trial
- EvalResult: aggregated result across trials for a task

Pass@k follows the unbiased estimator from Chen et al., *Evaluating Large
Language Models Trained on Code* (arXiv:2107.03374).  The empirical pass rate
and strict all-trials success are deliberately separate metrics.
"""

from __future__ import annotations

import math
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any


def codex_pass_at_k(total_samples: int, correct_samples: int, k: int) -> float:
    """Return the unbiased Codex pass@k estimate for one task.

    ``total_samples`` is the number of observed trials (``n``),
    ``correct_samples`` is the number that passed (``c``), and ``k`` is the
    size of the hypothetical subset.  This is the exact expectation over all
    ``C(n, k)`` subsets:

    ``1 - C(n - c, k) / C(n, k)``

    The product form avoids constructing large combinations and matches the
    numerically stable reference implementation in arXiv:2107.03374.
    """
    if total_samples < 1:
        raise ValueError(f"total_samples must be >= 1, got {total_samples}")
    if not 0 <= correct_samples <= total_samples:
        raise ValueError(
            "correct_samples must be between 0 and total_samples, "
            f"got {correct_samples} of {total_samples}"
        )
    if not 1 <= k <= total_samples:
        raise ValueError(f"k must be between 1 and total_samples, got {k} of {total_samples}")
    if total_samples - correct_samples < k:
        return 1.0

    miss_probability = math.prod(
        1.0 - k / denominator
        for denominator in range(total_samples - correct_samples + 1, total_samples + 1)
    )
    return min(1.0, max(0.0, 1.0 - miss_probability))


@dataclass
class GraderResult:
    """Output of a single grader applied to a single trial."""

    grader_name: str
    passed: bool
    score: float  # 0.0-1.0
    details: str = ""


@dataclass
class TrialResult:
    """Outcome of one trial (attempt) at a task."""

    trial_idx: int
    passed: bool  # True if ALL graders passed
    score: float  # Average score across graders (supports partial credit)
    elapsed_s: float = 0.0
    grader_results: list[GraderResult] = field(default_factory=list)
    error: str | None = None  # Non-None if the trial crashed


@dataclass
class TaskResult:
    """Aggregated result of running one task across ``n`` observed trials."""

    task_id: str
    suite: str
    trials: list[TrialResult] = field(default_factory=list)
    desc: str = ""
    contract_ids: tuple[str, ...] = ()

    @property
    def trial_count(self) -> int:
        return len(self.trials)

    @property
    def correct_samples(self) -> int:
        """Number of observed trials that passed every grader."""
        return sum(1 for trial in self.trials if trial.passed)

    @property
    def pass_rate(self) -> float:
        """Return the raw empirical success fraction ``c / n``."""
        if not self.trials:
            return 0.0
        return self.correct_samples / self.trial_count

    def pass_at(self, k: int) -> float:
        """Return the Codex pass@k estimate from this task's observed trials."""
        return codex_pass_at_k(self.trial_count, self.correct_samples, k)

    @property
    def pass_at_k(self) -> float:
        """Return pass@k where ``k`` is this result's full observed trial count."""
        if not self.trials:
            return 0.0
        return self.pass_at(self.trial_count)

    @property
    def pass_at_k_curve(self) -> dict[int, float]:
        """Return every pass@k estimate supported by the observed sample set."""
        return {k: self.pass_at(k) for k in range(1, self.trial_count + 1)}

    @property
    def pass_pow_k(self) -> float:
        """Return strict repeatability: 1.0 only when every trial passed."""
        return 1.0 if self.all_passed else 0.0

    @property
    def avg_score(self) -> float:
        """Average score across all trials (supports partial credit)."""
        if not self.trials:
            return 0.0
        return sum(t.score for t in self.trials) / len(self.trials)

    @property
    def all_passed(self) -> bool:
        return bool(self.trials) and all(t.passed for t in self.trials)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "suite": self.suite,
            "contract_ids": list(self.contract_ids),
            "desc": self.desc,
            "trial_count": self.trial_count,
            "correct_samples": self.correct_samples,
            "pass_rate": round(self.pass_rate, 4),
            "pass_at_k": round(self.pass_at_k, 4),
            "pass_at_k_curve": {
                str(k): round(estimate, 4) for k, estimate in self.pass_at_k_curve.items()
            },
            "pass_pow_k": round(self.pass_pow_k, 4),
            "avg_score": round(self.avg_score, 4),
            "all_passed": self.all_passed,
            "trials": [
                {
                    "trial": t.trial_idx,
                    "passed": t.passed,
                    "score": round(t.score, 4),
                    "elapsed_s": round(t.elapsed_s, 4),
                    "error": t.error,
                    "graders": [
                        {
                            "name": g.grader_name,
                            "passed": g.passed,
                            "score": round(g.score, 4),
                            "details": g.details,
                        }
                        for g in t.grader_results
                    ],
                }
                for t in self.trials
            ],
        }


def aggregate_pass_at_k(results: Collection[TaskResult]) -> dict[int, float]:
    """Average per-task Codex estimates into the suite pass@k curve.

    Only values of ``k`` supported by every task are returned.  Confidence
    intervals belong above this function and must resample tasks/questions,
    not individual trials within a task.
    """
    tasks = tuple(results)
    if not tasks:
        return {}
    if any(task.trial_count < 1 for task in tasks):
        raise ValueError("cannot aggregate pass@k for a task with no trials")

    return {
        k: sum(task.pass_at(k) for task in tasks) / len(tasks)
        for k in range(1, min(task.trial_count for task in tasks) + 1)
    }
