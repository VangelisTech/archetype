# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Core eval types following Anthropic's agent eval framework.

Vocabulary matches the article:
- Task: a single test with inputs, expected outcomes, and graders
- Trial: one attempt at a task (run multiple for non-determinism)
- Grader: scoring logic applied to the outcome of a trial
- EvalResult: aggregated result across trials for a task
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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
    """Aggregated result of running a task across k trials."""

    task_id: str
    suite: str  # "regression" or "capability"
    trials: list[TrialResult] = field(default_factory=list)
    desc: str = ""

    @property
    def k(self) -> int:
        return len(self.trials)

    @property
    def pass_at_k(self) -> float:
        """Probability of at least one success in k trials.

        If any trial passed, pass@k = 1.0.  This is the simplified
        version for small k; for large k with estimated p, use the
        unbiased estimator.
        """
        if not self.trials:
            return 0.0
        return 1.0 if any(t.passed for t in self.trials) else 0.0

    @property
    def pass_pow_k(self) -> float:
        """Did ALL k trials succeed?

        Returns 1.0 if every trial passed, 0.0 otherwise.
        For reliability-critical tasks where anything less than
        100% consistency is a failure.
        """
        if not self.trials:
            return 0.0
        return 1.0 if all(t.passed for t in self.trials) else 0.0

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
            "desc": self.desc,
            "k": self.k,
            "pass_at_k": self.pass_at_k,
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
