# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Result records for Archetype's repository verification runner."""

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
    """Aggregated result of running one repository check repeatedly."""

    task_id: str
    suite: str
    trials: list[TrialResult] = field(default_factory=list)
    desc: str = ""
    contract_ids: tuple[str, ...] = ()

    @property
    def trial_count(self) -> int:
        return len(self.trials)

    @property
    def pass_rate(self) -> float:
        """Fraction of recorded trials that passed."""
        if not self.trials:
            return 0.0
        return sum(1 for t in self.trials if t.passed) / len(self.trials)

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
            "pass_rate": round(self.pass_rate, 4),
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
