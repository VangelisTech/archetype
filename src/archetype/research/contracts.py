# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Stable family-owned configuration, callback, and result contracts."""

from __future__ import annotations

import json
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from uuid_utils import UUID

from archetype.world.models import EpisodeConfig, RolloutResult


@dataclass(frozen=True)
class AutoResearchConfig:
    """Configure one resumable autoresearch loop."""

    experiment_name: str
    experiment_id: str
    evaluator_id: str
    rollout_contract_id: str
    episode_config: EpisodeConfig = field(default_factory=EpisodeConfig)
    num_episodes: int = 10
    parallel: bool = False
    max_iterations: int = 100
    improvement_threshold: float = 0.0
    destroy_forks_on_complete: bool = False
    record_to_ledger: bool = True

    def __post_init__(self) -> None:
        if not self.experiment_id.strip():
            raise ValueError("experiment_id must be a non-empty caller-supplied identity")
        if not self.experiment_name.strip():
            raise ValueError("experiment_name must be non-empty")
        if not self.evaluator_id.strip():
            raise ValueError("evaluator_id must be a non-empty scoring contract identity")
        if not self.rollout_contract_id.strip():
            raise ValueError("rollout_contract_id must be a non-empty rollout contract identity")
        if self.num_episodes < 1:
            raise ValueError("num_episodes must be at least 1")
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be at least 1")
        if self.episode_config.max_steps < 1:
            raise ValueError("episode_config.max_steps must be at least 1")
        if not math.isfinite(self.improvement_threshold):
            raise ValueError("improvement_threshold must be finite")
        if self.improvement_threshold < 0:
            raise ValueError("improvement_threshold must be non-negative")


@dataclass(frozen=True)
class CandidateContext:
    """Context passed to a candidate-preparation callback."""

    experiment_id: str
    experiment_name: str
    iteration: int
    run_id: str
    base_world_id: str


@dataclass(frozen=True)
class EvaluationResult:
    """Return a finite score with evaluator identity and supporting evidence."""

    score: float
    evaluator: str
    evidence: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        score = float(self.score)
        if not math.isfinite(score):
            raise ValueError("evaluation score must be finite")
        if not self.evaluator.strip():
            raise ValueError("evaluation evaluator must be non-empty")
        json.dumps(self.evidence)
        json.dumps(self.metadata)
        object.__setattr__(self, "score", score)


@dataclass(frozen=True)
class IterationResult:
    """Result of one autoresearch iteration."""

    iteration: int
    rollout: RolloutResult
    score: float
    evaluation: EvaluationResult
    improved: bool
    incumbent_score: float


@dataclass(frozen=True)
class AutoResearchResult:
    """Summarize a completed or stopped autoresearch loop."""

    experiment_name: str
    iterations_completed: int
    final_score: float
    initial_score: float
    iterations: list[IterationResult] = field(default_factory=list)
    lab_world_id: str = ""

    @property
    def improved(self) -> bool:
        return self.final_score > self.initial_score


Evaluation = float | EvaluationResult
Evaluator = Callable[[RolloutResult], Evaluation | Awaitable[Evaluation]]
CandidatePreparer = Callable[
    [CandidateContext],
    str | UUID | None | Awaitable[str | UUID | None],
]
