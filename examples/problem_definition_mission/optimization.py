# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bounded GEPA search for evidence-bound problem-framing prompts.

GEPA is deliberately advisory here.  It explores prompts and reports its own
best candidate, while :func:`select_prompt_head` remains the only promotion
boundary.  A candidate cannot become the head without exact provenance,
passing hard gates, unanimous ratification, and a strict score improvement.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any, Protocol, cast

from .contracts import (
    EvidenceSnapshot,
    PanelEvaluation,
    ProblemDefinitionPolicy,
)


class ReflectionLanguageModel(Protocol):
    """Synchronous language model used only for GEPA reflection."""

    def __call__(self, prompt: str | list[dict[str, Any]]) -> str: ...


class PanelEvaluator(Protocol):
    """Synchronous provider that evaluates one prompt against one exact snapshot."""

    def evaluate(
        self,
        prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ) -> PanelEvaluation: ...


type PanelEvaluationFunction = Callable[
    [str, EvidenceSnapshot, ProblemDefinitionPolicy],
    PanelEvaluation,
]
type PanelEvaluatorLike = PanelEvaluator | PanelEvaluationFunction


@dataclass(frozen=True)
class GepaPromptConfig:
    """Finite search and promotion bounds for one optimization run."""

    max_metric_calls: int = 24
    max_candidate_proposals: int = 8
    patience: int = 3
    seed: int = 0
    improvement_threshold: float = 0.0

    def __post_init__(self) -> None:
        for field_name in (
            "max_metric_calls",
            "max_candidate_proposals",
            "patience",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise ValueError("seed must be a non-negative integer")
        if (
            isinstance(self.improvement_threshold, bool)
            or not isinstance(self.improvement_threshold, (int, float))
            or not math.isfinite(float(self.improvement_threshold))
            or self.improvement_threshold < 0
        ):
            raise ValueError("improvement_threshold must be a finite non-negative number")


@dataclass(frozen=True)
class GepaCandidateDiagnostic:
    """One explored prompt and its GEPA-reported ancestry."""

    index: int
    prompt: str
    parent_indices: tuple[int | None, ...]
    aggregate_score: float
    discovery_evaluation_count: int


@dataclass(frozen=True)
class GepaBestDiagnostic:
    """GEPA's search result, which has no authority to promote a prompt."""

    best_prompt: str
    best_score: float
    best_index: int
    candidate_count: int
    total_metric_calls: int
    optimizer_version: str
    objective_pareto_front: tuple[tuple[str, float], ...]
    candidates: tuple[GepaCandidateDiagnostic, ...]


@dataclass(frozen=True)
class PromptOptimizationResult:
    """Immutable outcome of search followed by provenance-safe selection."""

    head_prompt: str | None
    head_evaluation: PanelEvaluation | None
    accepted: bool
    records: tuple[PanelEvaluation, ...]
    gepa_best: GepaBestDiagnostic

    @property
    def gepa_best_diagnostic(self) -> GepaBestDiagnostic:
        """Return GEPA's advisory best candidate diagnostics."""

        return self.gepa_best


class _QuietLogger:
    """GEPA logger that keeps library calls silent by default."""

    def log(self, message: str) -> None:
        del message


def _binding_matches(
    evaluation: PanelEvaluation,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
) -> bool:
    """Check every binding represented by the public contracts."""

    evidence_ids = tuple(item.evidence_id for item in snapshot.items)
    checks = (
        evaluation.evidence_revision == snapshot.revision,
        evaluation.evidence_ids == evidence_ids,
        evaluation.evidence_digest == snapshot.digest,
        evaluation.policy_digest == policy.digest,
        evaluation.evaluator_id == policy.evaluator_id,
    )
    return all(checks)


def _eligible_for_promotion(
    evaluation: PanelEvaluation,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
) -> bool:
    score = float(evaluation.aggregate_score)
    return (
        _binding_matches(evaluation, snapshot, policy)
        and evaluation.hard_gate_passed
        and evaluation.unanimous
        and not evaluation.grounding_errors()
        and math.isfinite(score)
    )


def select_prompt_head(
    incumbent: PanelEvaluation,
    candidates: Iterable[PanelEvaluation],
    *,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
    improvement_threshold: float = 0.0,
) -> PanelEvaluation | None:
    """Select a strictly better ratified prompt without comparing revisions.

    The incumbent establishes the score to beat, but the caller-provided
    snapshot and policy establish the authoritative provenance.  Stale records
    are filtered rather than ranked.  Equal candidate scores are resolved in
    favor of the shorter prompt, then lexical prompt order, but a tie with the
    incumbent never replaces it.
    """

    if (
        isinstance(improvement_threshold, bool)
        or not isinstance(improvement_threshold, (int, float))
        or not math.isfinite(float(improvement_threshold))
        or improvement_threshold < 0
    ):
        raise ValueError("improvement_threshold must be a finite non-negative number")
    if not _binding_matches(incumbent, snapshot, policy):
        raise ValueError("incumbent is not bound to the expected evidence, policy, and evaluator")

    incumbent_score = float(incumbent.aggregate_score)
    if not math.isfinite(incumbent_score):
        raise ValueError("incumbent aggregate score must be finite")
    threshold = float(improvement_threshold)
    incumbent_is_eligible = _eligible_for_promotion(incumbent, snapshot, policy)
    eligible = [
        candidate
        for candidate in candidates
        if candidate.candidate_prompt != incumbent.candidate_prompt
        and _eligible_for_promotion(candidate, snapshot, policy)
        and (
            not incumbent_is_eligible
            or (
                float(candidate.aggregate_score) > incumbent_score
                and float(candidate.aggregate_score) - incumbent_score >= threshold
            )
        )
    ]
    if not eligible:
        return incumbent if incumbent_is_eligible else None

    return min(
        eligible,
        key=lambda candidate: (
            -float(candidate.aggregate_score),
            len(candidate.candidate_prompt),
            candidate.candidate_prompt,
        ),
    )


def _invoke_panel(
    evaluator: PanelEvaluatorLike,
    prompt: str,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
) -> PanelEvaluation:
    evaluate = getattr(evaluator, "evaluate", None)
    if callable(evaluate):
        result = evaluate(prompt, snapshot, policy)
    elif callable(evaluator):
        result = cast(PanelEvaluationFunction, evaluator)(prompt, snapshot, policy)
    else:  # pragma: no cover - protected by the public type and defensive at runtime
        raise TypeError("panel_evaluator must be callable or provide evaluate()")
    if not isinstance(result, PanelEvaluation):
        raise TypeError("panel_evaluator must return PanelEvaluation")
    return result


def _normalized_side_info(evaluation: PanelEvaluation) -> dict[str, object]:
    """Return GEPA-compatible multi-objective ASI without mutating the record."""

    side_info = dict(evaluation.side_info)
    raw_scores = side_info.get("scores", side_info.get("objectives"))
    if not isinstance(raw_scores, Mapping) or not raw_scores:
        raise ValueError("PanelEvaluation.side_info must contain non-empty objective scores")

    scores: dict[str, float] = {}
    for name, raw_score in raw_scores.items():
        if isinstance(raw_score, bool) or not isinstance(raw_score, (int, float)):
            raise ValueError("PanelEvaluation objective scores must be numeric")
        score = float(raw_score)
        if not str(name).strip() or not math.isfinite(score):
            raise ValueError("PanelEvaluation objective names and scores must be valid")
        scores[str(name)] = score
    side_info["scores"] = scores

    feedback = side_info.get("Feedback", side_info.get("feedback"))
    if not isinstance(feedback, str) or not feedback.strip():
        errors = evaluation.grounding_errors()
        error_text = "; ".join(errors) if errors else "none reported"
        feedback = (
            f"Unanimous ratification: {evaluation.unanimous}. "
            f"Hard gates passed: {evaluation.hard_gate_passed}. "
            f"Grounding errors: {error_text}."
        )
    side_info["Feedback"] = feedback
    return side_info


def _aggregate_candidate_evaluations(
    evaluations: tuple[PanelEvaluation, ...],
) -> tuple[float, dict[str, object]]:
    """Aggregate current and historical snapshots into one GEPA observation."""

    if not evaluations:
        raise ValueError("at least one panel evaluation is required")
    normalized = tuple(_normalized_side_info(evaluation) for evaluation in evaluations)
    score_names = set(cast(Mapping[str, float], normalized[0]["scores"]))
    if any(set(cast(Mapping[str, float], item["scores"])) != score_names for item in normalized):
        raise ValueError("panel evaluations must expose the same objective score names")
    scores = {
        name: sum(float(cast(Mapping[str, float], item["scores"])[name]) for item in normalized)
        / len(normalized)
        for name in sorted(score_names)
    }
    feedback = "\n\n".join(
        (
            f"Evidence revision {evaluation.evidence_revision} "
            f"({evaluation.evidence_digest[:12]}):\n{item['Feedback']}"
        )
        for evaluation, item in zip(evaluations, normalized, strict=True)
    )
    return (
        sum(float(evaluation.aggregate_score) for evaluation in evaluations) / len(evaluations),
        {
            "scores": scores,
            "Feedback": feedback,
            "snapshot_count": len(evaluations),
        },
    )


def _gepa_prompt(result: Any) -> str:
    best_candidate = result.best_candidate
    if isinstance(best_candidate, str):
        return best_candidate
    if isinstance(best_candidate, Mapping):
        current = best_candidate.get("current_candidate")
        if isinstance(current, str):
            return current
    raise TypeError("GEPA returned an unsupported best-candidate representation")


def _candidate_prompt_at(result: Any, index: int) -> str:
    candidate = result.candidates[index]
    if isinstance(candidate, str):
        return candidate
    if isinstance(candidate, Mapping):
        string_key = getattr(result, "_str_candidate_key", None)
        if isinstance(string_key, str):
            prompt = candidate.get(string_key)
            if isinstance(prompt, str):
                return prompt
        current = candidate.get("current_candidate")
        if isinstance(current, str):
            return current
        string_values = [value for value in candidate.values() if isinstance(value, str)]
        if len(string_values) == 1:
            return string_values[0]
    raise TypeError("GEPA returned an unsupported candidate representation")


def _diagnostic(result: Any) -> GepaBestDiagnostic:
    best_index = int(result.best_idx)
    raw_frontier = result.objective_pareto_front or {}
    frontier = tuple(sorted((str(name), float(score)) for name, score in raw_frontier.items()))
    candidates = tuple(
        GepaCandidateDiagnostic(
            index=index,
            prompt=_candidate_prompt_at(result, index),
            parent_indices=tuple(result.parents[index]),
            aggregate_score=float(result.val_aggregate_scores[index]),
            discovery_evaluation_count=int(result.discovery_eval_counts[index]),
        )
        for index in range(len(result.candidates))
    )
    return GepaBestDiagnostic(
        best_prompt=_gepa_prompt(result),
        best_score=float(result.val_aggregate_scores[best_index]),
        best_index=best_index,
        candidate_count=int(result.num_candidates),
        total_metric_calls=int(result.total_metric_calls or 0),
        optimizer_version=version("gepa"),
        objective_pareto_front=frontier,
        candidates=candidates,
    )


def optimize_problem_prompt(
    seed_prompt: str,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
    panel_evaluator: PanelEvaluatorLike,
    reflection_lm: ReflectionLanguageModel,
    *,
    config: GepaPromptConfig | None = None,
    historical_snapshots: Iterable[EvidenceSnapshot] = (),
) -> PromptOptimizationResult:
    """Run bounded GEPA search and ratify the resulting evaluation records.

    The optional GEPA dependency is imported at call time so the immutable
    contracts and pure selector remain usable from the base package.
    """

    if not seed_prompt.strip():
        raise ValueError("seed_prompt must not be empty")
    if config is None:
        config = GepaPromptConfig()
    history = tuple(
        sorted(
            historical_snapshots,
            key=lambda historical: (historical.revision, historical.digest),
        )
    )
    evaluation_snapshots = (snapshot, *history)
    snapshot_digests = [candidate.digest for candidate in evaluation_snapshots]
    if len(snapshot_digests) != len(set(snapshot_digests)):
        raise ValueError("current and historical evidence snapshots must be unique")

    try:
        from gepa.optimize_anything import (
            EngineConfig,
            GEPAConfig,
            ReflectionConfig,
            TrackingConfig,
            optimize_anything,
        )
        from gepa.utils import NoImprovementStopper
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised without the extra
        raise RuntimeError(
            "GEPA is required; run the example with --group problem-definition"
        ) from exc

    records: list[PanelEvaluation] = []

    def evaluate(candidate_prompt: str) -> tuple[float, dict[str, object]]:
        candidate_evaluations: list[PanelEvaluation] = []
        for evaluation_snapshot in evaluation_snapshots:
            evaluation = _invoke_panel(
                panel_evaluator,
                candidate_prompt,
                evaluation_snapshot,
                policy,
            )
            if evaluation.candidate_prompt != candidate_prompt:
                raise ValueError("panel_evaluator returned an evaluation for a different prompt")
            if not _binding_matches(evaluation, evaluation_snapshot, policy):
                raise ValueError(
                    "panel_evaluator returned an evaluation with the wrong evidence, "
                    "policy, or evaluator binding"
                )
            candidate_evaluations.append(evaluation)
        immutable_evaluations = tuple(candidate_evaluations)
        records.extend(immutable_evaluations)
        return _aggregate_candidate_evaluations(immutable_evaluations)

    gepa_result = optimize_anything(
        seed_candidate=seed_prompt,
        evaluator=evaluate,
        objective=(
            "Improve the reusable single-shot prompt that asks independent naive, "
            "expert, and orthogonal perspectives to identify the exact problem, "
            "construct concrete counterexample challenges to its material claims, "
            "and preserve those challenges until they are rejected or the claims revise."
        ),
        background=(
            "The prompt must produce atomic, evidence-addressable claims, separate "
            "observation from inference and unknowns, expose constraints and non-goals, "
            "distinguish a prospective falsifier from a concrete witnessed challenge, "
            "and support explicit three-perspective ratification. A confirmed, "
            "inconclusive, or unverified counterexample challenge vetoes promotion while "
            "its exact target claim remains. A rejected challenge does not. Preserve "
            "useful behavior and directly address the panel's textual feedback."
        ),
        config=GEPAConfig(
            engine=EngineConfig(
                seed=config.seed,
                max_metric_calls=config.max_metric_calls,
                max_candidate_proposals=config.max_candidate_proposals,
                display_progress_bar=False,
                parallel=False,
                max_workers=1,
                cache_evaluation=True,
                cache_evaluation_storage="memory",
                candidate_selection_strategy="pareto",
                frontier_type="objective",
            ),
            reflection=ReflectionConfig(
                reflection_lm=cast(Any, reflection_lm),
                reflection_minibatch_size=1,
            ),
            tracking=TrackingConfig(logger=_QuietLogger()),
            stop_callbacks=NoImprovementStopper(config.patience),
        ),
    )

    current_records = [
        record
        for record in records
        if record.candidate_prompt == seed_prompt
        and _binding_matches(record, snapshot, policy)
        and math.isfinite(float(record.aggregate_score))
    ]
    if not current_records:
        raise ValueError("GEPA did not produce a current-bound evaluation for the seed prompt")
    incumbent = max(current_records, key=lambda record: float(record.aggregate_score))
    head = select_prompt_head(
        incumbent,
        records,
        snapshot=snapshot,
        policy=policy,
        improvement_threshold=float(config.improvement_threshold),
    )

    return PromptOptimizationResult(
        head_prompt=head.candidate_prompt if head is not None else None,
        head_evaluation=head,
        accepted=head is not None and head.candidate_prompt != seed_prompt,
        records=tuple(records),
        gepa_best=_diagnostic(gepa_result),
    )


__all__ = [
    "GepaBestDiagnostic",
    "GepaCandidateDiagnostic",
    "GepaPromptConfig",
    "PanelEvaluationFunction",
    "PanelEvaluator",
    "PanelEvaluatorLike",
    "PromptOptimizationResult",
    "ReflectionLanguageModel",
    "optimize_problem_prompt",
    "select_prompt_head",
]
