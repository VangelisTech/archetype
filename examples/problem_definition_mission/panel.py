# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Role-separated panel orchestration for problem-definition evaluation."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Protocol, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .contracts import (
    ChallengeVerification,
    ChallengeVerificationOutcome,
    ClaimChallenge,
    CounterexampleSearchOutcome,
    CounterexampleSearchReceipt,
    EvaluationBinding,
    EvidenceSnapshot,
    PanelEvaluation,
    Perspective,
    PerspectiveObservation,
    ProblemDefinitionPolicy,
    ProblemFraming,
    RatificationVote,
    ScoreVector,
)

_PERSPECTIVE_ORDER = tuple(Perspective)


class _FrozenOutput(BaseModel):
    model_config = ConfigDict(frozen=True)


class PerspectiveAgentOutput(_FrozenOutput):
    """One role's independent interpretation of the supplied evidence."""

    framing: ProblemFraming
    confidence: float = Field(ge=0.0, le=1.0)
    feedback: str


class SynthesisOutput(_FrozenOutput):
    """One synthesis of the three independent observations."""

    framing: ProblemFraming
    feedback: tuple[str, ...] = ()


class RatificationOutput(_FrozenOutput):
    """One role's decision about the exact synthesized framing."""

    approved: bool
    reason: str = Field(min_length=1)


class ScoringOutput(_FrozenOutput):
    """Multi-objective scores and actionable feedback for prompt search."""

    scores: ScoreVector
    feedback: tuple[str, ...] = ()


class CounterexampleSearchDecision(_FrozenOutput):
    """Verifier-role result for one final claim's bounded witness search."""

    target_claim_id: str = Field(min_length=1)
    outcome: CounterexampleSearchOutcome
    challenge_ids: tuple[str, ...] = ()
    detail: str = Field(min_length=1)

    @model_validator(mode="after")
    def _valid_decision(self) -> CounterexampleSearchDecision:
        if len(self.challenge_ids) != len(set(self.challenge_ids)):
            raise ValueError("counterexample search challenge IDs must be unique")
        if self.outcome is CounterexampleSearchOutcome.FOUND:
            if not self.challenge_ids:
                raise ValueError("a found counterexample search must name a challenge")
        elif self.challenge_ids:
            raise ValueError("only a found counterexample search may name challenges")
        return self

    @property
    def found(self) -> bool:
        return self.outcome is CounterexampleSearchOutcome.FOUND


class ChallengeVerificationDecision(_FrozenOutput):
    """Verifier-role disposition of one model-proposed concrete witness."""

    challenge_id: str = Field(min_length=1)
    outcome: ChallengeVerificationOutcome
    detail: str = Field(min_length=1)


class CounterexampleReviewOutput(_FrozenOutput):
    """Untrusted verifier-role decisions bound into receipts by orchestration."""

    searches: tuple[CounterexampleSearchDecision, ...]
    verifications: tuple[ChallengeVerificationDecision, ...]


class PerspectiveContext(_FrozenOutput):
    """Exact context visible to one perspective agent and no other role."""

    candidate_prompt: str
    snapshot: EvidenceSnapshot
    policy: ProblemDefinitionPolicy
    binding: EvaluationBinding
    perspective: Perspective
    protocol_id: str


class SynthesisContext(_FrozenOutput):
    """Exact observations visible to the single synthesis call."""

    candidate_prompt: str
    snapshot: EvidenceSnapshot
    policy: ProblemDefinitionPolicy
    binding: EvaluationBinding
    protocol_id: str
    observations: tuple[PerspectiveObservation, ...]


class RatificationContext(_FrozenOutput):
    """Exact synthesis visible to one role's independent ratification call."""

    candidate_prompt: str
    snapshot: EvidenceSnapshot
    policy: ProblemDefinitionPolicy
    binding: EvaluationBinding
    perspective: Perspective
    protocol_id: str
    observation: PerspectiveObservation
    framing: ProblemFraming


class ScoringContext(_FrozenOutput):
    """Complete bound panel result visible to the single scoring call."""

    candidate_prompt: str
    snapshot: EvidenceSnapshot
    policy: ProblemDefinitionPolicy
    binding: EvaluationBinding
    protocol_id: str
    observations: tuple[PerspectiveObservation, ...]
    framing: ProblemFraming
    votes: tuple[RatificationVote, ...]


class CounterexampleReviewContext(_FrozenOutput):
    """Exact synthesis and observations visible to the independent verifier role."""

    candidate_prompt: str
    snapshot: EvidenceSnapshot
    policy: ProblemDefinitionPolicy
    binding: EvaluationBinding
    verifier_id: str
    search_protocol_id: str
    verification_protocol_id: str
    max_search_attempts: int = Field(ge=1)
    observations: tuple[PerspectiveObservation, ...]
    framing: ProblemFraming


class NaivePerspectiveAgent(Protocol):
    """Judge whether a framing is intelligible without privileged context."""

    def observe_naively(self, context: PerspectiveContext) -> PerspectiveAgentOutput: ...


class ExpertPerspectiveAgent(Protocol):
    """Judge whether a framing is grounded in the supplied research."""

    def observe_as_expert(self, context: PerspectiveContext) -> PerspectiveAgentOutput: ...


class OrthogonalPerspectiveAgent(Protocol):
    """Challenge the framing's assumptions, boundaries, and alternative views."""

    def observe_orthogonally(
        self,
        context: PerspectiveContext,
    ) -> PerspectiveAgentOutput: ...


type PerspectiveAgent = NaivePerspectiveAgent | ExpertPerspectiveAgent | OrthogonalPerspectiveAgent


class FramingSynthesizer(Protocol):
    """Synthesize the intersection of all three perspective observations."""

    def synthesize(self, context: SynthesisContext) -> SynthesisOutput: ...


class PerspectiveRatifier(Protocol):
    """Ratify a synthesis on behalf of exactly one mapped perspective."""

    def ratify(self, context: RatificationContext) -> RatificationOutput: ...


class PanelScorer(Protocol):
    """Score the complete, exact-bound panel result."""

    def score(self, context: ScoringContext) -> ScoringOutput: ...


class CounterexampleVerifier(Protocol):
    """Independently search final propositions and verify proposed witnesses."""

    verifier_id: str

    def review(self, context: CounterexampleReviewContext) -> CounterexampleReviewOutput: ...


def _require_all_roles[T](
    label: str,
    mapping: Mapping[Perspective, T],
) -> Mapping[Perspective, T]:
    if set(mapping) != set(Perspective):
        raise ValueError(f"{label} must map exactly one provider to each Perspective")
    if len({id(provider) for provider in mapping.values()}) != len(Perspective):
        raise ValueError(f"{label} must use a distinct provider instance for each Perspective")
    return MappingProxyType(
        {perspective: mapping[perspective] for perspective in _PERSPECTIVE_ORDER}
    )


def _observe(
    perspective: Perspective,
    agent: PerspectiveAgent,
    context: PerspectiveContext,
) -> PerspectiveAgentOutput:
    if perspective is Perspective.NAIVE:
        method = getattr(agent, "observe_naively", None)
    elif perspective is Perspective.EXPERT:
        method = getattr(agent, "observe_as_expert", None)
    else:
        method = getattr(agent, "observe_orthogonally", None)
    if not callable(method):
        raise TypeError(f"{perspective.value} agent does not implement its role-specific protocol")
    return cast(PerspectiveAgentOutput, method(context))


def _reject_self_authored_counterexample_receipts(
    framing: ProblemFraming,
    *,
    stage: str,
) -> None:
    """Keep model-authored framing separate from verification authority."""

    if framing.counterexample_searches or any(
        challenge.verifications for challenge in framing.challenges
    ):
        raise ValueError(
            f"{stage} cannot author counterexample search or verification receipts; "
            "use a PanelEvaluator with an independent verifier bound by the "
            "evaluation policy"
        )


def _apply_counterexample_review(
    framing: ProblemFraming,
    review: CounterexampleReviewOutput,
    *,
    evaluation_id: str,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
) -> ProblemFraming:
    """Validate untrusted decisions and mint exact authority-bound receipts."""

    claims = {claim.claim_id: claim for claim in framing.claims}
    challenges = {challenge.challenge_id: challenge for challenge in framing.challenges}
    search_ids = [decision.target_claim_id for decision in review.searches]
    if len(search_ids) != len(set(search_ids)):
        raise ValueError("counterexample verifier returned duplicate claim search decisions")
    verification_ids = [decision.challenge_id for decision in review.verifications]
    if len(verification_ids) != len(set(verification_ids)):
        raise ValueError("counterexample verifier returned duplicate challenge decisions")

    searches_by_proposition: dict[str, CounterexampleSearchDecision] = {}
    for decision in review.searches:
        claim = claims.get(decision.target_claim_id)
        if claim is None or not claim.challenge_eligible:
            raise ValueError("counterexample verifier searched an unknown or ineligible claim")
        if claim.proposition_digest in searches_by_proposition:
            raise ValueError("counterexample verifier returned duplicate proposition coverage")
        searches_by_proposition[claim.proposition_digest] = decision
    expected_propositions = {
        claim.proposition_digest for claim in framing.claims if claim.challenge_eligible
    }
    if set(searches_by_proposition) != expected_propositions:
        raise ValueError(
            "counterexample verifier must return exactly one search decision per eligible "
            "final proposition"
        )
    if set(verification_ids) != set(challenges):
        raise ValueError(
            "counterexample verifier must return exactly one decision per final challenge"
        )

    verification_by_challenge = {
        decision.challenge_id: decision for decision in review.verifications
    }
    reviewed_challenges: list[ClaimChallenge] = []
    challenge_digests_by_proposition: dict[str, set[str]] = {}
    for challenge in framing.challenges:
        decision = verification_by_challenge[challenge.challenge_id]
        receipt = ChallengeVerification(
            verification_id=(
                f"verification:{evaluation_id}:{challenge.challenge_id}:"
                f"{policy.counterexample_verifier_id}"
            ),
            challenge_digest=challenge.digest,
            target_proposition_digest=challenge.target_proposition_digest,
            evidence_revision=snapshot.revision,
            evidence_digest=snapshot.digest,
            policy_digest=policy.digest,
            verifier_id=policy.counterexample_verifier_id,
            protocol_id=policy.counterexample_verification_protocol_id,
            outcome=decision.outcome,
            detail=decision.detail,
        )
        reviewed_challenges.append(challenge.model_copy(update={"verifications": (receipt,)}))
        challenge_digests_by_proposition.setdefault(
            challenge.target_proposition_digest,
            set(),
        ).add(challenge.digest)

    searches: list[CounterexampleSearchReceipt] = []
    for proposition_digest in sorted(searches_by_proposition):
        decision = searches_by_proposition[proposition_digest]
        unknown_challenge_ids = sorted(set(decision.challenge_ids) - set(challenges))
        if unknown_challenge_ids:
            raise ValueError(
                "counterexample search decision names unknown challenges: "
                + ", ".join(unknown_challenge_ids)
            )
        named_challenges = tuple(
            challenges[challenge_id].digest for challenge_id in decision.challenge_ids
        )
        expected_challenges = challenge_digests_by_proposition.get(proposition_digest, set())
        if set(named_challenges) != expected_challenges:
            raise ValueError(
                "counterexample search decision must name every and only final challenge "
                "for its proposition"
            )
        if decision.found != bool(named_challenges):
            raise ValueError(
                "counterexample search outcome must be FOUND exactly when challenges exist"
            )
        searches.append(
            CounterexampleSearchReceipt(
                search_id=(
                    f"search:{evaluation_id}:{decision.target_claim_id}:"
                    f"{policy.counterexample_verifier_id}"
                ),
                target_claim_id=decision.target_claim_id,
                target_proposition_digest=proposition_digest,
                evidence_revision=snapshot.revision,
                evidence_digest=snapshot.digest,
                policy_digest=policy.digest,
                searcher_id=policy.counterexample_verifier_id,
                protocol_id=policy.counterexample_search_protocol_id,
                max_attempts=policy.counterexample_search_max_attempts,
                outcome=decision.outcome,
                challenge_digests=named_challenges,
                detail=decision.detail,
            )
        )
    return framing.model_copy(
        update={
            "challenges": tuple(reviewed_challenges),
            "counterexample_searches": tuple(searches),
        }
    )


class ThreePerspectivePanelEvaluator:
    """Evaluate one prompt through independent roles and exact-bound consensus."""

    def __init__(
        self,
        *,
        agents: Mapping[Perspective, PerspectiveAgent],
        synthesizer: FramingSynthesizer,
        verifier: CounterexampleVerifier,
        ratifiers: Mapping[Perspective, PerspectiveRatifier],
        scorer: PanelScorer,
    ) -> None:
        self._agents = _require_all_roles("agents", agents)
        self._synthesizer = synthesizer
        self._verifier = verifier
        self._ratifiers = _require_all_roles("ratifiers", ratifiers)
        self._scorer = scorer

    def evaluate(
        self,
        prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ) -> PanelEvaluation:
        """Run observe → synthesize → ratify → score exactly once per stage."""

        binding = EvaluationBinding.for_candidate(prompt, snapshot, policy)
        evaluation_id = uuid4().hex
        observations: list[PerspectiveObservation] = []

        for perspective in _PERSPECTIVE_ORDER:
            context = PerspectiveContext(
                candidate_prompt=prompt,
                snapshot=snapshot,
                policy=policy,
                binding=binding,
                perspective=perspective,
                protocol_id=policy.protocol_for(perspective),
            )
            output = _observe(perspective, self._agents[perspective], context)
            _reject_self_authored_counterexample_receipts(
                output.framing,
                stage=f"{perspective.value} observation",
            )
            observations.append(
                PerspectiveObservation(
                    observation_id=(f"observation:{evaluation_id}:{perspective.value}"),
                    binding=binding,
                    perspective=perspective,
                    protocol_id=context.protocol_id,
                    framing=output.framing,
                    confidence=output.confidence,
                    feedback=output.feedback,
                )
            )

        bound_observations = tuple(observations)
        synthesis = self._synthesizer.synthesize(
            SynthesisContext(
                candidate_prompt=prompt,
                snapshot=snapshot,
                policy=policy,
                binding=binding,
                protocol_id=policy.synthesis_protocol_id,
                observations=bound_observations,
            )
        )
        _reject_self_authored_counterexample_receipts(
            synthesis.framing,
            stage="synthesis",
        )
        verifier_id = getattr(self._verifier, "verifier_id", None)
        if verifier_id != policy.counterexample_verifier_id:
            raise ValueError(
                "counterexample verifier identity does not match the evaluation policy"
            )
        review = self._verifier.review(
            CounterexampleReviewContext(
                candidate_prompt=prompt,
                snapshot=snapshot,
                policy=policy,
                binding=binding,
                verifier_id=policy.counterexample_verifier_id,
                search_protocol_id=policy.counterexample_search_protocol_id,
                verification_protocol_id=policy.counterexample_verification_protocol_id,
                max_search_attempts=policy.counterexample_search_max_attempts,
                observations=bound_observations,
                framing=synthesis.framing,
            )
        )
        reviewed_framing = _apply_counterexample_review(
            synthesis.framing,
            review,
            evaluation_id=evaluation_id,
            snapshot=snapshot,
            policy=policy,
        )

        votes: list[RatificationVote] = []
        by_perspective = {
            observation.perspective: observation for observation in bound_observations
        }
        for perspective in _PERSPECTIVE_ORDER:
            ratification = self._ratifiers[perspective].ratify(
                RatificationContext(
                    candidate_prompt=prompt,
                    snapshot=snapshot,
                    policy=policy,
                    binding=binding,
                    perspective=perspective,
                    protocol_id=policy.ratification_protocol_id,
                    observation=by_perspective[perspective],
                    framing=reviewed_framing,
                )
            )
            votes.append(
                RatificationVote(
                    vote_id=f"vote:{evaluation_id}:{perspective.value}",
                    binding=binding,
                    perspective=perspective,
                    protocol_id=policy.ratification_protocol_id,
                    approved=ratification.approved,
                    reason=ratification.reason,
                    observation_id=by_perspective[perspective].observation_id,
                    observation_digest=by_perspective[perspective].digest,
                    framing_id=reviewed_framing.framing_id,
                    framing_digest=reviewed_framing.digest,
                )
            )

        bound_votes = tuple(votes)
        scoring = self._scorer.score(
            ScoringContext(
                candidate_prompt=prompt,
                snapshot=snapshot,
                policy=policy,
                binding=binding,
                protocol_id=policy.scoring_protocol_id,
                observations=bound_observations,
                framing=reviewed_framing,
                votes=bound_votes,
            )
        )
        return PanelEvaluation(
            candidate_prompt=prompt,
            evidence_revision=snapshot.revision,
            evidence_ids=snapshot.evidence_ids,
            evidence_digest=snapshot.digest,
            policy=policy,
            observations=bound_observations,
            synthesis_protocol_id=policy.synthesis_protocol_id,
            scoring_protocol_id=policy.scoring_protocol_id,
            framing=reviewed_framing,
            votes=bound_votes,
            scores=scoring.scores,
            feedback=(
                *(observation.feedback for observation in bound_observations),
                *synthesis.feedback,
                *scoring.feedback,
            ),
        )


__all__ = [
    "ChallengeVerificationDecision",
    "CounterexampleReviewContext",
    "CounterexampleReviewOutput",
    "CounterexampleSearchDecision",
    "CounterexampleVerifier",
    "ExpertPerspectiveAgent",
    "FramingSynthesizer",
    "NaivePerspectiveAgent",
    "OrthogonalPerspectiveAgent",
    "PanelScorer",
    "PerspectiveAgent",
    "PerspectiveAgentOutput",
    "PerspectiveContext",
    "PerspectiveRatifier",
    "RatificationContext",
    "RatificationOutput",
    "ScoringContext",
    "ScoringOutput",
    "SynthesisContext",
    "SynthesisOutput",
    "ThreePerspectivePanelEvaluator",
]
