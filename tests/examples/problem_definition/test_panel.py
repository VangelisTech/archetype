# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the role-separated problem-definition panel."""

from __future__ import annotations

from typing import Any

import pytest

from examples.problem_definition_mission.contracts import (
    AtomicClaim,
    ChallengeKind,
    ChallengeVerificationOutcome,
    ClaimChallenge,
    ClaimKind,
    CounterexampleSearchOutcome,
    EvaluationBinding,
    EvidenceDisposition,
    EvidenceDispositionKind,
    EvidenceItem,
    EvidenceSnapshot,
    Perspective,
    ProblemDefinitionPolicy,
    ProblemFraming,
    ScoreVector,
    bounded_no_counterexample_search_receipts,
)
from examples.problem_definition_mission.panel import (
    ChallengeVerificationDecision,
    CounterexampleReviewContext,
    CounterexampleReviewOutput,
    CounterexampleSearchDecision,
    PerspectiveAgentOutput,
    PerspectiveContext,
    RatificationContext,
    RatificationOutput,
    ScoringContext,
    ScoringOutput,
    SynthesisContext,
    SynthesisOutput,
    ThreePerspectivePanelEvaluator,
)


def _snapshot() -> EvidenceSnapshot:
    return EvidenceSnapshot(
        revision=1,
        items=(
            EvidenceItem(
                evidence_id="interview",
                source="user interview",
                content="The team repeatedly asks what problem it is solving.",
            ),
        ),
    )


def _framing(snapshot: EvidenceSnapshot, framing_id: str) -> ProblemFraming:
    return ProblemFraming(
        framing_id=framing_id,
        statement="The team cannot establish a shared problem boundary before solution search.",
        subject="The team's problem-definition workflow",
        current_state="Evidence and proposed solutions are mixed into competing framings.",
        desired_state="One evidence-bound and falsifiable framing is independently ratified.",
        gap="No explicit convergence step separates the problem from proposed solutions.",
        stakes="A wrong framing directs later work toward the wrong target.",
        in_scope=("Evidence interpretation", "Problem boundaries"),
        out_of_scope=("Selecting a solution",),
        success_criteria=("All three perspectives ratify the exact synthesis.",),
        claims=(
            AtomicClaim(
                claim_id=f"{framing_id}:observed-gap",
                kind=ClaimKind.OBSERVATION,
                statement="The team repeatedly questions its current problem framing.",
                evidence_ids=snapshot.evidence_ids,
                confidence=0.9,
                falsifier="The interview instead contains one stable shared framing.",
            ),
        ),
        evidence_dispositions=(
            EvidenceDisposition(
                evidence_id="interview",
                disposition=EvidenceDispositionKind.SUPPORTS,
                reason="It directly reports recurring uncertainty about the framing.",
            ),
        ),
        contradictions=(),
        unknowns=("Whether another stakeholder uses a different boundary.",),
        next_question="Which assumption most changes the problem boundary?",
    )


def _scores() -> ScoreVector:
    return ScoreVector(
        naive_clarity=0.9,
        expert_grounding=0.9,
        orthogonal_robustness=0.9,
        consensus=1.0,
        atomicity=0.9,
        falsifiability=0.9,
        scope_discipline=0.95,
        solution_independence=1.0,
        unsupported_claim_penalty=0.0,
        contradiction_penalty=0.0,
    )


class _Recorder:
    def __init__(self, snapshot: EvidenceSnapshot, policy: ProblemDefinitionPolicy) -> None:
        self.snapshot = snapshot
        self.policy = policy
        self.events: list[tuple[str, Perspective | None]] = []
        self.perspective_contexts: list[PerspectiveContext] = []
        self.synthesis_contexts: list[SynthesisContext] = []
        self.review_contexts: list[CounterexampleReviewContext] = []
        self.ratification_contexts: list[RatificationContext] = []
        self.scoring_contexts: list[ScoringContext] = []

    def observe(
        self,
        context: PerspectiveContext,
        expected: Perspective,
    ) -> PerspectiveAgentOutput:
        assert context.perspective is expected
        assert context.protocol_id == self.policy.protocol_for(expected)
        assert context.snapshot is self.snapshot
        assert not hasattr(context, "observations")
        self.events.append(("observe", expected))
        self.perspective_contexts.append(context)
        return PerspectiveAgentOutput(
            framing=_framing(self.snapshot, f"{expected.value}-framing"),
            confidence=0.8,
            feedback=f"{expected.value} feedback",
        )


class _NaiveAgent:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder

    def observe_naively(self, context: PerspectiveContext) -> PerspectiveAgentOutput:
        return self.recorder.observe(context, Perspective.NAIVE)


class _ExpertAgent:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder

    def observe_as_expert(self, context: PerspectiveContext) -> PerspectiveAgentOutput:
        return self.recorder.observe(context, Perspective.EXPERT)


class _OrthogonalAgent:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder

    def observe_orthogonally(
        self,
        context: PerspectiveContext,
    ) -> PerspectiveAgentOutput:
        return self.recorder.observe(context, Perspective.ORTHOGONAL)


class _Synthesizer:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder

    def synthesize(self, context: SynthesisContext) -> SynthesisOutput:
        self.recorder.events.append(("synthesize", None))
        self.recorder.synthesis_contexts.append(context)
        assert tuple(observation.perspective for observation in context.observations) == tuple(
            Perspective
        )
        return SynthesisOutput(
            framing=_framing(self.recorder.snapshot, "synthesized-framing"),
            feedback=("synthesis feedback",),
        )


class _Ratifier:
    def __init__(self, recorder: _Recorder, perspective: Perspective) -> None:
        self.recorder = recorder
        self.perspective = perspective

    def ratify(self, context: RatificationContext) -> RatificationOutput:
        assert context.perspective is self.perspective
        assert context.observation.perspective is self.perspective
        assert context.framing.framing_id == "synthesized-framing"
        self.recorder.events.append(("ratify", self.perspective))
        self.recorder.ratification_contexts.append(context)
        return RatificationOutput(
            approved=True,
            reason=f"{self.perspective.value} constraints survived synthesis.",
        )


class _Scorer:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder

    def score(self, context: ScoringContext) -> ScoringOutput:
        assert context.protocol_id == self.recorder.policy.scoring_protocol_id
        self.recorder.events.append(("score", None))
        self.recorder.scoring_contexts.append(context)
        return ScoringOutput(
            scores=_scores(),
            feedback=("scoring feedback",),
        )


class _Verifier:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder
        self.verifier_id = recorder.policy.counterexample_verifier_id

    def review(self, context: CounterexampleReviewContext) -> CounterexampleReviewOutput:
        assert context.verifier_id == self.verifier_id
        assert context.search_protocol_id == self.recorder.policy.counterexample_search_protocol_id
        assert (
            context.verification_protocol_id
            == self.recorder.policy.counterexample_verification_protocol_id
        )
        self.recorder.events.append(("verify", None))
        self.recorder.review_contexts.append(context)
        return CounterexampleReviewOutput(
            searches=tuple(
                CounterexampleSearchDecision(
                    target_claim_id=claim.claim_id,
                    outcome=CounterexampleSearchOutcome.NOT_FOUND_WITHIN_BUDGET,
                    detail="One bounded deterministic search found no concrete witness.",
                )
                for claim in context.framing.claims
                if claim.challenge_eligible
            ),
            verifications=(),
        )


class _ChallengeSynthesizer(_Synthesizer):
    def synthesize(self, context: SynthesisContext) -> SynthesisOutput:
        output = super().synthesize(context)
        target = output.framing.claims[0]
        challenge = ClaimChallenge(
            challenge_id="synthesized-counterexample",
            kind=ChallengeKind.COUNTEREXAMPLE,
            target_claim_id=target.claim_id,
            target_proposition_digest=target.proposition_digest,
            statement="A supplied occurrence may fall outside the asserted boundary.",
            witness="The interview is the concrete boundary witness.",
            reproduction="Replay the interview against the exact proposition.",
            evidence_ids=self.recorder.snapshot.evidence_ids,
        )
        return output.model_copy(
            update={"framing": output.framing.model_copy(update={"challenges": (challenge,)})}
        )


class _SelfAuthorizingSynthesizer(_Synthesizer):
    def synthesize(self, context: SynthesisContext) -> SynthesisOutput:
        output = super().synthesize(context)
        searches = bounded_no_counterexample_search_receipts(
            output.framing,
            self.recorder.snapshot,
            self.recorder.policy,
            detail="The synthesizer must not grant itself search authority.",
        )
        return output.model_copy(
            update={
                "framing": output.framing.model_copy(update={"counterexample_searches": searches})
            }
        )


class _DispositionVerifier(_Verifier):
    def __init__(
        self,
        recorder: _Recorder,
        outcome: ChallengeVerificationOutcome,
    ) -> None:
        super().__init__(recorder)
        self.outcome = outcome

    def review(self, context: CounterexampleReviewContext) -> CounterexampleReviewOutput:
        self.recorder.events.append(("verify", None))
        self.recorder.review_contexts.append(context)
        challenge = context.framing.challenges[0]
        target = context.framing.claims[0]
        return CounterexampleReviewOutput(
            searches=(
                CounterexampleSearchDecision(
                    target_claim_id=target.claim_id,
                    outcome=CounterexampleSearchOutcome.FOUND,
                    challenge_ids=(challenge.challenge_id,),
                    detail="The bounded search found this exact proposed witness.",
                ),
            ),
            verifications=(
                ChallengeVerificationDecision(
                    challenge_id=challenge.challenge_id,
                    outcome=self.outcome,
                    detail="The independent role replayed the exact witness.",
                ),
            ),
        )


def _evaluator(
    recorder: _Recorder,
    *,
    agents: dict[Perspective, Any] | None = None,
    ratifiers: dict[Perspective, Any] | None = None,
    verifier: Any | None = None,
    synthesizer: Any | None = None,
) -> ThreePerspectivePanelEvaluator:
    return ThreePerspectivePanelEvaluator(
        agents=agents
        or {
            Perspective.ORTHOGONAL: _OrthogonalAgent(recorder),
            Perspective.NAIVE: _NaiveAgent(recorder),
            Perspective.EXPERT: _ExpertAgent(recorder),
        },
        synthesizer=synthesizer or _Synthesizer(recorder),
        verifier=verifier or _Verifier(recorder),
        ratifiers=ratifiers
        or {
            Perspective.EXPERT: _Ratifier(recorder, Perspective.EXPERT),
            Perspective.ORTHOGONAL: _Ratifier(recorder, Perspective.ORTHOGONAL),
            Perspective.NAIVE: _Ratifier(recorder, Perspective.NAIVE),
        },
        scorer=_Scorer(recorder),
    )


def test_panel_isolates_roles_and_runs_the_canonical_sequence() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    prompt = "Define the evidence-supported problem without proposing a solution."

    evaluation = _evaluator(recorder).evaluate(prompt, snapshot, policy)

    assert recorder.events == [
        ("observe", Perspective.NAIVE),
        ("observe", Perspective.EXPERT),
        ("observe", Perspective.ORTHOGONAL),
        ("synthesize", None),
        ("verify", None),
        ("ratify", Perspective.NAIVE),
        ("ratify", Perspective.EXPERT),
        ("ratify", Perspective.ORTHOGONAL),
        ("score", None),
    ]
    assert len(recorder.perspective_contexts) == 3
    assert len({id(context) for context in recorder.perspective_contexts}) == 3
    assert len(recorder.synthesis_contexts) == 1
    assert len(recorder.review_contexts) == 1
    assert len(recorder.ratification_contexts) == 3
    assert len(recorder.scoring_contexts) == 1
    assert evaluation.framing.framing_id == "synthesized-framing"
    assert tuple(observation.framing.framing_id for observation in evaluation.observations) == (
        "naive-framing",
        "expert-framing",
        "orthogonal-framing",
    )
    assert evaluation.feedback == (
        "naive feedback",
        "expert feedback",
        "orthogonal feedback",
        "synthesis feedback",
        "scoring feedback",
    )


def test_panel_constructs_unique_ids_and_exact_bindings() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    prompt = "Define the evidence-supported problem without proposing a solution."

    evaluation = _evaluator(recorder).evaluate(prompt, snapshot, policy)
    expected_binding = EvaluationBinding.for_candidate(prompt, snapshot, policy)
    observation_ids = {observation.observation_id for observation in evaluation.observations}
    vote_ids = {vote.vote_id for vote in evaluation.votes}

    assert evaluation.binding == expected_binding
    assert len(observation_ids) == 3
    assert len(vote_ids) == 3
    assert observation_ids.isdisjoint(vote_ids)
    assert all(observation.binding == expected_binding for observation in evaluation.observations)
    assert all(vote.binding == expected_binding for vote in evaluation.votes)
    assert all(
        vote.framing_id == evaluation.framing.framing_id
        and vote.framing_digest == evaluation.framing.digest
        for vote in evaluation.votes
    )
    assert all(context.binding == expected_binding for context in recorder.perspective_contexts)
    assert all(
        context.binding == expected_binding
        and context.framing is evaluation.framing
        and context.observation.perspective is context.perspective
        for context in recorder.ratification_contexts
    )
    assert evaluation.grounding_errors() == ()
    assert evaluation.hard_gate_passed is True


@pytest.mark.parametrize("missing_mapping", ["agents", "ratifiers"])
def test_panel_rejects_a_missing_role_mapping(missing_mapping: str) -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    agents: dict[Perspective, Any] | None = None
    ratifiers: dict[Perspective, Any] | None = None
    missing = {
        Perspective.NAIVE: _NaiveAgent(recorder),
        Perspective.EXPERT: _ExpertAgent(recorder),
    }
    if missing_mapping == "agents":
        agents = missing
    else:
        ratifiers = {
            Perspective.NAIVE: _Ratifier(recorder, Perspective.NAIVE),
            Perspective.EXPERT: _Ratifier(recorder, Perspective.EXPERT),
        }

    with pytest.raises(
        ValueError,
        match=rf"{missing_mapping} must map exactly one provider to each Perspective",
    ):
        _evaluator(recorder, agents=agents, ratifiers=ratifiers)


def test_role_mapping_requires_the_role_specific_agent_protocol() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    agents: dict[Perspective, Any] = {
        Perspective.NAIVE: _ExpertAgent(recorder),
        Perspective.EXPERT: _ExpertAgent(recorder),
        Perspective.ORTHOGONAL: _OrthogonalAgent(recorder),
    }

    with pytest.raises(
        TypeError,
        match="naive agent does not implement its role-specific protocol",
    ):
        _evaluator(recorder, agents=agents).evaluate("Define the problem.", snapshot, policy)


@pytest.mark.parametrize("shared_mapping", ["agents", "ratifiers"])
def test_panel_requires_distinct_role_provider_instances(shared_mapping: str) -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    shared: Any
    agents: dict[Perspective, Any] | None = None
    ratifiers: dict[Perspective, Any] | None = None
    if shared_mapping == "agents":
        shared = _NaiveAgent(recorder)
        agents = {perspective: shared for perspective in Perspective}
    else:
        shared = _Ratifier(recorder, Perspective.NAIVE)
        ratifiers = {perspective: shared for perspective in Perspective}

    with pytest.raises(
        ValueError,
        match=rf"{shared_mapping} must use a distinct provider instance",
    ):
        _evaluator(recorder, agents=agents, ratifiers=ratifiers)


@pytest.mark.parametrize(
    ("outcome", "expected_active", "expected_gate"),
    [
        (ChallengeVerificationOutcome.REJECTED, False, True),
        (ChallengeVerificationOutcome.CONFIRMED, True, False),
        (ChallengeVerificationOutcome.INCONCLUSIVE, True, False),
    ],
)
def test_counterexample_review_runs_before_votes_and_controls_the_hard_gate(
    outcome: ChallengeVerificationOutcome,
    expected_active: bool,
    expected_gate: bool,
) -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    evaluation = _evaluator(
        recorder,
        synthesizer=_ChallengeSynthesizer(recorder),
        verifier=_DispositionVerifier(recorder, outcome),
    ).evaluate("Define and challenge the exact problem.", snapshot, policy)

    challenge = evaluation.framing.challenges[0]
    assert challenge.verifications[0].outcome is outcome
    assert challenge.verifications[0].policy_digest == policy.digest
    assert challenge.verifications[0].verifier_id == policy.counterexample_verifier_id
    assert challenge.active is expected_active
    assert evaluation.hard_gate_passed is expected_gate
    assert recorder.events.index(("verify", None)) < recorder.events.index(
        ("ratify", Perspective.NAIVE)
    )
    assert all(context.framing is evaluation.framing for context in recorder.ratification_contexts)
    assert all(vote.framing_digest == evaluation.framing.digest for vote in evaluation.votes)


def test_counterexample_verifier_identity_must_match_the_policy() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    verifier = _Verifier(recorder)
    verifier.verifier_id = "self-appointed-verifier"

    with pytest.raises(ValueError, match="identity does not match"):
        _evaluator(recorder, verifier=verifier).evaluate(
            "Define the problem.",
            snapshot,
            policy,
        )


def test_counterexample_verifier_must_cover_every_final_proposition() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)
    verifier = _Verifier(recorder)

    def omit_searches(context: CounterexampleReviewContext) -> CounterexampleReviewOutput:
        del context
        return CounterexampleReviewOutput(searches=(), verifications=())

    verifier.review = omit_searches  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="exactly one search decision"):
        _evaluator(recorder, verifier=verifier).evaluate(
            "Define the problem.",
            snapshot,
            policy,
        )


def test_synthesizer_cannot_author_counterexample_authority_receipts() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    recorder = _Recorder(snapshot, policy)

    with pytest.raises(ValueError, match="cannot author counterexample search"):
        _evaluator(
            recorder,
            synthesizer=_SelfAuthorizingSynthesizer(recorder),
        ).evaluate("Define the problem.", snapshot, policy)
