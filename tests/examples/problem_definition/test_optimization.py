# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free contracts for problem-prompt optimization and promotion."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from examples.problem_definition_mission import (
    AtomicClaim,
    ChallengeKind,
    ChallengeVerification,
    ChallengeVerificationOutcome,
    ClaimChallenge,
    ClaimKind,
    EvaluationBinding,
    EvidenceDisposition,
    EvidenceDispositionKind,
    EvidenceItem,
    EvidenceSnapshot,
    GepaPromptConfig,
    PanelEvaluation,
    Perspective,
    PerspectiveObservation,
    ProblemDefinitionPolicy,
    ProblemFraming,
    RatificationVote,
    ScoreVector,
    bounded_counterexample_search_receipts,
    optimize_problem_prompt,
    select_prompt_head,
)

def _snapshot(*evidence_ids: str, revision: int = 1) -> EvidenceSnapshot:
    return EvidenceSnapshot(
        revision=revision,
        items=tuple(
            EvidenceItem(
                evidence_id=evidence_id,
                source=f"source:{evidence_id}",
                content=f"Observed fact for {evidence_id}.",
            )
            for evidence_id in evidence_ids
        ),
    )


def _framing(
    snapshot: EvidenceSnapshot,
    *,
    framing_id: str,
    contradictions: tuple[str, ...] = (),
) -> ProblemFraming:
    evidence_id = snapshot.evidence_ids[0]
    return ProblemFraming(
        framing_id=framing_id,
        statement="People cannot identify the bounded problem from the available evidence.",
        subject="People defining a problem",
        current_state="The problem boundary is ambiguous.",
        desired_state="The problem is atomic, grounded, and testable.",
        gap="The evidence has not been converted into a ratified framing.",
        stakes="Solving the wrong problem wastes effort.",
        in_scope=["Evidence-bound problem definition"],
        out_of_scope=["Choosing or implementing a solution"],
        success_criteria=["All three perspectives ratify the same grounded framing."],
        claims=[
            AtomicClaim(
                claim_id=f"{framing_id}:claim",
                kind=ClaimKind.OBSERVATION,
                statement="The supplied evidence describes an unresolved definition gap.",
                evidence_ids=[evidence_id],
                confidence=0.9,
                falsifier="Contrary evidence shows the boundary is already explicit.",
            )
        ],
        evidence_dispositions=[
            EvidenceDisposition(
                evidence_id=item.evidence_id,
                disposition=EvidenceDispositionKind.SUPPORTS,
                reason="The item contributes to the observed definition gap.",
            )
            for item in snapshot.items
        ],
        contradictions=list(contradictions),
        unknowns=["Whether another stakeholder sees a different boundary."],
        next_question="What problem are we solving?",
    )


def _scores(quality: float) -> ScoreVector:
    return ScoreVector(
        naive_clarity=quality,
        expert_grounding=quality,
        orthogonal_robustness=quality,
        consensus=quality,
        atomicity=quality,
        falsifiability=quality,
        scope_discipline=quality,
        solution_independence=quality,
        unsupported_claim_penalty=0.0,
        contradiction_penalty=0.0,
    )


def _evaluation(
    prompt: str,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
    quality: float,
    *,
    approved: bool = True,
    contradictions: tuple[str, ...] = (),
    evaluation_policy: ProblemDefinitionPolicy | None = None,
    framing: ProblemFraming | None = None,
) -> PanelEvaluation:
    bound_policy = evaluation_policy or policy
    binding = EvaluationBinding.for_candidate(prompt, snapshot, bound_policy)
    evaluated_framing = framing or _framing(
        snapshot, framing_id=f"framing:{prompt}", contradictions=contradictions
    )
    observations = tuple(
        PerspectiveObservation(
            observation_id=f"observation:{prompt}:{perspective.value}",
            binding=binding,
            perspective=perspective,
            protocol_id=bound_policy.protocol_for(perspective),
            framing=evaluated_framing,
            confidence=quality,
            feedback=f"{perspective.value} feedback for {prompt}",
        )
        for perspective in Perspective
    )
    evaluated_framing = evaluated_framing.model_copy(
        update={
            "counterexample_searches": bounded_counterexample_search_receipts(
                evaluated_framing,
                snapshot,
                bound_policy,
                detail="One deterministic bounded optimization-test search was performed.",
            )
        }
    )
    observation_by_perspective = {
        observation.perspective: observation for observation in observations
    }
    votes = tuple(
        RatificationVote(
            vote_id=f"vote:{prompt}:{perspective.value}",
            binding=binding,
            perspective=perspective,
            protocol_id=bound_policy.ratification_protocol_id,
            approved=approved if perspective is not Perspective.ORTHOGONAL else approved,
            reason="The framing is acceptable." if approved else "The boundary still leaks.",
            observation_id=observation_by_perspective[perspective].observation_id,
            observation_digest=observation_by_perspective[perspective].digest,
            framing_id=evaluated_framing.framing_id,
            framing_digest=evaluated_framing.digest,
        )
        for perspective in Perspective
    )
    return PanelEvaluation(
        candidate_prompt=prompt,
        evidence_revision=snapshot.revision,
        evidence_ids=snapshot.evidence_ids,
        evidence_digest=snapshot.digest,
        policy=bound_policy,
        observations=observations,
        synthesis_protocol_id=bound_policy.synthesis_protocol_id,
        scoring_protocol_id=bound_policy.scoring_protocol_id,
        framing=evaluated_framing,
        votes=votes,
        scores=_scores(quality),
        feedback=(f"Improve atomicity and boundary discipline for {prompt}.",),
    )


def test_selector_filters_stale_and_invalid_records_without_regression() -> None:
    snapshot = _snapshot("interview")
    stale_snapshot = _snapshot("interview", "later-research", revision=2)
    policy = ProblemDefinitionPolicy.default_three_perspective()
    incumbent = _evaluation(
        "An incumbent prompt with enough detail.",
        snapshot,
        policy,
        0.5,
    )
    records = (
        _evaluation("worse", snapshot, policy, 0.4),
        _evaluation("tie", snapshot, policy, 0.5),
        _evaluation("high but rejected", snapshot, policy, 1.0, approved=False),
        _evaluation(
            "high but contradictory",
            snapshot,
            policy,
            1.0,
            contradictions=("The scope is unresolved.",),
        ),
        _evaluation("stale high score", stale_snapshot, policy, 1.0),
        _evaluation(
            "wrong policy",
            snapshot,
            policy,
            1.0,
            evaluation_policy=policy.model_copy(update={"version": "different-policy"}),
        ),
        _evaluation(
            "wrong evaluator",
            snapshot,
            policy,
            1.0,
            evaluation_policy=policy.model_copy(update={"evaluator_id": "different-evaluator"}),
        ),
    )

    selected = select_prompt_head(
        incumbent,
        records,
        snapshot=snapshot,
        policy=policy,
    )

    assert selected is incumbent


def test_selector_uses_score_then_shorter_prompt_and_minimum_improvement() -> None:
    snapshot = _snapshot("interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    incumbent = _evaluation("incumbent", snapshot, policy, 0.5)
    longer = _evaluation("a much longer candidate prompt", snapshot, policy, 0.8)
    shorter = _evaluation("concise", snapshot, policy, 0.8)

    selected = select_prompt_head(
        incumbent,
        (longer, shorter),
        snapshot=snapshot,
        policy=policy,
        improvement_threshold=0.3,
    )

    assert selected is shorter


def test_unratified_incumbent_cannot_block_a_lower_scoring_ratified_head() -> None:
    snapshot = _snapshot("interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    incumbent = _evaluation(
        "high scalar but rejected",
        snapshot,
        policy,
        1.0,
        approved=False,
    )
    ratified = _evaluation("lower scalar but ratified", snapshot, policy, 0.4)

    selected = select_prompt_head(
        incumbent,
        (ratified,),
        snapshot=snapshot,
        policy=policy,
    )

    assert selected is ratified


def test_confirmed_counterexample_incumbent_cannot_block_revised_head() -> None:
    snapshot = _snapshot("counterexample", "interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    incumbent_prompt = "Preserve the original claim despite its counterexample."
    incumbent_framing = _framing(
        snapshot,
        framing_id=f"framing:{incumbent_prompt}",
    )
    target_claim = incumbent_framing.claims[0]
    counterexample = ClaimChallenge(
        challenge_id="challenge:confirmed-counterexample",
        kind=ChallengeKind.COUNTEREXAMPLE,
        target_claim_id=target_claim.claim_id,
        target_proposition_digest=target_claim.proposition_digest,
        statement="A concrete witness contradicts the incumbent claim.",
        witness="The recorded counterexample satisfies the claim's falsifier.",
        reproduction="Re-run the witness against the exact incumbent claim.",
        evidence_ids=("counterexample",),
    )
    confirmed_counterexample = counterexample.model_copy(
        update={
            "verifications": (
                ChallengeVerification(
                    verification_id="verification:confirmed-counterexample",
                    challenge_digest=counterexample.digest,
                    target_proposition_digest=target_claim.proposition_digest,
                    evidence_revision=snapshot.revision,
                    evidence_digest=snapshot.digest,
                    policy_digest=policy.digest,
                    verifier_id=policy.counterexample_verifier_id,
                    protocol_id=policy.counterexample_verification_protocol_id,
                    outcome=ChallengeVerificationOutcome.CONFIRMED,
                    detail="The witness independently reproduces and falsifies the claim.",
                ),
            )
        }
    )
    incumbent = _evaluation(
        incumbent_prompt,
        snapshot,
        policy,
        1.0,
        framing=incumbent_framing.model_copy(update={"challenges": (confirmed_counterexample,)}),
    )
    revised = _evaluation(
        "Revise the framing so the falsified claim is no longer retained.",
        snapshot,
        policy,
        0.4,
    )

    assert incumbent.unanimous is True
    assert incumbent.active_challenges == (confirmed_counterexample,)
    assert incumbent.hard_gate_passed is False
    assert revised.hard_gate_passed is True
    assert revised.aggregate_score < incumbent.aggregate_score

    selected = select_prompt_head(
        incumbent,
        (revised,),
        snapshot=snapshot,
        policy=policy,
    )

    assert selected is revised


def test_selector_returns_no_head_when_nothing_is_ratified() -> None:
    snapshot = _snapshot("interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    incumbent = _evaluation("rejected incumbent", snapshot, policy, 1.0, approved=False)
    rejected = _evaluation("rejected candidate", snapshot, policy, 1.0, approved=False)

    assert (
        select_prompt_head(
            incumbent,
            (rejected,),
            snapshot=snapshot,
            policy=policy,
        )
        is None
    )


class _ReflectionModel:
    def __init__(self, proposal: str) -> None:
        self.proposal = proposal
        self.prompts: list[str | list[dict[str, object]]] = []

    def __call__(self, prompt: str | list[dict[str, object]]) -> str:
        self.prompts.append(prompt)
        return f"```{self.proposal}```"


class _Panel:
    def __init__(
        self,
        seed_prompt: str,
        proposal: str,
        *,
        proposal_approved: bool = True,
    ) -> None:
        self.seed_prompt = seed_prompt
        self.proposal = proposal
        self.proposal_approved = proposal_approved
        self.calls: list[str] = []

    def evaluate(
        self,
        prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ) -> PanelEvaluation:
        self.calls.append(prompt)
        if prompt == self.proposal:
            return _evaluation(
                prompt,
                snapshot,
                policy,
                0.9 if self.proposal_approved else 1.0,
                approved=self.proposal_approved,
            )
        assert prompt == self.seed_prompt
        return _evaluation(prompt, snapshot, policy, 0.3)


def test_optimize_problem_prompt_uses_real_gepa_and_objective_side_info(
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = _snapshot("interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    seed = "Ask what problem we are solving."
    proposal = (
        "Define one evidence-bound problem using atomic claims, explicit unknowns, "
        "constraints, non-goals, and falsifiable success criteria."
    )
    panel = _Panel(seed, proposal)
    reflection = _ReflectionModel(proposal)

    result = optimize_problem_prompt(
        seed,
        snapshot,
        policy,
        panel,
        reflection,
        config=GepaPromptConfig(
            max_metric_calls=6,
            max_candidate_proposals=3,
            patience=2,
            seed=7,
            improvement_threshold=0.1,
        ),
    )

    assert result.accepted is True
    assert result.head_prompt == proposal
    assert result.head_evaluation is not None
    assert result.head_evaluation.aggregate_score == pytest.approx(0.9)
    assert result.records == tuple(
        _evaluation(prompt, snapshot, policy, 0.9 if prompt == proposal else 0.3)
        for prompt in panel.calls
    )
    assert len(panel.calls) <= 6
    assert reflection.prompts
    assert any(
        "Improve atomicity and boundary discipline" in prompt
        for prompt in reflection.prompts
        if isinstance(prompt, str)
    )
    assert result.gepa_best.best_prompt == proposal
    assert result.gepa_best.best_score == pytest.approx(0.9)
    assert result.gepa_best.optimizer_version == "0.1.1"
    assert dict(result.gepa_best.objective_pareto_front)["expert_grounding"] == 0.9
    assert tuple(candidate.prompt for candidate in result.gepa_best.candidates) == (
        seed,
        proposal,
    )
    assert result.gepa_best.candidates[0].parent_indices == (None,)
    assert result.gepa_best.candidates[1].parent_indices == (0,)
    assert capsys.readouterr() == ("", "")
    field_name = "accepted"
    with pytest.raises(FrozenInstanceError):
        setattr(result, field_name, False)


def test_gepa_best_cannot_bypass_ratification_gate() -> None:
    snapshot = _snapshot("interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    seed = "Keep the valid incumbent framing prompt."
    rejected = "A higher-scoring prompt whose framing the orthogonal role rejects."
    panel = _Panel(seed, rejected, proposal_approved=False)

    result = optimize_problem_prompt(
        seed,
        snapshot,
        policy,
        panel,
        _ReflectionModel(rejected),
        config=GepaPromptConfig(
            max_metric_calls=4,
            max_candidate_proposals=2,
            patience=1,
        ),
    )

    assert result.gepa_best.best_prompt == rejected
    assert result.gepa_best.best_score == 1.0
    assert result.accepted is False
    assert result.head_prompt == seed
    assert result.head_evaluation is not None
    assert result.head_evaluation.unanimous is True


def test_optimizer_rejects_an_evaluation_for_a_different_requested_prompt() -> None:
    snapshot = _snapshot("interview")
    policy = ProblemDefinitionPolicy.default_three_perspective()
    requested = "Evaluate this exact prompt."

    def mismatched_panel(
        prompt: str,
        bound_snapshot: EvidenceSnapshot,
        bound_policy: ProblemDefinitionPolicy,
    ) -> PanelEvaluation:
        assert prompt == requested
        return _evaluation(
            "A different prompt.",
            bound_snapshot,
            bound_policy,
            1.0,
        )

    with pytest.raises(ValueError, match="different prompt"):
        optimize_problem_prompt(
            requested,
            snapshot,
            policy,
            mismatched_panel,
            _ReflectionModel(requested),
            config=GepaPromptConfig(
                max_metric_calls=1,
                max_candidate_proposals=1,
                patience=1,
            ),
        )


def test_optimizer_evaluates_each_candidate_on_current_and_historical_snapshots() -> None:
    current = _snapshot("interview", "research", revision=2)
    historical = _snapshot("interview", revision=1)
    policy = ProblemDefinitionPolicy.default_three_perspective()
    seed = "Ask what problem we are solving."
    proposal = "Define an atomic evidence-bound problem."
    panel = _Panel(seed, proposal)

    result = optimize_problem_prompt(
        seed,
        current,
        policy,
        panel,
        _ReflectionModel(proposal),
        config=GepaPromptConfig(
            max_metric_calls=4,
            max_candidate_proposals=2,
            patience=1,
        ),
        historical_snapshots=(historical,),
    )

    assert result.head_prompt == proposal
    assert {record.evidence_revision for record in result.records} == {1, 2}
    prompts = {record.candidate_prompt for record in result.records}
    for prompt in prompts:
        assert sum(record.candidate_prompt == prompt for record in result.records) == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_metric_calls", 0),
        ("max_candidate_proposals", 0),
        ("patience", 0),
        ("seed", -1),
        ("improvement_threshold", float("nan")),
    ],
)
def test_gepa_prompt_config_rejects_unbounded_values(
    field: str,
    value: Any,
) -> None:
    with pytest.raises(ValueError):
        GepaPromptConfig(**{field: value})
