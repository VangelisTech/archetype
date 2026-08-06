# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for counterexample-bound problem-definition evaluation."""

from __future__ import annotations

import pytest

from examples.problem_definition_mission.contracts import (
    AtomicClaim,
    ChallengeKind,
    ChallengeVerification,
    ChallengeVerificationOutcome,
    ClaimChallenge,
    ClaimKind,
    CounterexampleSearchOutcome,
    EvaluationBinding,
    EvidenceDisposition,
    EvidenceDispositionKind,
    EvidenceItem,
    EvidenceSnapshot,
    PanelEvaluation,
    Perspective,
    PerspectiveObservation,
    ProblemDefinitionPolicy,
    ProblemFraming,
    RatificationVote,
    ScoreVector,
    bounded_counterexample_search_receipts,
)

_CANDIDATE_PROMPT = (
    "Define the exact evidence-supported problem and try to falsify each material claim."
)


def _snapshot() -> EvidenceSnapshot:
    return EvidenceSnapshot(
        revision=2,
        items=(
            EvidenceItem(
                evidence_id="claim-source",
                source="user:problem-statement",
                content="The current workflow ratifies claims before testing concrete exceptions.",
            ),
            EvidenceItem(
                evidence_id="counterexample-witness",
                source="user:counterexample",
                content="A supplied case violates the proposed universal boundary.",
            ),
        ),
    )


def _claim(*, statement: str = "Every supplied case obeys the proposed boundary.") -> AtomicClaim:
    return AtomicClaim(
        claim_id="universal-boundary",
        kind=ClaimKind.OBSERVATION,
        statement=statement,
        evidence_ids=("claim-source",),
        confidence=0.9,
        falsifier="One reproducible supplied case outside the proposed boundary.",
    )


def _challenge(
    claim: AtomicClaim,
    *,
    target_claim_id: str | None = None,
    target_proposition_digest: str | None = None,
    evidence_ids: tuple[str, ...] = ("counterexample-witness",),
) -> ClaimChallenge:
    return ClaimChallenge(
        challenge_id="counterexample-1",
        kind=ChallengeKind.COUNTEREXAMPLE,
        target_claim_id=target_claim_id or claim.claim_id,
        target_proposition_digest=target_proposition_digest or claim.proposition_digest,
        statement="The witnessed case falls outside the claim's universal boundary.",
        witness='{"case": "outside-boundary"}',
        reproduction="Evaluate the supplied case against the stated boundary.",
        evidence_ids=evidence_ids,
    )


def _verification(
    snapshot: EvidenceSnapshot,
    challenge: ClaimChallenge,
    *,
    outcome: ChallengeVerificationOutcome,
    challenge_digest: str | None = None,
    target_proposition_digest: str | None = None,
    evidence_revision: int | None = None,
    evidence_digest: str | None = None,
    policy_digest: str | None = None,
    verifier_id: str | None = None,
    protocol_id: str | None = None,
) -> ChallengeVerification:
    policy = ProblemDefinitionPolicy.default_three_perspective()
    return ChallengeVerification(
        verification_id="verification-1",
        challenge_digest=challenge_digest or challenge.digest,
        target_proposition_digest=(
            target_proposition_digest or challenge.target_proposition_digest
        ),
        evidence_revision=(snapshot.revision if evidence_revision is None else evidence_revision),
        evidence_digest=evidence_digest or snapshot.digest,
        policy_digest=policy_digest or policy.digest,
        verifier_id=verifier_id or policy.counterexample_verifier_id,
        protocol_id=protocol_id or policy.counterexample_verification_protocol_id,
        outcome=outcome,
        detail="The witness was independently replayed against the exact target.",
    )


def _with_verification(
    challenge: ClaimChallenge,
    verification: ChallengeVerification,
) -> ClaimChallenge:
    return challenge.model_copy(update={"verifications": (verification,)})


def _framing(
    snapshot: EvidenceSnapshot,
    *,
    claim: AtomicClaim | None = None,
    challenges: tuple[ClaimChallenge, ...] = (),
) -> ProblemFraming:
    claim = claim or _claim()
    return ProblemFraming(
        framing_id="framing-1",
        statement="Material framing claims are promoted without concrete exception testing.",
        subject="Problem-definition claim testing",
        current_state="Universal framing claims can be ratified without adversarial witnesses.",
        desired_state="Concrete challenges remain attached until independently rejected.",
        gap="The workflow has no exact-claim counterexample gate.",
        stakes="A false framing can direct all later work toward the wrong problem.",
        in_scope=("Atomic claims", "Concrete counterexamples"),
        out_of_scope=("Selecting a solution",),
        success_criteria=("No active challenge survives promotion.",),
        claims=(claim,),
        challenges=challenges,
        evidence_dispositions=tuple(
            EvidenceDisposition(
                evidence_id=evidence_id,
                disposition=EvidenceDispositionKind.SUPPORTS,
                reason="The item grounds either the target claim or its concrete challenge.",
            )
            for evidence_id in snapshot.evidence_ids
        ),
        contradictions=(),
        unknowns=("Whether another boundary case exists.",),
        next_question="Which concrete case would disprove this claim?",
    )


def _scores() -> ScoreVector:
    return ScoreVector(
        naive_clarity=0.95,
        expert_grounding=0.95,
        orthogonal_robustness=0.95,
        consensus=1.0,
        atomicity=0.95,
        falsifiability=1.0,
        scope_discipline=0.95,
        solution_independence=1.0,
        unsupported_claim_penalty=0.0,
        contradiction_penalty=0.0,
    )


def _panel(
    snapshot: EvidenceSnapshot,
    framing: ProblemFraming,
    *,
    observation_framings: tuple[ProblemFraming, ...] | None = None,
    vote_framing: ProblemFraming | None = None,
    add_search_coverage: bool = True,
    no_challenge_outcome: CounterexampleSearchOutcome = (
        CounterexampleSearchOutcome.NOT_FOUND_WITHIN_BUDGET
    ),
) -> PanelEvaluation:
    policy = ProblemDefinitionPolicy.default_three_perspective()
    binding = EvaluationBinding.for_candidate(_CANDIDATE_PROMPT, snapshot, policy)
    observation_framings = observation_framings or (framing,) * len(Perspective)
    vote_framing = vote_framing or framing
    observations = tuple(
        PerspectiveObservation(
            observation_id=f"observation-{perspective.value}",
            binding=binding,
            perspective=perspective,
            protocol_id=policy.protocol_for(perspective),
            framing=observation_framing,
            confidence=0.9,
            feedback="The framing exposes an exact, contestable claim.",
        )
        for perspective, observation_framing in zip(
            Perspective,
            observation_framings,
            strict=True,
        )
    )
    if add_search_coverage:
        framing = framing.model_copy(
            update={
                "counterexample_searches": bounded_counterexample_search_receipts(
                    framing,
                    snapshot,
                    policy,
                    detail="One deterministic bounded test search was performed.",
                    no_challenge_outcome=no_challenge_outcome,
                )
            }
        )
        vote_framing = vote_framing.model_copy(
            update={
                "counterexample_searches": bounded_counterexample_search_receipts(
                    vote_framing,
                    snapshot,
                    policy,
                    detail="One deterministic bounded test search was performed.",
                    no_challenge_outcome=no_challenge_outcome,
                )
            }
        )
    votes = tuple(
        RatificationVote(
            vote_id=f"vote-{perspective.value}",
            binding=binding,
            perspective=perspective,
            protocol_id=policy.ratification_protocol_id,
            approved=True,
            reason="The exact synthesized framing is acceptable.",
            observation_id=observations[index].observation_id,
            observation_digest=observations[index].digest,
            framing_id=vote_framing.framing_id,
            framing_digest=vote_framing.digest,
        )
        for index, perspective in enumerate(Perspective)
    )
    return PanelEvaluation(
        candidate_prompt=_CANDIDATE_PROMPT,
        evidence_revision=snapshot.revision,
        evidence_ids=snapshot.evidence_ids,
        evidence_digest=snapshot.digest,
        policy=policy,
        observations=observations,
        synthesis_protocol_id=policy.synthesis_protocol_id,
        scoring_protocol_id=policy.scoring_protocol_id,
        framing=framing,
        votes=votes,
        scores=_scores(),
    )


def test_challenge_vocabularies_and_atomic_claim_digest_are_exact() -> None:
    assert tuple(ChallengeKind) == (ChallengeKind.COUNTEREXAMPLE,)
    assert tuple(ChallengeVerificationOutcome) == (
        ChallengeVerificationOutcome.CONFIRMED,
        ChallengeVerificationOutcome.REJECTED,
        ChallengeVerificationOutcome.INCONCLUSIVE,
    )

    claim = _claim()
    exact_replay = _claim()
    changed_claims = (
        claim.model_copy(update={"kind": ClaimKind.INFERENCE}),
        claim.model_copy(update={"statement": "Some supplied cases obey the boundary."}),
        claim.model_copy(update={"evidence_ids": ("counterexample-witness",)}),
        claim.model_copy(update={"confidence": 0.8}),
        claim.model_copy(update={"falsifier": "A different falsifier."}),
    )

    assert exact_replay.digest == claim.digest
    assert claim.model_copy(update={"claim_id": "other-local-label"}).digest == claim.digest
    assert len(claim.digest) == 64
    assert all(changed.digest != claim.digest for changed in changed_claims)


def test_claim_challenge_requires_a_concrete_evidence_bound_witness() -> None:
    claim = _claim()

    with pytest.raises(ValueError, match="claim challenge must cite evidence"):
        _challenge(claim, evidence_ids=())
    with pytest.raises(ValueError, match="claim challenge evidence IDs must be unique"):
        _challenge(
            claim,
            evidence_ids=("counterexample-witness", "counterexample-witness"),
        )
    with pytest.raises(ValueError, match="reproduction must not be empty"):
        ClaimChallenge(
            challenge_id="counterexample-1",
            kind=ChallengeKind.COUNTEREXAMPLE,
            target_claim_id=claim.claim_id,
            target_proposition_digest=claim.proposition_digest,
            statement="A concrete exception.",
            witness='{"case": "outside-boundary"}',
            reproduction=" ",
            evidence_ids=("counterexample-witness",),
        )


def test_claim_challenge_digest_ignores_local_labels_but_binds_witness_content() -> None:
    claim = _claim()
    challenge = _challenge(claim)
    relabeled = challenge.model_copy(
        update={
            "challenge_id": "other-challenge-label",
            "target_claim_id": "renamed-local-claim-label",
        }
    )
    changed_challenges = (
        challenge.model_copy(update={"target_proposition_digest": "other-target-digest"}),
        challenge.model_copy(update={"statement": "A materially different challenge."}),
        challenge.model_copy(update={"witness": '{"case": "different"}'}),
        challenge.model_copy(update={"reproduction": "Use a different replay procedure."}),
        challenge.model_copy(update={"evidence_ids": ("claim-source",)}),
    )

    assert relabeled.digest == challenge.digest
    assert all(changed.digest != challenge.digest for changed in changed_challenges)


@pytest.mark.parametrize(
    ("outcome", "expected_active", "feedback_outcome"),
    [
        (None, True, "unverified"),
        (ChallengeVerificationOutcome.CONFIRMED, True, "confirmed"),
        (ChallengeVerificationOutcome.INCONCLUSIVE, True, "inconclusive"),
        (ChallengeVerificationOutcome.REJECTED, False, None),
    ],
)
def test_only_a_rejected_exact_challenge_stops_blocking_promotion(
    outcome: ChallengeVerificationOutcome | None,
    expected_active: bool,
    feedback_outcome: str | None,
) -> None:
    snapshot = _snapshot()
    proposal = _challenge(_claim())
    challenge = proposal
    if outcome is not None:
        challenge = _with_verification(
            proposal,
            _verification(snapshot, proposal, outcome=outcome),
        )
    framing = _framing(snapshot, challenges=(challenge,))
    evaluation = _panel(snapshot, framing)

    assert evaluation.grounding_errors() == ()
    assert challenge.active is expected_active
    assert evaluation.active_challenges == ((challenge,) if expected_active else ())
    assert evaluation.hard_gate_passed is (not expected_active)
    feedback = str(evaluation.side_info["Feedback"])
    if feedback_outcome is None:
        assert "Active counterexample challenge" not in feedback
    else:
        assert f"({feedback_outcome})" in feedback


@pytest.mark.parametrize(
    ("corruption", "expected_error"),
    [
        ("target_id", "targets unknown claim stale-claim"),
        ("target_digest", "binds the wrong target proposition digest"),
        ("challenge_evidence", "cites unknown evidence: evidence-ghost"),
        ("receipt_challenge", "verification binds the wrong challenge digest"),
        ("receipt_target", "verification binds the wrong target proposition digest"),
        ("receipt_revision", "verification has the wrong evidence binding"),
        ("receipt_evidence", "verification has the wrong evidence binding"),
        ("receipt_policy", "verification has the wrong policy binding"),
        ("receipt_verifier", "verification has the wrong verifier authority"),
        ("receipt_protocol", "verification has the wrong protocol"),
    ],
)
def test_challenge_grounding_binds_exact_target_evidence_and_receipt(
    corruption: str,
    expected_error: str,
) -> None:
    snapshot = _snapshot()
    claim = _claim()
    target_claim_id = "stale-claim" if corruption == "target_id" else claim.claim_id
    target_proposition_digest = (
        "stale-target-digest" if corruption == "target_digest" else claim.proposition_digest
    )
    evidence_ids = (
        ("evidence-ghost",) if corruption == "challenge_evidence" else ("counterexample-witness",)
    )
    proposal = _challenge(
        claim,
        target_claim_id=target_claim_id,
        target_proposition_digest=target_proposition_digest,
        evidence_ids=evidence_ids,
    )
    verification = _verification(
        snapshot,
        proposal,
        outcome=ChallengeVerificationOutcome.REJECTED,
        challenge_digest=(
            "stale-challenge-digest" if corruption == "receipt_challenge" else proposal.digest
        ),
        target_proposition_digest=(
            "stale-receipt-target"
            if corruption == "receipt_target"
            else proposal.target_proposition_digest
        ),
        evidence_revision=(
            snapshot.revision - 1 if corruption == "receipt_revision" else snapshot.revision
        ),
        evidence_digest=(
            "stale-evidence-digest" if corruption == "receipt_evidence" else snapshot.digest
        ),
        policy_digest=("stale-policy-digest" if corruption == "receipt_policy" else None),
        verifier_id=("self-declared-verifier" if corruption == "receipt_verifier" else None),
        protocol_id=(
            "wrong-verification-protocol"
            if corruption == "receipt_protocol"
            else ProblemDefinitionPolicy.default_three_perspective().counterexample_verification_protocol_id
        ),
    )
    challenge = _with_verification(proposal, verification)
    final_framing = _framing(snapshot, claim=claim, challenges=(challenge,))
    unchallenged_observation = _framing(snapshot, claim=claim)
    evaluation = _panel(
        snapshot,
        final_framing,
        observation_framings=(unchallenged_observation,) * len(Perspective),
    )

    assert challenge.active is False
    assert any(expected_error in error for error in evaluation.grounding_errors())
    assert evaluation.hard_gate_passed is False


def test_active_observation_challenge_survives_synthesis_for_an_unchanged_claim() -> None:
    snapshot = _snapshot()
    claim = _claim()
    challenge = _challenge(claim)
    challenged_observation = _framing(snapshot, claim=claim, challenges=(challenge,))
    final_framing = _framing(snapshot, claim=claim)
    evaluation = _panel(
        snapshot,
        final_framing,
        observation_framings=(
            challenged_observation,
            final_framing,
            final_framing,
        ),
    )

    assert (
        "final framing omits active challenge counterexample-1 "
        "for retained proposition universal-boundary"
    ) in evaluation.grounding_errors()
    assert evaluation.hard_gate_passed is False


@pytest.mark.parametrize("resolution", ["rejected", "target-revised"])
def test_observation_challenge_can_be_omitted_after_resolution(
    resolution: str,
) -> None:
    snapshot = _snapshot()
    observed_claim = _claim()
    proposal = _challenge(observed_claim)
    final_claim = observed_claim
    observation_challenge = proposal
    if resolution == "rejected":
        observation_challenge = _with_verification(
            proposal,
            _verification(
                snapshot,
                proposal,
                outcome=ChallengeVerificationOutcome.REJECTED,
            ),
        )
    else:
        final_claim = _claim(
            statement="The supplied evidence does not establish a universal boundary."
        )

    challenged_observation = _framing(
        snapshot,
        claim=observed_claim,
        challenges=(observation_challenge,),
    )
    final_framing = _framing(snapshot, claim=final_claim)
    evaluation = _panel(
        snapshot,
        final_framing,
        observation_framings=(
            challenged_observation,
            final_framing,
            final_framing,
        ),
    )

    assert evaluation.grounding_errors() == ()
    assert evaluation.active_challenges == ()
    assert evaluation.hard_gate_passed is True


def test_verification_changes_framing_digest_and_makes_prior_votes_stale() -> None:
    snapshot = _snapshot()
    claim = _claim()
    proposal = _challenge(claim)
    challenged_framing = _framing(snapshot, claim=claim, challenges=(proposal,))
    rejected = _with_verification(
        proposal,
        _verification(
            snapshot,
            proposal,
            outcome=ChallengeVerificationOutcome.REJECTED,
        ),
    )
    resolved_framing = _framing(snapshot, claim=claim, challenges=(rejected,))

    assert rejected.digest == proposal.digest
    assert resolved_framing.digest != challenged_framing.digest

    stale_votes = _panel(
        snapshot,
        resolved_framing,
        vote_framing=challenged_framing,
    )
    assert any(
        "vote binds the wrong framing digest" in error for error in stale_votes.grounding_errors()
    )
    assert stale_votes.active_challenges == ()
    assert stale_votes.hard_gate_passed is False

    refreshed_votes = _panel(snapshot, resolved_framing)
    assert refreshed_votes.grounding_errors() == ()
    assert refreshed_votes.hard_gate_passed is True


def test_proposition_digest_ignores_annotations_but_not_the_assertion() -> None:
    claim = _claim().model_copy(update={"evidence_ids": ("claim-source", "counterexample-witness")})
    annotation_changes = (
        claim.model_copy(update={"confidence": 0.1}),
        claim.model_copy(update={"falsifier": "A more precise replay condition."}),
        claim.model_copy(update={"evidence_ids": tuple(reversed(claim.evidence_ids))}),
        claim.model_copy(update={"kind": ClaimKind.CONSTRAINT}),
    )

    assert claim.challenge_eligible is True
    assert all(
        changed.proposition_digest == claim.proposition_digest for changed in annotation_changes
    )
    assert all(changed.digest != claim.digest for changed in annotation_changes)
    assert (
        claim.model_copy(
            update={"statement": "Only some cases obey the boundary."}
        ).proposition_digest
        != claim.proposition_digest
    )
    assert claim.model_copy(update={"kind": ClaimKind.UNKNOWN}).challenge_eligible is False


@pytest.mark.parametrize(
    "update",
    [
        {"confidence": 0.1},
        {"falsifier": "A different prospective falsifier."},
        {"kind": ClaimKind.INFERENCE},
    ],
)
def test_metadata_only_claim_change_cannot_discard_an_active_challenge(
    update: dict[str, object],
) -> None:
    snapshot = _snapshot()
    observed_claim = _claim()
    challenge = _challenge(observed_claim)
    observation = _framing(snapshot, claim=observed_claim, challenges=(challenge,))
    final_claim = observed_claim.model_copy(update=update)
    final = _framing(snapshot, claim=final_claim)

    evaluation = _panel(
        snapshot,
        final,
        observation_framings=(observation, final, final),
    )

    assert final_claim.proposition_digest == observed_claim.proposition_digest
    assert any("omits active challenge" in error for error in evaluation.grounding_errors())
    assert evaluation.hard_gate_passed is False


def test_confirmed_verification_cannot_be_downgraded_during_synthesis() -> None:
    snapshot = _snapshot()
    claim = _claim()
    proposal = _challenge(claim)
    confirmed_receipt = _verification(
        snapshot,
        proposal,
        outcome=ChallengeVerificationOutcome.CONFIRMED,
    ).model_copy(update={"verification_id": "verification-confirmed"})
    rejected_receipt = _verification(
        snapshot,
        proposal,
        outcome=ChallengeVerificationOutcome.REJECTED,
    ).model_copy(update={"verification_id": "verification-rejected"})
    confirmed = _with_verification(proposal, confirmed_receipt)
    rejected = _with_verification(proposal, rejected_receipt)
    confirmed_observation = _framing(snapshot, claim=claim, challenges=(confirmed,))
    rejected_final = _framing(snapshot, claim=claim, challenges=(rejected,))

    evaluation = _panel(
        snapshot,
        rejected_final,
        observation_framings=(confirmed_observation, rejected_final, rejected_final),
    )

    errors = evaluation.grounding_errors()
    assert any("conflicting verification outcomes" in error for error in errors)
    assert any("omits immutable verification receipts" in error for error in errors)
    assert evaluation.active_challenges == ()
    assert evaluation.hard_gate_passed is False


def test_absent_counterexample_search_is_not_treated_as_no_counterexample_found() -> None:
    snapshot = _snapshot()
    evaluation = _panel(snapshot, _framing(snapshot), add_search_coverage=False)

    assert any(
        "lacks bounded counterexample search coverage" in error
        for error in evaluation.grounding_errors()
    )
    assert evaluation.hard_gate_passed is False


def test_bounded_not_found_search_is_recorded_as_evidence_not_proof() -> None:
    snapshot = _snapshot()
    evaluation = _panel(snapshot, _framing(snapshot))

    assert evaluation.grounding_errors() == ()
    assert evaluation.hard_gate_passed is True
    feedback = str(evaluation.side_info["Feedback"])
    assert "within the recorded budget" in feedback
    assert "bounded search evidence, not proof" in feedback


def test_inconclusive_counterexample_search_blocks_promotion() -> None:
    snapshot = _snapshot()
    evaluation = _panel(
        snapshot,
        _framing(snapshot),
        no_challenge_outcome=CounterexampleSearchOutcome.INCONCLUSIVE,
    )

    assert any("is inconclusive" in error for error in evaluation.grounding_errors())
    assert evaluation.hard_gate_passed is False


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("evidence_revision", 1, "wrong evidence binding"),
        ("evidence_digest", "stale-evidence", "wrong evidence binding"),
        ("policy_digest", "stale-policy", "wrong policy binding"),
        ("searcher_id", "self-appointed-searcher", "wrong searcher authority"),
        ("protocol_id", "stale-search-protocol", "wrong protocol"),
        ("max_attempts", 2, "wrong budget"),
        ("target_proposition_digest", "other-proposition", "wrong target proposition"),
    ],
)
def test_counterexample_search_receipt_is_exact_bound(
    field: str,
    value: object,
    expected_error: str,
) -> None:
    snapshot = _snapshot()
    evaluation = _panel(snapshot, _framing(snapshot))
    receipt = evaluation.framing.counterexample_searches[0].model_copy(update={field: value})
    corrupted = evaluation.model_copy(
        update={
            "framing": evaluation.framing.model_copy(update={"counterexample_searches": (receipt,)})
        }
    )

    assert any(expected_error in error for error in corrupted.grounding_errors())
    assert corrupted.hard_gate_passed is False


def test_counterexample_cannot_target_a_non_propositional_claim() -> None:
    snapshot = _snapshot()
    unknown = AtomicClaim(
        claim_id="open-question",
        kind=ClaimKind.UNKNOWN,
        statement="Whether another boundary case exists.",
        evidence_ids=(),
        confidence=0.0,
        falsifier="",
    )
    challenge = ClaimChallenge(
        challenge_id="misapplied-counterexample",
        kind=ChallengeKind.COUNTEREXAMPLE,
        target_claim_id=unknown.claim_id,
        target_proposition_digest=unknown.proposition_digest,
        statement="An unknown is not an asserted proposition.",
        witness="A purported witness.",
        reproduction="Attempt to replay the purported witness.",
        evidence_ids=("counterexample-witness",),
    )
    evaluation = _panel(
        snapshot,
        _framing(snapshot, claim=unknown, challenges=(challenge,)),
    )

    assert any("not challenge-eligible" in error for error in evaluation.grounding_errors())
    assert evaluation.hard_gate_passed is False
