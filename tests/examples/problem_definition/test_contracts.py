# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for evidence-bound problem-definition evaluation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from examples.problem_definition_mission.contracts import (
    AtomicClaim,
    ClaimKind,
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
    append_evidence,
    bounded_counterexample_search_receipts,
)

_CANDIDATE_PROMPT = "Define only the evidence-supported problem. Do not propose a solution."


def _evidence(evidence_id: str, content: str) -> EvidenceItem:
    return EvidenceItem(
        evidence_id=evidence_id,
        source=f"user:{evidence_id}",
        content=content,
    )


def _framing(
    snapshot: EvidenceSnapshot,
    *,
    claim_evidence_ids: list[str] | None = None,
    dispositions: list[EvidenceDisposition] | None = None,
    contradictions: list[str] | None = None,
) -> ProblemFraming:
    evidence_ids = list(snapshot.evidence_ids) if claim_evidence_ids is None else claim_evidence_ids
    if dispositions is None:
        dispositions = [
            EvidenceDisposition(
                evidence_id=evidence_id,
                disposition=EvidenceDispositionKind.SUPPORTS,
                reason="Directly constrains the stated gap.",
            )
            for evidence_id in snapshot.evidence_ids
        ]
    return ProblemFraming(
        framing_id="framing-1",
        statement="Operators cannot reliably identify the problem before choosing a solution.",
        subject="Problem-definition workflow",
        current_state="Incoming evidence is interpreted without an explicit shared framing.",
        desired_state="Independent perspectives ratify one evidence-bound framing.",
        gap="No convergent, inspectable problem-definition step exists.",
        stakes="A wrong framing sends all later problem solving in the wrong direction.",
        in_scope=["The problem statement", "Evidence provenance"],
        out_of_scope=["Selecting or implementing a solution"],
        success_criteria=["All three perspectives ratify the exact same framing."],
        claims=[
            AtomicClaim(
                claim_id="claim-observed-gap",
                kind=ClaimKind.OBSERVATION,
                statement="The supplied evidence describes framing divergence.",
                evidence_ids=evidence_ids,
                confidence=0.9,
                falsifier="The evidence instead contains one explicit, shared framing.",
            )
        ],
        evidence_dispositions=dispositions,
        contradictions=contradictions or [],
        unknowns=["Which additional evidence would most reduce uncertainty?"],
        next_question="What problem are we solving?",
    )


def _scores() -> ScoreVector:
    return ScoreVector(
        naive_clarity=0.95,
        expert_grounding=0.95,
        orthogonal_robustness=0.9,
        consensus=1.0,
        atomicity=0.9,
        falsifiability=0.9,
        scope_discipline=0.95,
        solution_independence=1.0,
        unsupported_claim_penalty=0.0,
        contradiction_penalty=0.0,
    )


def _panel(
    snapshot: EvidenceSnapshot,
    *,
    framing: ProblemFraming | None = None,
    observation_perspectives: tuple[Perspective, ...] = tuple(Perspective),
    vote_perspectives: tuple[Perspective, ...] = tuple(Perspective),
    observation_ids: tuple[str, ...] | None = None,
    vote_ids: tuple[str, ...] | None = None,
    observation_binding: EvaluationBinding | None = None,
    vote_binding: EvaluationBinding | None = None,
    observation_protocol_id: str | None = None,
    vote_protocol_id: str | None = None,
    synthesis_protocol_id: str | None = None,
    scoring_protocol_id: str | None = None,
    vote_framing_id: str | None = None,
    vote_framing_digest: str | None = None,
) -> PanelEvaluation:
    policy = ProblemDefinitionPolicy.default_three_perspective()
    raw_framing = framing or _framing(snapshot)
    binding = EvaluationBinding.for_candidate(_CANDIDATE_PROMPT, snapshot, policy)
    observation_ids = observation_ids or tuple(
        f"observation-{index}" for index in range(len(observation_perspectives))
    )
    vote_ids = vote_ids or tuple(f"vote-{index}" for index in range(len(vote_perspectives)))
    observations = tuple(
        PerspectiveObservation(
            observation_id=observation_id,
            binding=observation_binding or binding,
            perspective=perspective,
            protocol_id=observation_protocol_id or policy.protocol_for(perspective),
            framing=raw_framing,
            confidence=0.9,
            feedback=f"{perspective.value} perspective accepts the bounded gap.",
        )
        for observation_id, perspective in zip(
            observation_ids,
            observation_perspectives,
            strict=True,
        )
    )
    framing = raw_framing.model_copy(
        update={
            "counterexample_searches": bounded_counterexample_search_receipts(
                raw_framing,
                snapshot,
                policy,
                detail="One deterministic bounded contract-test search was performed.",
            )
        }
    )
    observation_by_perspective = {
        observation.perspective: observation for observation in observations
    }
    votes = tuple(
        RatificationVote(
            vote_id=vote_id,
            binding=vote_binding or binding,
            perspective=perspective,
            protocol_id=vote_protocol_id or policy.ratification_protocol_id,
            approved=True,
            reason="The synthesis preserves this perspective's material constraints.",
            observation_id=(
                observation_by_perspective[perspective].observation_id
                if perspective in observation_by_perspective
                else "missing-observation"
            ),
            observation_digest=(
                observation_by_perspective[perspective].digest
                if perspective in observation_by_perspective
                else "missing-observation-digest"
            ),
            framing_id=vote_framing_id or framing.framing_id,
            framing_digest=vote_framing_digest or framing.digest,
        )
        for vote_id, perspective in zip(vote_ids, vote_perspectives, strict=True)
    )
    return PanelEvaluation(
        candidate_prompt=_CANDIDATE_PROMPT,
        evidence_revision=snapshot.revision,
        evidence_ids=snapshot.evidence_ids,
        evidence_digest=snapshot.digest,
        policy=policy,
        observations=observations,
        synthesis_protocol_id=synthesis_protocol_id or policy.synthesis_protocol_id,
        scoring_protocol_id=scoring_protocol_id or policy.scoring_protocol_id,
        framing=framing,
        votes=votes,
        scores=_scores(),
    )


def test_evidence_snapshot_is_canonical_and_append_is_idempotent() -> None:
    first = _evidence("evidence-a", "The team repeatedly asks what problem it is solving.")
    second = _evidence("evidence-b", "Three independent perspectives should converge.")

    left = EvidenceSnapshot(revision=2, items=(second, first))
    right = EvidenceSnapshot(revision=2, items=(first, second))

    assert left.items == (first, second)
    assert left.evidence_ids == ("evidence-a", "evidence-b")
    assert left.digest == right.digest
    assert (
        first.digest
        == _evidence(
            "evidence-a",
            "The team repeatedly asks what problem it is solving.",
        ).digest
    )

    seed = EvidenceSnapshot(revision=0)
    once = append_evidence(seed, first)
    replayed = append_evidence(once, _evidence(first.evidence_id, first.content))
    assert replayed is once
    assert replayed.revision == 1


def test_evidence_identity_rejects_changed_content_and_duplicate_snapshot_ids() -> None:
    original = _evidence("evidence-a", "Original")
    changed = EvidenceItem(
        evidence_id=original.evidence_id,
        source=original.source,
        content="Changed",
    )
    snapshot = append_evidence(EvidenceSnapshot(revision=0), original)

    with pytest.raises(ValueError, match="already exists with different content"):
        append_evidence(snapshot, changed)
    with pytest.raises(ValueError, match="evidence IDs must be unique"):
        EvidenceSnapshot(revision=1, items=(original, changed))


def test_atomic_observations_require_evidence_and_hypotheses_require_falsifiers() -> None:
    with pytest.raises(ValueError, match="observation claim must cite evidence"):
        AtomicClaim(
            claim_id="observation",
            kind=ClaimKind.OBSERVATION,
            statement="A factual observation.",
            evidence_ids=[],
            confidence=1.0,
            falsifier="Contrary source evidence.",
        )
    with pytest.raises(ValueError, match="challenge-eligible claim must state a falsifier"):
        AtomicClaim(
            claim_id="hypothesis",
            kind=ClaimKind.HYPOTHESIS,
            statement="A testable explanation.",
            evidence_ids=[],
            confidence=0.5,
            falsifier=" ",
        )

    observation = AtomicClaim(
        claim_id="observation",
        kind=ClaimKind.OBSERVATION,
        statement="A factual observation.",
        evidence_ids=["evidence-a"],
        confidence=1.0,
        falsifier="Contrary source evidence.",
    )
    hypothesis = AtomicClaim(
        claim_id="hypothesis",
        kind=ClaimKind.HYPOTHESIS,
        statement="A testable explanation.",
        evidence_ids=["evidence-a"],
        confidence=0.5,
        falsifier="A ratified framing performs no better than the baseline.",
    )
    assert observation.kind is ClaimKind.OBSERVATION
    assert hypothesis.kind is ClaimKind.HYPOTHESIS


def test_complete_grounded_panel_passes_the_hard_gate() -> None:
    snapshot = EvidenceSnapshot(
        revision=2,
        items=(
            _evidence("evidence-a", "The current problem is underspecified."),
            _evidence("evidence-b", "Agreement must include an orthogonal view."),
        ),
    )
    evaluation = _panel(snapshot)
    expected_binding = EvaluationBinding.for_candidate(
        _CANDIDATE_PROMPT,
        snapshot,
        evaluation.policy,
    )

    assert evaluation.grounding_errors() == ()
    assert evaluation.unanimous is True
    assert evaluation.hard_gate_passed is True
    assert evaluation.binding == expected_binding
    assert all(observation.binding == expected_binding for observation in evaluation.observations)
    assert all(vote.binding == expected_binding for vote in evaluation.votes)
    assert 0.0 <= evaluation.aggregate_score <= 1.0
    assert evaluation.side_info["scores"]["supported_claims"] == 1.0
    assert evaluation.side_info["scores"]["contradiction_free"] == 1.0
    assert evaluation.side_info["Feedback"]
    assert evaluation.candidate_digest == _panel(snapshot).candidate_digest


def test_candidate_binding_changes_with_the_exact_evaluation_context() -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "One evidence item."),),
    )
    policy = ProblemDefinitionPolicy.default_three_perspective()
    binding = EvaluationBinding.for_candidate(_CANDIDATE_PROMPT, snapshot, policy)

    changed_prompt = EvaluationBinding.for_candidate(
        _CANDIDATE_PROMPT + " Be concise.",
        snapshot,
        policy,
    )
    changed_snapshot = EvaluationBinding.for_candidate(
        _CANDIDATE_PROMPT,
        append_evidence(snapshot, _evidence("evidence-b", "New information.")),
        policy,
    )

    assert binding.candidate_digest != changed_prompt.candidate_digest
    assert binding.candidate_digest != changed_snapshot.candidate_digest
    assert binding.evidence_digest == snapshot.digest
    assert binding.policy_digest == policy.digest
    assert binding.evaluator_id == policy.evaluator_id


@pytest.mark.parametrize(
    ("observations", "votes", "expected_error"),
    [
        (
            (Perspective.NAIVE, Perspective.EXPERT),
            tuple(Perspective),
            "one or more perspective observations are missing",
        ),
        (
            (
                Perspective.NAIVE,
                Perspective.NAIVE,
                Perspective.EXPERT,
                Perspective.ORTHOGONAL,
            ),
            tuple(Perspective),
            "perspective observations are duplicated",
        ),
        (
            tuple(Perspective),
            (Perspective.NAIVE, Perspective.EXPERT),
            "one or more ratification votes are missing",
        ),
        (
            tuple(Perspective),
            (
                Perspective.NAIVE,
                Perspective.NAIVE,
                Perspective.EXPERT,
                Perspective.ORTHOGONAL,
            ),
            "ratification votes are duplicated",
        ),
    ],
)
def test_missing_or_duplicate_panel_roles_fail_binding(
    observations: tuple[Perspective, ...],
    votes: tuple[Perspective, ...],
    expected_error: str,
) -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "One evidence item."),),
    )
    evaluation = _panel(
        snapshot,
        observation_perspectives=observations,
        vote_perspectives=votes,
    )

    assert expected_error in evaluation.grounding_errors()
    assert evaluation.hard_gate_passed is False


def test_vote_must_bind_the_exact_synthesized_framing() -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "One evidence item."),),
    )
    evaluation = _panel(
        snapshot,
        vote_framing_id="other-framing",
        vote_framing_digest="wrong-digest",
    )

    assert any("vote names the wrong framing" in error for error in evaluation.grounding_errors())
    assert any(
        "vote binds the wrong framing digest" in error for error in evaluation.grounding_errors()
    )
    assert evaluation.hard_gate_passed is False


@pytest.mark.parametrize(
    ("panel_overrides", "expected_error"),
    [
        (
            {"observation_protocol_id": "wrong-observation-protocol"},
            "observation has the wrong protocol",
        ),
        (
            {"vote_protocol_id": "wrong-vote-protocol"},
            "vote has the wrong protocol",
        ),
        (
            {"synthesis_protocol_id": "wrong-synthesis-protocol"},
            "synthesis has the wrong protocol",
        ),
        (
            {"scoring_protocol_id": "wrong-scoring-protocol"},
            "scoring has the wrong protocol",
        ),
    ],
)
def test_wrong_panel_protocols_fail_grounding(
    panel_overrides: dict[str, str],
    expected_error: str,
) -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "One evidence item."),),
    )
    evaluation = _panel(snapshot, **panel_overrides)

    assert any(expected_error in error for error in evaluation.grounding_errors())
    assert evaluation.hard_gate_passed is False


@pytest.mark.parametrize(
    ("binding_target", "expected_error"),
    [
        ("observation", "observation has the wrong evaluation binding"),
        ("vote", "vote has the wrong evaluation binding"),
    ],
)
def test_wrong_independent_call_bindings_fail_grounding(
    binding_target: str,
    expected_error: str,
) -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "One evidence item."),),
    )
    policy = ProblemDefinitionPolicy.default_three_perspective()
    correct = EvaluationBinding.for_candidate(_CANDIDATE_PROMPT, snapshot, policy)
    wrong = EvaluationBinding(
        candidate_digest="wrong-candidate-digest",
        evidence_revision=correct.evidence_revision,
        evidence_digest=correct.evidence_digest,
        policy_digest=correct.policy_digest,
        evaluator_id=correct.evaluator_id,
    )
    overrides = {f"{binding_target}_binding": wrong}
    evaluation = _panel(snapshot, **overrides)

    assert any(expected_error in error for error in evaluation.grounding_errors())
    assert evaluation.hard_gate_passed is False


@pytest.mark.parametrize(
    ("panel_overrides", "expected_error"),
    [
        (
            {"observation_ids": ("duplicate", "duplicate", "observation-3")},
            "perspective observation IDs are duplicated",
        ),
        (
            {"vote_ids": ("duplicate", "duplicate", "vote-3")},
            "ratification vote IDs are duplicated",
        ),
    ],
)
def test_duplicate_independent_call_ids_fail_grounding(
    panel_overrides: dict[str, tuple[str, ...]],
    expected_error: str,
) -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "One evidence item."),),
    )
    evaluation = _panel(snapshot, **panel_overrides)

    assert expected_error in evaluation.grounding_errors()
    assert evaluation.hard_gate_passed is False


def test_missing_and_unknown_evidence_dispositions_fail_grounding() -> None:
    snapshot = EvidenceSnapshot(
        revision=2,
        items=(
            _evidence("evidence-a", "First evidence item."),
            _evidence("evidence-b", "Second evidence item."),
        ),
    )
    dispositions = [
        EvidenceDisposition(
            evidence_id="evidence-a",
            disposition=EvidenceDispositionKind.SUPPORTS,
            reason="Supports the observed gap.",
        ),
        EvidenceDisposition(
            evidence_id="evidence-ghost",
            disposition=EvidenceDispositionKind.UNRESOLVED,
            reason="This occurrence does not belong to the snapshot.",
        ),
    ]
    evaluation = _panel(snapshot, framing=_framing(snapshot, dispositions=dispositions))

    assert "final framing omits evidence dispositions: evidence-b" in evaluation.grounding_errors()
    assert (
        "final framing dispositions cite unknown evidence: evidence-ghost"
        in evaluation.grounding_errors()
    )
    assert evaluation.hard_gate_passed is False


def test_unknown_claim_citation_fails_grounding() -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "Known evidence."),),
    )
    framing = _framing(snapshot, claim_evidence_ids=["evidence-ghost"])
    evaluation = _panel(snapshot, framing=framing)

    errors = evaluation.grounding_errors()
    assert "final claim claim-observed-gap cites unknown evidence: evidence-ghost" in errors
    assert any("cites unknown evidence: evidence-ghost" in error for error in errors)
    assert evaluation.hard_gate_passed is False


def test_unresolved_contradictions_block_an_otherwise_valid_panel() -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "Known evidence."),),
    )
    framing = _framing(
        snapshot,
        contradictions=["The evidence names two incompatible subjects."],
    )
    evaluation = _panel(snapshot, framing=framing)

    assert evaluation.grounding_errors() == ()
    assert evaluation.unanimous is True
    assert evaluation.hard_gate_passed is False
    assert "Unresolved contradictions" in evaluation.side_info["Feedback"]


def test_vote_binds_the_exact_role_observation() -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "Known evidence."),),
    )
    evaluation = _panel(snapshot)
    observations = list(evaluation.observations)
    observations[0] = observations[0].model_copy(
        update={"feedback": "A materially different role observation."}
    )
    stale = evaluation.model_copy(update={"observations": tuple(observations)})

    assert any(
        "vote binds the wrong observation digest" in error for error in stale.grounding_errors()
    )
    assert stale.hard_gate_passed is False


def test_default_policy_versions_every_counterexample_aware_stage() -> None:
    policy = ProblemDefinitionPolicy.default_three_perspective()

    assert policy.version == "2"
    assert "panel-v2" in policy.evaluator_id
    assert policy.counterexample_verifier_id != policy.evaluator_id
    protocol_ids = (
        policy.naive_protocol_id,
        policy.expert_protocol_id,
        policy.orthogonal_protocol_id,
        policy.synthesis_protocol_id,
        policy.ratification_protocol_id,
        policy.counterexample_search_protocol_id,
        policy.counterexample_verification_protocol_id,
        policy.scoring_protocol_id,
    )
    assert all(protocol_id.endswith("-v2") for protocol_id in protocol_ids)

    payload = policy.model_dump(mode="python", exclude={"digest"})
    payload["counterexample_verifier_id"] = policy.evaluator_id
    with pytest.raises(ValueError, match="independent"):
        ProblemDefinitionPolicy.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    [
        "counterexample_search_protocol_id",
        "counterexample_verification_protocol_id",
        "counterexample_verifier_id",
        "counterexample_search_max_attempts",
        "scoring_protocol_id",
    ],
)
def test_counterexample_policy_changes_invalidate_the_policy_digest(field: str) -> None:
    policy = ProblemDefinitionPolicy.default_three_perspective()
    value: object = (
        policy.counterexample_search_max_attempts + 1
        if field == "counterexample_search_max_attempts"
        else f"changed-{getattr(policy, field)}"
    )
    changed = policy.model_copy(update={field: value})

    assert changed.digest != policy.digest


def test_framing_nested_collections_are_immutable_and_digest_stable() -> None:
    evidence_ids = ["evidence-a"]
    in_scope = ["Problem framing"]
    out_of_scope = ["Solution selection"]
    success_criteria = ["All roles ratify."]
    claims = [
        AtomicClaim(
            claim_id="claim-a",
            kind=ClaimKind.OBSERVATION,
            statement="The evidence describes an underspecified problem.",
            evidence_ids=evidence_ids,
            confidence=0.9,
            falsifier="The evidence supplies a precise shared framing.",
        )
    ]
    dispositions = [
        EvidenceDisposition(
            evidence_id="evidence-a",
            disposition=EvidenceDispositionKind.SUPPORTS,
            reason="Direct observation.",
        )
    ]
    contradictions = ["One unresolved tension."]
    unknowns = ["The highest-value next evidence."]
    framing = ProblemFraming(
        framing_id="framing-immutable",
        statement="The problem is underspecified.",
        subject="Problem definition",
        current_state="No shared framing exists.",
        desired_state="A shared framing exists.",
        gap="The perspectives have not converged.",
        stakes="Work may optimize the wrong target.",
        in_scope=in_scope,
        out_of_scope=out_of_scope,
        success_criteria=success_criteria,
        claims=claims,
        evidence_dispositions=dispositions,
        contradictions=contradictions,
        unknowns=unknowns,
        next_question="What problem are we solving?",
    )
    original_digest = framing.digest

    evidence_ids.append("evidence-b")
    in_scope.append("An attempted mutation")
    out_of_scope.clear()
    success_criteria.clear()
    claims.clear()
    dispositions.clear()
    contradictions.clear()
    unknowns.clear()

    assert framing.in_scope == ("Problem framing",)
    assert framing.out_of_scope == ("Solution selection",)
    assert framing.success_criteria == ("All roles ratify.",)
    assert len(framing.claims) == 1
    assert framing.claims[0].evidence_ids == ("evidence-a",)
    assert len(framing.evidence_dispositions) == 1
    assert framing.contradictions == ("One unresolved tension.",)
    assert framing.unknowns == ("The highest-value next evidence.",)
    assert framing.digest == original_digest

    with pytest.raises(TypeError):
        framing.in_scope[0] = "Changed"
    with pytest.raises(ValidationError):
        framing.claims[0].evidence_ids = ("evidence-b",)
    assert framing.digest == original_digest


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"stakes": " "}, "stakes must not be empty"),
        ({"success_criteria": ()}, "success_criteria must not be empty"),
        ({"claims": ()}, "claims must not be empty"),
        ({"evidence_dispositions": ()}, "evidence_dispositions must not be empty"),
        (
            {"in_scope": ("Same boundary",), "out_of_scope": ("Same boundary",)},
            "in_scope and out_of_scope must not overlap",
        ),
    ],
)
def test_framing_requires_a_contestable_boundary(
    updates: dict[str, object],
    message: str,
) -> None:
    snapshot = EvidenceSnapshot(
        revision=1,
        items=(_evidence("evidence-a", "Known evidence."),),
    )
    payload = _framing(snapshot).model_dump(mode="python", exclude={"digest"})
    payload.update(updates)

    with pytest.raises(ValueError, match=message):
        ProblemFraming.model_validate(payload)
