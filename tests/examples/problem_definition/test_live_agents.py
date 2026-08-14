# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free contracts for model-backed problem-definition agents."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel

from examples.problem_definition_mission.contracts import (
    AtomicClaim,
    ChallengeKind,
    ClaimChallenge,
    ClaimKind,
    CounterexampleSearchOutcome,
    EvidenceDisposition,
    EvidenceDispositionKind,
    EvidenceItem,
    EvidenceSnapshot,
    Perspective,
    ProblemDefinitionPolicy,
    ProblemFraming,
    ScoreVector,
)
from examples.problem_definition_mission.live_agents import (
    OpenAIReflectionLanguageModel,
    OpenAIStructuredLanguageModel,
    StructuredModelContextError,
    StructuredModelOutputError,
    _bind_challenge_target_digests,
    build_model_backed_panel,
)
from examples.problem_definition_mission.panel import (
    CounterexampleReviewOutput,
    CounterexampleSearchDecision,
    PerspectiveAgentOutput,
    RatificationOutput,
    ScoringOutput,
    SynthesisOutput,
)

_CANDIDATE = "Define the one evidence-supported problem without proposing a solution."


def _snapshot() -> EvidenceSnapshot:
    return EvidenceSnapshot(
        revision=2,
        items=(
            EvidenceItem(
                evidence_id="support-ticket-17",
                source="support export:ticket/17",
                content="New users ask which policy is safe for a first run.",
            ),
            EvidenceItem(
                evidence_id="setup-interview-04",
                source="research interview:participant/04",
                content="The participant stopped setup at the policy selection step.",
            ),
        ),
    )


def _framing(
    snapshot: EvidenceSnapshot,
    framing_id: str,
    *,
    claim_evidence_ids: tuple[str, ...] | None = None,
) -> ProblemFraming:
    return ProblemFraming(
        framing_id=framing_id,
        statement=(
            "First-time users cannot confidently finish setup because the safe "
            "first-run policy is unclear."
        ),
        subject="First-time setup users",
        current_state="Users encounter an unexplained policy choice during setup.",
        desired_state="Users can identify a safe first-run policy and complete setup.",
        gap="The consequential policy choice lacks enough context for a safe decision.",
        stakes="Users abandon setup or require support before receiving product value.",
        in_scope=("First-run policy comprehension", "Setup completion"),
        out_of_scope=("Choosing an onboarding implementation",),
        success_criteria=(
            "First-time users identify a safe policy without support.",
            "Policy-step abandonment materially declines.",
        ),
        claims=(
            AtomicClaim(
                claim_id=f"{framing_id}:observation",
                kind=ClaimKind.OBSERVATION,
                statement="Supplied occurrences report policy uncertainty and setup abandonment.",
                evidence_ids=claim_evidence_ids or snapshot.evidence_ids,
                confidence=0.9,
                falsifier="The cited occurrences are corrected or retracted.",
            ),
        ),
        evidence_dispositions=tuple(
            EvidenceDisposition(
                evidence_id=item.evidence_id,
                disposition=EvidenceDispositionKind.SUPPORTS,
                reason="This occurrence directly constrains the observed setup gap.",
            )
            for item in snapshot.items
        ),
        contradictions=(),
        unknowns=("The baseline abandonment rate is not yet measured.",),
        next_question="Which context changes what policy is safest for a first run?",
    )


def _scores() -> ScoreVector:
    return ScoreVector(
        naive_clarity=0.9,
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


@dataclass(frozen=True)
class _ModelCall:
    system_prompt: str
    user_prompt: str
    response_model: type[BaseModel]


class _QueuedStructuredModel:
    """Structured-model fake that retains prompts and returns typed outputs."""

    def __init__(self) -> None:
        self.calls: list[_ModelCall] = []
        self._responses: defaultdict[type[BaseModel], deque[BaseModel | BaseException]] = (
            defaultdict(deque)
        )

    def queue(self, response_model: type[BaseModel], *responses: BaseModel | BaseException) -> None:
        self._responses[response_model].extend(responses)

    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_model: type[BaseModel],
    ) -> Any:
        self.calls.append(
            _ModelCall(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=response_model,
            )
        )
        response = self._responses[response_model].popleft()
        if isinstance(response, BaseException):
            raise response
        return response


def _model_with_valid_outputs(
    snapshot: EvidenceSnapshot,
    *,
    unknown_citation_perspective: Perspective | None = None,
) -> _QueuedStructuredModel:
    model = _QueuedStructuredModel()
    perspective_outputs = []
    for perspective in Perspective:
        evidence_ids = (
            ("evidence-ghost",)
            if perspective is unknown_citation_perspective
            else snapshot.evidence_ids
        )
        perspective_outputs.append(
            PerspectiveAgentOutput(
                framing=_framing(
                    snapshot,
                    f"{perspective.value}-model-framing",
                    claim_evidence_ids=evidence_ids,
                ),
                confidence=0.9,
                feedback=f"{perspective.value} model feedback",
            )
        )
    synthesis = _framing(snapshot, "model-synthesis")
    model.queue(PerspectiveAgentOutput, *perspective_outputs)
    model.queue(
        SynthesisOutput,
        SynthesisOutput(
            framing=synthesis,
            feedback=("The synthesis preserves the intersection of all three views.",),
        ),
    )
    model.queue(
        CounterexampleReviewOutput,
        CounterexampleReviewOutput(
            searches=(
                CounterexampleSearchDecision(
                    target_claim_id=synthesis.claims[0].claim_id,
                    outcome=CounterexampleSearchOutcome.NOT_FOUND_WITHIN_BUDGET,
                    detail="One stateless bounded review found no concrete witness.",
                ),
            ),
            verifications=(),
        ),
    )
    model.queue(
        RatificationOutput,
        *(
            RatificationOutput(
                approved=True,
                reason=f"The exact synthesis preserves the {perspective.value} constraints.",
            )
            for perspective in Perspective
        ),
    )
    model.queue(
        ScoringOutput,
        ScoringOutput(
            scores=_scores(),
            feedback=("The result is grounded, bounded, and independently ratified.",),
        ),
    )
    return model


def test_model_panel_supplies_grounded_role_separated_context_and_exact_binding() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    model = _model_with_valid_outputs(snapshot)

    evaluation = build_model_backed_panel(model).evaluate(_CANDIDATE, snapshot, policy)

    assert [call.response_model for call in model.calls] == [
        PerspectiveAgentOutput,
        PerspectiveAgentOutput,
        PerspectiveAgentOutput,
        SynthesisOutput,
        CounterexampleReviewOutput,
        RatificationOutput,
        RatificationOutput,
        RatificationOutput,
        ScoringOutput,
    ]

    observation_calls = model.calls[:3]
    normalized_system_prompts = [call.system_prompt.lower() for call in observation_calls]
    assert len(set(normalized_system_prompts)) == 3
    for perspective, call in zip(Perspective, observation_calls, strict=True):
        assert perspective.value in call.system_prompt.lower()
        assert _CANDIDATE in call.user_prompt
        assert str(snapshot.revision) in call.user_prompt
        assert snapshot.digest in call.user_prompt
        assert policy.digest in call.user_prompt
        assert policy.evaluator_id in call.user_prompt
        assert "untrusted" in call.system_prompt.lower()
        for item in snapshot.items:
            assert item.evidence_id in call.user_prompt
            assert item.source in call.user_prompt
            assert item.content in call.user_prompt
        for other_perspective in Perspective:
            assert f"{other_perspective.value}-model-framing" not in call.user_prompt

    synthesis_call = model.calls[3]
    for perspective in Perspective:
        assert f"{perspective.value}-model-framing" in synthesis_call.user_prompt

    review_call = model.calls[4]
    assert policy.counterexample_verifier_id in review_call.user_prompt
    assert "never proof" in review_call.system_prompt.lower()

    synthesis = evaluation.framing
    for perspective, call in zip(Perspective, model.calls[5:8], strict=True):
        assert synthesis.framing_id in call.user_prompt
        assert synthesis.digest in call.user_prompt
        assert f"{perspective.value}-model-framing" in call.user_prompt
        for other_perspective in Perspective:
            if other_perspective is not perspective:
                assert f"{other_perspective.value}-model-framing" not in call.user_prompt

    assert evaluation.grounding_errors() == ()
    assert evaluation.hard_gate_passed is True
    assert all(vote.binding == evaluation.binding for vote in evaluation.votes)
    assert all(
        vote.framing_id == synthesis.framing_id and vote.framing_digest == synthesis.digest
        for vote in evaluation.votes
    )


def test_model_panel_accepts_a_separate_stateless_verifier_adapter() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    panel_model = _model_with_valid_outputs(snapshot)
    verifier_model = _QueuedStructuredModel()
    verifier_model.queue(
        CounterexampleReviewOutput,
        CounterexampleReviewOutput(
            searches=(
                CounterexampleSearchDecision(
                    target_claim_id="model-synthesis:observation",
                    outcome=CounterexampleSearchOutcome.NOT_FOUND_WITHIN_BUDGET,
                    detail="The separate verifier completed one bounded search pass.",
                ),
            ),
            verifications=(),
        ),
    )

    evaluation = build_model_backed_panel(
        panel_model,
        verifier_model=verifier_model,
        verifier_id=policy.counterexample_verifier_id,
    ).evaluate(_CANDIDATE, snapshot, policy)

    assert len(panel_model.calls) == 8
    assert all(call.response_model is not CounterexampleReviewOutput for call in panel_model.calls)
    assert [call.response_model for call in verifier_model.calls] == [CounterexampleReviewOutput]
    assert evaluation.hard_gate_passed is True


def test_model_challenge_target_digest_is_bound_by_orchestration() -> None:
    snapshot = _snapshot()
    framing = _framing(snapshot, "challenged-framing")
    target = framing.claims[0]
    challenge = ClaimChallenge(
        challenge_id="challenge-1",
        kind=ChallengeKind.COUNTEREXAMPLE,
        target_claim_id=target.claim_id,
        target_proposition_digest="model-is-not-a-hash-authority",
        statement="A cited occurrence may not support the full claim.",
        witness="The interview reports stopping but not the support cause.",
        reproduction="Compare the exact claim with both cited occurrences.",
        evidence_ids=snapshot.evidence_ids,
    )

    bound = _bind_challenge_target_digests(framing.model_copy(update={"challenges": (challenge,)}))

    assert bound.challenges[0].target_proposition_digest == target.proposition_digest
    assert bound.challenges[0].target_claim_id == target.claim_id


def test_unknown_challenge_target_remains_for_the_grounding_gate() -> None:
    snapshot = _snapshot()
    framing = _framing(snapshot, "unknown-target-framing")
    challenge = ClaimChallenge(
        challenge_id="challenge-unknown",
        kind=ChallengeKind.COUNTEREXAMPLE,
        target_claim_id="not-a-framing-claim",
        target_proposition_digest="untrusted-model-value",
        statement="The candidate prompt may presuppose an unsupported fact.",
        witness="The prompt names a joint problem that the evidence may not establish.",
        reproduction="Compare the prompt premise with the supplied evidence.",
        evidence_ids=snapshot.evidence_ids,
    )

    bound = _bind_challenge_target_digests(framing.model_copy(update={"challenges": (challenge,)}))

    assert bound.challenges[0] == challenge


def test_model_panel_fails_closed_when_structured_output_parsing_fails() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    model = _QueuedStructuredModel()
    model.queue(
        PerspectiveAgentOutput,
        PerspectiveAgentOutput(
            framing=_framing(snapshot, "naive-model-framing"),
            confidence=0.8,
            feedback="The statement is intelligible.",
        ),
        RuntimeError("structured output could not be parsed"),
    )

    with pytest.raises(RuntimeError, match="structured output could not be parsed"):
        build_model_backed_panel(model).evaluate(_CANDIDATE, snapshot, policy)

    assert [call.response_model for call in model.calls] == [
        PerspectiveAgentOutput,
        PerspectiveAgentOutput,
    ]
    assert "naive" in model.calls[0].system_prompt.lower()
    assert "expert" in model.calls[1].system_prompt.lower()


def test_unknown_model_citation_is_recorded_but_cannot_pass_the_hard_gate() -> None:
    snapshot = _snapshot()
    policy = ProblemDefinitionPolicy.default_three_perspective()
    model = _model_with_valid_outputs(
        snapshot,
        unknown_citation_perspective=Perspective.ORTHOGONAL,
    )

    evaluation = build_model_backed_panel(model).evaluate(_CANDIDATE, snapshot, policy)

    assert (
        "orthogonal claim orthogonal-model-framing:observation "
        "cites unknown evidence: evidence-ghost"
    ) in evaluation.grounding_errors()
    assert evaluation.unanimous is True
    assert evaluation.hard_gate_passed is False


class _FakeResponses:
    def __init__(self, parsed: BaseModel | None) -> None:
        self.parsed = parsed
        self.calls: list[dict[str, object]] = []

    def parse(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(kwargs)
        return SimpleNamespace(output_parsed=self.parsed)


class _FakeOpenAIClient:
    def __init__(self, parsed: BaseModel | None) -> None:
        self.responses = _FakeResponses(parsed)


def test_openai_structured_model_uses_responses_parse_without_credentials() -> None:
    snapshot = _snapshot()
    output = PerspectiveAgentOutput(
        framing=_framing(snapshot, "parsed-framing"),
        confidence=0.85,
        feedback="Parsed structured output.",
    )
    client = _FakeOpenAIClient(output)
    model = OpenAIStructuredLanguageModel(model="test-model", client=client)

    result = model.complete(
        system_prompt="system role",
        user_prompt="grounded input",
        response_model=PerspectiveAgentOutput,
    )

    assert result is output
    assert client.responses.calls == [
        {
            "model": "test-model",
            "instructions": "system role",
            "input": "grounded input",
            "text_format": PerspectiveAgentOutput,
            "store": False,
            "max_output_tokens": 10_000,
        }
    ]


def test_openai_structured_model_rejects_an_empty_parsed_response() -> None:
    client = _FakeOpenAIClient(None)
    model = OpenAIStructuredLanguageModel(model="test-model", client=client)

    with pytest.raises(StructuredModelOutputError, match="structured"):
        model.complete(
            system_prompt="system role",
            user_prompt="grounded input",
            response_model=PerspectiveAgentOutput,
        )

    assert len(client.responses.calls) == 1


def test_openai_structured_model_rejects_oversized_context_without_truncation_or_call() -> None:
    client = _FakeOpenAIClient(None)
    model = OpenAIStructuredLanguageModel(
        model="test-model",
        client=client,
        max_input_chars=20,
    )

    with pytest.raises(StructuredModelContextError, match="never silently truncated"):
        model.complete(
            system_prompt="system role",
            user_prompt="complete grounded input",
            response_model=PerspectiveAgentOutput,
        )

    assert client.responses.calls == []


class _FakeReflectionResponses:
    def __init__(self, output_text: object) -> None:
        self.output_text = output_text
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(kwargs)
        return SimpleNamespace(output_text=self.output_text)


class _FakeReflectionClient:
    def __init__(self, output_text: object) -> None:
        self.responses = _FakeReflectionResponses(output_text)


def test_openai_reflection_model_uses_a_bounded_stateless_response_call() -> None:
    prompt: list[dict[str, object]] = [
        {
            "role": "user",
            "content": "Revise the candidate using the evidence-bound panel feedback.",
        }
    ]
    client = _FakeReflectionClient("```\nA more exact problem-framing prompt.\n```")
    model = OpenAIReflectionLanguageModel(model="test-reflection-model", client=client)

    result = model(prompt)

    assert result == "```\nA more exact problem-framing prompt.\n```"
    assert model.provider_id == "openai.responses"
    assert model.model_id == "test-reflection-model"
    assert len(client.responses.calls) == 1
    call = client.responses.calls[0]
    assert call["model"] == "test-reflection-model"
    assert call["input"] is prompt
    assert call["store"] is False
    assert call["max_output_tokens"] == 8_000
    instructions = " ".join(str(call["instructions"]).split())
    assert "bounded GEPA search" in instructions
    assert "untrusted data" in instructions
    assert "Do not invent research, citations, or evidence IDs" in instructions


def test_openai_reflection_model_rejects_empty_output_and_oversized_input() -> None:
    empty_client = _FakeReflectionClient(" ")
    empty_model = OpenAIReflectionLanguageModel(
        model="test-reflection-model",
        client=empty_client,
    )

    with pytest.raises(StructuredModelOutputError, match="no text output"):
        empty_model("GEPA reflection request")

    assert len(empty_client.responses.calls) == 1

    bounded_client = _FakeReflectionClient("unused")
    bounded_model = OpenAIReflectionLanguageModel(
        model="test-reflection-model",
        client=bounded_client,
        max_input_chars=100,
    )

    with pytest.raises(StructuredModelContextError, match="never truncated"):
        bounded_model("x" * 101)

    assert bounded_client.responses.calls == []
