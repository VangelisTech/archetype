# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Immutable contracts for the example problem-definition mission."""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

_MAX_EVIDENCE_CHARS = 32_000
_MAX_PROMPT_CHARS = 64_000


def _digest(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _candidate_digest(
    candidate_prompt: str,
    evidence_revision: int,
    evidence_digest: str,
    policy_digest: str,
    evaluator_id: str,
) -> str:
    return _digest(
        {
            "candidate_prompt": candidate_prompt,
            "evidence_revision": evidence_revision,
            "evidence_digest": evidence_digest,
            "policy_digest": policy_digest,
            "evaluator_id": evaluator_id,
        }
    )


def _nonempty(value: str, field_name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class EvidenceItem(_FrozenModel):
    """One immutable occurrence supplied to a problem-definition session."""

    evidence_id: str
    source: str
    content: str

    @model_validator(mode="after")
    def _valid_item(self) -> EvidenceItem:
        _nonempty(self.evidence_id, "evidence_id")
        _nonempty(self.source, "source")
        _nonempty(self.content, "content")
        if len(self.evidence_id) > 128:
            raise ValueError("evidence_id must not exceed 128 characters")
        if len(self.source) > 512:
            raise ValueError("evidence source must not exceed 512 characters")
        if len(self.content) > _MAX_EVIDENCE_CHARS:
            raise ValueError(f"evidence content must not exceed {_MAX_EVIDENCE_CHARS} characters")
        return self

    @computed_field
    @property
    def digest(self) -> str:
        return _digest(
            {
                "evidence_id": self.evidence_id,
                "source": self.source,
                "content": self.content,
            }
        )


class EvidenceSnapshot(_FrozenModel):
    """Canonical evidence collection for one immutable revision."""

    revision: int = Field(ge=0)
    items: tuple[EvidenceItem, ...] = ()

    @model_validator(mode="after")
    def _canonical_items(self) -> EvidenceSnapshot:
        ordered = tuple(sorted(self.items, key=lambda item: item.evidence_id))
        ids = tuple(item.evidence_id for item in ordered)
        if len(ids) != len(set(ids)):
            raise ValueError("evidence IDs must be unique within a snapshot")
        object.__setattr__(self, "items", ordered)
        return self

    @computed_field
    @property
    def digest(self) -> str:
        return _digest(
            {
                "revision": self.revision,
                "items": [
                    {
                        "evidence_id": item.evidence_id,
                        "source": item.source,
                        "content": item.content,
                        "digest": item.digest,
                    }
                    for item in self.items
                ],
            }
        )

    @property
    def evidence_ids(self) -> tuple[str, ...]:
        return tuple(item.evidence_id for item in self.items)


def append_evidence(snapshot: EvidenceSnapshot, item: EvidenceItem) -> EvidenceSnapshot:
    """Append one item, treating an exact identity replay as an idempotent no-op."""

    existing = {candidate.evidence_id: candidate for candidate in snapshot.items}
    prior = existing.get(item.evidence_id)
    if prior is not None:
        if prior == item:
            return snapshot
        raise ValueError(f"evidence ID {item.evidence_id!r} already exists with different content")
    return EvidenceSnapshot(revision=snapshot.revision + 1, items=(*snapshot.items, item))


class Perspective(StrEnum):
    NAIVE = "naive"
    EXPERT = "expert"
    ORTHOGONAL = "orthogonal"


class ClaimKind(StrEnum):
    OBSERVATION = "observation"
    INFERENCE = "inference"
    HYPOTHESIS = "hypothesis"
    CONSTRAINT = "constraint"
    NON_GOAL = "non_goal"
    UNKNOWN = "unknown"


CHALLENGE_ELIGIBLE_CLAIM_KINDS = frozenset(
    {
        ClaimKind.OBSERVATION,
        ClaimKind.INFERENCE,
        ClaimKind.HYPOTHESIS,
        ClaimKind.CONSTRAINT,
    }
)


class ChallengeKind(StrEnum):
    """How a concrete witness is intended to challenge a framing claim."""

    COUNTEREXAMPLE = "counterexample"


class ChallengeVerificationOutcome(StrEnum):
    """Independent disposition of one exact claim challenge."""

    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INCONCLUSIVE = "inconclusive"


class CounterexampleSearchOutcome(StrEnum):
    """Bounded result of searching one exact proposition for a witness."""

    FOUND = "found"
    NOT_FOUND_WITHIN_BUDGET = "not_found_within_budget"
    INCONCLUSIVE = "inconclusive"


class EvidenceDispositionKind(StrEnum):
    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    IRRELEVANT = "irrelevant"
    UNRESOLVED = "unresolved"


class AtomicClaim(_FrozenModel):
    """One independently contestable claim in a proposed framing."""

    claim_id: str
    kind: ClaimKind
    statement: str
    evidence_ids: tuple[str, ...]
    confidence: float = Field(ge=0.0, le=1.0)
    falsifier: str

    @model_validator(mode="after")
    def _valid_claim(self) -> AtomicClaim:
        _nonempty(self.claim_id, "claim_id")
        _nonempty(self.statement, "claim statement")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("claim evidence IDs must be unique")
        if self.kind is ClaimKind.OBSERVATION and not self.evidence_ids:
            raise ValueError("an observation claim must cite evidence")
        if self.challenge_eligible and not self.falsifier.strip():
            raise ValueError("a challenge-eligible claim must state a falsifier")
        return self

    @computed_field
    @property
    def challenge_eligible(self) -> bool:
        """Return whether this claim asserts a proposition a witness can challenge."""

        return self.kind in CHALLENGE_ELIGIBLE_CLAIM_KINDS

    @computed_field
    @property
    def proposition_digest(self) -> str:
        """Identify the asserted proposition independently of mutable annotations."""

        return _digest(
            {
                "assertion_class": "challenge-eligible" if self.challenge_eligible else self.kind,
                "statement": self.statement.strip(),
            }
        )

    @computed_field
    @property
    def digest(self) -> str:
        """Identify exact claim content independently of its local label."""

        return _digest(
            self.model_dump(
                mode="json",
                exclude={
                    "challenge_eligible",
                    "claim_id",
                    "digest",
                    "proposition_digest",
                },
            )
        )


class ChallengeVerification(_FrozenModel):
    """Exact-snapshot verification receipt for one concrete claim challenge."""

    verification_id: str
    challenge_digest: str
    target_proposition_digest: str
    evidence_revision: int = Field(ge=0)
    evidence_digest: str
    policy_digest: str
    verifier_id: str
    protocol_id: str
    outcome: ChallengeVerificationOutcome
    detail: str

    @model_validator(mode="after")
    def _valid_verification(self) -> ChallengeVerification:
        for field_name in (
            "verification_id",
            "challenge_digest",
            "target_proposition_digest",
            "evidence_digest",
            "policy_digest",
            "verifier_id",
            "protocol_id",
            "detail",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        return self

    @computed_field
    @property
    def digest(self) -> str:
        return _digest(self.model_dump(mode="json", exclude={"digest"}))


class CounterexampleSearchReceipt(_FrozenModel):
    """Exact bounded-search evidence for one retained proposition.

    ``NOT_FOUND_WITHIN_BUDGET`` records only what one bounded search attempted;
    it is never a proof that no counterexample exists.
    """

    search_id: str
    target_claim_id: str
    target_proposition_digest: str
    evidence_revision: int = Field(ge=0)
    evidence_digest: str
    policy_digest: str
    searcher_id: str
    protocol_id: str
    max_attempts: int = Field(ge=1)
    outcome: CounterexampleSearchOutcome
    challenge_digests: tuple[str, ...] = ()
    detail: str

    @model_validator(mode="after")
    def _valid_search(self) -> CounterexampleSearchReceipt:
        for field_name in (
            "search_id",
            "target_claim_id",
            "target_proposition_digest",
            "evidence_digest",
            "policy_digest",
            "searcher_id",
            "protocol_id",
            "detail",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        if len(self.challenge_digests) != len(set(self.challenge_digests)):
            raise ValueError("counterexample search challenge digests must be unique")
        if self.outcome is CounterexampleSearchOutcome.FOUND:
            if not self.challenge_digests:
                raise ValueError("a found counterexample search must name a challenge")
        elif self.challenge_digests:
            raise ValueError("only a found counterexample search may name challenges")
        return self

    @computed_field
    @property
    def digest(self) -> str:
        return _digest(self.model_dump(mode="json", exclude={"digest"}))


class ClaimChallenge(_FrozenModel):
    """Concrete witness that may falsify or force revision of one exact claim."""

    challenge_id: str
    kind: ChallengeKind
    target_claim_id: str
    target_proposition_digest: str
    statement: str
    witness: str
    reproduction: str
    evidence_ids: tuple[str, ...]
    verifications: tuple[ChallengeVerification, ...] = ()

    @model_validator(mode="after")
    def _valid_challenge(self) -> ClaimChallenge:
        for field_name in (
            "challenge_id",
            "target_claim_id",
            "target_proposition_digest",
            "statement",
            "witness",
            "reproduction",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        if not self.evidence_ids:
            raise ValueError("claim challenge must cite evidence")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("claim challenge evidence IDs must be unique")
        verification_ids = [verification.verification_id for verification in self.verifications]
        if len(verification_ids) != len(set(verification_ids)):
            raise ValueError("claim challenge verification IDs must be unique")
        return self

    @computed_field
    @property
    def digest(self) -> str:
        """Identify the proposed witness independently of its later verification."""

        return _digest(
            self.model_dump(
                mode="json",
                exclude={
                    "challenge_id",
                    "digest",
                    "target_claim_id",
                    "verifications",
                },
            )
        )

    @property
    def active(self) -> bool:
        """Return whether the exact target remains challenged for promotion."""

        return not self.verifications or not all(
            verification.outcome is ChallengeVerificationOutcome.REJECTED
            for verification in self.verifications
        )


class EvidenceDisposition(_FrozenModel):
    """How the final framing accounts for one supplied evidence item."""

    evidence_id: str
    disposition: EvidenceDispositionKind
    reason: str

    @model_validator(mode="after")
    def _valid_disposition(self) -> EvidenceDisposition:
        _nonempty(self.evidence_id, "evidence_id")
        _nonempty(self.reason, "evidence disposition reason")
        return self


class ProblemFraming(_FrozenModel):
    """One structured, solution-independent statement of the current problem."""

    framing_id: str
    statement: str
    subject: str
    current_state: str
    desired_state: str
    gap: str
    stakes: str
    in_scope: tuple[str, ...]
    out_of_scope: tuple[str, ...]
    success_criteria: tuple[str, ...]
    claims: tuple[AtomicClaim, ...]
    challenges: tuple[ClaimChallenge, ...] = ()
    counterexample_searches: tuple[CounterexampleSearchReceipt, ...] = ()
    evidence_dispositions: tuple[EvidenceDisposition, ...]
    contradictions: tuple[str, ...]
    unknowns: tuple[str, ...]
    next_question: str

    @model_validator(mode="after")
    def _valid_framing(self) -> ProblemFraming:
        for field_name in (
            "framing_id",
            "statement",
            "subject",
            "current_state",
            "desired_state",
            "gap",
            "stakes",
            "next_question",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        for field_name in ("in_scope", "out_of_scope", "success_criteria"):
            values = getattr(self, field_name)
            if not values:
                raise ValueError(f"{field_name} must not be empty")
            normalized = tuple(_nonempty(value, field_name) for value in values)
            if len(normalized) != len(set(normalized)):
                raise ValueError(f"{field_name} values must be unique")
        if set(self.in_scope) & set(self.out_of_scope):
            raise ValueError("in_scope and out_of_scope must not overlap")
        if not self.claims:
            raise ValueError("claims must not be empty")
        if not self.evidence_dispositions:
            raise ValueError("evidence_dispositions must not be empty")
        claim_ids = [claim.claim_id for claim in self.claims]
        if len(claim_ids) != len(set(claim_ids)):
            raise ValueError("framing claim IDs must be unique")
        challenge_ids = [challenge.challenge_id for challenge in self.challenges]
        if len(challenge_ids) != len(set(challenge_ids)):
            raise ValueError("framing challenge IDs must be unique")
        verification_ids = [
            verification.verification_id
            for challenge in self.challenges
            for verification in challenge.verifications
        ]
        if len(verification_ids) != len(set(verification_ids)):
            raise ValueError("framing challenge verification IDs must be unique")
        search_ids = [search.search_id for search in self.counterexample_searches]
        if len(search_ids) != len(set(search_ids)):
            raise ValueError("framing counterexample search IDs must be unique")
        search_targets = [
            search.target_proposition_digest for search in self.counterexample_searches
        ]
        if len(search_targets) != len(set(search_targets)):
            raise ValueError("framing counterexample searches must target unique propositions")
        disposition_ids = [disposition.evidence_id for disposition in self.evidence_dispositions]
        if len(disposition_ids) != len(set(disposition_ids)):
            raise ValueError("framing evidence dispositions must be unique")
        return self

    @computed_field
    @property
    def digest(self) -> str:
        payload = self.model_dump(mode="json", exclude={"digest"})
        return _digest(payload)


class EvaluationBinding(_FrozenModel):
    """Exact candidate, evidence, policy, and evaluator observed by one call."""

    candidate_digest: str
    evidence_revision: int = Field(ge=0)
    evidence_digest: str
    policy_digest: str
    evaluator_id: str

    @model_validator(mode="after")
    def _valid_binding(self) -> EvaluationBinding:
        for field_name in (
            "candidate_digest",
            "evidence_digest",
            "policy_digest",
            "evaluator_id",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        return self

    @classmethod
    def for_candidate(
        cls,
        candidate_prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ) -> EvaluationBinding:
        """Bind one independent call to an exact prompt evaluation context."""

        return cls(
            candidate_digest=_candidate_digest(
                candidate_prompt,
                snapshot.revision,
                snapshot.digest,
                policy.digest,
                policy.evaluator_id,
            ),
            evidence_revision=snapshot.revision,
            evidence_digest=snapshot.digest,
            policy_digest=policy.digest,
            evaluator_id=policy.evaluator_id,
        )


class PerspectiveObservation(_FrozenModel):
    """Independent framing emitted by one member of the panel."""

    observation_id: str
    binding: EvaluationBinding
    perspective: Perspective
    protocol_id: str
    framing: ProblemFraming
    confidence: float = Field(ge=0.0, le=1.0)
    feedback: str

    @field_validator("protocol_id")
    @classmethod
    def _protocol_nonempty(cls, value: str) -> str:
        return _nonempty(value, "protocol_id")

    @field_validator("observation_id")
    @classmethod
    def _observation_id_nonempty(cls, value: str) -> str:
        return _nonempty(value, "observation_id")

    @computed_field
    @property
    def digest(self) -> str:
        """Identify the exact role observation consumed during ratification."""

        return _digest(self.model_dump(mode="json", exclude={"digest"}))


class RatificationVote(_FrozenModel):
    """One perspective's explicit vote on the exact synthesized framing."""

    vote_id: str
    binding: EvaluationBinding
    perspective: Perspective
    protocol_id: str
    approved: bool
    reason: str
    observation_id: str
    observation_digest: str
    framing_id: str
    framing_digest: str

    @model_validator(mode="after")
    def _valid_vote(self) -> RatificationVote:
        for field_name in (
            "vote_id",
            "protocol_id",
            "reason",
            "observation_id",
            "observation_digest",
            "framing_id",
            "framing_digest",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        return self


class ScoreVector(_FrozenModel):
    """Panel quality axes; penalties are stored in their natural direction."""

    naive_clarity: float = Field(ge=0.0, le=1.0)
    expert_grounding: float = Field(ge=0.0, le=1.0)
    orthogonal_robustness: float = Field(ge=0.0, le=1.0)
    consensus: float = Field(ge=0.0, le=1.0)
    atomicity: float = Field(ge=0.0, le=1.0)
    falsifiability: float = Field(ge=0.0, le=1.0)
    scope_discipline: float = Field(ge=0.0, le=1.0)
    solution_independence: float = Field(ge=0.0, le=1.0)
    unsupported_claim_penalty: float = Field(ge=0.0, le=1.0)
    contradiction_penalty: float = Field(ge=0.0, le=1.0)

    @computed_field
    @property
    def aggregate_score(self) -> float:
        positives = (
            self.naive_clarity,
            self.expert_grounding,
            self.orthogonal_robustness,
            self.consensus,
            self.atomicity,
            self.falsifiability,
            self.scope_discipline,
            self.solution_independence,
        )
        penalties = (
            self.unsupported_claim_penalty,
            self.contradiction_penalty,
        )
        value = sum(positives) / len(positives) - 0.5 * sum(penalties) / len(penalties)
        return max(0.0, min(1.0, value))

    def objectives(self) -> dict[str, float]:
        """Return GEPA objectives using the required higher-is-better direction."""

        return {
            "naive_clarity": self.naive_clarity,
            "expert_grounding": self.expert_grounding,
            "orthogonal_robustness": self.orthogonal_robustness,
            "consensus": self.consensus,
            "atomicity": self.atomicity,
            "falsifiability": self.falsifiability,
            "scope_discipline": self.scope_discipline,
            "solution_independence": self.solution_independence,
            "supported_claims": 1.0 - self.unsupported_claim_penalty,
            "contradiction_free": 1.0 - self.contradiction_penalty,
        }


class ProblemDefinitionPolicy(_FrozenModel):
    """Versioned identity of the panel, evaluator, and ratification protocol."""

    policy_id: str
    version: str
    evaluator_id: str
    naive_protocol_id: str
    expert_protocol_id: str
    orthogonal_protocol_id: str
    synthesis_protocol_id: str
    ratification_protocol_id: str
    counterexample_search_protocol_id: str
    counterexample_verification_protocol_id: str
    counterexample_verifier_id: str
    counterexample_search_max_attempts: int = Field(ge=1)
    scoring_protocol_id: str
    required_perspectives: tuple[Perspective, ...]

    @model_validator(mode="after")
    def _valid_policy(self) -> ProblemDefinitionPolicy:
        for field_name in (
            "policy_id",
            "version",
            "evaluator_id",
            "naive_protocol_id",
            "expert_protocol_id",
            "orthogonal_protocol_id",
            "synthesis_protocol_id",
            "ratification_protocol_id",
            "counterexample_search_protocol_id",
            "counterexample_verification_protocol_id",
            "counterexample_verifier_id",
            "scoring_protocol_id",
        ):
            _nonempty(str(getattr(self, field_name)), field_name)
        if self.counterexample_verifier_id == self.evaluator_id:
            raise ValueError("counterexample verifier must be independent of the panel evaluator")
        if set(self.required_perspectives) != set(Perspective):
            raise ValueError("problem-definition policy requires all three perspectives")
        if len(self.required_perspectives) != len(set(self.required_perspectives)):
            raise ValueError("required perspectives must be unique")
        return self

    @computed_field
    @property
    def digest(self) -> str:
        return _digest(self.model_dump(mode="json", exclude={"digest"}))

    @classmethod
    def default_three_perspective(cls) -> ProblemDefinitionPolicy:
        return cls(
            policy_id="archetype.problem-definition.three-perspective",
            version="2",
            evaluator_id="archetype.problem-definition.panel-v2",
            naive_protocol_id="naive-clarity-v2",
            expert_protocol_id="expert-grounding-v2",
            orthogonal_protocol_id="orthogonal-counterexample-search-v2",
            synthesis_protocol_id="challenge-preserving-intersection-synthesis-v2",
            ratification_protocol_id="exact-framing-ratification-v2",
            counterexample_search_protocol_id="bounded-counterexample-search-v2",
            counterexample_verification_protocol_id=(
                "exact-proposition-counterexample-verification-v2"
            ),
            counterexample_verifier_id=(
                "archetype.problem-definition.independent-counterexample-verifier-v2"
            ),
            counterexample_search_max_attempts=1,
            scoring_protocol_id="counterexample-aware-panel-scoring-v2",
            required_perspectives=tuple(Perspective),
        )

    def protocol_for(self, perspective: Perspective) -> str:
        return {
            Perspective.NAIVE: self.naive_protocol_id,
            Perspective.EXPERT: self.expert_protocol_id,
            Perspective.ORTHOGONAL: self.orthogonal_protocol_id,
        }[perspective]


def bounded_counterexample_search_receipts(
    framing: ProblemFraming,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
    *,
    detail: str,
    no_challenge_outcome: CounterexampleSearchOutcome = (
        CounterexampleSearchOutcome.NOT_FOUND_WITHIN_BUDGET
    ),
) -> tuple[CounterexampleSearchReceipt, ...]:
    """Build exact search receipts for an explicit deterministic/offline search double.

    Calling this helper is an assertion by the caller that the policy-bounded
    search was actually performed. A no-challenge result is bounded evidence,
    never proof that no witness exists.
    """

    _nonempty(detail, "counterexample search detail")
    if no_challenge_outcome is CounterexampleSearchOutcome.FOUND:
        raise ValueError("no_challenge_outcome cannot be FOUND")
    claims_by_proposition: dict[str, AtomicClaim] = {}
    for claim in sorted(framing.claims, key=lambda candidate: candidate.claim_id):
        if claim.challenge_eligible:
            claims_by_proposition.setdefault(claim.proposition_digest, claim)
    receipts: list[CounterexampleSearchReceipt] = []
    for proposition_digest, claim in sorted(claims_by_proposition.items()):
        challenge_digests = tuple(
            sorted(
                challenge.digest
                for challenge in framing.challenges
                if challenge.target_proposition_digest == proposition_digest
            )
        )
        receipts.append(
            CounterexampleSearchReceipt(
                search_id="search:"
                + _digest(
                    {
                        "target_proposition_digest": proposition_digest,
                        "evidence_digest": snapshot.digest,
                        "policy_digest": policy.digest,
                    }
                ),
                target_claim_id=claim.claim_id,
                target_proposition_digest=proposition_digest,
                evidence_revision=snapshot.revision,
                evidence_digest=snapshot.digest,
                policy_digest=policy.digest,
                searcher_id=policy.counterexample_verifier_id,
                protocol_id=policy.counterexample_search_protocol_id,
                max_attempts=policy.counterexample_search_max_attempts,
                outcome=(
                    CounterexampleSearchOutcome.FOUND if challenge_digests else no_challenge_outcome
                ),
                challenge_digests=challenge_digests,
                detail=detail,
            )
        )
    return tuple(receipts)


def bounded_no_counterexample_search_receipts(
    framing: ProblemFraming,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
    *,
    detail: str,
) -> tuple[CounterexampleSearchReceipt, ...]:
    """Build bounded-NOT-FOUND receipts when an offline search found no witness."""

    if framing.challenges:
        raise ValueError("a no-counterexample-found receipt cannot cover recorded challenges")
    return bounded_counterexample_search_receipts(
        framing,
        snapshot,
        policy,
        detail=detail,
    )


class PanelEvaluation(_FrozenModel):
    """Exact-snapshot observation bundle consumed by the pure prompt selector."""

    candidate_prompt: str
    evidence_revision: int = Field(ge=0)
    evidence_ids: tuple[str, ...]
    evidence_digest: str
    policy: ProblemDefinitionPolicy
    observations: tuple[PerspectiveObservation, ...]
    synthesis_protocol_id: str
    scoring_protocol_id: str
    framing: ProblemFraming
    votes: tuple[RatificationVote, ...]
    scores: ScoreVector
    feedback: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _valid_evaluation(self) -> PanelEvaluation:
        _nonempty(self.candidate_prompt, "candidate_prompt")
        if len(self.candidate_prompt) > _MAX_PROMPT_CHARS:
            raise ValueError(f"candidate prompt must not exceed {_MAX_PROMPT_CHARS} characters")
        for field_name in ("evidence_digest", "synthesis_protocol_id", "scoring_protocol_id"):
            _nonempty(str(getattr(self, field_name)), field_name)
        return self

    @property
    def policy_digest(self) -> str:
        return self.policy.digest

    @property
    def evaluator_id(self) -> str:
        return self.policy.evaluator_id

    @computed_field
    @property
    def candidate_digest(self) -> str:
        return _candidate_digest(
            self.candidate_prompt,
            self.evidence_revision,
            self.evidence_digest,
            self.policy_digest,
            self.evaluator_id,
        )

    @property
    def binding(self) -> EvaluationBinding:
        """Return the binding every independent observation and vote must carry."""

        return EvaluationBinding(
            candidate_digest=self.candidate_digest,
            evidence_revision=self.evidence_revision,
            evidence_digest=self.evidence_digest,
            policy_digest=self.policy_digest,
            evaluator_id=self.evaluator_id,
        )

    @property
    def aggregate_score(self) -> float:
        return self.scores.aggregate_score

    @property
    def unanimous(self) -> bool:
        by_role = {vote.perspective: vote for vote in self.votes}
        return (
            len(by_role) == len(Perspective)
            and set(by_role) == set(Perspective)
            and all(vote.approved for vote in by_role.values())
        )

    @property
    def active_challenges(self) -> tuple[ClaimChallenge, ...]:
        """Return concrete final-framing challenges that still require revision."""

        return tuple(challenge for challenge in self.framing.challenges if challenge.active)

    def _challenge_grounding_errors(
        self,
        *,
        label: str,
        framing: ProblemFraming,
        known_evidence_ids: set[str],
    ) -> list[str]:
        errors: list[str] = []
        claims = {claim.claim_id: claim for claim in framing.claims}
        for challenge in framing.challenges:
            unknown = sorted(set(challenge.evidence_ids) - known_evidence_ids)
            if unknown:
                errors.append(
                    f"{label} challenge {challenge.challenge_id} cites unknown evidence: "
                    + ", ".join(unknown)
                )

            target = claims.get(challenge.target_claim_id)
            if target is None:
                errors.append(
                    f"{label} challenge {challenge.challenge_id} targets unknown claim "
                    f"{challenge.target_claim_id}"
                )
            elif not target.challenge_eligible:
                errors.append(
                    f"{label} challenge {challenge.challenge_id} targets a claim kind that "
                    "is not challenge-eligible"
                )
            elif target.proposition_digest != challenge.target_proposition_digest:
                errors.append(
                    f"{label} challenge {challenge.challenge_id} binds the wrong target "
                    "proposition digest"
                )

            for verification in challenge.verifications:
                if verification.challenge_digest != challenge.digest:
                    errors.append(
                        f"{label} challenge {challenge.challenge_id} verification binds the "
                        "wrong challenge digest"
                    )
                if verification.target_proposition_digest != challenge.target_proposition_digest:
                    errors.append(
                        f"{label} challenge {challenge.challenge_id} verification binds the "
                        "wrong target proposition digest"
                    )
                if (
                    verification.evidence_revision != self.evidence_revision
                    or verification.evidence_digest != self.evidence_digest
                ):
                    errors.append(
                        f"{label} challenge {challenge.challenge_id} verification has the "
                        "wrong evidence binding"
                    )
                if verification.policy_digest != self.policy_digest:
                    errors.append(
                        f"{label} challenge {challenge.challenge_id} verification has the "
                        "wrong policy binding"
                    )
                if verification.verifier_id != self.policy.counterexample_verifier_id:
                    errors.append(
                        f"{label} challenge {challenge.challenge_id} verification has the "
                        "wrong verifier authority"
                    )
                if verification.protocol_id != self.policy.counterexample_verification_protocol_id:
                    errors.append(
                        f"{label} challenge {challenge.challenge_id} verification has the "
                        "wrong protocol"
                    )
        return errors

    def _search_grounding_errors(self) -> list[str]:
        errors: list[str] = []
        claims = {claim.claim_id: claim for claim in self.framing.claims}
        eligible_propositions = {
            claim.proposition_digest for claim in self.framing.claims if claim.challenge_eligible
        }
        challenges_by_digest = {
            challenge.digest: challenge for challenge in self.framing.challenges
        }
        challenge_digests_by_proposition: dict[str, set[str]] = {}
        for challenge in self.framing.challenges:
            challenge_digests_by_proposition.setdefault(
                challenge.target_proposition_digest,
                set(),
            ).add(challenge.digest)

        covered: set[str] = set()
        for search in self.framing.counterexample_searches:
            target = claims.get(search.target_claim_id)
            if target is None:
                errors.append(
                    f"counterexample search {search.search_id} targets unknown claim "
                    f"{search.target_claim_id}"
                )
            elif not target.challenge_eligible:
                errors.append(
                    f"counterexample search {search.search_id} targets a claim kind that "
                    "is not challenge-eligible"
                )
            elif target.proposition_digest != search.target_proposition_digest:
                errors.append(
                    f"counterexample search {search.search_id} binds the wrong target "
                    "proposition digest"
                )

            if search.target_proposition_digest in covered:
                errors.append(
                    f"counterexample search coverage is duplicated for proposition "
                    f"{search.target_proposition_digest}"
                )
            covered.add(search.target_proposition_digest)

            if (
                search.evidence_revision != self.evidence_revision
                or search.evidence_digest != self.evidence_digest
            ):
                errors.append(
                    f"counterexample search {search.search_id} has the wrong evidence binding"
                )
            if search.policy_digest != self.policy_digest:
                errors.append(
                    f"counterexample search {search.search_id} has the wrong policy binding"
                )
            if search.searcher_id != self.policy.counterexample_verifier_id:
                errors.append(
                    f"counterexample search {search.search_id} has the wrong searcher authority"
                )
            if search.protocol_id != self.policy.counterexample_search_protocol_id:
                errors.append(f"counterexample search {search.search_id} has the wrong protocol")
            if search.max_attempts != self.policy.counterexample_search_max_attempts:
                errors.append(f"counterexample search {search.search_id} has the wrong budget")

            named = set(search.challenge_digests)
            expected = challenge_digests_by_proposition.get(
                search.target_proposition_digest,
                set(),
            )
            unknown = sorted(named - set(challenges_by_digest))
            if unknown:
                errors.append(
                    f"counterexample search {search.search_id} names unknown challenges: "
                    + ", ".join(unknown)
                )
            wrong_target = sorted(
                digest
                for digest in named & set(challenges_by_digest)
                if (
                    challenges_by_digest[digest].target_proposition_digest
                    != search.target_proposition_digest
                )
            )
            if wrong_target:
                errors.append(
                    f"counterexample search {search.search_id} names challenges for another "
                    "proposition: " + ", ".join(wrong_target)
                )
            if search.outcome is CounterexampleSearchOutcome.FOUND and named != expected:
                errors.append(
                    f"counterexample search {search.search_id} does not name every exact "
                    "challenge it found"
                )
            if search.outcome is not CounterexampleSearchOutcome.FOUND and expected:
                errors.append(
                    f"counterexample search {search.search_id} reports {search.outcome.value} "
                    "despite recorded challenges"
                )
            if search.outcome is CounterexampleSearchOutcome.INCONCLUSIVE:
                errors.append(f"counterexample search {search.search_id} is inconclusive")

        missing = sorted(eligible_propositions - covered)
        if missing:
            errors.append(
                "final framing lacks bounded counterexample search coverage for propositions: "
                + ", ".join(missing)
            )
        unknown_coverage = sorted(covered - eligible_propositions)
        if unknown_coverage:
            errors.append(
                "final framing has counterexample search coverage for unknown propositions: "
                + ", ".join(unknown_coverage)
            )
        return errors

    def grounding_errors(self) -> tuple[str, ...]:
        """Return deterministic provenance and epistemic hard-gate failures."""

        errors: list[str] = []
        known = set(self.evidence_ids)
        if len(known) != len(self.evidence_ids):
            errors.append("snapshot evidence IDs are duplicated")

        observation_roles = [observation.perspective for observation in self.observations]
        observation_ids = [observation.observation_id for observation in self.observations]
        if len(observation_ids) != len(set(observation_ids)):
            errors.append("perspective observation IDs are duplicated")
        if len(observation_roles) != len(set(observation_roles)):
            errors.append("perspective observations are duplicated")
        if set(observation_roles) != set(self.policy.required_perspectives):
            errors.append("one or more perspective observations are missing")

        for observation in self.observations:
            if observation.binding != self.binding:
                errors.append(
                    f"{observation.perspective.value} observation has the wrong evaluation binding"
                )
            expected_protocol = self.policy.protocol_for(observation.perspective)
            if observation.protocol_id != expected_protocol:
                errors.append(f"{observation.perspective.value} observation has the wrong protocol")
            if observation.framing.counterexample_searches:
                errors.append(
                    f"{observation.perspective.value} observation cannot author "
                    "counterexample search receipts"
                )
            for claim in observation.framing.claims:
                unknown = sorted(set(claim.evidence_ids) - known)
                if unknown:
                    errors.append(
                        f"{observation.perspective.value} claim {claim.claim_id} "
                        f"cites unknown evidence: {', '.join(unknown)}"
                    )
            errors.extend(
                self._challenge_grounding_errors(
                    label=observation.perspective.value,
                    framing=observation.framing,
                    known_evidence_ids=known,
                )
            )

        disposition_ids = [
            disposition.evidence_id for disposition in self.framing.evidence_dispositions
        ]
        missing_dispositions = sorted(known - set(disposition_ids))
        unknown_dispositions = sorted(set(disposition_ids) - known)
        if missing_dispositions:
            errors.append(
                "final framing omits evidence dispositions: " + ", ".join(missing_dispositions)
            )
        if unknown_dispositions:
            errors.append(
                "final framing dispositions cite unknown evidence: "
                + ", ".join(unknown_dispositions)
            )

        for claim in self.framing.claims:
            unknown = sorted(set(claim.evidence_ids) - known)
            if unknown:
                errors.append(
                    f"final claim {claim.claim_id} cites unknown evidence: " + ", ".join(unknown)
                )
            if claim.kind is ClaimKind.OBSERVATION and not claim.evidence_ids:
                errors.append(f"observation claim {claim.claim_id} has no evidence")

        errors.extend(
            self._challenge_grounding_errors(
                label="final",
                framing=self.framing,
                known_evidence_ids=known,
            )
        )
        errors.extend(self._search_grounding_errors())
        final_proposition_digests = {claim.proposition_digest for claim in self.framing.claims}
        final_challenges = {challenge.digest: challenge for challenge in self.framing.challenges}
        verification_by_id: dict[str, ChallengeVerification] = {}
        outcomes_by_challenge: dict[str, set[ChallengeVerificationOutcome]] = {}
        for framing in (
            *(observation.framing for observation in self.observations),
            self.framing,
        ):
            for challenge in framing.challenges:
                for verification in challenge.verifications:
                    existing = verification_by_id.get(verification.verification_id)
                    if existing is not None and existing.digest != verification.digest:
                        errors.append(
                            f"verification ID {verification.verification_id} is reused with "
                            "different receipt content"
                        )
                    else:
                        verification_by_id[verification.verification_id] = verification
                    outcomes_by_challenge.setdefault(challenge.digest, set()).add(
                        verification.outcome
                    )
        for challenge_digest, outcomes in outcomes_by_challenge.items():
            if len(outcomes) > 1:
                errors.append(f"challenge {challenge_digest} has conflicting verification outcomes")

        for observation in self.observations:
            for challenge in observation.framing.challenges:
                if challenge.target_proposition_digest not in final_proposition_digests:
                    continue
                final_challenge = final_challenges.get(challenge.digest)
                if challenge.active and final_challenge is None:
                    errors.append(
                        "final framing omits active challenge "
                        f"{challenge.challenge_id} for retained proposition "
                        f"{challenge.target_claim_id}"
                    )
                    continue
                if final_challenge is not None:
                    final_receipts = {
                        verification.digest for verification in final_challenge.verifications
                    }
                    omitted_receipts = sorted(
                        verification.verification_id
                        for verification in challenge.verifications
                        if verification.digest not in final_receipts
                    )
                    if omitted_receipts:
                        errors.append(
                            f"final challenge {final_challenge.challenge_id} omits immutable "
                            "verification receipts: " + ", ".join(omitted_receipts)
                        )

        if self.synthesis_protocol_id != self.policy.synthesis_protocol_id:
            errors.append("synthesis has the wrong protocol")
        if self.scoring_protocol_id != self.policy.scoring_protocol_id:
            errors.append("scoring has the wrong protocol")

        vote_roles = [vote.perspective for vote in self.votes]
        vote_ids = [vote.vote_id for vote in self.votes]
        if len(vote_ids) != len(set(vote_ids)):
            errors.append("ratification vote IDs are duplicated")
        if len(vote_roles) != len(set(vote_roles)):
            errors.append("ratification votes are duplicated")
        if set(vote_roles) != set(self.policy.required_perspectives):
            errors.append("one or more ratification votes are missing")
        for vote in self.votes:
            observation = next(
                (
                    candidate
                    for candidate in self.observations
                    if candidate.perspective is vote.perspective
                ),
                None,
            )
            if vote.binding != self.binding:
                errors.append(f"{vote.perspective.value} vote has the wrong evaluation binding")
            if vote.protocol_id != self.policy.ratification_protocol_id:
                errors.append(f"{vote.perspective.value} vote has the wrong protocol")
            if observation is None:
                errors.append(f"{vote.perspective.value} vote has no role observation")
            else:
                if vote.observation_id != observation.observation_id:
                    errors.append(f"{vote.perspective.value} vote names the wrong observation")
                if vote.observation_digest != observation.digest:
                    errors.append(
                        f"{vote.perspective.value} vote binds the wrong observation digest"
                    )
            if vote.framing_id != self.framing.framing_id:
                errors.append(f"{vote.perspective.value} vote names the wrong framing")
            if vote.framing_digest != self.framing.digest:
                errors.append(f"{vote.perspective.value} vote binds the wrong framing digest")

        return tuple(errors)

    @property
    def hard_gate_passed(self) -> bool:
        return (
            self.unanimous
            and not self.grounding_errors()
            and not self.framing.contradictions
            and not self.active_challenges
        )

    @property
    def side_info(self) -> dict[str, object]:
        errors = self.grounding_errors()
        messages = [
            *self.feedback,
            *(vote.reason for vote in self.votes if not vote.approved),
            *errors,
        ]
        if self.framing.contradictions:
            messages.append("Unresolved contradictions: " + "; ".join(self.framing.contradictions))
        for search in self.framing.counterexample_searches:
            if search.outcome is CounterexampleSearchOutcome.NOT_FOUND_WITHIN_BUDGET:
                messages.append(
                    f"No counterexample was found for claim {search.target_claim_id} within "
                    f"the recorded budget of {search.max_attempts} attempt(s); this is "
                    "bounded search evidence, not proof."
                )
            elif search.outcome is CounterexampleSearchOutcome.INCONCLUSIVE:
                messages.append(
                    f"Counterexample search {search.search_id} for claim "
                    f"{search.target_claim_id} was inconclusive: {search.detail}"
                )
        for challenge in self.active_challenges:
            if not challenge.verifications:
                outcome = "unverified"
                detail = "No independent verification receipt was recorded."
            else:
                outcome = ",".join(
                    sorted({verification.outcome.value for verification in challenge.verifications})
                )
                detail = " ".join(verification.detail for verification in challenge.verifications)
            messages.append(
                f"Active {challenge.kind.value} challenge {challenge.challenge_id} "
                f"targets claim {challenge.target_claim_id} ({outcome}): "
                f"{challenge.statement} {detail}"
            )
        return {
            "scores": {
                **self.scores.objectives(),
                "ratified": float(self.hard_gate_passed),
            },
            "Feedback": "\n".join(messages) or "No panel feedback was recorded.",
            "hard_gate_passed": self.hard_gate_passed,
            "unanimous": self.unanimous,
            "candidate_digest": self.candidate_digest,
        }


__all__ = [
    "AtomicClaim",
    "CHALLENGE_ELIGIBLE_CLAIM_KINDS",
    "ChallengeKind",
    "ChallengeVerification",
    "ChallengeVerificationOutcome",
    "ClaimChallenge",
    "ClaimKind",
    "CounterexampleSearchOutcome",
    "CounterexampleSearchReceipt",
    "EvidenceDisposition",
    "EvidenceDispositionKind",
    "EvidenceItem",
    "EvidenceSnapshot",
    "EvaluationBinding",
    "PanelEvaluation",
    "Perspective",
    "PerspectiveObservation",
    "ProblemDefinitionPolicy",
    "ProblemFraming",
    "RatificationVote",
    "ScoreVector",
    "append_evidence",
    "bounded_counterexample_search_receipts",
    "bounded_no_counterexample_search_receipts",
]
