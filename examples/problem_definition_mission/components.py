# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local ECS ledger schema for problem-definition prompt research."""

from __future__ import annotations

from pydantic import field_validator

from archetype import Component


class ProblemDefinitionSession(Component):
    """Current session coordinates; immutable evidence and heads remain separate."""

    session_id: str = ""
    question: str = ""
    status: str = "open"
    provider: str = ""
    model: str = ""
    evidence_revision: int = 0
    evidence_digest: str = ""
    policy_json: str = "{}"
    policy_digest: str = ""
    head_prompt: str = ""
    head_prompt_digest: str = ""


class ProblemDefinitionEvidence(Component):
    """One immutable evidence occurrence."""

    session_id: str = ""
    evidence_id: str = ""
    revision: int = 0
    source: str = ""
    content: str = ""
    content_digest: str = ""


class ProblemFramingRun(Component):
    """Bounded GEPA-run intent and its durable lifecycle state."""

    run_id: str = ""
    session_id: str = ""
    evidence_revision: int = 0
    evidence_digest: str = ""
    policy_digest: str = ""
    seed_prompt_digest: str = ""
    optimizer_id: str = "gepa.optimize_anything"
    optimizer_version: str = ""
    config_json: str = "{}"
    historical_evidence_digests_json: str = "[]"
    status: str = "running"
    error: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        if value not in {"running", "observed", "stopped", "crashed"}:
            raise ValueError("problem-framing run status is invalid")
        return value


class ProblemFramingCandidate(Component):
    """One prompt candidate evaluated within a bounded run."""

    run_id: str = ""
    candidate_id: str = ""
    prompt: str = ""
    prompt_digest: str = ""
    parent_prompt_digest: str = ""
    gepa_index: int = -1
    gepa_parent_indices_json: str = "[]"
    gepa_aggregate_score: float = 0.0
    discovery_evaluation_count: int = 0


class ProblemFramingEvaluation(Component):
    """Panel observations and ratification bound to one exact prompt candidate."""

    run_id: str = ""
    candidate_id: str = ""
    evidence_revision: int = 0
    evidence_ids_json: str = "[]"
    evidence_digest: str = ""
    policy_digest: str = ""
    evaluator_id: str = ""
    synthesis_protocol_id: str = ""
    scoring_protocol_id: str = ""
    binding_json: str = "{}"
    aggregate_score: float = 0.0
    unanimous: bool = False
    hard_gate_passed: bool = False
    framing_json: str = "{}"
    observations_json: str = "[]"
    votes_json: str = "[]"
    scores_json: str = "{}"
    feedback_json: str = "[]"


class ProblemFramingHead(Component):
    """Decision record for the best admissible prompt at one evidence revision."""

    session_id: str = ""
    run_id: str = ""
    prompt: str = ""
    prompt_digest: str = ""
    parent_prompt_digest: str = ""
    evidence_revision: int = 0
    evidence_digest: str = ""
    policy_digest: str = ""
    evaluator_id: str = ""
    aggregate_score: float = 0.0
    framing_json: str = "{}"
    status: str = "unresolved"

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        if value not in {"ratified", "retained", "unresolved"}:
            raise ValueError("problem-framing head status is invalid")
        return value


PROBLEM_DEFINITION_COMPONENTS = (
    ProblemDefinitionSession,
    ProblemDefinitionEvidence,
    ProblemFramingRun,
    ProblemFramingCandidate,
    ProblemFramingEvaluation,
    ProblemFramingHead,
)


__all__ = [
    "PROBLEM_DEFINITION_COMPONENTS",
    "ProblemDefinitionEvidence",
    "ProblemDefinitionSession",
    "ProblemFramingCandidate",
    "ProblemFramingEvaluation",
    "ProblemFramingHead",
    "ProblemFramingRun",
]
