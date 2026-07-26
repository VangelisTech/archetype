# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral values for independent exact-head mission review."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Protocol, runtime_checkable

from archetype.missions.contracts import CriticPolicy
from archetype.missions.sandboxes import SandboxIdentity, SandboxSession, SandboxStatus
from archetype.missions.transitions import (
    CriticConclusion,
    CriticExecutionStatus,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def canonical_digest(value: object) -> str:
    """Return a stable SHA-256 digest for a JSON-compatible value."""

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def candidate_subject_digest(
    *,
    candidate_id: str,
    mission_id: int,
    task_id: int,
    dispatch_id: str,
    author_execution_id: int,
    repository: str,
    branch: str,
    base_ref: str,
    base_revision: str,
    head_revision: str,
    diff_digest: str,
    validator_bundle_digest: str,
    policy_digest: str,
) -> str:
    """Digest every immutable identity consumed by task-promotion policy."""

    return canonical_digest(
        {
            "candidate_id": candidate_id,
            "mission_id": mission_id,
            "task_id": task_id,
            "dispatch_id": dispatch_id,
            "author_execution_id": author_execution_id,
            "repository": repository,
            "branch": branch,
            "base_ref": base_ref,
            "base_revision": base_revision,
            "head_revision": head_revision,
            "diff_digest": diff_digest,
            "validator_bundle_digest": validator_bundle_digest,
            "policy_digest": policy_digest,
        }
    )


@dataclass(frozen=True)
class CriticValidationEvidence:
    """One exact-revision validator observation shown to the critic."""

    validator_id: int
    name: str
    command: tuple[str, ...]
    expected_returncode: int
    actual_returncode: int
    revision: str
    stdout: str = ""
    stderr: str = ""


def validator_bundle_digest(
    validators: tuple[tuple[int, str, tuple[str, ...], int, int], ...],
) -> str:
    """Digest ordered validator identities, definitions, and time budgets."""

    return canonical_digest(
        [
            {
                "validator_id": validator_id,
                "name": name,
                "command": list(command),
                "expected_returncode": expected,
                "timeout_seconds": timeout,
            }
            for validator_id, name, command, expected, timeout in validators
        ]
    )


@dataclass(frozen=True)
class CriticPrewarmRequest:
    """Committed author dispatch context sufficient to hydrate a public base."""

    mission_id: int
    task_id: int
    dispatch_id: str
    repository: str
    branch: str
    base_ref: str


@dataclass(frozen=True)
class CandidateReviewRequest:
    """Immutable exact-head subject for one bounded critic attempt."""

    candidate_entity_id: int
    candidate_id: str
    mission_id: int
    task_id: int
    task_name: str
    task_prompt: str
    dispatch_id: str
    dispatch_sequence: int
    author_execution_id: int
    author_sandbox_id: str
    repository: str
    branch: str
    base_ref: str
    base_revision: str
    head_revision: str
    diff_digest: str
    validator_bundle_digest: str
    policy: CriticPolicy
    validation: tuple[CriticValidationEvidence, ...]
    candidate_published_at_ms: int
    attempt: int
    diff: str = ""
    subject_ref: str = ""
    subject_transport: str = ""
    subject_size_bytes: int = 0
    subject_digest: str = ""

    def __post_init__(self) -> None:
        required = (
            self.candidate_id,
            self.dispatch_id,
            self.author_sandbox_id,
            self.repository,
            self.branch,
            self.base_ref,
            self.base_revision,
            self.head_revision,
            self.diff_digest,
            self.validator_bundle_digest,
        )
        if any(not value.strip() for value in required):
            raise ValueError("critic request exact-subject fields must not be empty")
        if self.dispatch_sequence < 1 or self.attempt < 1:
            raise ValueError("critic dispatch sequence and attempt must be positive")
        if self.subject_size_bytes < 0:
            raise ValueError("critic subject size must not be negative")
        subject_fields = (
            bool(self.subject_ref),
            bool(self.subject_transport),
            self.subject_size_bytes > 0,
            bool(self.subject_digest),
        )
        if any(subject_fields) and not all(subject_fields):
            raise ValueError("critic subject transport binding must be complete")
        if all(subject_fields):
            if self.subject_transport not in {"sandbox_file", "stdin"}:
                raise ValueError("critic subject transport is invalid")
            if not _DIGEST.fullmatch(self.subject_digest):
                raise ValueError("critic subject digest must be lowercase SHA-256")
            if self.subject_transport == "sandbox_file":
                path = PurePosixPath(self.subject_ref)
                if (
                    not path.is_absolute()
                    or str(path) in {"/", "."}
                    or ".." in path.parts
                    or "\x00" in self.subject_ref
                ):
                    raise ValueError(
                        "critic sandbox-file subject requires a safe non-root absolute path"
                    )
            elif self.subject_ref != "stdin":
                raise ValueError("critic stdin subject reference must be 'stdin'")

    @property
    def review_id(self) -> str:
        return canonical_digest(
            {
                "candidate_id": self.candidate_id,
                "attempt": self.attempt,
                "policy_digest": self.policy.digest,
            }
        )

    @property
    def candidate_digest(self) -> str:
        return candidate_subject_digest(
            candidate_id=self.candidate_id,
            mission_id=self.mission_id,
            task_id=self.task_id,
            dispatch_id=self.dispatch_id,
            author_execution_id=self.author_execution_id,
            repository=self.repository,
            branch=self.branch,
            base_ref=self.base_ref,
            base_revision=self.base_revision,
            head_revision=self.head_revision,
            diff_digest=self.diff_digest,
            validator_bundle_digest=self.validator_bundle_digest,
            policy_digest=self.policy.digest,
        )


@dataclass(frozen=True)
class CriticFindingValue:
    """Normalized structured finding returned by the critic harness."""

    finding_id: str
    severity: str
    category: str
    confidence: float
    title: str
    detail: str
    evidence_location: str = ""
    reproduction: str = ""

    def __post_init__(self) -> None:
        if self.severity not in {"blocking", "advisory"}:
            raise ValueError("critic finding severity must be blocking or advisory")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("critic finding confidence must be between zero and one")
        if not self.finding_id or not self.category or not self.title or not self.detail:
            raise ValueError("critic finding identity and description must not be empty")


@dataclass(frozen=True)
class CriticProcessObservation:
    """Raw process facts returned by a critic driver."""

    returncode: int
    stdout: str = ""
    stderr: str = ""
    trace_uri: str = ""


@dataclass(frozen=True)
class CriticReceiptValue:
    """Complete, verifiable receipt for one immutable review subject."""

    review_id: str
    conclusion: CriticConclusion
    candidate_digest: str
    policy_digest: str
    evidence_digest: str
    reviewed_base_revision: str
    reviewed_head_revision: str
    reviewed_diff_digest: str
    validator_bundle_digest: str
    subject_metadata_digest: str
    subject_digest: str
    subject_content_size_bytes: int
    subject_metadata_size_bytes: int
    subject_size_bytes: int
    subject_media_type: str
    subject_transport: str
    subject_ref: str
    reviewed_scope: str
    finding_count: int
    blocking_count: int
    output_schema_version: int
    completed_at_ms: int


@dataclass(frozen=True)
class CriticExecutionResult:
    """Factual review result staged by application composition."""

    request: CandidateReviewRequest
    status: CriticExecutionStatus
    sandbox: SandboxIdentity
    sandbox_status: SandboxStatus
    sandbox_acquired: bool
    started_at_ms: int
    ended_at_ms: int
    provision_started_at_ms: int = 0
    sandbox_ready_at_ms: int = 0
    base_hydrated_at_ms: int = 0
    head_ready_at_ms: int = 0
    critic_started_at_ms: int = 0
    raw_output: str = ""
    trace_uri: str = ""
    findings: tuple[CriticFindingValue, ...] = ()
    receipt: CriticReceiptValue | None = None
    error: str = ""


@runtime_checkable
class CriticDriver(Protocol):
    """Invoke one independent critic process through a sandbox session."""

    driver_id: str

    async def run(
        self,
        session: SandboxSession,
        request: CandidateReviewRequest,
        prompt: str,
    ) -> CriticProcessObservation: ...


__all__ = [
    "CandidateReviewRequest",
    "CriticDriver",
    "CriticExecutionResult",
    "CriticFindingValue",
    "CriticPrewarmRequest",
    "CriticProcessObservation",
    "CriticReceiptValue",
    "CriticValidationEvidence",
    "candidate_subject_digest",
    "canonical_digest",
    "validator_bundle_digest",
]
