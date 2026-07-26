# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable value contracts for exact-candidate Mission critic Activities.

This module owns only Mission meaning and deterministic value preparation. It
does not own Activity claims, leases, workers, projectors, or storage. The
generic Activity delivery attempt is deliberately absent from these values:
``domain_review_attempt`` is the review budget consumed only after a factual
``CriticExecution`` observation commits.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any, Protocol, runtime_checkable

from pydantic import TypeAdapter

from archetype.missions.contracts import CriticPolicy
from archetype.missions.critics.contracts import (
    CandidateReviewRequest,
    CriticExecutionResult,
    CriticFindingValue,
    CriticValidationEvidence,
    candidate_subject_digest,
    canonical_digest,
)
from archetype.missions.sandboxes import SandboxIdentity, SandboxStatus
from archetype.missions.transitions import (
    CriticConclusion,
    CriticExecutionStatus,
)

CRITIC_ACTIVITY_KIND = "missions.critic"
CRITIC_ACTIVITY_MEDIA_TYPE = "application/vnd.archetype.missions.critic+json"

_SCHEMA_VERSION = 1
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REF_PREFIX = "mission-critic+json:sha256:"
_MAX_DURABLE_VALUE_BYTES = 1 << 20
_MAX_VALIDATION_ITEMS = 128
_MAX_COMMAND_ARGUMENTS = 128
_MAX_FINDINGS = 256


def _require_digest(value: str, *, field: str) -> str:
    if not _DIGEST.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _require_text(value: str, *, field: str, limit: int = 4_096) -> str:
    if not value.strip():
        raise ValueError(f"{field} must not be empty")
    if len(value.encode()) > limit:
        raise ValueError(f"{field} exceeds its {limit}-byte bound")
    return value


class CriticSubjectTransport(StrEnum):
    """Provider-neutral ways to carry a large exact review subject."""

    SANDBOX_FILE = "sandbox_file"
    STDIN = "stdin"


@dataclass(frozen=True, slots=True)
class CriticSubjectPolicy:
    """Immutable subject identity and byte budget admitted with the review."""

    digest: str
    max_bytes: int
    media_type: str = "application/vnd.git-diff"
    allowed_transports: tuple[CriticSubjectTransport, ...] = (
        CriticSubjectTransport.SANDBOX_FILE,
        CriticSubjectTransport.STDIN,
    )

    def __post_init__(self) -> None:
        _require_digest(self.digest, field="critic subject digest")
        if self.max_bytes < 1:
            raise ValueError("critic subject max_bytes must be positive")
        _require_text(self.media_type, field="critic subject media_type", limit=256)
        transports = tuple(CriticSubjectTransport(item) for item in self.allowed_transports)
        if not transports or len(transports) != len(set(transports)):
            raise ValueError("critic subject transports must be non-empty and unique")
        object.__setattr__(self, "allowed_transports", transports)


@dataclass(frozen=True, slots=True)
class CriticSubjectBinding:
    """Observed bounded subject placement used by one critic invocation."""

    content_digest: str
    metadata_digest: str
    subject_digest: str
    content_size_bytes: int
    metadata_size_bytes: int
    total_size_bytes: int
    media_type: str
    transport: CriticSubjectTransport
    ref: str

    def __post_init__(self) -> None:
        _require_digest(self.content_digest, field="critic subject content digest")
        _require_digest(self.metadata_digest, field="critic subject metadata digest")
        _require_digest(self.subject_digest, field="critic subject aggregate digest")
        if (
            min(
                self.content_size_bytes,
                self.metadata_size_bytes,
                self.total_size_bytes,
            )
            < 0
        ):
            raise ValueError("critic subject sizes must not be negative")
        if self.total_size_bytes != self.content_size_bytes + self.metadata_size_bytes:
            raise ValueError("critic subject total size does not match its parts")
        _require_text(self.media_type, field="critic subject media_type", limit=256)
        transport = CriticSubjectTransport(self.transport)
        object.__setattr__(self, "transport", transport)
        if transport is CriticSubjectTransport.SANDBOX_FILE:
            path = PurePosixPath(self.ref)
            if (
                not path.is_absolute()
                or str(path) in {"/", "."}
                or ".." in path.parts
                or "\x00" in self.ref
            ):
                raise ValueError(
                    "critic sandbox-file subject requires a safe non-root absolute path"
                )
        elif self.ref != "stdin":
            raise ValueError("critic stdin subject reference must be 'stdin'")
        expected_digest = canonical_digest(
            {
                "content_digest": self.content_digest,
                "content_size_bytes": self.content_size_bytes,
                "media_type": self.media_type,
                "metadata_digest": self.metadata_digest,
                "metadata_size_bytes": self.metadata_size_bytes,
                "total_size_bytes": self.total_size_bytes,
                "transport": transport.value,
            }
        )
        if self.subject_digest != expected_digest:
            raise ValueError("critic subject aggregate digest does not match its binding")


class CriticSubjectTooLarge(ValueError):
    """The verified exact subject cannot be admitted under its policy budget."""

    def __init__(
        self,
        *,
        content_digest: str,
        observed_bytes: int,
        max_bytes: int,
    ) -> None:
        self.content_digest = _require_digest(
            content_digest,
            field="critic subject content digest",
        )
        self.observed_bytes = observed_bytes
        self.max_bytes = max_bytes
        super().__init__(
            "critic subject exceeds its policy byte bound "
            f"(digest={content_digest}, observed_bytes={observed_bytes}, "
            f"max_bytes={max_bytes})"
        )


def bind_critic_subject(
    policy: CriticSubjectPolicy,
    *,
    metadata: bytes,
    content: bytes,
    transport: CriticSubjectTransport,
    ref: str,
) -> CriticSubjectBinding:
    """Verify exact bytes, enforce the total budget, and bind their transport."""

    content_digest = hashlib.sha256(content).hexdigest()
    return bind_critic_subject_observation(
        policy,
        metadata=metadata,
        content_digest=content_digest,
        content_size_bytes=len(content),
        transport=transport,
        ref=ref,
    )


def bind_critic_subject_observation(
    policy: CriticSubjectPolicy,
    *,
    metadata: bytes,
    content_digest: str,
    content_size_bytes: int,
    transport: CriticSubjectTransport,
    ref: str,
) -> CriticSubjectBinding:
    """Bind a provider-measured subject without transporting its large bytes."""

    _require_digest(content_digest, field="critic subject content digest")
    if content_digest != policy.digest:
        raise ValueError("critic subject bytes do not match the admitted digest")
    if content_size_bytes < 0:
        raise ValueError("critic subject content size must not be negative")
    selected = CriticSubjectTransport(transport)
    if selected not in policy.allowed_transports:
        raise ValueError("critic subject transport is not admitted by policy")
    total_size = len(metadata) + content_size_bytes
    if total_size > policy.max_bytes:
        raise CriticSubjectTooLarge(
            content_digest=content_digest,
            observed_bytes=total_size,
            max_bytes=policy.max_bytes,
        )
    metadata_digest = hashlib.sha256(metadata).hexdigest()
    aggregate = canonical_digest(
        {
            "content_digest": content_digest,
            "content_size_bytes": content_size_bytes,
            "media_type": policy.media_type,
            "metadata_digest": metadata_digest,
            "metadata_size_bytes": len(metadata),
            "total_size_bytes": total_size,
            "transport": selected.value,
        }
    )
    return CriticSubjectBinding(
        content_digest=content_digest,
        metadata_digest=metadata_digest,
        subject_digest=aggregate,
        content_size_bytes=content_size_bytes,
        metadata_size_bytes=len(metadata),
        total_size_bytes=total_size,
        media_type=policy.media_type,
        transport=selected,
        ref=ref,
    )


@dataclass(frozen=True, slots=True)
class CriticActivityRequest:
    """Secret-safe immutable exact-candidate request admitted as an Activity."""

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
    domain_review_attempt: int
    subject: CriticSubjectPolicy
    redaction_policy_id: str

    def __post_init__(self) -> None:
        if (
            min(
                self.candidate_entity_id,
                self.mission_id,
                self.task_id,
                self.author_execution_id,
                self.dispatch_sequence,
                self.domain_review_attempt,
            )
            < 1
        ):
            raise ValueError("critic Activity identities and domain attempt must be positive")
        if self.candidate_published_at_ms < 0:
            raise ValueError("critic candidate publication time must not be negative")
        for field, value in (
            ("candidate_id", self.candidate_id),
            ("task_name", self.task_name),
            ("task_prompt", self.task_prompt),
            ("dispatch_id", self.dispatch_id),
            ("author_sandbox_id", self.author_sandbox_id),
            ("repository", self.repository),
            ("branch", self.branch),
            ("base_ref", self.base_ref),
            ("base_revision", self.base_revision),
            ("head_revision", self.head_revision),
            ("redaction_policy_id", self.redaction_policy_id),
        ):
            _require_text(value, field=f"critic Activity {field}", limit=64 << 10)
        _require_digest(self.diff_digest, field="critic Activity diff_digest")
        _require_digest(
            self.validator_bundle_digest,
            field="critic Activity validator_bundle_digest",
        )
        if self.subject.digest != self.diff_digest:
            raise ValueError("critic subject digest does not match the candidate diff")
        if self.subject.max_bytes != self.policy.max_subject_bytes:
            raise ValueError("critic subject budget does not match the critic policy")
        if len(self.validation) > _MAX_VALIDATION_ITEMS:
            raise ValueError("critic validation evidence exceeds its item bound")

    @property
    def review_id(self) -> str:
        """Stable logical Activity identity, independent of delivery attempts."""

        return canonical_digest(
            {
                "candidate_id": self.candidate_id,
                # Preserve the established logical identity codec while making
                # the semantic name explicit in the durable value schema.
                "attempt": self.domain_review_attempt,
                "policy_digest": self.policy.digest,
            }
        )

    @property
    def candidate_digest(self) -> str:
        """Return the complete immutable review-subject identity."""

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

    def as_review_request(self) -> CandidateReviewRequest:
        """Reconstruct the provider-neutral request without embedding diff bytes."""

        return CandidateReviewRequest(
            candidate_entity_id=self.candidate_entity_id,
            candidate_id=self.candidate_id,
            mission_id=self.mission_id,
            task_id=self.task_id,
            task_name=self.task_name,
            task_prompt=self.task_prompt,
            dispatch_id=self.dispatch_id,
            dispatch_sequence=self.dispatch_sequence,
            author_execution_id=self.author_execution_id,
            author_sandbox_id=self.author_sandbox_id,
            repository=self.repository,
            branch=self.branch,
            base_ref=self.base_ref,
            base_revision=self.base_revision,
            head_revision=self.head_revision,
            diff_digest=self.diff_digest,
            validator_bundle_digest=self.validator_bundle_digest,
            policy=self.policy,
            validation=self.validation,
            candidate_published_at_ms=self.candidate_published_at_ms,
            attempt=self.domain_review_attempt,
        )


@dataclass(frozen=True, slots=True)
class CriticActivityReceipt:
    """Exact, self-contained durable receipt for one completed review."""

    review_id: str
    domain_review_attempt: int
    conclusion: CriticConclusion
    candidate_digest: str
    policy_digest: str
    author_sandbox_id: str
    critic_sandbox: SandboxIdentity
    reviewed_base_revision: str
    reviewed_head_revision: str
    reviewed_diff_digest: str
    validator_bundle_digest: str
    subject: CriticSubjectBinding
    evidence_digest: str
    reviewed_scope: str
    finding_count: int
    blocking_count: int
    output_schema_version: int
    completed_at_ms: int
    redaction_policy_id: str

    def __post_init__(self) -> None:
        for field, value in (
            ("review_id", self.review_id),
            ("candidate_digest", self.candidate_digest),
            ("policy_digest", self.policy_digest),
            ("reviewed_diff_digest", self.reviewed_diff_digest),
            ("validator_bundle_digest", self.validator_bundle_digest),
            ("evidence_digest", self.evidence_digest),
        ):
            _require_digest(value, field=f"critic receipt {field}")
        _require_text(
            self.reviewed_base_revision,
            field="critic receipt reviewed_base_revision",
        )
        _require_text(
            self.reviewed_head_revision,
            field="critic receipt reviewed_head_revision",
        )
        _require_text(self.reviewed_scope, field="critic receipt reviewed_scope", limit=4_096)
        _require_text(
            self.redaction_policy_id,
            field="critic receipt redaction_policy_id",
        )
        _require_text(
            self.author_sandbox_id,
            field="critic receipt author_sandbox_id",
        )
        if self.critic_sandbox.sandbox_id == self.author_sandbox_id:
            raise ValueError("critic receipt reused the author sandbox identity")
        if self.domain_review_attempt < 1:
            raise ValueError("critic receipt domain review attempt must be positive")
        if (
            min(
                self.finding_count,
                self.blocking_count,
                self.completed_at_ms,
            )
            < 0
        ):
            raise ValueError("critic receipt counts and time must not be negative")
        if self.blocking_count > self.finding_count:
            raise ValueError("critic receipt blocking count exceeds finding count")
        if (self.conclusion is CriticConclusion.APPROVED) == (self.blocking_count > 0):
            raise ValueError("critic receipt conclusion conflicts with blocking findings")
        if self.output_schema_version < 1:
            raise ValueError("critic receipt schema version must be positive")
        if self.subject.content_digest != self.reviewed_diff_digest:
            raise ValueError("critic receipt subject does not bind its reviewed diff")


@dataclass(frozen=True, slots=True)
class CriticActivityResult:
    """Bounded factual critic result safe to publish before ECS observation."""

    review_id: str
    domain_review_attempt: int
    candidate_digest: str
    policy_digest: str
    base_revision: str
    head_revision: str
    diff_digest: str
    validator_bundle_digest: str
    author_sandbox_id: str
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
    receipt: CriticActivityReceipt | None = None
    error: str = ""
    redaction_policy_id: str = ""

    def __post_init__(self) -> None:
        for field, value in (
            ("review_id", self.review_id),
            ("candidate_digest", self.candidate_digest),
            ("policy_digest", self.policy_digest),
            ("diff_digest", self.diff_digest),
            ("validator_bundle_digest", self.validator_bundle_digest),
        ):
            _require_digest(value, field=f"critic result {field}")
        if self.domain_review_attempt < 1:
            raise ValueError("critic result domain review attempt must be positive")
        for field, value in (
            ("base_revision", self.base_revision),
            ("head_revision", self.head_revision),
            ("author_sandbox_id", self.author_sandbox_id),
            ("redaction_policy_id", self.redaction_policy_id),
        ):
            _require_text(value, field=f"critic result {field}")
        if self.sandbox.sandbox_id == self.author_sandbox_id and (
            self.status is CriticExecutionStatus.EXITED or self.receipt is not None
        ):
            raise ValueError("completed critic result reused the author sandbox identity")
        if (
            min(
                self.started_at_ms,
                self.ended_at_ms,
                self.provision_started_at_ms,
                self.sandbox_ready_at_ms,
                self.base_hydrated_at_ms,
                self.head_ready_at_ms,
                self.critic_started_at_ms,
            )
            < 0
        ):
            raise ValueError("critic result times must not be negative")
        if self.ended_at_ms < self.started_at_ms:
            raise ValueError("critic result ended before it started")
        if len(self.findings) > _MAX_FINDINGS:
            raise ValueError("critic result findings exceed their item bound")
        if (self.status is CriticExecutionStatus.EXITED) != (self.receipt is not None):
            raise ValueError("only a completed critic result may carry a receipt")
        receipt = self.receipt
        if receipt is not None:
            expected = (
                self.review_id,
                self.domain_review_attempt,
                self.candidate_digest,
                self.policy_digest,
                self.author_sandbox_id,
                self.sandbox,
                self.base_revision,
                self.head_revision,
                self.diff_digest,
                self.validator_bundle_digest,
            )
            observed = (
                receipt.review_id,
                receipt.domain_review_attempt,
                receipt.candidate_digest,
                receipt.policy_digest,
                receipt.author_sandbox_id,
                receipt.critic_sandbox,
                receipt.reviewed_base_revision,
                receipt.reviewed_head_revision,
                receipt.reviewed_diff_digest,
                receipt.validator_bundle_digest,
            )
            if observed != expected:
                raise ValueError("critic receipt identity does not match its result")
            if receipt.finding_count != len(self.findings):
                raise ValueError("critic receipt finding count does not match its result")
            blocking = sum(item.severity == "blocking" for item in self.findings)
            if receipt.blocking_count != blocking:
                raise ValueError("critic receipt blocking count does not match its result")
            if receipt.redaction_policy_id != self.redaction_policy_id:
                raise ValueError("critic receipt redaction policy does not match its result")


@dataclass(frozen=True, slots=True)
class CriticActivityValue:
    """One canonical encoded durable value and its bounded content identity."""

    value_kind: str
    ref: str
    digest: str
    media_type: str
    size_bytes: int
    payload: bytes

    def __post_init__(self) -> None:
        if self.value_kind not in {"request", "result", "receipt"}:
            raise ValueError("critic Activity value kind is invalid")
        _require_digest(self.digest, field="critic Activity value digest")
        if self.ref != f"{_REF_PREFIX}{self.digest}":
            raise ValueError("critic Activity value ref does not match its digest")
        if self.media_type != CRITIC_ACTIVITY_MEDIA_TYPE:
            raise ValueError("critic Activity value media type is invalid")
        if self.size_bytes != len(self.payload):
            raise ValueError("critic Activity value size does not match its payload")
        if self.size_bytes > _MAX_DURABLE_VALUE_BYTES:
            raise ValueError("critic Activity value exceeds its durable bound")
        if hashlib.sha256(self.payload).hexdigest() != self.digest:
            raise ValueError("critic Activity value digest does not match its payload")


class _RedactedText(Protocol):
    text: str


@runtime_checkable
class CriticActivityRedactor(Protocol):
    """Pre-durability scanner supplied by outer composition."""

    @property
    def policy_id(self) -> str: ...

    def redact_text(self, value: str, *, scope: str) -> _RedactedText: ...

    def assert_safe_metadata(self, value: str, *, field: str) -> object: ...


class CriticActivityCodec:
    """Prepare and canonically encode bounded, redacted critic values."""

    def __init__(self, redactor: CriticActivityRedactor) -> None:
        self._redactor = redactor
        if not redactor.policy_id.strip():
            raise ValueError("critic Activity redaction policy cannot be empty")

    def prepare_request(self, request: CandidateReviewRequest) -> CriticActivityRequest:
        """Sanitize one committed exact-candidate request without diff bytes."""

        if request.diff:
            raise ValueError("critic Activity requests must not embed exact diff bytes")
        if request.subject_ref or request.subject_transport or request.subject_digest:
            raise ValueError("critic Activity admission cannot contain a provider subject binding")
        for field, value in (
            ("candidate_id", request.candidate_id),
            ("dispatch_id", request.dispatch_id),
            ("author_sandbox_id", request.author_sandbox_id),
            ("repository", request.repository),
            ("branch", request.branch),
            ("base_ref", request.base_ref),
            ("base_revision", request.base_revision),
            ("head_revision", request.head_revision),
            ("diff_digest", request.diff_digest),
            ("validator_bundle_digest", request.validator_bundle_digest),
        ):
            self._safe(value, f"critic.request.{field}")
        if len(request.validation) > _MAX_VALIDATION_ITEMS:
            raise ValueError("critic validation evidence exceeds its item bound")
        validation = tuple(
            self._validation(item, request.review_id, position)
            for position, item in enumerate(request.validation)
        )
        durable = CriticActivityRequest(
            candidate_entity_id=request.candidate_entity_id,
            candidate_id=request.candidate_id,
            mission_id=request.mission_id,
            task_id=request.task_id,
            task_name=self._redact(
                request.task_name,
                limit=1_000,
                scope=f"critic:{request.review_id}:task-name",
            ),
            task_prompt=self._redact(
                request.task_prompt,
                limit=64 << 10,
                scope=f"critic:{request.review_id}:task-prompt",
            ),
            dispatch_id=request.dispatch_id,
            dispatch_sequence=request.dispatch_sequence,
            author_execution_id=request.author_execution_id,
            author_sandbox_id=request.author_sandbox_id,
            repository=request.repository,
            branch=request.branch,
            base_ref=request.base_ref,
            base_revision=request.base_revision,
            head_revision=request.head_revision,
            diff_digest=request.diff_digest,
            validator_bundle_digest=request.validator_bundle_digest,
            policy=request.policy,
            validation=validation,
            candidate_published_at_ms=request.candidate_published_at_ms,
            domain_review_attempt=request.attempt,
            subject=CriticSubjectPolicy(
                digest=request.diff_digest,
                max_bytes=request.policy.max_subject_bytes,
            ),
            redaction_policy_id=self._redactor.policy_id,
        )
        self.encode_request(durable)
        return durable

    def prepare_result(
        self,
        result: CriticExecutionResult,
        request: CriticActivityRequest,
    ) -> CriticActivityResult:
        """Validate exact identity and redact one raw provider result."""

        raw_request = result.request
        # Re-prepare the provider-returned request under the same redaction
        # policy and require the complete durable value to match admission.
        # The logical review digest intentionally excludes presentation text
        # and observed validator output, but those inputs still affect the
        # critic's decision and therefore cannot drift during execution.
        if self.prepare_request(raw_request) != request:
            raise ValueError("critic provider result belongs to another Activity request")
        if result.sandbox.sandbox_id in {
            request.author_sandbox_id,
            raw_request.author_sandbox_id,
        } and (result.status is CriticExecutionStatus.EXITED or result.receipt is not None):
            raise ValueError("completed critic result reused an author sandbox identity")
        for field, value in (
            ("sandbox.provider", result.sandbox.provider),
            ("sandbox.id", result.sandbox.sandbox_id),
            ("sandbox.environment", result.sandbox.environment),
            ("trace_uri", result.trace_uri),
        ):
            self._safe(value, f"critic.result.{field}")
        findings = tuple(
            self._finding(item, request.review_id, position)
            for position, item in enumerate(result.findings)
        )
        if len(findings) > _MAX_FINDINGS:
            raise ValueError("critic result findings exceed their item bound")
        receipt = self._receipt(result, request, findings) if result.receipt is not None else None
        durable = CriticActivityResult(
            review_id=request.review_id,
            domain_review_attempt=request.domain_review_attempt,
            candidate_digest=request.candidate_digest,
            policy_digest=request.policy.digest,
            base_revision=request.base_revision,
            head_revision=request.head_revision,
            diff_digest=request.diff_digest,
            validator_bundle_digest=request.validator_bundle_digest,
            author_sandbox_id=request.author_sandbox_id,
            status=result.status,
            sandbox=result.sandbox,
            sandbox_status=result.sandbox_status,
            sandbox_acquired=result.sandbox_acquired,
            started_at_ms=result.started_at_ms,
            ended_at_ms=result.ended_at_ms,
            provision_started_at_ms=result.provision_started_at_ms,
            sandbox_ready_at_ms=result.sandbox_ready_at_ms,
            base_hydrated_at_ms=result.base_hydrated_at_ms,
            head_ready_at_ms=result.head_ready_at_ms,
            critic_started_at_ms=result.critic_started_at_ms,
            raw_output=self._redact(
                result.raw_output,
                limit=request.policy.max_output_chars,
                scope=f"critic:{request.review_id}:raw-output",
                tail=True,
            ),
            trace_uri=result.trace_uri,
            findings=findings,
            receipt=receipt,
            error=self._redact(
                result.error,
                limit=4_000,
                scope=f"critic:{request.review_id}:error",
                tail=True,
            ),
            redaction_policy_id=self._redactor.policy_id,
        )
        self.encode_result(durable)
        return durable

    def encode_request(self, value: CriticActivityRequest) -> CriticActivityValue:
        encoded = _encode("request", value, _REQUEST_ADAPTER)
        self._assert_safe_payload(encoded, policy_id=value.redaction_policy_id)
        return encoded

    def decode_request(self, payload: bytes) -> CriticActivityRequest:
        value = _decode("request", payload, _REQUEST_ADAPTER)
        self._assert_safe_encoded(payload, policy_id=value.redaction_policy_id)
        return value

    def encode_result(self, value: CriticActivityResult) -> CriticActivityValue:
        encoded = _encode("result", value, _RESULT_ADAPTER)
        self._assert_safe_payload(encoded, policy_id=value.redaction_policy_id)
        return encoded

    def decode_result(self, payload: bytes) -> CriticActivityResult:
        value = _decode("result", payload, _RESULT_ADAPTER)
        self._assert_safe_encoded(payload, policy_id=value.redaction_policy_id)
        return value

    def encode_receipt(self, value: CriticActivityReceipt) -> CriticActivityValue:
        encoded = _encode("receipt", value, _RECEIPT_ADAPTER)
        self._assert_safe_payload(encoded, policy_id=value.redaction_policy_id)
        return encoded

    def decode_receipt(self, payload: bytes) -> CriticActivityReceipt:
        value = _decode("receipt", payload, _RECEIPT_ADAPTER)
        self._assert_safe_encoded(payload, policy_id=value.redaction_policy_id)
        return value

    def _validation(
        self,
        value: CriticValidationEvidence,
        review_id: str,
        position: int,
    ) -> CriticValidationEvidence:
        if len(value.command) > _MAX_COMMAND_ARGUMENTS:
            raise ValueError("critic validator command exceeds its argument bound")
        self._safe(value.revision, "critic.request.validation.revision")
        return replace(
            value,
            name=self._redact(
                value.name,
                limit=1_000,
                scope=f"critic:{review_id}:validation:{position}:name",
            ),
            command=tuple(
                self._redact(
                    argument,
                    limit=2_000,
                    scope=f"critic:{review_id}:validation:{position}:command:{index}",
                )
                for index, argument in enumerate(value.command)
            ),
            stdout=self._redact(
                value.stdout,
                limit=4_000,
                scope=f"critic:{review_id}:validation:{position}:stdout",
                tail=True,
            ),
            stderr=self._redact(
                value.stderr,
                limit=4_000,
                scope=f"critic:{review_id}:validation:{position}:stderr",
                tail=True,
            ),
        )

    def _finding(
        self,
        value: CriticFindingValue,
        review_id: str,
        position: int,
    ) -> CriticFindingValue:
        scope = f"critic:{review_id}:finding:{position}"
        return replace(
            value,
            finding_id=self._redact(value.finding_id, limit=256, scope=f"{scope}:id"),
            category=self._redact(value.category, limit=256, scope=f"{scope}:category"),
            title=self._redact(value.title, limit=1_000, scope=f"{scope}:title"),
            detail=self._redact(value.detail, limit=4_000, scope=f"{scope}:detail"),
            evidence_location=self._redact(
                value.evidence_location,
                limit=1_000,
                scope=f"{scope}:location",
            ),
            reproduction=self._redact(
                value.reproduction,
                limit=4_000,
                scope=f"{scope}:reproduction",
            ),
        )

    def _receipt(
        self,
        result: CriticExecutionResult,
        request: CriticActivityRequest,
        findings: tuple[CriticFindingValue, ...],
    ) -> CriticActivityReceipt:
        raw = result.receipt
        if raw is None:
            raise AssertionError("critic receipt preparation requires a receipt")
        subject = CriticSubjectBinding(
            content_digest=raw.reviewed_diff_digest,
            metadata_digest=raw.subject_metadata_digest,
            subject_digest=raw.subject_digest,
            content_size_bytes=raw.subject_content_size_bytes,
            metadata_size_bytes=raw.subject_metadata_size_bytes,
            total_size_bytes=raw.subject_size_bytes,
            media_type=raw.subject_media_type,
            transport=CriticSubjectTransport(raw.subject_transport),
            ref=raw.subject_ref,
        )
        if subject.total_size_bytes > request.subject.max_bytes:
            raise CriticSubjectTooLarge(
                content_digest=subject.content_digest,
                observed_bytes=subject.total_size_bytes,
                max_bytes=request.subject.max_bytes,
            )
        if subject.transport not in request.subject.allowed_transports:
            raise ValueError("critic receipt used a non-admitted subject transport")
        if subject.media_type != request.subject.media_type:
            raise ValueError("critic receipt used a non-admitted subject media type")
        self._safe(raw.subject_ref, "critic.receipt.subject_ref")
        receipt = CriticActivityReceipt(
            review_id=raw.review_id,
            domain_review_attempt=request.domain_review_attempt,
            conclusion=raw.conclusion,
            candidate_digest=raw.candidate_digest,
            policy_digest=raw.policy_digest,
            author_sandbox_id=request.author_sandbox_id,
            critic_sandbox=result.sandbox,
            reviewed_base_revision=raw.reviewed_base_revision,
            reviewed_head_revision=raw.reviewed_head_revision,
            reviewed_diff_digest=raw.reviewed_diff_digest,
            validator_bundle_digest=raw.validator_bundle_digest,
            subject=subject,
            evidence_digest=raw.evidence_digest,
            reviewed_scope=self._redact(
                raw.reviewed_scope,
                limit=1_000,
                scope=f"critic:{request.review_id}:reviewed-scope",
            ),
            finding_count=raw.finding_count,
            blocking_count=raw.blocking_count,
            output_schema_version=raw.output_schema_version,
            completed_at_ms=raw.completed_at_ms,
            redaction_policy_id=self._redactor.policy_id,
        )
        if receipt.finding_count != len(findings):
            raise ValueError("critic receipt finding count does not match provider result")
        return receipt

    def _redact(
        self,
        value: str,
        *,
        limit: int,
        scope: str,
        tail: bool = False,
    ) -> str:
        if not value:
            return ""
        redacted = self._redactor.redact_text(value, scope=scope).text
        encoded = redacted.encode()
        if len(encoded) <= limit:
            return redacted
        if not tail:
            raise ValueError(f"{scope} exceeds its {limit}-byte durable bound")
        # Text limits are byte bounds. Trim from the requested side without
        # splitting a UTF-8 code point.
        selected = encoded[-limit:] if tail else encoded[:limit]
        return selected.decode(errors="ignore")

    def _safe(self, value: str, field: str) -> str:
        if value:
            if len(value.encode()) > 64 << 10:
                raise ValueError(f"{field} exceeds its metadata bound")
            self._redactor.assert_safe_metadata(value, field=field)
        return value

    def _assert_safe_payload(
        self,
        value: CriticActivityValue,
        *,
        policy_id: str,
    ) -> None:
        self._assert_safe_encoded(value.payload, policy_id=policy_id)

    def _assert_safe_encoded(self, payload: bytes, *, policy_id: str) -> None:
        if policy_id != self._redactor.policy_id:
            raise ValueError("critic Activity value belongs to another redaction policy")
        text = payload.decode()
        scanned = self._redactor.redact_text(
            text,
            scope="critic-activity:canonical-value",
        ).text
        if scanned != text:
            raise ValueError("critic Activity value was not sanitized before encoding")


_REQUEST_ADAPTER = TypeAdapter(CriticActivityRequest)
_RESULT_ADAPTER = TypeAdapter(CriticActivityResult)
_RECEIPT_ADAPTER = TypeAdapter(CriticActivityReceipt)


def _encode(
    value_kind: str,
    value: Any,
    adapter: TypeAdapter[Any],
) -> CriticActivityValue:
    encoded = json.dumps(
        {
            "kind": value_kind,
            "schema_version": _SCHEMA_VERSION,
            "value": adapter.dump_python(value, mode="json"),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    if len(encoded) > _MAX_DURABLE_VALUE_BYTES:
        raise ValueError("critic Activity value exceeds its durable bound")
    digest = hashlib.sha256(encoded).hexdigest()
    return CriticActivityValue(
        value_kind=value_kind,
        ref=f"{_REF_PREFIX}{digest}",
        digest=digest,
        media_type=CRITIC_ACTIVITY_MEDIA_TYPE,
        size_bytes=len(encoded),
        payload=encoded,
    )


def _decode(
    value_kind: str,
    payload: bytes,
    adapter: TypeAdapter[Any],
) -> Any:
    if len(payload) > _MAX_DURABLE_VALUE_BYTES:
        raise ValueError("critic Activity value exceeds its durable bound")
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("critic Activity value envelope must be an object")
    if parsed.get("kind") != value_kind or parsed.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("critic Activity value envelope identity is invalid")
    value = adapter.validate_python(parsed.get("value"))
    if _encode(value_kind, value, adapter).payload != payload:
        raise ValueError("critic Activity value must use canonical JSON encoding")
    return value


__all__ = [
    "CRITIC_ACTIVITY_KIND",
    "CRITIC_ACTIVITY_MEDIA_TYPE",
    "CriticActivityCodec",
    "CriticActivityReceipt",
    "CriticActivityRedactor",
    "CriticActivityRequest",
    "CriticActivityResult",
    "CriticActivityValue",
    "CriticSubjectBinding",
    "CriticSubjectPolicy",
    "CriticSubjectTooLarge",
    "CriticSubjectTransport",
    "bind_critic_subject",
    "bind_critic_subject_observation",
]
