# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persisted mission state and provider-neutral attempt requests."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from pydantic import field_validator, model_validator

from archetype.app.errors import ConflictError
from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.missions.transitions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimStatus,
    AttemptRecoveryAction,
    AttemptStatus,
    CheckpointStatus,
    FinalizationPhase,
    MissionStatus,
    MissionTaskState,
    MissionTransitionEvent,
    TaskStatus,
)
from archetype.app.redaction.models import RedactedRecord, RedactionReceipt
from archetype.core.component import Component

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class AttemptArtifactExpiration:
    """Typed artifact-outbox evidence that one exact publication expired."""

    status: Literal["expired"]
    bundle_id: str
    request_digest: str
    producer_digest: str
    redaction_policy_id: str

    def __post_init__(self) -> None:
        if self.status != "expired":
            raise ValueError("artifact expiration status must be expired")
        for name in ("bundle_id", "request_digest", "producer_digest"):
            if not _SHA256_RE.fullmatch(getattr(self, name)):
                raise ValueError(f"artifact expiration {name} must be a lowercase SHA-256 digest")
        if not self.redaction_policy_id.strip():
            raise ValueError("artifact expiration redaction_policy_id must not be empty")


class MissionArtifactFinalizationExpiredError(ConflictError):
    """The exact staged artifact publication can no longer reach INDEXED."""

    def __init__(self, receipt: AttemptArtifactExpiration) -> None:
        if not isinstance(receipt, AttemptArtifactExpiration):
            raise TypeError("artifact finalization expiry requires a typed expiration receipt")
        self.receipt = receipt
        self.bundle_id = receipt.bundle_id
        super().__init__(f"artifact publication {receipt.bundle_id} expired")


class Mission(Component):
    """Episode-level mission; ``finished`` is its terminal latch."""

    name: str = ""
    repo: str = ""
    branch: str = "agent/mission"
    plan_json: str = "[]"
    status: str = MissionStatus.READY.value
    finished: bool = False
    succeeded: bool = False
    failure_reason: str = ""
    pr_ready: bool = False
    pr_url: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return MissionStatus(value).value

    @model_validator(mode="after")
    def _consistent_terminal_flags(self) -> Mission:
        status = MissionStatus(self.status)
        expected = {
            MissionStatus.READY: (False, False),
            MissionStatus.RUNNING: (False, False),
            MissionStatus.SUCCEEDED: (True, True),
            MissionStatus.FAILED: (True, False),
        }[status]
        if (self.finished, self.succeeded) != expected:
            raise ValueError(
                f"mission status {status.value!r} requires "
                f"finished={expected[0]} and succeeded={expected[1]}"
            )
        return self


class TaskGate(Component):
    """Current task and the durable evidence threshold required to advance."""

    step_index: int = 0
    step_name: str = ""
    prompt: str = ""
    validators_json: str = "[]"
    attempts: int = 0
    max_attempts: int = 5
    status: str = TaskStatus.READY.value
    required_finalization_phase: str = FinalizationPhase.CHECKPOINTED.value
    passed: bool = False

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return TaskStatus(value).value

    @field_validator("required_finalization_phase")
    @classmethod
    def _valid_phase(cls, value: str) -> str:
        return FinalizationPhase(value).value

    @model_validator(mode="after")
    def _valid_counters_and_flags(self) -> TaskGate:
        if self.step_index < 0 or self.attempts < 0 or self.max_attempts < 1:
            raise ValueError("task indexes are non-negative and max_attempts is positive")
        if self.attempts > self.max_attempts:
            raise ValueError("task attempts cannot exceed max_attempts")
        if self.passed != (TaskStatus(self.status) is TaskStatus.PASSED):
            raise ValueError("task passed flag must agree with task status")
        return self


class Attempt(Component):
    """Exactly one submission, persisted whether accepted or rejected."""

    attempt_id: str = ""
    attempt_index: int = 0
    status: str = AttemptStatus.PENDING.value
    provider_status: str = ""
    harness: str = ""
    agent_session_id: str = ""
    validator_details_json: str = "[]"
    transition_event: str = ""
    mission_status_before: str = ""
    task_status_before: str = ""
    mission_status_after: str = ""
    task_status_after: str = ""

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return AttemptStatus(value).value

    @field_validator("transition_event")
    @classmethod
    def _valid_event(cls, value: str) -> str:
        return MissionTransitionEvent(value).value if value else ""

    @field_validator("mission_status_before", "mission_status_after")
    @classmethod
    def _valid_mission_edge(cls, value: str) -> str:
        return MissionStatus(value).value if value else ""

    @field_validator("task_status_before", "task_status_after")
    @classmethod
    def _valid_task_edge(cls, value: str) -> str:
        return TaskStatus(value).value if value else ""


class Checkpoint(Component):
    """Provider-native recovery point captured after an attempt."""

    provider: str = ""
    status: str = CheckpointStatus.PENDING.value
    state_ref: str = ""
    restorable: bool = False
    created_at_ms: int = 0
    expires_at_ms: int | None = None

    @field_validator("status")
    @classmethod
    def _valid_status(cls, value: str) -> str:
        return CheckpointStatus(value).value


class Finalization(Component):
    """Progress from evidence capture through durable publication."""

    phase: str = FinalizationPhase.PENDING.value
    idempotency_key: str = ""
    manifest_ref: str = ""
    bundle_id: str = ""
    request_digest: str = ""
    producer_digest: str = ""
    redaction_policy_id: str = ""
    index_snapshot_id: int = 0
    legacy_unbound: bool = False
    error: str = ""

    @field_validator("phase")
    @classmethod
    def _valid_phase(cls, value: str) -> str:
        return FinalizationPhase(value).value

    @field_validator("index_snapshot_id", mode="before")
    @classmethod
    def _exact_snapshot_integer(cls, value: object) -> int:
        if type(value) is not int:
            raise ValueError("finalization index_snapshot_id must be an exact integer")
        if value < 0 or value > MAX_ICEBERG_SNAPSHOT_ID:
            raise ValueError("finalization index_snapshot_id is outside the signed 64-bit range")
        return value

    @model_validator(mode="after")
    def _indexed_authority_is_complete(self) -> Finalization:
        if self.index_snapshot_id < 0:
            raise ValueError("finalization index_snapshot_id must be non-negative")
        phase = FinalizationPhase(self.phase)
        if self.legacy_unbound:
            if phase not in {FinalizationPhase.PUBLISHED, FinalizationPhase.INDEXED}:
                raise ValueError(
                    "legacy unbound finalization must retain a published or indexed phase"
                )
            if any(
                (
                    self.bundle_id,
                    self.request_digest,
                    self.producer_digest,
                    self.redaction_policy_id,
                    self.index_snapshot_id,
                )
            ):
                raise ValueError(
                    "legacy unbound finalization cannot contain current authority evidence"
                )
            return self
        if phase is FinalizationPhase.INDEXED:
            for name in ("bundle_id", "request_digest", "producer_digest"):
                if not _SHA256_RE.fullmatch(getattr(self, name)):
                    raise ValueError(f"indexed finalization requires a lowercase SHA-256 {name}")
            if not self.redaction_policy_id.strip():
                raise ValueError("indexed finalization requires a redaction policy identity")
            if not self.manifest_ref.strip():
                raise ValueError("indexed finalization requires a manifest reference")
            if self.index_snapshot_id < 1:
                raise ValueError("indexed finalization requires a positive index snapshot")
        return self


class Commit(Component):
    """Verified Git identity produced by the task gate."""

    sha: str = ""
    message: str = ""
    pushed: bool = False


class Evidence(Component):
    """Queryable references to portable and provider-native attempt evidence."""

    results_json: str = "{}"
    trace_ref: str = ""
    traces_ref: str = ""
    live_status_ref: str = ""
    live_events_ref: str = ""
    sandbox_state_ref: str = ""
    filesystem_start_ref: str = ""
    filesystem_end_ref: str = ""
    filesystem_diff_ref: str = ""
    git_status_ref: str = ""
    git_patch_ref: str = ""
    git_bundle_ref: str = ""
    context_ref: str = ""


class FrictionLog(Component):
    """Agent-reported operational friction retained as episode evidence."""

    entries_json: str = "[]"


@dataclass(frozen=True)
class MissionAttemptRequest:
    """One deterministic submission requested by the mission state machine."""

    prompt: str
    validators: tuple[dict[str, Any], ...]
    step_name: str
    step_index: int
    attempt_index: int
    plan_digest: str
    max_attempts: int
    required_finalization_phase: FinalizationPhase
    idempotency_key: str
    mission_id: str
    task_id: str
    attempt_id: str
    request_fingerprint: str
    previous_session_id: str
    previous_validator_details: tuple[dict[str, Any], ...]
    correlation: dict[str, Any]
    source: MissionTaskState
    observation_tick: int = 0

    def __post_init__(self) -> None:
        if type(self.observation_tick) is not int:
            raise TypeError("mission attempt observation_tick must be an exact integer")
        if self.observation_tick < 0:
            raise ValueError("mission attempt observation_tick must be non-negative")


@dataclass(frozen=True)
class AttemptArtifactProjection:
    """Exact artifact request staged with a claim before publication I/O."""

    request_json: str
    request_digest: str
    publication_key: str
    producer_digest: str
    redaction_policy_id: str

    def __post_init__(self) -> None:
        try:
            value = json.loads(self.request_json)
        except json.JSONDecodeError as exc:
            raise ValueError("prepared artifact request_json is invalid") from exc
        if not isinstance(value, dict):
            raise ValueError("prepared artifact request_json must be an object")
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        if canonical != self.request_json:
            raise ValueError("prepared artifact request_json must be canonical")
        if hashlib.sha256(self.request_json.encode()).hexdigest() != self.request_digest:
            raise ValueError("prepared artifact request digest does not match its JSON")
        for name in ("request_digest", "publication_key", "producer_digest"):
            if not _SHA256_RE.fullmatch(getattr(self, name)):
                raise ValueError(f"prepared artifact {name} must be a lowercase SHA-256 digest")
        if not self.redaction_policy_id.strip():
            raise ValueError("prepared artifact redaction_policy_id must not be empty")


@dataclass(frozen=True)
class AttemptArtifactPublication:
    """Mission-facing artifact receipt checked before indexed advancement."""

    status: FinalizationPhase
    bundle_id: str
    manifest_uri: str
    index_snapshot_id: int
    request_digest: str
    producer_digest: str
    redaction_policy_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.status, FinalizationPhase):
            raise TypeError("artifact publication status must be a FinalizationPhase")
        for name in ("bundle_id", "request_digest", "producer_digest"):
            if not _SHA256_RE.fullmatch(getattr(self, name)):
                raise ValueError(f"artifact publication {name} must be a lowercase SHA-256 digest")
        if not self.manifest_uri.strip():
            raise ValueError("artifact publication manifest_uri must not be empty")
        if type(self.index_snapshot_id) is not int:
            raise TypeError("artifact publication index_snapshot_id must be an exact integer")
        if not 1 <= self.index_snapshot_id <= MAX_ICEBERG_SNAPSHOT_ID:
            raise ValueError(
                "artifact publication index_snapshot_id must be within the positive "
                "signed 64-bit range"
            )
        if not self.redaction_policy_id.strip():
            raise ValueError("artifact publication redaction_policy_id must not be empty")


FinalizationSettlementKind = Literal["indexed", "expired"]


@dataclass(frozen=True)
class PreparedFinalizationSettlement:
    """Claim-bound, service-sealed terminal outcome for one finalizing attempt."""

    claim_key: str
    world_id: str
    attempt_id: str
    claimant: str
    fence_epoch: int
    attempt_status: AttemptStatus
    kind: FinalizationSettlementKind
    outcome: RedactedRecord
    seal: str = field(repr=False)

    def __post_init__(self) -> None:
        for name in ("claim_key", "world_id", "attempt_id", "claimant"):
            if not getattr(self, name).strip():
                raise ValueError(f"prepared finalization {name} must not be empty")
        if type(self.fence_epoch) is not int or self.fence_epoch < 1:
            raise ValueError("prepared finalization fence_epoch must be a positive integer")
        if not isinstance(self.attempt_status, AttemptStatus):
            raise TypeError("prepared finalization attempt_status must be typed")
        if self.attempt_status is AttemptStatus.PENDING:
            raise ValueError("prepared finalization cannot settle as pending")
        if self.kind not in {"indexed", "expired"}:
            raise ValueError("prepared finalization kind is invalid")
        if not isinstance(self.outcome, RedactedRecord):
            raise TypeError("prepared finalization outcome must be a RedactedRecord")
        if not _SHA256_RE.fullmatch(self.seal):
            raise ValueError("prepared finalization seal must be a lowercase SHA-256 digest")

    @property
    def value(self) -> dict[str, Any]:
        """Expose the sanitized value that may be projected only after settlement."""

        return self.outcome.value

    @property
    def receipt(self) -> RedactionReceipt:
        """Expose the outcome's redaction receipt without weakening the seal."""

        return self.outcome.receipt


def normalize_attempt_validators(
    validators: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Validate and canonicalize every validator field consumed by a sandbox."""

    normalized: list[dict[str, Any]] = []
    names: set[str] = set()
    for value in validators:
        if not isinstance(value, Mapping):
            raise TypeError("mission validators must be JSON objects")
        name = str(value.get("name", "")).strip()
        if not name or name in names:
            raise ValueError("mission validators require unique non-empty names")
        if name == "git_tree_change":
            raise ValueError("mission validator name 'git_tree_change' is reserved")
        raw_command = value.get("command")
        if (
            not isinstance(raw_command, (list, tuple))
            or not raw_command
            or any(not isinstance(part, str) for part in raw_command)
        ):
            raise ValueError(f"mission validator {name!r} requires a non-empty string command")
        try:
            expected_returncode = int(value.get("expected_returncode", 0))
            timeout_seconds = int(value.get("timeout_seconds", 900))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"mission validator {name!r} has invalid numeric fields") from exc
        if timeout_seconds < 1:
            raise ValueError(f"mission validator {name!r} timeout_seconds must be at least 1")
        normalized.append(
            {
                "name": name,
                "command": list(raw_command),
                "expected_returncode": expected_returncode,
                "timeout_seconds": timeout_seconds,
            }
        )
        names.add(name)
    if not normalized:
        raise ValueError("mission tasks require at least one validator")
    return tuple(normalized)


def mission_attempt_request_fingerprint(
    *,
    idempotency_key: str,
    prompt: str,
    validators: tuple[dict[str, Any], ...],
    step_name: str,
    step_index: int,
    attempt_index: int,
    plan_digest: str,
    max_attempts: int,
    required_finalization_phase: FinalizationPhase,
    previous_session_id: str,
    previous_validator_details: tuple[dict[str, Any], ...],
    correlation: dict[str, Any],
) -> str:
    """Digest the provider-neutral invocation fields owned by a mission claim."""

    payload = {
        "domain": "archetype.mission-attempt-request.v1",
        "idempotency_key": idempotency_key,
        "prompt": prompt,
        "validators": validators,
        "step_name": step_name,
        "step_index": step_index,
        "attempt_index": attempt_index,
        "plan_digest": plan_digest,
        "max_attempts": max_attempts,
        "required_finalization_phase": required_finalization_phase.value,
        "previous_session_id": previous_session_id,
        "previous_validator_details": previous_validator_details,
        "correlation": correlation,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def attempt_invocation_fingerprint(
    *,
    prompt: str,
    validators: tuple[dict[str, Any], ...],
    step_name: str,
    attempt_index: int,
    previous_session_id: str,
    previous_validator_details: tuple[dict[str, Any], ...],
    correlation: dict[str, Any],
) -> str:
    """Digest the exact normalized invocation consumed by the sandbox kernel."""

    normalized_validators = normalize_attempt_validators(validators)
    payload = {
        "prompt": prompt,
        "validators": normalized_validators,
        "step_name": step_name,
        "attempt_index": attempt_index,
        "previous_session_id": previous_session_id,
        "previous_validator_details": list(previous_validator_details),
        "correlation": correlation,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True)
class ProviderExecutionCapabilities:
    """Provider guarantees that may permit recovery-time execution."""

    provider: str
    request_fingerprint: str
    supports_idempotent_replay: bool = False
    supports_session_resume: bool = False
    provider_idempotency_key: str = ""

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider must not be empty")
        if not self.request_fingerprint.strip():
            raise ValueError("provider request_fingerprint must not be empty")
        if self.supports_idempotent_replay != bool(self.provider_idempotency_key):
            raise ValueError(
                "idempotent replay capability requires exactly one provider idempotency key"
            )


@dataclass(frozen=True)
class AttemptClaim:
    """Typed mission projection of one durable catalog claim."""

    claim_key: str
    world_id: str
    run_id: str
    mission_id: str
    task_id: str
    attempt_id: str
    idempotency_key: str
    request_fingerprint: str
    request_json: str
    redaction_policy_id: str
    redaction_evidence_json: str
    status: AttemptClaimStatus
    provider: str
    provider_request_fingerprint: str
    supports_idempotent_replay: bool
    supports_session_resume: bool
    provider_idempotency_key: str
    claimant: str
    lease_expires_at: float
    fence_epoch: int
    execution_nonce: str
    execution_consumed_at: str | None
    provider_session_id: str
    provider_request_id: str
    settlement_status: str
    outcome_digest: str
    outcome_json: str
    artifact_request_json: str
    artifact_request_digest: str
    artifact_publication_key: str
    last_error: str
    created_at: str
    updated_at: str
    possibly_submitted_at: str | None
    acknowledged_at: str | None
    finalizing_at: str | None
    settled_at: str | None
    contract_version: int = 8
    legacy_unbound_eligible: bool = False
    legacy_unbound: bool = False


@dataclass(frozen=True)
class AttemptClaimAcquisition:
    """Lease-acquisition outcome plus the typed claim projection."""

    outcome: AttemptClaimAcquireOutcome
    claim: AttemptClaim


@dataclass(frozen=True)
class FencedExecutionAuthorization:
    """Claim-bound authorization consumed by the sandbox kernel."""

    action: AttemptRecoveryAction
    claim_key: str
    world_id: str
    run_id: str
    mission_id: str
    task_id: str
    attempt_id: str
    idempotency_key: str
    request_fingerprint: str
    sandbox_request_fingerprint: str
    execution_nonce: str
    claimant: str
    fence_epoch: int
    lease_expires_at: float
    provider_session_id: str = ""
    provider_idempotency_key: str = ""


@dataclass(frozen=True)
class AttemptRecoveryDecision:
    """Recovery action and the exact fence authorizing that decision."""

    action: AttemptRecoveryAction
    claim: AttemptClaim
    authorization: FencedExecutionAuthorization


class FencedAttemptRunner(Protocol):
    """Structural sandbox port consumed by mission attempt orchestration."""

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities: ...

    async def run_attempt(
        self,
        *,
        prompt: str,
        validators: Sequence[dict[str, Any]],
        step_name: str,
        attempt_index: int,
        idempotency_key: str,
        authorization: FencedExecutionAuthorization,
        authorize_execution: Callable[[FencedExecutionAuthorization], Awaitable[None]],
        acknowledge_provider: Callable[[str, str], Awaitable[None]],
        previous_session_id: str = "",
        previous_validator_details: Sequence[dict[str, Any]] = (),
        correlation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class MissionAttemptExecution:
    """One replayable orchestration result and its terminal claim."""

    request: MissionAttemptRequest
    acquisition: AttemptClaimAcquisition
    decision: AttemptRecoveryDecision
    claim: AttemptClaim
    outcome: dict[str, Any]
    updated_row: dict[str, Any]
    replayed: bool


MISSION_COMPONENTS = (
    Mission,
    TaskGate,
    Attempt,
    Checkpoint,
    Finalization,
    Commit,
    Evidence,
    FrictionLog,
)
