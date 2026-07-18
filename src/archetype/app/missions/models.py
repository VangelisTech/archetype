# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persisted mission state and provider-neutral attempt requests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import field_validator, model_validator

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
from archetype.core.component import Component


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
    error: str = ""

    @field_validator("phase")
    @classmethod
    def _valid_phase(cls, value: str) -> str:
        return FinalizationPhase(value).value


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
    last_error: str
    created_at: str
    updated_at: str
    possibly_submitted_at: str | None
    acknowledged_at: str | None
    settled_at: str | None


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
