# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Missions-owned MissionRun values, identity, and legal lifecycle edges.

This control-plane record is not an ECS Component. Processors still own
Mission/Task transitions; a run only wraps the governed SubmitMission and
RunMission path with a durable asynchronous handle.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from pydantic import TypeAdapter

from archetype.errors import ConflictError
from archetype.missions.contracts import (
    AgentMissionConfig,
    MissionResult,
    MissionSubmission,
)
from archetype.missions.execution_profiles import ExecutionProfileIdentity

_MAX_ID_CHARS = 512
_MAX_REASON_CHARS = 4096
_MAX_EVENT_PAYLOAD_CHARS = 4096
DEFAULT_EXECUTION_PROFILE_ID = "archetype.missions.default"
DEFAULT_EXECUTION_PROFILE_VERSION = "1"
MISSION_RUN_EVENT_SCHEMA_VERSION = 1
MISSION_RUN_EVENT_MAX_PAGE = 500

MISSION_RUN_EVENT_PHASES = MappingProxyType(
    {
        "accepted": "admission",
        "running": "execution",
        "mission_bound": "execution",
        "cancel_requested": "cancellation",
        "cancelling": "cancellation",
        "succeeded": "terminal",
        "failed": "terminal",
        "cancelled": "terminal",
        "interrupted": "terminal",
    }
)


class MissionRunStatus(StrEnum):
    """Execution lifecycle for one externally requested MissionRun."""

    ACCEPTED = "accepted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class MissionRunCleanupState(StrEnum):
    """Cleanup/recovery facts recorded independently of execution outcome."""

    NONE = "none"
    PENDING = "pending"


TERMINAL_MISSION_RUN_STATUSES = frozenset(
    {
        MissionRunStatus.SUCCEEDED,
        MissionRunStatus.FAILED,
        MissionRunStatus.CANCELLED,
        MissionRunStatus.INTERRUPTED,
    }
)

MISSION_RUN_TRANSITIONS = MappingProxyType(
    {
        MissionRunStatus.ACCEPTED: frozenset(
            {
                MissionRunStatus.RUNNING,
                MissionRunStatus.CANCELLED,
            }
        ),
        MissionRunStatus.RUNNING: frozenset(
            {
                MissionRunStatus.SUCCEEDED,
                MissionRunStatus.FAILED,
                MissionRunStatus.CANCELLING,
                MissionRunStatus.INTERRUPTED,
            }
        ),
        MissionRunStatus.CANCELLING: frozenset(
            {
                MissionRunStatus.CANCELLED,
                MissionRunStatus.INTERRUPTED,
            }
        ),
        MissionRunStatus.SUCCEEDED: frozenset(),
        MissionRunStatus.FAILED: frozenset(),
        MissionRunStatus.CANCELLED: frozenset(),
        MissionRunStatus.INTERRUPTED: frozenset(),
    }
)


class MissionRunConflictError(ConflictError):
    """The same idempotency identity was reused with a different request."""

    public_detail = "Mission run conflicts with existing state"


class MissionRunNotFoundError(KeyError):
    """No durable MissionRun exists for the supplied run identity."""

    def __init__(self, run_id: str) -> None:
        super().__init__(f"Mission run {run_id!r} was not found")
        self.run_id = run_id


def require_mission_run_transition(
    source: MissionRunStatus | str,
    target: MissionRunStatus | str,
) -> None:
    """Reject a MissionRun execution edge absent from the v1 graph."""

    parsed_source = MissionRunStatus(source)
    parsed_target = MissionRunStatus(target)
    if parsed_target not in MISSION_RUN_TRANSITIONS[parsed_source]:
        raise ValueError(
            f"illegal mission-run transition: {parsed_source.value} to {parsed_target.value}"
        )


def _require_bounded_text(value: str, field_name: str, max_chars: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} must not be empty")
    if len(stripped) > max_chars:
        raise ValueError(f"{field_name} must be at most {max_chars} characters")
    return stripped


@dataclass(frozen=True)
class MissionRunRequest:
    """Caller identity plus the canonical mission submission for one run."""

    principal: str
    idempotency_key: str
    submission: MissionSubmission

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "principal",
            _require_bounded_text(self.principal, "principal", _MAX_ID_CHARS),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _require_bounded_text(self.idempotency_key, "idempotency_key", _MAX_ID_CHARS),
        )


@dataclass(frozen=True)
class MissionRun:
    """Durable asynchronous handle for one externally requested Agent Mission."""

    run_id: str
    principal: str
    idempotency_key: str
    request_digest: str
    profile: ExecutionProfileIdentity
    status: MissionRunStatus
    submission: MissionSubmission
    world_id: str = ""
    mission_id: int | None = None
    episode_id: str = ""
    task_ids: tuple[tuple[str, int], ...] = ()
    active_operation: str = ""
    cancellation_intent: bool = False
    cancellation_reason: str = ""
    result: MissionResult | None = None
    cleanup_state: MissionRunCleanupState = MissionRunCleanupState.NONE
    accepted_at_ms: int = 0
    running_at_ms: int | None = None
    terminal_at_ms: int | None = None
    updated_at_ms: int = 0
    interrupted_reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            _require_bounded_text(self.run_id, "run_id", _MAX_ID_CHARS),
        )
        object.__setattr__(
            self,
            "principal",
            _require_bounded_text(self.principal, "principal", _MAX_ID_CHARS),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _require_bounded_text(self.idempotency_key, "idempotency_key", _MAX_ID_CHARS),
        )
        digest = self.request_digest.strip().lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("request_digest must be a 64-character hex SHA-256")
        object.__setattr__(self, "request_digest", digest)
        object.__setattr__(self, "status", MissionRunStatus(self.status))
        object.__setattr__(self, "cleanup_state", MissionRunCleanupState(self.cleanup_state))
        if self.accepted_at_ms < 0:
            raise ValueError("accepted_at_ms must be non-negative")
        if self.running_at_ms is not None and self.running_at_ms < 0:
            raise ValueError("running_at_ms must be non-negative")
        if self.terminal_at_ms is not None and self.terminal_at_ms < 0:
            raise ValueError("terminal_at_ms must be non-negative")
        if self.updated_at_ms < 0:
            raise ValueError("updated_at_ms must be non-negative")
        if self.updated_at_ms == 0:
            object.__setattr__(self, "updated_at_ms", self.accepted_at_ms)
        if self.cancellation_reason and len(self.cancellation_reason) > _MAX_REASON_CHARS:
            raise ValueError("cancellation_reason exceeds its bound")
        if self.interrupted_reason and len(self.interrupted_reason) > _MAX_REASON_CHARS:
            raise ValueError("interrupted_reason exceeds its bound")
        if self.status in TERMINAL_MISSION_RUN_STATUSES and self.terminal_at_ms is None:
            raise ValueError("terminal mission runs require terminal_at_ms")
        if self.status is MissionRunStatus.SUCCEEDED:
            if self.result is None or self.result.status != "succeeded":
                raise ValueError(
                    "a succeeded MissionRun requires an independent succeeded MissionResult"
                )

    @property
    def terminal(self) -> bool:
        """Whether execution has reached an immutable outcome."""

        return self.status in TERMINAL_MISSION_RUN_STATUSES


@dataclass(frozen=True)
class MissionRunEvent:
    """One durable ordered progress fact for a MissionRun.

    Identity is ``(run_id, cursor)``; the cursor is a contiguous run-local
    sequence assigned in the same transaction as the durable transition, so
    ``after`` replay has no gaps, reordering, or duplicate logical events.
    """

    run_id: str
    cursor: int
    event_type: str
    phase: str
    payload_json: str
    created_at_ms: int
    schema_version: int = MISSION_RUN_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            _require_bounded_text(self.run_id, "run_id", _MAX_ID_CHARS),
        )
        if isinstance(self.cursor, bool) or not isinstance(self.cursor, int) or self.cursor < 1:
            raise ValueError("event cursor must be a positive integer")
        if self.event_type not in MISSION_RUN_EVENT_PHASES:
            raise ValueError(f"unknown mission-run event type {self.event_type!r}")
        expected_phase = MISSION_RUN_EVENT_PHASES[self.event_type]
        if self.phase != expected_phase:
            raise ValueError(f"event {self.event_type!r} belongs to phase {expected_phase!r}")
        if not isinstance(self.payload_json, str):
            raise TypeError("payload_json must be a string")
        if len(self.payload_json) > _MAX_EVENT_PAYLOAD_CHARS:
            raise ValueError("event payload exceeds its bound")
        if self.created_at_ms < 0:
            raise ValueError("created_at_ms must be non-negative")
        if self.schema_version != MISSION_RUN_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported mission-run event schema version")

    @property
    def event_id(self) -> str:
        """Deterministic event identity derived from run and cursor."""

        return f"{self.run_id}/{self.cursor}"

    @property
    def payload(self) -> dict[str, object]:
        """Decode the sanitized bounded payload."""

        decoded = json.loads(self.payload_json)
        if not isinstance(decoded, dict):
            raise ValueError("event payload must be an object")
        return {str(key): value for key, value in decoded.items()}


def execution_profile_identity(config: AgentMissionConfig) -> ExecutionProfileIdentity:
    """Derive the inspectable profile identity of one process-bound config."""

    payload = {
        "backend": str(getattr(config.sandbox_backend, "name", "") or ""),
        "checkpoint_after_dispatch": config.checkpoint_after_dispatch,
        "critic_workspace": config.critic_workspace,
        "max_ticks": config.max_ticks,
        "model": config.model,
        "profile_id": DEFAULT_EXECUTION_PROFILE_ID,
        "sandbox_environment": config.sandbox_environment,
        "version": DEFAULT_EXECUTION_PROFILE_VERSION,
        "workspace": config.workspace,
    }
    digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    return ExecutionProfileIdentity(
        profile_id=DEFAULT_EXECUTION_PROFILE_ID,
        version=DEFAULT_EXECUTION_PROFILE_VERSION,
        digest=digest,
    )


def mission_request_digest(
    submission: MissionSubmission,
    profile: ExecutionProfileIdentity,
) -> str:
    """Return the canonical identity of one requested mission plus profile."""

    payload = {
        "kind": "archetype.missions.run-request",
        "profile": {
            "digest": profile.digest,
            "profile_id": profile.profile_id,
            "version": profile.version,
        },
        "schema_version": 1,
        "submission": submission_payload(submission),
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


# One canonical codec instead of a hand-rolled field-by-field mirror. Strict
# JSON validation keeps the exact type-guard semantics on decode, and
# ``dump_python(mode="json")`` reproduces the previous canonical payloads
# byte-identically under the sorted-key digest encoding below.
_SUBMISSION_ADAPTER: TypeAdapter[MissionSubmission] = TypeAdapter(MissionSubmission)
_RESULT_ADAPTER: TypeAdapter[MissionResult] = TypeAdapter(MissionResult)
_TASK_IDS_ADAPTER: TypeAdapter[tuple[tuple[str, int], ...]] = TypeAdapter(
    tuple[tuple[str, int], ...]
)


def submission_payload(submission: MissionSubmission) -> dict[str, object]:
    """Return a canonical JSON-native encoding of one mission submission."""

    return _SUBMISSION_ADAPTER.dump_python(submission, mode="json")


def submission_from_json(value: str) -> MissionSubmission:
    """Rehydrate one mission submission from its canonical JSON encoding."""

    return _SUBMISSION_ADAPTER.validate_json(value, strict=True)


def mission_result_payload(result: MissionResult) -> dict[str, object]:
    """Return a JSON-native encoding of one terminal mission projection."""

    return _RESULT_ADAPTER.dump_python(result, mode="json")


def mission_result_from_json(value: str) -> MissionResult:
    """Rehydrate one terminal mission projection from its JSON encoding."""

    return _RESULT_ADAPTER.validate_json(value, strict=True)


def task_ids_payload(task_ids: tuple[tuple[str, int], ...]) -> list[object]:
    """Return a JSON-native encoding of the bound task identity pairs."""

    return _TASK_IDS_ADAPTER.dump_python(task_ids, mode="json")


def task_ids_from_json(value: str) -> tuple[tuple[str, int], ...]:
    """Rehydrate the bound task identity pairs from their JSON encoding."""

    return _TASK_IDS_ADAPTER.validate_json(value, strict=True)


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


__all__ = [
    "DEFAULT_EXECUTION_PROFILE_ID",
    "DEFAULT_EXECUTION_PROFILE_VERSION",
    "MISSION_RUN_EVENT_MAX_PAGE",
    "MISSION_RUN_EVENT_PHASES",
    "MISSION_RUN_EVENT_SCHEMA_VERSION",
    "MISSION_RUN_TRANSITIONS",
    "TERMINAL_MISSION_RUN_STATUSES",
    "ExecutionProfileIdentity",
    "MissionRun",
    "MissionRunCleanupState",
    "MissionRunConflictError",
    "MissionRunEvent",
    "MissionRunNotFoundError",
    "MissionRunRequest",
    "MissionRunStatus",
    "execution_profile_identity",
    "mission_request_digest",
    "mission_result_from_json",
    "mission_result_payload",
    "require_mission_run_transition",
    "submission_from_json",
    "submission_payload",
    "task_ids_from_json",
    "task_ids_payload",
]
