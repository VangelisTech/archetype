# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Serialization-safe contracts for Temporal-owned MissionRun orchestration.

Temporal persists these values in Workflow history.  Keep them deliberately
small and JSON-native: Archetype domain values cross the boundary as canonical
JSON rather than importing the complete ECS model into deterministic Workflow
code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from archetype.orchestration.temporal import durable_workflow_id

MISSION_WORKFLOW_NAME = "archetype.missions.MissionWorkflow"
MISSION_SUBMIT_ACTIVITY = "archetype.missions.submit"
MISSION_EXECUTE_ACTIVITY = "archetype.missions.execute"
MISSION_TASK_QUEUE = "archetype-missions"
MISSION_MODAL_JOB_WORKFLOW_NAME = "archetype.missions.ModalJobWorkflow"
MISSION_MODAL_JOB_START_ACTIVITY = "archetype.missions.modal-job.start"
MISSION_MODAL_JOB_POLL_ACTIVITY = "archetype.missions.modal-job.poll"
MISSION_MODAL_JOB_COLLECT_ACTIVITY = "archetype.missions.modal-job.collect"
MISSION_MODAL_JOB_CANCEL_ACTIVITY = "archetype.missions.modal-job.cancel"
MISSION_MODAL_JOB_CLEANUP_ACTIVITY = "archetype.missions.modal-job.cleanup"

MissionModalJobFamily = Literal["author", "critic"]
MissionModalJobStatus = Literal["running", "ready", "unknown"]


def mission_workflow_id(principal: str, idempotency_key: str) -> str:
    """Derive one stable Workflow ID from the caller's idempotency identity."""

    return durable_workflow_id(
        "archetype.missions.workflow",
        principal,
        idempotency_key,
        prefix="mission",
    )


def mission_modal_job_workflow_id(
    family: MissionModalJobFamily,
    operation_id: str,
    namespace_digest: str,
) -> str:
    """Derive one stable provider-job Workflow ID from exact authority."""

    return durable_workflow_id(
        "archetype.missions.modal-job",
        family,
        f"{operation_id}:{namespace_digest}",
        prefix="mission-job",
    )


@dataclass(frozen=True, slots=True)
class MissionWorkflowInput:
    """Canonical input recorded once at Workflow admission."""

    run_id: str
    world_id: str
    principal: str
    idempotency_key: str
    request_digest: str
    profile_id: str
    profile_version: str
    profile_digest: str
    submission_json: str
    accepted_at_ms: int
    start_paused: bool = False


@dataclass(frozen=True, slots=True)
class SubmittedMissionPayload:
    """Canonical submitted-mission identity returned by the admission Activity."""

    submitted_json: str


@dataclass(frozen=True, slots=True)
class ExecuteMissionInput:
    """Input for the effectful mission execution Activity."""

    mission: MissionWorkflowInput
    submitted_json: str


@dataclass(frozen=True, slots=True)
class MissionExecutionPayload:
    """Canonical terminal domain projection returned by the execution Activity."""

    status: str
    result_json: str


@dataclass(frozen=True, slots=True)
class MissionWorkflowEvent:
    """Small operational event projected from durable Workflow state."""

    cursor: int
    event_type: str
    phase: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class MissionWorkflowState:
    """Inspectable Workflow state used by the X0 control-plane facade."""

    run_id: str
    world_id: str
    principal: str
    idempotency_key: str
    request_digest: str
    status: str
    active_operation: str = ""
    submitted_json: str = ""
    result_json: str = ""
    cancellation_requested: bool = False
    cancellation_reason: str = ""
    failure_reason: str = ""


@dataclass(frozen=True, slots=True)
class MissionJobValueRef:
    """Bounded external value identity safe to retain in Workflow history."""

    ref: str
    digest: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.ref.strip() or len(self.ref) > 4096:
            raise ValueError("Mission value reference is invalid")
        if len(self.digest) != 64 or any(
            character not in "0123456789abcdef" for character in self.digest
        ):
            raise ValueError("Mission value digest is invalid")
        if self.size_bytes < 1 or self.size_bytes > 1 << 20:
            raise ValueError("Mission value size is outside its durability bound")


@dataclass(frozen=True, slots=True)
class MissionModalJobRefPayload:
    """JSON-native durable Modal call identity."""

    family: MissionModalJobFamily
    operation_id: str
    request_digest: str
    namespace_digest: str
    call_id: str

    def __post_init__(self) -> None:
        if self.family not in {"author", "critic"}:
            raise ValueError("Modal Mission job family is invalid")
        if not self.operation_id.strip() or not self.call_id.strip():
            raise ValueError("Modal Mission job identity is incomplete")
        for label, value in (
            ("request_digest", self.request_digest),
            ("namespace_digest", self.namespace_digest),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"Modal Mission {label} is invalid")


@dataclass(frozen=True, slots=True)
class MissionModalJobWorkflowInput:
    """One provider job supervised without embedding canonical request bytes."""

    family: MissionModalJobFamily
    operation_id: str
    request: MissionJobValueRef
    namespace_digest: str
    poll_interval_seconds: int = 5
    polls_per_run: int = 64
    ref: MissionModalJobRefPayload | None = None
    poll_cursor: int = 0

    def __post_init__(self) -> None:
        if self.family not in {"author", "critic"} or not self.operation_id.strip():
            raise ValueError("Modal Mission Workflow identity is invalid")
        if len(self.namespace_digest) != 64 or any(
            character not in "0123456789abcdef" for character in self.namespace_digest
        ):
            raise ValueError("Modal Mission Workflow namespace digest is invalid")
        if self.poll_interval_seconds < 1 or self.poll_interval_seconds > 3600:
            raise ValueError("Modal Mission poll interval is outside its bound")
        if self.polls_per_run < 1 or self.polls_per_run > 256:
            raise ValueError("Modal Mission polls-per-run is outside its bound")
        if self.poll_cursor < 0:
            raise ValueError("Modal Mission poll cursor cannot be negative")
        if self.ref is not None and (
            self.ref.family != self.family
            or self.ref.operation_id != self.operation_id
            or self.ref.request_digest != self.request.digest
            or self.ref.namespace_digest != self.namespace_digest
        ):
            raise ValueError("Modal Mission Workflow ref conflicts with its immutable input")


@dataclass(frozen=True, slots=True)
class MissionModalJobPhaseInput:
    """Ref-only input shared by poll, collect, cancel, and cleanup Activities."""

    job: MissionModalJobWorkflowInput
    ref: MissionModalJobRefPayload


@dataclass(frozen=True, slots=True)
class MissionModalJobPhaseResult:
    """Durable phase observation; Unknown is terminal and never replay authority."""

    status: MissionModalJobStatus
    ref: MissionModalJobRefPayload | None = None
    reason: str = ""


@dataclass(frozen=True, slots=True)
class MissionModalJobCollection:
    """Exact family result reference returned by read-only collection."""

    status: Literal["ready", "unknown"]
    ref: MissionModalJobRefPayload
    result: MissionJobValueRef | None = None
    reason: str = ""


@dataclass(frozen=True, slots=True)
class MissionModalJobWorkflowState:
    """Bounded inspectable state for one provider-native Mission job."""

    family: MissionModalJobFamily
    operation_id: str
    request_digest: str
    status: str
    ref: MissionModalJobRefPayload | None = None
    result: MissionJobValueRef | None = None
    poll_cursor: int = 0
    cancellation_requested: bool = False
    cancellation_reason: str = ""
    failure_reason: str = ""


__all__ = [
    "ExecuteMissionInput",
    "MISSION_EXECUTE_ACTIVITY",
    "MISSION_MODAL_JOB_CANCEL_ACTIVITY",
    "MISSION_MODAL_JOB_CLEANUP_ACTIVITY",
    "MISSION_MODAL_JOB_COLLECT_ACTIVITY",
    "MISSION_MODAL_JOB_POLL_ACTIVITY",
    "MISSION_MODAL_JOB_START_ACTIVITY",
    "MISSION_MODAL_JOB_WORKFLOW_NAME",
    "MISSION_SUBMIT_ACTIVITY",
    "MISSION_TASK_QUEUE",
    "MISSION_WORKFLOW_NAME",
    "MissionExecutionPayload",
    "MissionJobValueRef",
    "MissionModalJobCollection",
    "MissionModalJobFamily",
    "MissionModalJobPhaseInput",
    "MissionModalJobPhaseResult",
    "MissionModalJobRefPayload",
    "MissionModalJobStatus",
    "MissionModalJobWorkflowInput",
    "MissionModalJobWorkflowState",
    "MissionWorkflowEvent",
    "MissionWorkflowInput",
    "MissionWorkflowState",
    "SubmittedMissionPayload",
    "mission_workflow_id",
    "mission_modal_job_workflow_id",
]
