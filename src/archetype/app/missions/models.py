# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persisted mission state and provider-neutral attempt requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import field_validator, model_validator

from archetype.app.missions.transitions import (
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
    idempotency_key: str
    previous_session_id: str
    previous_validator_details: tuple[dict[str, Any], ...]
    correlation: dict[str, Any]
    source: MissionTaskState


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
