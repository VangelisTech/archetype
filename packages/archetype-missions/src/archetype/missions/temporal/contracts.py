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

from archetype.activities.temporal import durable_workflow_id

MISSION_WORKFLOW_NAME = "archetype.missions.MissionWorkflow"
MISSION_SUBMIT_ACTIVITY = "archetype.missions.submit"
MISSION_EXECUTE_ACTIVITY = "archetype.missions.execute"
MISSION_TASK_QUEUE = "archetype-missions"


def mission_workflow_id(principal: str, idempotency_key: str) -> str:
    """Derive one stable Workflow ID from the caller's idempotency identity."""

    return durable_workflow_id(
        "archetype.missions.workflow",
        principal,
        idempotency_key,
        prefix="mission",
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


__all__ = [
    "ExecuteMissionInput",
    "MISSION_EXECUTE_ACTIVITY",
    "MISSION_SUBMIT_ACTIVITY",
    "MISSION_TASK_QUEUE",
    "MISSION_WORKFLOW_NAME",
    "MissionExecutionPayload",
    "MissionWorkflowEvent",
    "MissionWorkflowInput",
    "MissionWorkflowState",
    "SubmittedMissionPayload",
    "mission_workflow_id",
]
