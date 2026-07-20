# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small persisted status vocabularies and legal Agent Missions transitions."""

from enum import StrEnum
from types import MappingProxyType


class MissionStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TaskStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    ACCEPTED = "accepted"
    FAILED = "failed"


class AgentExecutionStatus(StrEnum):
    """Factual lifecycle of one agent process and its repository harness."""

    STARTING = "starting"
    RUNNING = "running"
    EXITED = "exited"
    ERRORED = "errored"
    INTERRUPTED = "interrupted"


MISSION_TRANSITIONS = MappingProxyType(
    {
        MissionStatus.RUNNING: frozenset({MissionStatus.SUCCEEDED, MissionStatus.FAILED}),
        MissionStatus.SUCCEEDED: frozenset(),
        MissionStatus.FAILED: frozenset(),
    }
)

TASK_TRANSITIONS = MappingProxyType(
    {
        TaskStatus.PENDING: frozenset({TaskStatus.READY}),
        TaskStatus.READY: frozenset({TaskStatus.DISPATCHED}),
        TaskStatus.DISPATCHED: frozenset(
            {TaskStatus.READY, TaskStatus.ACCEPTED, TaskStatus.FAILED}
        ),
        TaskStatus.ACCEPTED: frozenset(),
        TaskStatus.FAILED: frozenset(),
    }
)


def require_mission_transition(source: MissionStatus | str, target: MissionStatus | str) -> None:
    """Reject a mission-state edge absent from the V1 graph."""

    parsed_source = MissionStatus(source)
    parsed_target = MissionStatus(target)
    if parsed_target not in MISSION_TRANSITIONS[parsed_source]:
        raise ValueError(
            f"illegal mission transition: {parsed_source.value} to {parsed_target.value}"
        )


def require_task_transition(source: TaskStatus | str, target: TaskStatus | str) -> None:
    """Reject a task-state edge absent from the V1 graph."""

    parsed_source = TaskStatus(source)
    parsed_target = TaskStatus(target)
    if parsed_target not in TASK_TRANSITIONS[parsed_source]:
        raise ValueError(f"illegal task transition: {parsed_source.value} to {parsed_target.value}")
