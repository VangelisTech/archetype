# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Temporal-backed durable orchestration for Agent Missions."""

from typing import TYPE_CHECKING

from .client import MissionTemporalClient
from .contracts import (
    MISSION_MODAL_JOB_TASK_QUEUE,
    MISSION_TASK_QUEUE,
    MissionJobValueRef,
    MissionModalJobRefPayload,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
    MissionWorkflowEvent,
    MissionWorkflowInput,
    MissionWorkflowState,
    mission_modal_job_workflow_id,
    mission_workflow_id,
)
from .modal_job_worker import create_mission_modal_job_worker
from .modal_job_workflow import MissionModalJobWorkflow
from .workflow import MissionWorkflow

if TYPE_CHECKING:
    from .worker import create_mission_worker


def __getattr__(name: str) -> object:
    """Keep the legacy Worker available without importing it on split-worker startup."""

    if name == "create_mission_worker":
        from .worker import create_mission_worker

        return create_mission_worker
    raise AttributeError(name)


__all__ = [
    "MISSION_TASK_QUEUE",
    "MISSION_MODAL_JOB_TASK_QUEUE",
    "MissionTemporalClient",
    "MissionJobValueRef",
    "MissionModalJobRefPayload",
    "MissionModalJobWorkflow",
    "MissionModalJobWorkflowInput",
    "MissionModalJobWorkflowState",
    "MissionWorkflow",
    "MissionWorkflowEvent",
    "MissionWorkflowInput",
    "MissionWorkflowState",
    "create_mission_modal_job_worker",
    "create_mission_worker",
    "mission_modal_job_workflow_id",
    "mission_workflow_id",
]
