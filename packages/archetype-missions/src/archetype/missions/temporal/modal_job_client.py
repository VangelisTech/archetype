# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Idempotent client admission for provider-native Mission job Workflows."""

from __future__ import annotations

from typing import Protocol, cast, runtime_checkable

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from archetype.errors import ConflictError

from .contracts import (
    MISSION_MODAL_JOB_TASK_QUEUE,
    MISSION_MODAL_JOB_WORKFLOW_NAME,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
    mission_modal_job_workflow_id,
)
from .modal_job_workflow import MissionModalJobWorkflow


@runtime_checkable
class MissionModalJobWorkflowHandle(Protocol):
    """Small handle surface consumed by the ECS-to-Workflow bridge."""

    async def result(self) -> MissionModalJobWorkflowState: ...


@runtime_checkable
class MissionModalJobWorkflowLauncher(Protocol):
    """Admit or recover one exact provider-job Workflow."""

    async def start(
        self,
        command: MissionModalJobWorkflowInput,
    ) -> MissionModalJobWorkflowHandle: ...


class MissionModalJobTemporalClient:
    """Start the deterministic Workflow and reject immutable-input conflicts."""

    def __init__(
        self,
        client: Client,
        *,
        task_queue: str = MISSION_MODAL_JOB_TASK_QUEUE,
    ) -> None:
        if not task_queue.strip():
            raise ValueError("Temporal Modal Mission task_queue must not be empty")
        self._client = client
        self._task_queue = task_queue

    async def start(
        self,
        command: MissionModalJobWorkflowInput,
    ) -> WorkflowHandle[MissionModalJobWorkflow, MissionModalJobWorkflowState]:
        workflow_id = mission_modal_job_workflow_id(
            command.family,
            command.operation_id,
            command.namespace_digest,
        )
        try:
            handle = cast(
                WorkflowHandle[MissionModalJobWorkflow, MissionModalJobWorkflowState],
                await self._client.start_workflow(
                    MISSION_MODAL_JOB_WORKFLOW_NAME,
                    command,
                    id=workflow_id,
                    task_queue=self._task_queue,
                    result_type=MissionModalJobWorkflowState,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                    static_summary=(
                        f"Mission {command.family} provider job {command.operation_id}"
                    ),
                ),
            )
        except WorkflowAlreadyStartedError:
            handle = self._client.get_workflow_handle(
                workflow_id,
                result_type=MissionModalJobWorkflowState,
            )
        observed_digest = await handle.query(MissionModalJobWorkflow.request_digest)
        if observed_digest != command.request.digest:
            raise ConflictError("Modal Mission Workflow identity has another canonical request")
        return handle


__all__ = [
    "MissionModalJobTemporalClient",
    "MissionModalJobWorkflowHandle",
    "MissionModalJobWorkflowLauncher",
]
