# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""X0-facing Temporal client facade for durable MissionRun admission."""

from __future__ import annotations

import json
import time
from typing import cast

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from archetype.missions.run_contracts import (
    ExecutionProfileIdentity,
    MissionRunConflictError,
    MissionRunRequest,
    mission_request_digest,
    submission_payload,
)

from .contracts import (
    MISSION_TASK_QUEUE,
    MISSION_WORKFLOW_NAME,
    MissionWorkflowInput,
    MissionWorkflowState,
)
from .contracts import mission_workflow_id as derive_workflow_id
from .workflow import MissionWorkflow


class MissionTemporalClient:
    """Admit idempotent Mission Workflows through an existing Temporal client."""

    def __init__(self, client: Client, *, task_queue: str = MISSION_TASK_QUEUE) -> None:
        if not task_queue.strip():
            raise ValueError("Temporal mission task_queue must not be empty")
        self._client = client
        self._task_queue = task_queue

    async def start(
        self,
        request: MissionRunRequest,
        profile: ExecutionProfileIdentity,
        *,
        start_paused: bool = False,
    ) -> WorkflowHandle[MissionWorkflow, MissionWorkflowState]:
        run_id = derive_workflow_id(request.principal, request.idempotency_key)
        now_ms = int(time.time() * 1000)
        command = MissionWorkflowInput(
            run_id=run_id,
            world_id=f"world-{run_id}",
            principal=request.principal,
            idempotency_key=request.idempotency_key,
            request_digest=mission_request_digest(request.submission, profile),
            profile_id=profile.profile_id,
            profile_version=profile.version,
            profile_digest=profile.digest,
            submission_json=json.dumps(
                submission_payload(request.submission),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
            accepted_at_ms=now_ms,
            start_paused=start_paused,
        )
        try:
            handle = cast(
                WorkflowHandle[MissionWorkflow, MissionWorkflowState],
                await self._client.start_workflow(
                    MISSION_WORKFLOW_NAME,
                    command,
                    id=run_id,
                    task_queue=self._task_queue,
                    result_type=MissionWorkflowState,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                    static_summary=f"Agent Mission {request.submission.name}",
                ),
            )
        except WorkflowAlreadyStartedError:
            handle = self._client.get_workflow_handle(
                run_id,
                result_type=MissionWorkflowState,
            )
        observed_digest = await handle.query(MissionWorkflow.request_digest)
        if observed_digest != command.request_digest:
            raise MissionRunConflictError(
                "idempotency key reused with a different canonical mission request"
            )
        return handle

    def get(self, run_id: str) -> WorkflowHandle[MissionWorkflow, MissionWorkflowState]:
        return self._client.get_workflow_handle(
            run_id,
            result_type=MissionWorkflowState,
        )

    @property
    def client(self) -> Client:
        """Return the host-owned Temporal connection for Worker composition."""

        return self._client

    @property
    def task_queue(self) -> str:
        """Return the queue shared by Mission Workflows and their Activities."""

        return self._task_queue

    async def list_for_principal(
        self,
        principal: str,
        *,
        limit: int,
    ) -> tuple[WorkflowHandle[MissionWorkflow, MissionWorkflowState], ...]:
        """Return Mission Workflows through Temporal Visibility, never SQLite."""

        query = f"WorkflowType = '{MISSION_WORKFLOW_NAME}'"
        handles: list[WorkflowHandle[MissionWorkflow, MissionWorkflowState]] = []
        # This is intentionally the simple initial Visibility path.  If the
        # control plane needs indexed principal lookups at scale, provision a
        # Temporal Search Attribute there; do not recreate an SQLite index.
        async for execution in self._client.list_workflows(query=query):
            handle = self._client.get_workflow_handle(
                execution.id,
                run_id=execution.run_id,
                result_type=MissionWorkflowState,
            )
            state = await handle.query(MissionWorkflow.state)
            if state is not None and state.principal == principal:
                handles.append(handle)
                if len(handles) >= limit:
                    break
        return tuple(handles)


__all__ = ["MissionTemporalClient"]
