# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Temporal Workflow that replaces process-local MissionRun supervision."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from .contracts import (
    MISSION_EXECUTE_ACTIVITY,
    MISSION_SUBMIT_ACTIVITY,
    MISSION_WORKFLOW_NAME,
    ExecuteMissionInput,
    MissionExecutionPayload,
    MissionWorkflowEvent,
    MissionWorkflowInput,
    MissionWorkflowState,
    SubmittedMissionPayload,
)

_MAX_REASON_CHARS = 4096


@workflow.defn(name=MISSION_WORKFLOW_NAME)
class MissionWorkflow:
    """Durably admit and execute one Agent Mission.

    The Workflow owns orchestration facts only.  Mission/task state and agent
    observations remain Archetype ECS facts written by Activities.
    """

    def __init__(self) -> None:
        self._state: MissionWorkflowState | None = None
        self._events: tuple[MissionWorkflowEvent, ...] = ()
        self._release_execution = False

    @workflow.run
    async def run(self, command: MissionWorkflowInput) -> MissionWorkflowState:
        self._state = MissionWorkflowState(
            run_id=command.run_id,
            world_id=command.world_id,
            principal=command.principal,
            idempotency_key=command.idempotency_key,
            request_digest=command.request_digest,
            profile_id=command.profile_id,
            profile_version=command.profile_version,
            profile_digest=command.profile_digest,
            status="accepted",
            submission_json=command.submission_json,
        )
        self._record("accepted", "admission")
        try:
            self._replace(status="running", active_operation="submit_mission")
            self._record("running", "execution")
            submitted = await workflow.execute_activity(
                MISSION_SUBMIT_ACTIVITY,
                command,
                result_type=SubmittedMissionPayload,
                start_to_close_timeout=timedelta(minutes=15),
                retry_policy=RetryPolicy(maximum_attempts=3),
                activity_id=f"{command.run_id}:submit",
            )
            self._replace(
                active_operation="await_execution",
                submitted_json=submitted.submitted_json,
            )
            self._record("mission_bound", "execution")

            if command.start_paused:
                await workflow.wait_condition(
                    lambda: (
                        self._release_execution
                        or bool(self._state and self._state.cancellation_requested)
                    )
                )
            if self._state is not None and self._state.cancellation_requested:
                self._replace(status="cancelled", active_operation="")
                self._record("cancelled", "terminal")
                return self._require_state()

            self._replace(status="running", active_operation="run_mission")
            result = await workflow.execute_activity(
                MISSION_EXECUTE_ACTIVITY,
                ExecuteMissionInput(
                    mission=command,
                    submitted_json=submitted.submitted_json,
                ),
                result_type=MissionExecutionPayload,
                start_to_close_timeout=timedelta(hours=24),
                heartbeat_timeout=timedelta(minutes=1),
                # The transition adapter still enters the legacy MissionService
                # once.  Do not retry that non-atomic call until Modal execution
                # is split into start/poll Activities with a durable provider ID.
                retry_policy=RetryPolicy(maximum_attempts=1),
                activity_id=f"{command.run_id}:execute",
            )
        except ActivityError as exc:
            self._replace(
                status="failed",
                active_operation="",
                failure_reason=str(exc)[:_MAX_REASON_CHARS],
            )
            self._record("failed", "terminal")
            return self._require_state()

        if self._state is not None and self._state.cancellation_requested:
            self._replace(
                status="cancelled",
                active_operation="",
                result_json=result.result_json,
            )
            self._record("cancelled", "terminal")
            return self._require_state()
        status = "succeeded" if result.status == "succeeded" else "failed"
        self._replace(status=status, active_operation="", result_json=result.result_json)
        self._record(status, "terminal")
        return self._require_state()

    @workflow.signal
    def request_cancel(self, reason: str = "") -> None:
        state = self._state
        if state is None or state.status in {"succeeded", "failed", "cancelled"}:
            return
        bounded_reason = reason[:_MAX_REASON_CHARS]
        if state.cancellation_requested:
            if state.cancellation_reason != bounded_reason:
                self._replace(cancellation_reason=bounded_reason)
            return
        self._replace(
            status="cancelling",
            cancellation_requested=True,
            cancellation_reason=bounded_reason,
        )
        self._record("cancel_requested", "cancellation")
        self._record("cancelling", "cancellation")

    @workflow.signal
    def release_execution(self) -> None:
        """Release an intentionally paused run; useful for governed recovery tests."""

        self._release_execution = True

    @workflow.query
    def state(self) -> MissionWorkflowState | None:
        return self._state

    @workflow.query
    def events(self) -> tuple[MissionWorkflowEvent, ...]:
        return self._events

    @workflow.query
    def request_digest(self) -> str:
        return self._state.request_digest if self._state is not None else ""

    def _replace(self, **changes: object) -> None:
        state = self._require_state()
        values = {field: getattr(state, field) for field in state.__dataclass_fields__}
        values.update(changes)
        self._state = MissionWorkflowState(**values)

    def _record(self, event_type: str, phase: str) -> None:
        self._events = (
            *self._events,
            MissionWorkflowEvent(
                cursor=len(self._events) + 1,
                event_type=event_type,
                phase=phase,
                created_at_ms=int(workflow.now().timestamp() * 1000),
            ),
        )

    def _require_state(self) -> MissionWorkflowState:
        if self._state is None:
            raise RuntimeError("Mission Workflow state is not initialized")
        return self._state


__all__ = ["MissionWorkflow"]
