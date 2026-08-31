# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bounded Temporal supervision for one provider-native Mission job."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from .contracts import (
    MISSION_MODAL_JOB_CANCEL_ACTIVITY,
    MISSION_MODAL_JOB_CLEANUP_ACTIVITY,
    MISSION_MODAL_JOB_COLLECT_ACTIVITY,
    MISSION_MODAL_JOB_POLL_ACTIVITY,
    MISSION_MODAL_JOB_START_ACTIVITY,
    MISSION_MODAL_JOB_WORKFLOW_NAME,
    MissionModalJobCollection,
    MissionModalJobPhaseInput,
    MissionModalJobPhaseResult,
    MissionModalJobRefPayload,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
)

_MAX_REASON_CHARS = 4096
_OBSERVE_RETRY = RetryPolicy(maximum_attempts=3)
_EFFECT_RETRY = RetryPolicy(maximum_attempts=10)


@workflow.defn(name=MISSION_MODAL_JOB_WORKFLOW_NAME)
class MissionModalJobWorkflow:
    """Start once, then durably poll, collect, cancel, and clean one job."""

    def __init__(self) -> None:
        self._state: MissionModalJobWorkflowState | None = None
        self._cancel_requested = False
        self._cancel_reason = ""

    @workflow.run
    async def run(
        self,
        command: MissionModalJobWorkflowInput,
    ) -> MissionModalJobWorkflowState:
        self._state = MissionModalJobWorkflowState(
            family=command.family,
            operation_id=command.operation_id,
            request_digest=command.request.digest,
            status="accepted",
            ref=command.ref,
            poll_cursor=command.poll_cursor,
        )
        phase = "start"
        ref = command.ref
        try:
            if ref is None:
                self._replace(status="starting")
                started = await workflow.execute_activity(
                    MISSION_MODAL_JOB_START_ACTIVITY,
                    command,
                    result_type=MissionModalJobPhaseResult,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=_OBSERVE_RETRY,
                    activity_id=f"{command.operation_id}:start",
                )
                if started.status == "unknown" or started.ref is None:
                    self._replace(
                        status="unknown",
                        failure_reason=_reason(started.reason or "start returned no durable call"),
                    )
                    return self._require_state()
                ref = started.ref
                self._replace(status="running", ref=ref)

            polls_this_run = 0
            while True:
                phase_input = MissionModalJobPhaseInput(job=command, ref=ref)
                if self._cancel_requested:
                    phase = "cancel"
                    self._replace(
                        status="cancelling",
                        cancellation_requested=True,
                        cancellation_reason=self._cancel_reason,
                    )
                    await workflow.execute_activity(
                        MISSION_MODAL_JOB_CANCEL_ACTIVITY,
                        phase_input,
                        result_type=MissionModalJobPhaseResult,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=_EFFECT_RETRY,
                        activity_id=f"{command.operation_id}:cancel",
                    )
                    phase = "cleanup"
                    await self._cleanup(command, ref)
                    self._replace(status="cancelled")
                    return self._require_state()

                phase = "poll"
                cursor = self._require_state().poll_cursor + 1
                polled = await workflow.execute_activity(
                    MISSION_MODAL_JOB_POLL_ACTIVITY,
                    phase_input,
                    result_type=MissionModalJobPhaseResult,
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=_OBSERVE_RETRY,
                    activity_id=f"{command.operation_id}:poll:{cursor}",
                )
                self._replace(poll_cursor=cursor)
                polls_this_run += 1
                if polled.status == "unknown":
                    self._replace(status="unknown", failure_reason=_reason(polled.reason))
                    phase = "cleanup"
                    await self._cleanup(command, ref)
                    return self._require_state()
                if polled.status == "ready":
                    phase = "collect"
                    self._replace(status="collecting")
                    collected = await workflow.execute_activity(
                        MISSION_MODAL_JOB_COLLECT_ACTIVITY,
                        phase_input,
                        result_type=MissionModalJobCollection,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=_OBSERVE_RETRY,
                        activity_id=f"{command.operation_id}:collect",
                    )
                    if collected.status == "unknown" or collected.result is None:
                        self._replace(
                            status="unknown",
                            failure_reason=_reason(
                                collected.reason or "collect returned no durable result"
                            ),
                        )
                        phase = "cleanup"
                        await self._cleanup(command, ref)
                        return self._require_state()
                    self._replace(result=collected.result, status="cleaning")
                    phase = "cleanup"
                    await self._cleanup(command, ref)
                    self._replace(status="succeeded")
                    return self._require_state()

                self._replace(status="running")
                # Signals can arrive while the poll Activity is running.  Handle
                # them before Continue-As-New so cancellation authority is not
                # lost at the history boundary.
                if self._cancel_requested:
                    continue
                if polls_this_run >= command.polls_per_run:
                    workflow.continue_as_new(
                        replace(
                            command,
                            ref=ref,
                            poll_cursor=cursor,
                        )
                    )
                await workflow.sleep(timedelta(seconds=command.poll_interval_seconds))
        except ActivityError:
            self._replace(
                status="unknown",
                failure_reason=f"{phase} Activity failed",
            )
            return self._require_state()

    @workflow.signal
    def request_cancel(self, reason: str = "") -> None:
        if self._state is None or self._state.status in {
            "succeeded",
            "cancelled",
            "unknown",
        }:
            return
        self._cancel_requested = True
        self._cancel_reason = _reason(reason)
        self._replace(
            cancellation_requested=True,
            cancellation_reason=self._cancel_reason,
        )

    @workflow.query
    def state(self) -> MissionModalJobWorkflowState | None:
        return self._state

    @workflow.query
    def request_digest(self) -> str:
        return self._state.request_digest if self._state is not None else ""

    async def _cleanup(
        self,
        command: MissionModalJobWorkflowInput,
        ref: MissionModalJobRefPayload,
    ) -> None:
        await workflow.execute_activity(
            MISSION_MODAL_JOB_CLEANUP_ACTIVITY,
            MissionModalJobPhaseInput(job=command, ref=ref),
            result_type=MissionModalJobPhaseResult,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_EFFECT_RETRY,
            activity_id=f"{command.operation_id}:cleanup",
        )

    def _replace(self, **changes: object) -> None:
        state = self._require_state()
        values = {field: getattr(state, field) for field in state.__dataclass_fields__}
        values.update(changes)
        self._state = MissionModalJobWorkflowState(**values)

    def _require_state(self) -> MissionModalJobWorkflowState:
        if self._state is None:
            raise RuntimeError("Modal Mission job Workflow state is not initialized")
        return self._state


def _reason(value: str) -> str:
    return value[:_MAX_REASON_CHARS]


__all__ = ["MissionModalJobWorkflow"]
