# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavior contracts for split Temporal supervision of Modal Mission jobs."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from temporalio.testing import WorkflowEnvironment

from archetype.missions.modal_jobs import (
    ModalMissionJobReady,
    ModalMissionJobRef,
    ModalMissionJobResult,
    ModalMissionJobRunning,
)
from archetype.missions.temporal import (
    MissionJobValueRef,
    MissionModalJobWorkflow,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
    create_mission_modal_job_worker,
    mission_modal_job_workflow_id,
)
from archetype.missions.temporal.contracts import (
    MISSION_MODAL_JOB_WORKFLOW_NAME,
)


class _Values:
    def __init__(self, request: bytes) -> None:
        self.request = request
        self.results: dict[str, bytes] = {}

    async def get_request(self, ref: MissionJobValueRef) -> bytes:
        assert ref.digest == hashlib.sha256(self.request).hexdigest()
        return self.request

    async def put_result(
        self,
        *,
        family: str,
        operation_id: str,
        payload: bytes,
        payload_digest: str,
    ) -> MissionJobValueRef:
        assert family == "author"
        assert operation_id == "missions.author:operation-1"
        assert payload_digest == hashlib.sha256(payload).hexdigest()
        self.results.setdefault(payload_digest, payload)
        assert self.results[payload_digest] == payload
        return MissionJobValueRef(
            ref=f"memory://mission-results/{payload_digest}",
            digest=payload_digest,
            size_bytes=len(payload),
        )


class _Jobs:
    def __init__(self, *, ready_after: int) -> None:
        self.ready_after = ready_after
        self.start_calls = 0
        self.poll_calls = 0
        self.collect_calls = 0
        self.cancel_calls = 0
        self.cleanup_calls = 0
        self.poll_entered = asyncio.Event()
        self.release_poll = asyncio.Event()
        self.block_poll = False

    async def start(
        self,
        *,
        family: str,
        operation_id: str,
        request_bytes: bytes,
    ) -> ModalMissionJobRef:
        self.start_calls += 1
        return ModalMissionJobRef(
            family=family,
            operation_id=operation_id,
            request_digest=hashlib.sha256(request_bytes).hexdigest(),
            namespace_digest="a" * 64,
            call_id="fc-temporal-job-1",
        )

    async def poll(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRunning | ModalMissionJobReady:
        assert ref.request_digest == hashlib.sha256(request_bytes).hexdigest()
        self.poll_calls += 1
        self.poll_entered.set()
        if self.block_poll:
            await self.release_poll.wait()
        if self.poll_calls >= self.ready_after:
            return ModalMissionJobReady(ref)
        return ModalMissionJobRunning(ref)

    async def collect(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobResult:
        assert ref.request_digest == hashlib.sha256(request_bytes).hexdigest()
        self.collect_calls += 1
        payload = b'{"kind":"author-result","schema_version":1}'
        return ModalMissionJobResult(
            ref=ref,
            payload=payload,
            payload_digest=hashlib.sha256(payload).hexdigest(),
        )

    async def cancel(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRef:
        assert ref.request_digest == hashlib.sha256(request_bytes).hexdigest()
        self.cancel_calls += 1
        return ref

    async def cleanup(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRef:
        assert ref.request_digest == hashlib.sha256(request_bytes).hexdigest()
        self.cleanup_calls += 1
        return ref


def _command(request: bytes, *, polls_per_run: int = 64) -> MissionModalJobWorkflowInput:
    digest = hashlib.sha256(request).hexdigest()
    return MissionModalJobWorkflowInput(
        family="author",
        operation_id="missions.author:operation-1",
        request=MissionJobValueRef(
            ref=f"memory://mission-requests/{digest}",
            digest=digest,
            size_bytes=len(request),
        ),
        namespace_digest="a" * 64,
        poll_interval_seconds=1,
        polls_per_run=polls_per_run,
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_job_continues_as_new_then_collects_and_cleans_exactly_once() -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    request = b'{"kind":"author-request","schema_version":1}'
    values = _Values(request)
    jobs = _Jobs(ready_after=3)
    task_queue = "temporal-modal-job-continue"
    worker = create_mission_modal_job_worker(
        environment.client,
        jobs,
        values,
        task_queue=task_queue,
    )
    command = _command(request, polls_per_run=2)
    try:
        async with worker:
            result = await environment.client.execute_workflow(
                MISSION_MODAL_JOB_WORKFLOW_NAME,
                command,
                id=mission_modal_job_workflow_id(
                    command.family,
                    command.operation_id,
                    command.namespace_digest,
                ),
                task_queue=task_queue,
                result_type=MissionModalJobWorkflowState,
            )
    finally:
        await environment.shutdown()

    assert result.status == "succeeded"
    assert result.result is not None
    assert jobs.start_calls == 1
    assert jobs.poll_calls == 3
    assert jobs.collect_calls == 1
    assert jobs.cancel_calls == 0
    assert jobs.cleanup_calls == 1
    assert values.results == {result.result.digest: values.results[result.result.digest]}


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cancellation_targets_exact_call_then_cleans_before_terminal_state() -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    request = b'{"kind":"author-request","schema_version":1}'
    values = _Values(request)
    jobs = _Jobs(ready_after=100)
    jobs.block_poll = True
    task_queue = "temporal-modal-job-cancel"
    worker = create_mission_modal_job_worker(
        environment.client,
        jobs,
        values,
        task_queue=task_queue,
    )
    # A one-poll history boundary proves a cancellation delivered during the
    # poll is honored before Continue-As-New instead of being dropped.
    command = _command(request, polls_per_run=1)
    try:
        async with worker:
            handle = await environment.client.start_workflow(
                MISSION_MODAL_JOB_WORKFLOW_NAME,
                command,
                id=mission_modal_job_workflow_id(
                    command.family,
                    command.operation_id,
                    command.namespace_digest,
                ),
                task_queue=task_queue,
                result_type=MissionModalJobWorkflowState,
            )
            await asyncio.wait_for(jobs.poll_entered.wait(), timeout=10)
            await handle.signal(MissionModalJobWorkflow.request_cancel, "operator stop")
            jobs.release_poll.set()
            result = await asyncio.wait_for(handle.result(), timeout=10)
    finally:
        await environment.shutdown()

    assert result.status == "cancelled"
    assert result.cancellation_requested
    assert result.cancellation_reason == "operator stop"
    assert jobs.start_calls == 1
    assert jobs.cancel_calls == 1
    assert jobs.cleanup_calls == 1
    assert jobs.collect_calls == 0
