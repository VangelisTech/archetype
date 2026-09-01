# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durability evidence for the Temporal MissionRun replacement slice."""

from __future__ import annotations

import asyncio

import pytest
from temporalio.testing import WorkflowEnvironment

from archetype.missions._extension import _temporal_request
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    MissionResult,
    MissionSubmission,
    SubmittedMission,
    TaskResult,
)
from archetype.missions.run_contracts import (
    MissionRunConflictError,
    MissionRunNotFoundError,
    MissionRunRequest,
    execution_profile_identity,
)
from archetype.missions.sandboxes.contracts import SandboxBackend
from archetype.missions.temporal import (
    MissionTemporalClient,
    MissionWorkflow,
    MissionWorkflowState,
    create_mission_worker,
)


class _Backend(SandboxBackend):
    name = "temporal-test"

    async def start(self, spec):  # pragma: no cover - config identity only
        del spec
        raise AssertionError("sandbox backend must not run in Temporal contract tests")


class _Executor:
    def __init__(self) -> None:
        self.prepared: list[str] = []
        self.submissions: dict[str, SubmittedMission] = {}
        self.submit_calls: list[str] = []
        self.run_calls: list[str] = []

    def prepare(self, run) -> None:
        self.prepared.append(run.run_id)

    async def load_existing(self, run):
        return self.submissions.get(run.run_id)

    async def submit(self, run):
        self.submit_calls.append(run.run_id)
        submitted = SubmittedMission(
            mission_id=len(self.submissions) + 1,
            task_ids=((run.submission.tasks[0].name, len(self.submissions) + 101),),
            episode_id=f"episode-{run.run_id}",
            repository=run.submission.repository,
            branch=run.submission.branch,
            base_ref=run.submission.base_ref,
            world_id=run.world_id,
        )
        self.submissions[run.run_id] = submitted
        return submitted

    async def run(self, run, mission):
        self.run_calls.append(run.run_id)
        task_name, task_id = mission.task_ids[0]
        return MissionResult(
            mission_id=mission.mission_id,
            episode_id=mission.episode_id,
            status="succeeded",
            repository=mission.repository,
            branch=mission.branch,
            ticks_completed=1,
            tasks=(
                TaskResult(
                    task_id=task_id,
                    name=task_name,
                    status="succeeded",
                    dispatches=1,
                    commit_shas=("abc123",),
                ),
            ),
        )


def _profile():
    return execution_profile_identity(
        AgentMissionConfig(
            sandbox_backend=_Backend(),
            sandbox_environment="test",
        )
    )


def _request(key: str, *, branch: str | None = None) -> MissionRunRequest:
    return MissionRunRequest(
        principal="operator@example.test",
        idempotency_key=key,
        submission=MissionSubmission(
            repository="VangelisTech/archetype",
            branch=branch or f"agent/{key}",
            tasks=(
                AgentTask(
                    name="implementation",
                    prompt="Prove Temporal-owned mission orchestration.",
                    validators=(CommandValidator(name="tests", command=("pytest", "-q")),),
                ),
            ),
        ),
    )


async def _wait_for_bound(handle) -> MissionWorkflowState:
    async def poll() -> MissionWorkflowState:
        while True:
            state = await handle.query(MissionWorkflow.state)
            if state is not None and state.submitted_json:
                return state
            await asyncio.sleep(0.01)

    return await asyncio.wait_for(poll(), timeout=10)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.process
async def test_parallel_missions_resume_on_a_replacement_worker_without_duplicate_submit() -> None:
    """Workflow history, not one process, owns admission and forward progress."""

    # This test exercises worker replacement rather than Workflow timers.  Use
    # the local dev server so stopping the first worker cannot be coupled to the
    # test server's automatic time-skipping lock.
    environment = await WorkflowEnvironment.start_local()
    executor = _Executor()
    task_queue = "temporal-mission-worker-replacement"
    client = MissionTemporalClient(environment.client, task_queue=task_queue)
    try:
        worker = create_mission_worker(environment.client, executor, task_queue=task_queue)
        async with worker:
            first, second = await asyncio.gather(
                client.start(_request("first"), _profile(), start_paused=True),
                client.start(_request("second"), _profile(), start_paused=True),
            )
            await asyncio.gather(_wait_for_bound(first), _wait_for_bound(second))

        assert sorted(executor.submit_calls) == sorted([first.id, second.id])
        assert executor.run_calls == []

        replacement = create_mission_worker(
            environment.client,
            executor,
            task_queue=task_queue,
        )
        async with replacement:
            await asyncio.gather(
                first.signal(MissionWorkflow.release_execution),
                second.signal(MissionWorkflow.release_execution),
            )
            first_result, second_result = await asyncio.gather(
                first.result(),
                second.result(),
            )

        assert first_result.status == "succeeded"
        assert second_result.status == "succeeded"
        assert sorted(executor.submit_calls) == sorted([first.id, second.id])
        assert sorted(executor.run_calls) == sorted([first.id, second.id])
    finally:
        await environment.shutdown()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_workflow_idempotency_reuses_same_request_and_rejects_changed_request() -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    executor = _Executor()
    task_queue = "temporal-mission-idempotency"
    client = MissionTemporalClient(environment.client, task_queue=task_queue)
    try:
        worker = create_mission_worker(environment.client, executor, task_queue=task_queue)
        async with worker:
            first = await client.start(_request("same"), _profile(), start_paused=True)
            await _wait_for_bound(first)
            replay = await client.start(_request("same"), _profile(), start_paused=True)
            assert replay.id == first.id
            assert executor.submit_calls == [first.id]

            with pytest.raises(MissionRunConflictError):
                await client.start(
                    _request("same", branch="agent/changed-request"),
                    _profile(),
                    start_paused=True,
                )

            await first.signal(MissionWorkflow.request_cancel, "test complete")
            result = await first.result()
            assert result.status == "cancelled"
            events = await first.query(MissionWorkflow.events)
            assert "cancelling" in [event.event_type for event in events]
    finally:
        await environment.shutdown()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cancel_signal_projects_cancelling_before_terminal_completion() -> None:
    class BlockingSubmitExecutor(_Executor):
        def __init__(self) -> None:
            super().__init__()
            self.submit_started = asyncio.Event()
            self.release_submit = asyncio.Event()

        async def submit(self, run):
            self.submit_started.set()
            await self.release_submit.wait()
            return await super().submit(run)

    environment = await WorkflowEnvironment.start_time_skipping()
    executor = BlockingSubmitExecutor()
    task_queue = "temporal-mission-cancelling"
    client = MissionTemporalClient(environment.client, task_queue=task_queue)
    try:
        worker = create_mission_worker(environment.client, executor, task_queue=task_queue)
        async with worker:
            handle = await client.start(_request("cancel"), _profile())
            await asyncio.wait_for(executor.submit_started.wait(), timeout=5)
            await handle.signal(MissionWorkflow.request_cancel, "operator stop")

            cancelling = await handle.query(MissionWorkflow.state)
            assert cancelling is not None
            assert cancelling.status == "cancelling"
            events = await handle.query(MissionWorkflow.events)
            assert [event.event_type for event in events] == [
                "accepted",
                "running",
                "cancel_requested",
                "cancelling",
            ]

            executor.release_submit.set()
            result = await handle.result()
            assert result.status == "cancelled"
    finally:
        await environment.shutdown()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cancel_during_in_flight_provider_work_records_cancelling_once() -> None:
    class BlockingRunExecutor(_Executor):
        def __init__(self) -> None:
            super().__init__()
            self.run_started = asyncio.Event()
            self.release_run = asyncio.Event()

        async def run(self, run, mission):
            self.run_started.set()
            await self.release_run.wait()
            return await super().run(run, mission)

    environment = await WorkflowEnvironment.start_time_skipping()
    executor = BlockingRunExecutor()
    task_queue = "temporal-mission-provider-cancel"
    client = MissionTemporalClient(environment.client, task_queue=task_queue)
    try:
        worker = create_mission_worker(environment.client, executor, task_queue=task_queue)
        async with worker:
            handle = await client.start(_request("provider-cancel"), _profile())
            await _wait_for_bound(handle)
            await asyncio.wait_for(executor.run_started.wait(), timeout=5)

            await handle.signal(MissionWorkflow.request_cancel, "operator stop")
            cancelling = await handle.query(MissionWorkflow.state)
            assert cancelling is not None
            assert cancelling.status == "cancelling"
            assert cancelling.active_operation == "run_mission"
            assert executor.run_calls == []

            executor.release_run.set()
            result = await handle.result()
            assert result.status == "cancelled"
            assert executor.run_calls == [handle.id]
            events = await handle.query(MissionWorkflow.events)
            assert [event.event_type for event in events].count("cancelling") == 1
    finally:
        await environment.shutdown()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_unknown_temporal_run_translates_to_mission_run_not_found() -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    client = MissionTemporalClient(environment.client, task_queue="temporal-mission-missing")
    handle = client.get("missing-run")
    try:
        with pytest.raises(MissionRunNotFoundError):
            await _temporal_request("missing-run", handle.query(MissionWorkflow.state))
        with pytest.raises(MissionRunNotFoundError):
            await _temporal_request("missing-run", handle.query(MissionWorkflow.events))
        with pytest.raises(MissionRunNotFoundError):
            await _temporal_request(
                "missing-run",
                handle.signal(MissionWorkflow.request_cancel, "stop"),
            )
    finally:
        await environment.shutdown()
