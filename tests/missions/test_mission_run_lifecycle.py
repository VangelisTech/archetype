# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable MissionRun identity, idempotency, and lifecycle transitions."""

from __future__ import annotations

import asyncio

import pytest

from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    MissionResult,
    MissionSubmission,
    TaskResult,
)
from archetype.missions.run_catalog import SqliteMissionRunCatalog
from archetype.missions.run_contracts import (
    MISSION_RUN_TRANSITIONS,
    MissionRun,
    MissionRunCleanupState,
    MissionRunConflictError,
    MissionRunNotFoundError,
    MissionRunRequest,
    MissionRunStatus,
    execution_profile_identity,
    mission_request_digest,
    require_mission_run_transition,
)
from archetype.missions.run_lifecycle import MissionRunLifecycle, submitted_from_run
from archetype.missions.run_supervisor import MissionRunSupervisor


class _Backend:
    name = "mission-run-contract"


def _task() -> AgentTask:
    return AgentTask(
        name="implementation",
        prompt="Implement the requested change.",
        validators=(CommandValidator(name="tests", command=("pytest", "-q")),),
    )


def _submission() -> MissionSubmission:
    return MissionSubmission(
        repository="VangelisTech/archetype",
        branch="agent/run",
        tasks=(_task(),),
    )


def _config() -> AgentMissionConfig:
    return AgentMissionConfig(
        sandbox_backend=_Backend(),
        sandbox_environment="pinned@sha256:digest",
    )


def _request(**overrides: object) -> MissionRunRequest:
    values: dict[str, object] = {
        "principal": "agent:buzz",
        "idempotency_key": "mission-1",
        "submission": _submission(),
    }
    values.update(overrides)
    return MissionRunRequest(**values)  # type: ignore[arg-type]


def _result(*, status: str = "succeeded") -> MissionResult:
    return MissionResult(
        mission_id=1,
        episode_id="mission-episode-1",
        status=status,
        repository="VangelisTech/archetype",
        branch="agent/run",
        ticks_completed=4,
        tasks=(
            TaskResult(
                task_id=2,
                name="implementation",
                status="accepted" if status == "succeeded" else "failed",
                dispatches=1,
                commit_shas=("abc",),
            ),
        ),
    )


@pytest.fixture
def lifecycle(tmp_path) -> MissionRunLifecycle:
    return MissionRunLifecycle(SqliteMissionRunCatalog(tmp_path / "mission-runs.db"))


def test_request_digest_is_stable_and_profile_sensitive() -> None:
    profile = execution_profile_identity(_config())
    first = mission_request_digest(_submission(), profile)
    second = mission_request_digest(_submission(), profile)
    other = execution_profile_identity(
        AgentMissionConfig(
            sandbox_backend=_Backend(),
            sandbox_environment="other@sha256:digest",
        )
    )

    assert first == second
    assert len(first) == 64
    assert first != mission_request_digest(_submission(), other)


def test_succeeded_run_requires_independent_succeeded_result() -> None:
    profile = execution_profile_identity(_config())
    with pytest.raises(ValueError, match="succeeded MissionResult"):
        MissionRun(
            run_id="run-1",
            principal="agent:buzz",
            idempotency_key="mission-1",
            request_digest=mission_request_digest(_submission(), profile),
            profile=profile,
            status=MissionRunStatus.SUCCEEDED,
            submission=_submission(),
            accepted_at_ms=1,
            terminal_at_ms=2,
            result=_result(status="failed"),
        )


def test_terminal_transitions_are_immutable() -> None:
    require_mission_run_transition("accepted", "running")
    require_mission_run_transition("running", "interrupted")
    require_mission_run_transition("cancelling", "cancelled")
    with pytest.raises(ValueError, match="illegal mission-run transition"):
        require_mission_run_transition("succeeded", "failed")
    assert MISSION_RUN_TRANSITIONS[MissionRunStatus.SUCCEEDED] == frozenset()


@pytest.mark.asyncio
async def test_accept_returns_run_id_and_is_idempotent(lifecycle: MissionRunLifecycle) -> None:
    profile = execution_profile_identity(_config())
    first = await lifecycle.accept(_request(), profile)
    second = await lifecycle.accept(_request(), profile)

    assert first.status is MissionRunStatus.ACCEPTED
    assert first.run_id
    assert first.world_id
    assert first.mission_id is None
    assert second.run_id == first.run_id
    assert second.world_id == first.world_id


@pytest.mark.asyncio
async def test_same_key_different_digest_conflicts(lifecycle: MissionRunLifecycle) -> None:
    profile = execution_profile_identity(_config())
    await lifecycle.accept(_request(), profile)
    changed = MissionSubmission(
        repository="VangelisTech/other",
        branch="agent/run",
        tasks=(_task(),),
    )
    with pytest.raises(MissionRunConflictError, match="different canonical"):
        await lifecycle.accept(_request(submission=changed), profile)


@pytest.mark.asyncio
async def test_unknown_run_is_not_found(lifecycle: MissionRunLifecycle) -> None:
    with pytest.raises(MissionRunNotFoundError):
        await lifecycle.get("missing")


@pytest.mark.asyncio
async def test_cancel_before_running_is_cancelled(lifecycle: MissionRunLifecycle) -> None:
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    cancelled = await lifecycle.record_cancellation_intent(run, reason="caller-stop")
    assert cancelled.status is MissionRunStatus.CANCELLED
    assert cancelled.cancellation_intent is True
    assert cancelled.cancellation_reason == "caller-stop"
    assert cancelled.cleanup_state is MissionRunCleanupState.NONE


@pytest.mark.asyncio
async def test_cancel_while_running_records_intent_without_rewriting_outcome(
    lifecycle: MissionRunLifecycle,
) -> None:
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    running = await lifecycle.mark_running(run, operation="submit_mission")
    cancelling = await lifecycle.record_cancellation_intent(running, reason="operator")
    assert cancelling.status is MissionRunStatus.CANCELLING
    assert cancelling.cancellation_intent is True
    with pytest.raises(ValueError, match="illegal mission-run transition"):
        await lifecycle.mark_succeeded(cancelling, _result())
    cancelled = await lifecycle.mark_cancelled(cancelling, result=_result())
    assert cancelled.status is MissionRunStatus.CANCELLED
    assert cancelled.result is not None
    assert cancelled.result.status == "succeeded"


@pytest.mark.asyncio
async def test_cleanup_state_is_queryable_after_terminal(
    lifecycle: MissionRunLifecycle,
) -> None:
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    running = await lifecycle.mark_running(run, operation="run_mission")
    interrupted = await lifecycle.mark_interrupted(running, reason="provider unknown")
    pending = await lifecycle.mark_cleanup(interrupted, MissionRunCleanupState.PENDING)
    loaded = await lifecycle.get(run.run_id)
    assert loaded.status is MissionRunStatus.INTERRUPTED
    assert loaded.cleanup_state is MissionRunCleanupState.PENDING
    assert pending.interrupted_reason == "provider unknown"


class _FakeExecutor:
    def __init__(self) -> None:
        self.submits = 0
        self.runs = 0
        self.reconciles = 0
        self.submit_gate: asyncio.Event | None = None
        self.reconciliation = "retry"
        self.activity: tuple[str, str] | None = None
        self.existing = None
        self.result = _result()
        self.submitted = None

    async def submit(self, run):
        self.submits += 1
        if self.submit_gate is not None:
            await self.submit_gate.wait()
        from archetype.missions.contracts import SubmittedMission

        self.submitted = SubmittedMission(
            mission_id=1,
            task_ids=(("implementation", 2),),
            episode_id="mission-episode-1",
            repository=run.submission.repository,
            branch=run.submission.branch,
            world_id=run.world_id,
        )
        return self.submitted

    async def load_existing(self, run):
        del run
        return self.existing

    async def run(self, run, mission):
        del run, mission
        self.runs += 1
        return self.result

    async def reconcile(self, run):
        del run
        self.reconciles += 1
        return self.reconciliation

    async def active_activity(self, run):
        del run
        return self.activity


def _supervisor(
    tmp_path, executor: _FakeExecutor
) -> tuple[MissionRunLifecycle, MissionRunSupervisor]:
    lifecycle = MissionRunLifecycle(SqliteMissionRunCatalog(tmp_path / "runs.db"))
    tasks: list[asyncio.Task[None]] = []

    def spawn(factory, label: str) -> asyncio.Task[None]:
        del label
        task = asyncio.create_task(factory())
        tasks.append(task)
        return task

    return lifecycle, MissionRunSupervisor(lifecycle, executor, spawn=spawn)


@pytest.mark.asyncio
async def test_submit_returns_before_work_completes_and_caller_cancel_does_not_stop_it(
    tmp_path,
) -> None:
    executor = _FakeExecutor()
    executor.submit_gate = asyncio.Event()
    lifecycle, supervisor = _supervisor(tmp_path, executor)
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    task = supervisor.ensure(run)
    assert task is not None
    await asyncio.sleep(0)
    waiter = asyncio.create_task(asyncio.wait_for(asyncio.shield(task), timeout=10))
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not task.done()
    loaded = await lifecycle.get(run.run_id)
    assert loaded.status in {MissionRunStatus.ACCEPTED, MissionRunStatus.RUNNING}
    executor.submit_gate.set()
    await asyncio.wait_for(task, timeout=2)
    finished = await lifecycle.get(run.run_id)
    assert finished.status is MissionRunStatus.SUCCEEDED
    assert finished.result is not None
    assert executor.submits == 1
    assert executor.runs == 1


@pytest.mark.asyncio
async def test_restart_before_world_creation_submits_once(tmp_path) -> None:
    executor = _FakeExecutor()
    lifecycle, supervisor = _supervisor(tmp_path, executor)
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    recovered = MissionRunLifecycle(SqliteMissionRunCatalog(tmp_path / "runs.db"))
    _, recovered_supervisor = _supervisor(tmp_path, executor)
    recovered_supervisor._lifecycle = recovered
    opened = await recovered.get(run.run_id)
    task = recovered_supervisor.ensure(opened)
    assert task is not None
    await asyncio.wait_for(task, timeout=2)
    finished = await recovered.get(run.run_id)
    assert finished.world_id == run.world_id
    assert finished.mission_id == 1
    assert executor.submits == 1


@pytest.mark.asyncio
async def test_existing_mission_is_not_created_twice(tmp_path) -> None:
    executor = _FakeExecutor()
    from archetype.missions.contracts import SubmittedMission

    executor.existing = SubmittedMission(
        mission_id=9,
        task_ids=(("implementation", 10),),
        episode_id="mission-episode-existing",
        repository="VangelisTech/archetype",
        branch="agent/run",
        world_id="",
    )
    lifecycle, supervisor = _supervisor(tmp_path, executor)
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    run = await lifecycle.mark_running(run, operation="submit_mission")
    task = supervisor.ensure(run)
    assert task is not None
    await asyncio.wait_for(task, timeout=2)
    assert executor.submits == 0
    finished = await lifecycle.get(run.run_id)
    assert finished.mission_id == 9


@pytest.mark.asyncio
async def test_unknown_provider_outcome_is_interrupted_not_failed(tmp_path) -> None:
    executor = _FakeExecutor()
    executor.activity = ("missions.author", "dispatch-1")
    executor.reconciliation = "unknown"
    lifecycle, supervisor = _supervisor(tmp_path, executor)
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    run = await lifecycle.mark_running(run, operation="run_mission")
    task = supervisor.ensure(run)
    assert task is not None
    await asyncio.wait_for(task, timeout=2)
    finished = await lifecycle.get(run.run_id)
    assert finished.status is MissionRunStatus.INTERRUPTED
    assert finished.cleanup_state is MissionRunCleanupState.PENDING
    assert executor.runs == 0
    assert executor.reconciles == 1


@pytest.mark.asyncio
async def test_authored_green_result_cannot_become_succeeded(tmp_path) -> None:
    executor = _FakeExecutor()
    executor.result = _result(status="running")
    lifecycle, supervisor = _supervisor(tmp_path, executor)
    run = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    task = supervisor.ensure(run)
    assert task is not None
    await asyncio.wait_for(task, timeout=2)
    finished = await lifecycle.get(run.run_id)
    assert finished.status is MissionRunStatus.INTERRUPTED
    assert finished.result is None


@pytest.mark.asyncio
async def test_same_key_returns_original_run_without_second_submit(tmp_path) -> None:
    executor = _FakeExecutor()
    lifecycle, supervisor = _supervisor(tmp_path, executor)
    first = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    task = supervisor.ensure(first)
    assert task is not None
    await asyncio.wait_for(task, timeout=2)
    second = await lifecycle.accept(_request(), execution_profile_identity(_config()))
    assert second.run_id == first.run_id
    assert executor.submits == 1
    bound = submitted_from_run(await lifecycle.get(first.run_id))
    assert bound.world_id == first.world_id
