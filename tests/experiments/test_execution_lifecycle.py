# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood tests for ECS-native external trial execution state."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from archetype import ArchetypeRuntime, StorageConfig
from archetype.core.config import RunConfig
from archetype.experiments import (
    PollTrialsProcessor,
    SubmitTrialsProcessor,
    Trial,
    TrialBackendOutcome,
    TrialExecution,
    TrialExecutionBackendResource,
    TrialExecutionConfig,
    TrialExecutionStatus,
    TrialJobRef,
    TrialManifest,
    TrialOutcome,
    TrialRequest,
)


@dataclass
class FakeTrialBackend:
    """Small deterministic backend resource used by the dogfood tests."""

    complete_after_polls: int = 2
    terminal_status: str = TrialExecutionStatus.SUCCEEDED.value
    verified: bool = True
    name: str = "fake"

    def __post_init__(self) -> None:
        self.submissions: list[tuple[TrialRequest, str]] = []
        self.jobs: dict[str, TrialRequest] = {}
        self.poll_counts: dict[str, int] = {}

    def seed_job(self, job_id: str, request: TrialRequest) -> None:
        self.jobs[job_id] = request
        self.poll_counts.setdefault(job_id, 0)

    async def submit(self, request: TrialRequest, *, idempotency_key: str) -> TrialJobRef:
        job_id = f"fake://{idempotency_key}"
        self.submissions.append((request, idempotency_key))
        self.seed_job(job_id, request)
        return TrialJobRef(job_id=job_id)

    async def poll(self, job_id: str) -> TrialBackendOutcome | None:
        request = self.jobs[job_id]
        count = self.poll_counts[job_id] + 1
        self.poll_counts[job_id] = count

        if count < self.complete_after_polls:
            return TrialBackendOutcome(status=TrialExecutionStatus.RUNNING.value)

        passed = self.terminal_status == TrialExecutionStatus.SUCCEEDED.value
        return TrialBackendOutcome(
            status=self.terminal_status,
            passed=passed,
            score=1.0 if passed else 0.0,
            steps=7 + request.trial_idx,
            failure_mode="success" if passed else "backend_error",
            manifest_uri=f"mem://manifest/{request.trial_id}",
            storage_uri=f"mem://storage/{request.trial_id}",
            verified=self.verified,
            row_count=3 if self.verified else 0,
            details_json=json.dumps({"job_id": job_id}),
            error="" if passed else "sim crashed",
        )


def _trial_request(idx: int) -> TrialRequest:
    return TrialRequest(
        trial_id=f"trial-{idx}",
        run_id="run-1",
        suite="dogfood",
        task_id=f"task-{idx}",
        trial_idx=idx,
        spec_json=json.dumps({"seed": idx}),
    )


async def _spawn_trial(
    world,
    idx: int,
    *,
    status: str = TrialExecutionStatus.PENDING.value,
    job_id: str = "",
    attempt: int = 0,
) -> None:
    request = _trial_request(idx)
    await world.spawn(
        Trial(
            trial_id=request.trial_id,
            run_id=request.run_id,
            suite=request.suite,
            task_id=request.task_id,
            trial_idx=request.trial_idx,
            spec_json=request.spec_json,
        ),
        TrialExecution(
            trial_id=request.trial_id,
            status=status,
            job_id=job_id,
            attempt=attempt,
        ),
        TrialOutcome(trial_id=request.trial_id),
        TrialManifest(trial_id=request.trial_id),
    )


async def _latest_rows(world) -> list[dict]:
    df = await world.query(Trial, TrialExecution, TrialOutcome, TrialManifest)
    rows = df.collect().to_pylist()
    tick = max(row["tick"] for row in rows)
    return [row for row in rows if row["tick"] == tick]


def _world(runtime: ArchetypeRuntime, tmp_path, backend: FakeTrialBackend, *resources):
    return runtime.world(
        "execution-dogfood",
        storage=StorageConfig(uri=str(tmp_path / "store"), namespace="execution"),
        processors=[SubmitTrialsProcessor(), PollTrialsProcessor()],
        resources=[TrialExecutionBackendResource(backend), *resources],
    )


@pytest.mark.asyncio
async def test_trial_execution_lifecycle_is_queryable_state(tmp_path):
    backend = FakeTrialBackend()

    async with ArchetypeRuntime() as runtime:
        world = _world(runtime, tmp_path, backend)
        for idx in range(3):
            await _spawn_trial(world, idx)

        await world.run(config=RunConfig.benchmark(steps=3))
        rows = await _latest_rows(world)

    assert len(backend.submissions) == 3
    assert len(rows) == 3

    for row in rows:
        trial_id = row["trial__trial_id"]
        assert row["trialexecution__status"] == TrialExecutionStatus.SUCCEEDED.value
        assert row["trialexecution__backend"] == "fake"
        assert row["trialexecution__job_id"] == f"fake://{trial_id}:attempt-1"
        assert row["trialexecution__attempt"] == 1
        assert row["trialoutcome__passed"] is True
        assert row["trialoutcome__score"] == 1.0
        assert row["trialoutcome__failure_mode"] == "success"
        assert row["trialmanifest__manifest_uri"] == f"mem://manifest/{trial_id}"
        assert row["trialmanifest__storage_uri"] == f"mem://storage/{trial_id}"
        assert row["trialmanifest__verified"] is True
        assert row["trialmanifest__row_count"] == 3

    assert all(count == 2 for count in backend.poll_counts.values())


@pytest.mark.asyncio
async def test_failure_path_persists_error_and_unverified_manifest(tmp_path):
    backend = FakeTrialBackend(
        complete_after_polls=1,
        terminal_status=TrialExecutionStatus.FAILED.value,
        verified=False,
    )

    async with ArchetypeRuntime() as runtime:
        world = _world(runtime, tmp_path, backend)
        await _spawn_trial(world, 0)

        await world.run(config=RunConfig.benchmark(steps=2))
        row = (await _latest_rows(world))[0]

    assert row["trialexecution__status"] == TrialExecutionStatus.FAILED.value
    assert row["trialexecution__error"] == "sim crashed"
    assert row["trialoutcome__passed"] is False
    assert row["trialoutcome__failure_mode"] == "backend_error"
    assert row["trialoutcome__error"] == "sim crashed"
    assert row["trialmanifest__verified"] is False
    assert row["trialmanifest__row_count"] == 0


@pytest.mark.asyncio
async def test_success_without_verified_manifest_becomes_failure(tmp_path):
    backend = FakeTrialBackend(complete_after_polls=1, verified=False)

    async with ArchetypeRuntime() as runtime:
        world = _world(runtime, tmp_path, backend, TrialExecutionConfig())
        await _spawn_trial(world, 0)

        await world.run(config=RunConfig.benchmark(steps=2))
        row = (await _latest_rows(world))[0]

    assert row["trialexecution__status"] == TrialExecutionStatus.FAILED.value
    assert row["trialoutcome__failure_mode"] == "unverified_manifest"
    assert row["trialmanifest__verified"] is False


@pytest.mark.asyncio
async def test_recovery_polls_existing_running_job_without_resubmitting(tmp_path):
    backend = FakeTrialBackend(complete_after_polls=1)
    request = _trial_request(0)
    job_id = "fake://trial-0:attempt-1"
    backend.seed_job(job_id, request)

    async with ArchetypeRuntime() as runtime:
        world = _world(runtime, tmp_path, backend)
        await _spawn_trial(
            world,
            0,
            status=TrialExecutionStatus.RUNNING.value,
            job_id=job_id,
            attempt=1,
        )

        await world.run(config=RunConfig.benchmark(steps=2))
        row = (await _latest_rows(world))[0]

    assert backend.submissions == []
    assert backend.poll_counts[job_id] == 1
    assert row["trialexecution__status"] == TrialExecutionStatus.SUCCEEDED.value
    assert row["trialexecution__job_id"] == job_id
    assert row["trialmanifest__verified"] is True


@pytest.mark.asyncio
async def test_submit_processor_respects_submission_limit(tmp_path):
    backend = FakeTrialBackend(complete_after_polls=10)
    config = TrialExecutionConfig(max_submissions_per_tick=1, max_polls_per_tick=10)

    async with ArchetypeRuntime() as runtime:
        world = _world(runtime, tmp_path, backend, config)
        await _spawn_trial(world, 0)
        await _spawn_trial(world, 1)

        await world.run(config=RunConfig.benchmark(steps=2))
        rows = sorted(await _latest_rows(world), key=lambda row: row["trial__trial_id"])

    assert len(backend.submissions) == 1
    assert rows[0]["trialexecution__status"] == TrialExecutionStatus.RUNNING.value
    assert rows[1]["trialexecution__status"] == TrialExecutionStatus.PENDING.value


@pytest.mark.asyncio
async def test_pending_trial_fails_when_max_attempts_exceeded(tmp_path):
    backend = FakeTrialBackend()
    config = TrialExecutionConfig(max_attempts=0)

    async with ArchetypeRuntime() as runtime:
        world = _world(runtime, tmp_path, backend, config)
        await _spawn_trial(world, 0)

        await world.run(config=RunConfig.benchmark(steps=2))
        row = (await _latest_rows(world))[0]

    assert backend.submissions == []
    assert row["trialexecution__status"] == TrialExecutionStatus.FAILED.value
    assert row["trialoutcome__failure_mode"] == "max_attempts_exceeded"
    assert row["trialmanifest__verified"] is False
