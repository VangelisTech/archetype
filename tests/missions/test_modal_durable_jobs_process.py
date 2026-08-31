# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""OS-process crash contracts for provider-native durable Mission jobs."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from temporalio.testing import WorkflowEnvironment

from archetype.missions.modal_jobs import ModalMissionJobNamespace
from archetype.missions.temporal import (
    MissionJobValueRef,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
    mission_modal_job_workflow_id,
)
from archetype.missions.temporal.contracts import MISSION_MODAL_JOB_WORKFLOW_NAME

pytestmark = pytest.mark.process

_CALL_IDENTITY_CRASH = 86
_RESULT_RECORD_CRASH = 87
_CALL_ID = "fc-process-1"
_OPERATION_ID = "mission:author:process-crash-1"
_WORLD_ID = "mission-process-world"
_RUN_ID = "mission-process-run"
_REQUEST_BYTES = b'{"mission":"process-crash-proof","schema_version":1}'
_REQUEST_DIGEST = hashlib.sha256(_REQUEST_BYTES).hexdigest()


@dataclass(frozen=True, slots=True)
class _ProcessHarness:
    root: Path

    @property
    def fixture(self) -> Path:
        return Path(__file__).resolve().parents[1] / "fixtures" / "modal_mission_job_process.py"

    def invoke(
        self,
        action: str,
        *,
        failpoint: str = "none",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(self.fixture),
                str(self.root),
                action,
                "--failpoint",
                failpoint,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def run(self, action: str) -> dict[str, Any]:
        process = self.invoke(action)
        assert process.returncode == 0, (
            f"process fixture action {action!r} failed with {process.returncode}:\n"
            f"stdout={process.stdout[-2000:]}\nstderr={process.stderr[-4000:]}"
        )
        return json.loads(process.stdout)

    def start_temporal_worker(
        self,
        *,
        temporal_target: str,
        task_queue: str,
    ) -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            [
                sys.executable,
                str(self.fixture),
                str(self.root),
                "temporal-worker",
                "--temporal-target",
                temporal_target,
                "--task-queue",
                task_queue,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def _namespace_digest() -> str:
    return ModalMissionJobNamespace(
        deployment_digest="a" * 64,
        image_id="process-test-image",
        result_dict_name="process-test-results",
        redaction_policy_id="process-test-redaction-v1",
    ).digest


def _process_command() -> MissionModalJobWorkflowInput:
    return MissionModalJobWorkflowInput(
        family="author",
        operation_id=_OPERATION_ID,
        request=MissionJobValueRef(
            ref=f"sqlite-value://mission-requests/{_REQUEST_DIGEST}",
            digest=_REQUEST_DIGEST,
            size_bytes=len(_REQUEST_BYTES),
        ),
        namespace_digest=_namespace_digest(),
        poll_interval_seconds=1,
        polls_per_run=8,
    )


async def _wait_for_provider_event(
    path: Path,
    event: str,
    *,
    timeout: float = 20,
) -> None:
    async def observed() -> bool:
        if not path.exists():
            return False
        try:
            connection = sqlite3.connect(path, timeout=1)
            try:
                row = connection.execute(
                    "SELECT 1 FROM provider_events WHERE event = ? LIMIT 1",
                    (event,),
                ).fetchone()
                return row is not None
            finally:
                connection.close()
        except sqlite3.OperationalError:
            return False

    async def wait() -> None:
        while not await observed():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(wait(), timeout=timeout)


async def _kill_process(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.kill()
    await asyncio.to_thread(process.wait, 10)


def _assert_one_self_registered_call(
    state: dict[str, Any],
    *,
    job_record_phases: tuple[str, ...] = ("start", "call"),
) -> None:
    provider = state["provider"]
    assert provider["spawn_count"] == 1
    assert provider["call_identity_insertions"] == 1
    assert provider["job_record_count"] == len(job_record_phases)
    assert sorted(provider["job_record_phases"]) == sorted(job_record_phases)
    assert [call["call_id"] for call in provider["calls"]] == [_CALL_ID]
    assert provider["calls"][0]["operation_id"] == _OPERATION_ID


def test_hard_killed_worker_replacement_polls_exact_self_registered_call(
    tmp_path: Path,
) -> None:
    harness = _ProcessHarness(tmp_path)

    crashed = harness.invoke("start", failpoint="after-call-identity")
    assert crashed.returncode == _CALL_IDENTITY_CRASH

    after_crash = harness.run("inspect")
    _assert_one_self_registered_call(after_crash)
    assert after_crash["provider"]["calls"][0]["status"] == "running"
    assert after_crash["provider"]["reattach_call_ids"] == []
    assert after_crash["activity"]["execution"] == {
        "operation_id": _OPERATION_ID,
        "provider": "modal-process-double",
    }
    assert after_crash["activity"]["result"] is None
    assert after_crash["activity"]["settlement"] is None

    replacement = harness.run("poll")
    _assert_one_self_registered_call(replacement)
    assert replacement["call_id"] == _CALL_ID
    assert replacement["poll"] == "running"
    assert replacement["provider"]["reattach_call_ids"] == [_CALL_ID]
    assert replacement["activity_inventory"]["attempt_count"] == 0


def test_published_result_survives_worker_death_before_exact_settlement(
    tmp_path: Path,
) -> None:
    harness = _ProcessHarness(tmp_path)

    crashed_start = harness.invoke("start", failpoint="after-call-identity")
    assert crashed_start.returncode == _CALL_IDENTITY_CRASH

    published = harness.run("publish")
    _assert_one_self_registered_call(published)
    result_digest = published["published_result_digest"]
    assert published["provider"]["result_publication_count"] == 1
    # The provider output is deliberately terminally unavailable. A later
    # poll can succeed only if it consults the durable first result first.
    assert published["provider"]["calls"][0]["status"] == "terminal-failed"
    assert published["provider"]["calls"][0]["result_digest"] == result_digest
    assert published["activity"]["result"] is None
    assert published["activity"]["settlement"] is None

    crashed_collector = harness.invoke("collect", failpoint="after-result-record")
    assert crashed_collector.returncode == _RESULT_RECORD_CRASH

    before_settlement = harness.run("inspect")
    _assert_one_self_registered_call(before_settlement)
    assert before_settlement["activity"]["result"]["digest"] == result_digest
    assert before_settlement["activity"]["result_pending_observation"] is True
    assert before_settlement["activity"]["settlement"] is None
    assert before_settlement["activity_inventory"] == {
        "activity_count": 1,
        "attempt_count": 0,
        "provider_operation_count": 0,
    }

    replacement = harness.run("settle")
    _assert_one_self_registered_call(replacement)
    assert replacement["call_id"] == _CALL_ID
    assert replacement["poll"] == "ready"
    assert replacement["settled_result_digest"] == result_digest
    assert replacement["provider"]["reattach_call_ids"] == []
    assert replacement["activity"]["result_pending_observation"] is False
    assert replacement["activity"]["settlement"] == {
        "committed_tick": 2,
        "result_digest": result_digest,
        "run_id": _RUN_ID,
        "visibility_token": "mission-process-observation-2",
        "world_id": _WORLD_ID,
    }

    repeated = harness.run("settle")
    _assert_one_self_registered_call(repeated)
    assert repeated["activity"] == replacement["activity"]
    assert repeated["provider"]["result_publication_count"] == 1
    assert repeated["provider"]["reattach_call_ids"] == []


@pytest.mark.asyncio
@pytest.mark.integration
async def test_temporal_workflow_survives_hard_killed_worker_without_respawn(
    tmp_path: Path,
) -> None:
    """A new OS process resumes the same Workflow and exact provider call."""

    harness = _ProcessHarness(tmp_path)
    environment = await WorkflowEnvironment.start_local()
    target_host = environment.client.service_client.config.target_host
    task_queue = "temporal-modal-job-process-replacement"
    command = _process_command()
    first_worker: subprocess.Popen[bytes] | None = None
    replacement_worker: subprocess.Popen[bytes] | None = None
    try:
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
        first_worker = harness.start_temporal_worker(
            temporal_target=target_host,
            task_queue=task_queue,
        )
        await _wait_for_provider_event(tmp_path / "provider.sqlite3", "reattach")

        # SIGKILL permits no Python shutdown or in-memory handoff. The provider
        # call and Temporal history are the only recovery authorities.
        await _kill_process(first_worker)
        assert first_worker.returncode is not None and first_worker.returncode < 0
        after_kill = await asyncio.to_thread(harness.run, "inspect")
        _assert_one_self_registered_call(after_kill)
        assert after_kill["provider"]["calls"][0]["status"] == "running"

        published = await asyncio.to_thread(harness.run, "publish")
        assert published["provider"]["result_publication_count"] == 1

        replacement_worker = harness.start_temporal_worker(
            temporal_target=target_host,
            task_queue=task_queue,
        )
        result = await asyncio.wait_for(handle.result(), timeout=30)
        assert result.status == "succeeded"
        assert result.ref is not None
        assert result.ref.call_id == _CALL_ID
        assert result.result is not None

        final = await asyncio.to_thread(harness.run, "inspect")
        _assert_one_self_registered_call(
            final,
            job_record_phases=("start", "call", "cleanup"),
        )
        assert final["provider"]["result_publication_count"] == 1
        assert final["provider"]["cleanup_record_count"] == 1
        assert final["provider"]["workflow_values"] == [
            {
                "digest": result.result.digest,
                "ref": result.result.ref,
                "size_bytes": result.result.size_bytes,
            }
        ]
    finally:
        await _kill_process(first_worker)
        await _kill_process(replacement_worker)
        await environment.shutdown()
