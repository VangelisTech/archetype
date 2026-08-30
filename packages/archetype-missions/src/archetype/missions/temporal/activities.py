# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Temporal Activity adapters over the existing governed mission executor."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable
from contextlib import suppress

from pydantic import TypeAdapter
from temporalio import activity

from archetype.missions.contracts import MissionResult, SubmittedMission
from archetype.missions.run_contracts import (
    ExecutionProfileIdentity,
    MissionRun,
    MissionRunStatus,
    mission_result_payload,
    submission_from_json,
)
from archetype.missions.run_supervisor import MissionRunExecutor

from .contracts import (
    MISSION_EXECUTE_ACTIVITY,
    MISSION_SUBMIT_ACTIVITY,
    ExecuteMissionInput,
    MissionExecutionPayload,
    MissionWorkflowInput,
    SubmittedMissionPayload,
)

_SUBMITTED_ADAPTER = TypeAdapter(SubmittedMission)
_HEARTBEAT_SECONDS = 20.0


class MissionActivities:
    """Expose submit/run ports as Temporal Activities during migration."""

    def __init__(self, executor: MissionRunExecutor) -> None:
        self._executor = executor

    @activity.defn(name=MISSION_SUBMIT_ACTIVITY)
    async def submit(self, command: MissionWorkflowInput) -> SubmittedMissionPayload:
        run = _run_from_input(
            command,
            active_operation="submit_mission",
        )
        self._executor.prepare(run)
        submitted = await self._executor.load_existing(run)
        if submitted is None:
            submitted = await self._executor.submit(run)
        encoded = json.dumps(
            _SUBMITTED_ADAPTER.dump_python(submitted, mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        return SubmittedMissionPayload(submitted_json=encoded)

    @activity.defn(name=MISSION_EXECUTE_ACTIVITY)
    async def execute(self, command: ExecuteMissionInput) -> MissionExecutionPayload:
        submitted = _SUBMITTED_ADAPTER.validate_json(command.submitted_json, strict=True)
        run = _run_from_input(
            command.mission,
            active_operation="run_mission",
            submitted=submitted,
        )
        result = await _await_with_heartbeats(self._executor.run(run, submitted))
        return MissionExecutionPayload(status=result.status, result_json=_result_json(result))


def _run_from_input(
    command: MissionWorkflowInput,
    *,
    active_operation: str,
    submitted: SubmittedMission | None = None,
) -> MissionRun:
    return MissionRun(
        run_id=command.run_id,
        principal=command.principal,
        idempotency_key=command.idempotency_key,
        request_digest=command.request_digest,
        profile=ExecutionProfileIdentity(
            profile_id=command.profile_id,
            version=command.profile_version,
            digest=command.profile_digest,
        ),
        status=MissionRunStatus.RUNNING,
        submission=submission_from_json(command.submission_json),
        world_id=command.world_id if submitted is None else submitted.world_id,
        mission_id=None if submitted is None else submitted.mission_id,
        episode_id="" if submitted is None else submitted.episode_id,
        task_ids=() if submitted is None else submitted.task_ids,
        active_operation=active_operation,
        accepted_at_ms=command.accepted_at_ms,
        running_at_ms=command.accepted_at_ms,
        updated_at_ms=command.accepted_at_ms,
    )


def _result_json(result: MissionResult) -> str:
    return json.dumps(
        mission_result_payload(result),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


async def _await_with_heartbeats(awaitable: Awaitable[MissionResult]) -> MissionResult:
    task = asyncio.ensure_future(awaitable)
    try:
        while not task.done():
            done, _pending = await asyncio.wait({task}, timeout=_HEARTBEAT_SECONDS)
            if done:
                break
            activity.heartbeat({"phase": "run_mission"})
        return await task
    except asyncio.CancelledError:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        raise


__all__ = ["MissionActivities"]
