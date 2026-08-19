# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supervise admitted MissionRuns independently of the caller coroutine.

The supervisor records durable transitions, then invokes the existing
SubmitMission and RunMission path. It never constructs Mission ECS state
itself and never fabricates a succeeded or failed provider outcome.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Literal, Protocol

from archetype.missions.contracts import MissionResult, SubmittedMission
from archetype.missions.run_contracts import (
    TERMINAL_MISSION_RUN_STATUSES,
    MissionRun,
    MissionRunCleanupState,
    MissionRunStatus,
)
from archetype.missions.run_lifecycle import MissionRunLifecycle, submitted_from_run

type ProviderReconciliation = Literal["retry", "settled", "unknown"]


class MissionRunExecutor(Protocol):
    """Ports over governed submit/run and Activity reconciliation."""

    async def submit(self, run: MissionRun) -> SubmittedMission: ...

    async def load_existing(self, run: MissionRun) -> SubmittedMission | None: ...

    async def run(self, run: MissionRun, mission: SubmittedMission) -> MissionResult: ...

    async def reconcile(self, run: MissionRun) -> ProviderReconciliation: ...

    async def active_activity(self, run: MissionRun) -> tuple[str, str] | None: ...


class MissionRunSupervisor:
    """Process-local supervision of durable MissionRun execution."""

    def __init__(
        self,
        lifecycle: MissionRunLifecycle,
        executor: MissionRunExecutor,
        *,
        spawn: Callable[[Callable[[], Awaitable[None]], str], asyncio.Task[None]],
    ) -> None:
        self._lifecycle = lifecycle
        self._executor = executor
        self._spawn = spawn
        self._inflight: dict[str, asyncio.Task[None]] = {}

    def ensure(self, run: MissionRun) -> asyncio.Task[None] | None:
        """Start or reuse supervised execution for one non-terminal run."""

        if run.status in TERMINAL_MISSION_RUN_STATUSES:
            return self._inflight.get(run.run_id)
        existing = self._inflight.get(run.run_id)
        if existing is not None and not existing.done():
            return existing
        # Runtime task labels are bounded identifiers without ":"; keep the
        # label spawnable through a real owner reservation.
        task = self._spawn(lambda: self._drive(run.run_id), f"mission-run-{run.run_id}")
        self._inflight[run.run_id] = task
        task.add_done_callback(lambda _task: self._inflight.pop(run.run_id, None))
        return task

    async def recover_open(self) -> tuple[MissionRun, ...]:
        """Reconstruct supervision for every durable non-terminal run."""

        open_runs = await self._lifecycle.list_open()
        for run in open_runs:
            self.ensure(run)
        return open_runs

    async def _drive(self, run_id: str) -> None:
        run = await self._lifecycle.get(run_id)
        try:
            await self._execute(run)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            current = await self._lifecycle.get(run_id)
            if current.status not in TERMINAL_MISSION_RUN_STATUSES:
                await self._lifecycle.mark_failed(current, reason=str(exc)[:4096])
            raise

    async def _execute(self, run: MissionRun) -> None:
        if run.status is MissionRunStatus.ACCEPTED and run.cancellation_intent:
            await self._lifecycle.record_cancellation_intent(
                run,
                reason=run.cancellation_reason,
            )
            return
        if run.status is MissionRunStatus.ACCEPTED:
            run = await self._lifecycle.mark_running(run, operation="submit_mission")
        if run.status in TERMINAL_MISSION_RUN_STATUSES:
            return
        if run.status is MissionRunStatus.CANCELLING:
            await self._lifecycle.mark_cancelled(run)
            return

        activity = await self._executor.active_activity(run)
        if activity is not None:
            run = await self._lifecycle.bind_activity(
                run,
                kind=activity[0],
                activity_id=activity[1],
            )
            if run.status in TERMINAL_MISSION_RUN_STATUSES:
                return
            reconciliation = await self._executor.reconcile(run)
            if reconciliation == "unknown":
                run = await self._lifecycle.mark_interrupted(
                    run,
                    reason="provider completion cannot be proven",
                )
                await self._lifecycle.mark_cleanup(run, MissionRunCleanupState.PENDING)
                return
            if reconciliation == "settled" and run.mission_id is not None:
                mission = submitted_from_run(run)
                result = await self._executor.run(run, mission)
                await self._finish(run, result)
                return

        run = await self._admit_mission(run)
        if run.status in TERMINAL_MISSION_RUN_STATUSES:
            return
        run = await self._lifecycle.mark_running(run, operation="run_mission")
        current = await self._lifecycle.get(run.run_id)
        if current.cancellation_intent and current.status is MissionRunStatus.RUNNING:
            current = await self._lifecycle.record_cancellation_intent(
                current,
                reason=current.cancellation_reason,
            )
        if current.status is MissionRunStatus.CANCELLING:
            # Explicit cancel is recorded. Admitted work still runs; #628 owns
            # the cooperative handoff. A completed governed result is stored
            # as evidence when the run finishes, then the run becomes cancelled.
            pass
        mission = submitted_from_run(current)
        result = await self._executor.run(current, mission)
        await self._finish(current, result)

    async def _admit_mission(self, run: MissionRun) -> MissionRun:
        if run.mission_id is not None:
            return run
        existing = await self._executor.load_existing(run)
        submitted = existing if existing is not None else await self._executor.submit(run)
        if submitted.world_id and run.world_id and submitted.world_id != run.world_id:
            raise RuntimeError("recovery created a second World for one MissionRun")
        return await self._lifecycle.bind_mission(run, submitted)

    async def _finish(self, run: MissionRun, result: MissionResult) -> None:
        current = await self._lifecycle.get(run.run_id)
        if current.status is MissionRunStatus.CANCELLING:
            if result.status in {"succeeded", "failed"}:
                current = await self._lifecycle.mark_cancelled(current, result=result)
            else:
                current = await self._lifecycle.mark_interrupted(
                    current,
                    reason="cancelled while provider outcome was not terminal",
                )
            await self._lifecycle.mark_cleanup(current, MissionRunCleanupState.PENDING)
            return
        if result.status == "succeeded":
            await self._lifecycle.mark_succeeded(current, result)
            return
        if result.status == "failed":
            await self._lifecycle.mark_failed(current, result=result)
            return
        current = await self._lifecycle.mark_interrupted(
            current,
            reason=f"governed run returned non-terminal status {result.status!r}",
        )
        await self._lifecycle.mark_cleanup(current, MissionRunCleanupState.PENDING)


__all__ = ["MissionRunExecutor", "MissionRunSupervisor"]
