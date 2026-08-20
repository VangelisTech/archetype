# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""MissionRun transitions, idempotency, and recovery meaning.

This module decides legal durable facts. It does not construct Mission ECS
state and does not start provider work.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from uuid_utils import uuid7

from archetype.missions.contracts import MissionResult, SubmittedMission
from archetype.missions.run_catalog import SqliteMissionRunCatalog, encode_json
from archetype.missions.run_contracts import (
    MISSION_RUN_EVENT_MAX_PAGE,
    MISSION_RUN_EVENT_PHASES,
    MISSION_RUN_EVENT_SCHEMA_VERSION,
    TERMINAL_MISSION_RUN_STATUSES,
    ExecutionProfileIdentity,
    MissionRun,
    MissionRunCleanupState,
    MissionRunConflictError,
    MissionRunEvent,
    MissionRunNotFoundError,
    MissionRunRequest,
    MissionRunStatus,
    mission_request_digest,
    mission_result_from_json,
    mission_result_payload,
    require_mission_run_transition,
    submission_from_json,
    submission_payload,
    task_ids_from_json,
    task_ids_payload,
)

_MAX_EVENT_REASON_CHARS = 512

type ClockMs = Callable[[], int]
type IdentityFactory = Callable[[], str]


def _default_clock_ms() -> int:
    from time import time

    return int(time() * 1000)


def _default_identity() -> str:
    return str(uuid7())


class MissionRunLifecycle:
    """CAS lifecycle authority for one MissionRun catalog."""

    def __init__(
        self,
        catalog: SqliteMissionRunCatalog,
        *,
        now_ms: ClockMs | None = None,
        new_run_id: IdentityFactory | None = None,
        new_world_id: IdentityFactory | None = None,
    ) -> None:
        self._catalog = catalog
        self._now_ms = now_ms or _default_clock_ms
        self._new_run_id = new_run_id or _default_identity
        self._new_world_id = new_world_id or _default_identity

    async def accept(
        self,
        request: MissionRunRequest,
        profile: ExecutionProfileIdentity,
    ) -> MissionRun:
        """Admit one run or return the original run for the same digest."""

        digest = mission_request_digest(request.submission, profile)
        now_ms = self._now_ms()
        inserted = await self._catalog.insert_accepted(
            {
                "run_id": self._new_run_id(),
                "principal": request.principal,
                "idempotency_key": request.idempotency_key,
                "request_digest": digest,
                "profile_id": profile.profile_id,
                "profile_version": profile.version,
                "profile_digest": profile.digest,
                "status": MissionRunStatus.ACCEPTED.value,
                "submission_json": encode_json(submission_payload(request.submission)),
                "world_id": self._new_world_id(),
                "accepted_at_ms": now_ms,
                "created_at_ms": now_ms,
                "updated_at_ms": now_ms,
            },
            events=(self._event("accepted", {"status": "accepted"}, now_ms=now_ms),),
        )
        run = record_to_run(inserted)
        # The canonical digest already folds the pinned profile identity, so
        # one comparison covers both a changed submission and a rolled profile.
        if run.request_digest != digest:
            raise MissionRunConflictError(
                "idempotency key reused with a different canonical mission request"
            )
        return run

    async def get(self, run_id: str) -> MissionRun:
        record = await self._catalog.get(run_id)
        if record is None:
            raise MissionRunNotFoundError(run_id)
        return record_to_run(record)

    async def list_open(self) -> tuple[MissionRun, ...]:
        return tuple(record_to_run(record) for record in await self._catalog.list_open())

    async def list_for_principal(
        self,
        principal: str,
        *,
        limit: int = 100,
    ) -> tuple[MissionRun, ...]:
        """Return one bounded newest-first page owned by this principal."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if limit > MISSION_RUN_EVENT_MAX_PAGE:
            raise ValueError(f"limit must be at most {MISSION_RUN_EVENT_MAX_PAGE}")
        return tuple(
            record_to_run(record)
            for record in await self._catalog.list_for_principal(principal, limit=limit)
        )

    async def mark_running(self, run: MissionRun, *, operation: str) -> MissionRun:
        now_ms = self._now_ms()
        events: tuple[dict[str, object], ...] = ()
        if run.status is not MissionRunStatus.RUNNING:
            events = (self._event("running", {"status": "running"}, now_ms=now_ms),)
        return await self._transition(
            run,
            MissionRunStatus.RUNNING,
            {
                "status": MissionRunStatus.RUNNING.value,
                "active_operation": operation,
                "running_at_ms": run.running_at_ms if run.running_at_ms is not None else now_ms,
                "updated_at_ms": now_ms,
            },
            events=events,
        )

    async def bind_mission(self, run: MissionRun, submitted: SubmittedMission) -> MissionRun:
        if run.mission_id is not None and run.mission_id != submitted.mission_id:
            raise MissionRunConflictError("mission creation is keyed by run_id")
        if run.world_id and submitted.world_id and run.world_id != submitted.world_id:
            raise MissionRunConflictError("world creation is keyed by run_id")
        now_ms = self._now_ms()
        events: tuple[dict[str, object], ...] = ()
        if run.mission_id is None:
            events = (
                self._event(
                    "mission_bound",
                    {
                        "world_id": submitted.world_id or run.world_id,
                        "mission_id": submitted.mission_id,
                        "episode_id": submitted.episode_id,
                        "task_count": len(submitted.task_ids),
                    },
                    now_ms=now_ms,
                ),
            )
        return await self._replace(
            run,
            {
                "world_id": submitted.world_id or run.world_id,
                "mission_id": submitted.mission_id,
                "episode_id": submitted.episode_id,
                "task_ids_json": encode_json(task_ids_payload(submitted.task_ids)),
                "updated_at_ms": now_ms,
            },
            events=events,
        )

    async def record_cancellation_intent(self, run: MissionRun, *, reason: str = "") -> MissionRun:
        if run.cancellation_intent and (not reason or reason == run.cancellation_reason):
            return run
        now_ms = self._now_ms()
        events: list[dict[str, object]] = []
        if not run.cancellation_intent:
            events.append(
                self._event(
                    "cancel_requested",
                    {"reason": reason[:_MAX_EVENT_REASON_CHARS]},
                    now_ms=now_ms,
                )
            )
        values: dict[str, object] = {
            "cancellation_intent": 1,
            "cancellation_reason": reason,
            "updated_at_ms": now_ms,
        }
        if run.status is MissionRunStatus.ACCEPTED:
            require_mission_run_transition(run.status, MissionRunStatus.CANCELLED)
            values.update(
                {
                    "status": MissionRunStatus.CANCELLED.value,
                    "active_operation": "",
                    "terminal_at_ms": now_ms,
                }
            )
            events.append(self._event("cancelled", {"status": "cancelled"}, now_ms=now_ms))
        elif run.status is MissionRunStatus.RUNNING:
            require_mission_run_transition(run.status, MissionRunStatus.CANCELLING)
            values["status"] = MissionRunStatus.CANCELLING.value
            events.append(self._event("cancelling", {"status": "cancelling"}, now_ms=now_ms))
        return await self._replace(run, values, events=tuple(events))

    async def mark_succeeded(self, run: MissionRun, result: MissionResult) -> MissionRun:
        if result.status != "succeeded":
            raise ValueError("MissionRun cannot become succeeded without a succeeded MissionResult")
        return await self._terminal(run, MissionRunStatus.SUCCEEDED, result=result)

    async def mark_failed(
        self,
        run: MissionRun,
        *,
        result: MissionResult | None = None,
        reason: str = "",
    ) -> MissionRun:
        return await self._terminal(
            run,
            MissionRunStatus.FAILED,
            result=result,
            interrupted_reason=reason,
        )

    async def mark_cancelled(
        self,
        run: MissionRun,
        *,
        result: MissionResult | None = None,
    ) -> MissionRun:
        return await self._terminal(run, MissionRunStatus.CANCELLED, result=result)

    async def mark_interrupted(self, run: MissionRun, *, reason: str) -> MissionRun:
        return await self._terminal(
            run,
            MissionRunStatus.INTERRUPTED,
            interrupted_reason=reason,
        )

    async def mark_cleanup(
        self,
        run: MissionRun,
        state: MissionRunCleanupState,
    ) -> MissionRun:
        return await self._replace(
            run,
            {
                "cleanup_state": state.value,
                "updated_at_ms": self._now_ms(),
            },
        )

    async def events(
        self,
        run_id: str,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> tuple[MissionRunEvent, ...]:
        """Return one bounded ordered event page for an existing run."""

        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise ValueError("after must be a non-negative integer cursor")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if limit > MISSION_RUN_EVENT_MAX_PAGE:
            raise ValueError(f"limit must be at most {MISSION_RUN_EVENT_MAX_PAGE}")
        if await self._catalog.get(run_id) is None:
            raise MissionRunNotFoundError(run_id)
        rows = await self._catalog.list_events(run_id, after=after, limit=limit)
        return tuple(
            MissionRunEvent(
                run_id=str(row["run_id"]),
                cursor=_required_int(row["cursor"]),
                event_type=str(row["event_type"]),
                phase=str(row["phase"]),
                payload_json=str(row["payload_json"]),
                created_at_ms=_required_int(row["created_at_ms"]),
                schema_version=_required_int(row["schema_version"]),
            )
            for row in rows
        )

    def _event(
        self,
        event_type: str,
        payload: dict[str, object],
        *,
        now_ms: int,
    ) -> dict[str, object]:
        return {
            "schema_version": MISSION_RUN_EVENT_SCHEMA_VERSION,
            "event_type": event_type,
            "phase": MISSION_RUN_EVENT_PHASES[event_type],
            "payload_json": encode_json(payload),
            "created_at_ms": now_ms,
        }

    async def _terminal(
        self,
        run: MissionRun,
        status: MissionRunStatus,
        *,
        result: MissionResult | None = None,
        interrupted_reason: str = "",
    ) -> MissionRun:
        if run.status in TERMINAL_MISSION_RUN_STATUSES:
            if run.status is not status:
                raise ValueError(
                    f"terminal mission-run {run.run_id} cannot change from "
                    f"{run.status.value} to {status.value}"
                )
            return run
        require_mission_run_transition(run.status, status)
        now_ms = self._now_ms()
        values: dict[str, object] = {
            "status": status.value,
            "active_operation": "",
            "terminal_at_ms": now_ms,
            "updated_at_ms": now_ms,
        }
        if result is not None:
            values["result_json"] = encode_json(mission_result_payload(result))
        if interrupted_reason:
            values["interrupted_reason"] = interrupted_reason
        event = self._event(
            status.value,
            {
                "status": status.value,
                "reason": interrupted_reason[:_MAX_EVENT_REASON_CHARS],
                "has_result": result is not None,
            },
            now_ms=now_ms,
        )
        return await self._replace(run, values, events=(event,))

    async def _transition(
        self,
        run: MissionRun,
        target: MissionRunStatus,
        values: dict[str, object],
        *,
        events: tuple[dict[str, object], ...] = (),
    ) -> MissionRun:
        if run.status is target:
            return await self._replace(run, values, events=events)
        require_mission_run_transition(run.status, target)
        return await self._replace(run, values, events=events)

    async def _replace(
        self,
        run: MissionRun,
        values: dict[str, object],
        *,
        events: tuple[dict[str, object], ...] = (),
    ) -> MissionRun:
        updated = await self._catalog.compare_and_set(
            run.run_id,
            expected_status=run.status.value,
            expected_updated_at_ms=run.updated_at_ms,
            values=values,
            events=events,
        )
        if updated is None:
            current = await self.get(run.run_id)
            if current.status in TERMINAL_MISSION_RUN_STATUSES:
                return current
            raise MissionRunConflictError(f"mission run {run.run_id} changed concurrently")
        return record_to_run(updated)


def record_to_run(record: dict[str, Any]) -> MissionRun:
    """Rehydrate one MissionRun from a physical catalog row."""

    result_json = record.get("result_json")
    return MissionRun(
        run_id=str(record["run_id"]),
        principal=str(record["principal"]),
        idempotency_key=str(record["idempotency_key"]),
        request_digest=str(record["request_digest"]),
        profile=ExecutionProfileIdentity(
            profile_id=str(record["profile_id"]),
            version=str(record["profile_version"]),
            digest=str(record["profile_digest"]),
        ),
        status=MissionRunStatus(str(record["status"])),
        submission=submission_from_json(str(record["submission_json"])),
        world_id=str(record.get("world_id") or ""),
        mission_id=_optional_int(record.get("mission_id")),
        episode_id=str(record.get("episode_id") or ""),
        task_ids=task_ids_from_json(str(record.get("task_ids_json") or "[]")),
        active_operation=str(record.get("active_operation") or ""),
        cancellation_intent=bool(record.get("cancellation_intent")),
        cancellation_reason=str(record.get("cancellation_reason") or ""),
        result=mission_result_from_json(str(result_json)) if result_json else None,
        cleanup_state=MissionRunCleanupState(str(record.get("cleanup_state") or "none")),
        accepted_at_ms=int(record["accepted_at_ms"]),
        running_at_ms=_optional_int(record.get("running_at_ms")),
        terminal_at_ms=_optional_int(record.get("terminal_at_ms")),
        updated_at_ms=int(record["updated_at_ms"]),
        interrupted_reason=str(record.get("interrupted_reason") or ""),
    )


def submitted_from_run(run: MissionRun) -> SubmittedMission:
    """Rebuild the governed SubmittedMission identity bound to one run."""

    if run.mission_id is None or not run.episode_id:
        raise ValueError("mission run has not admitted a Mission yet")
    return SubmittedMission(
        mission_id=run.mission_id,
        task_ids=run.task_ids,
        episode_id=run.episode_id,
        repository=run.submission.repository,
        branch=run.submission.branch,
        base_ref=run.submission.base_ref,
        world_id=run.world_id,
    )


def _required_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("expected an integer event field")
    return value


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("expected an integer timestamp")
    return value


__all__ = [
    "MissionRunLifecycle",
    "record_to_run",
    "submitted_from_run",
]
