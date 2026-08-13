# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported runtime and world adapters for coding-agent missions."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from functools import wraps
from typing import TYPE_CHECKING, Any, Concatenate

from daft import DataFrame
from uuid_utils import uuid7

from archetype.core.config import StorageConfig
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    MissionResult,
    MissionSubmission,
    SubmittedMission,
)
from archetype.missions.models import RestoreMissionSandbox, RunMission, SubmitMission
from archetype.missions.sandboxes import CheckpointRef, SandboxIdentity
from archetype.missions.trajectories.models import (
    GradeTrajectory,
    IngestClaudeTranscript,
    QueryTrajectory,
    QueryTranscriptRows,
)

if TYPE_CHECKING:
    from pathlib import Path

    from archetype.core.component import Component
    from archetype.evaluation.contracts import GraderOutput, TrajectoryGrader
    from archetype.missions.trajectories.contracts import (
        ClaudeTranscriptSource,
        TrajectorySelection,
        TranscriptIngestionResult,
    )


def _admitted_mission_operation[**P, R](
    operation: Callable[Concatenate[Missions, P], Awaitable[R]],
) -> Callable[Concatenate[Missions, P], Awaitable[R]]:
    """Keep one complete public mission call inside process and owner admission."""

    @wraps(operation)
    async def admitted(self: Missions, *args: P.args, **kwargs: P.kwargs) -> R:
        async with self._resources.admit_operation():
            continuation = self._reservation.operation_admitted()
            # Reject an explicitly closed handle through its public contract
            # before the process owner can expose an inventory error.
            self._ensure_open(continuation=continuation)
            async with self._resources.admit_owner_operation(self._reservation):
                self._ensure_open(continuation=continuation)
                return await operation(self, *args, **kwargs)

    return admitted


class Missions:
    """Batteries-included authoring handle for one mission-capable world."""

    def __init__(
        self,
        runtime: Any,
        name: str = "agent-missions",
        *,
        config: AgentMissionConfig,
        storage: str | Path | StorageConfig | None = None,
        owner_id: str | None = None,
        reservation: Any | None = None,
    ) -> None:
        self._runtime = runtime
        self._resources = runtime._resources
        self._dispatcher = self._resources.dispatcher
        self._owner_id = owner_id or f"mission:{uuid7()}"
        self._name = name
        self._config = config
        self._storage = storage
        created_reservation = reservation is None
        self._reservation = (
            reservation
            if reservation is not None
            else self._resources.reserve_owner(
                self._owner_id,
                phase="workflow-handles",
                closed_message="Agent Missions handle is closed",
            )
        )
        # A directly constructed typed adapter is the supported surface. Keep
        # it strongly reachable from the process owner just like a handle
        # obtained through the dynamic library lookup.
        if created_reservation:
            self._reservation.retain_anchor(self)
        self._operation_admission = self._reservation.operation_admission
        self._close_lock = asyncio.Lock()
        self._public_closing = False
        self._public_closed = False

    async def __aenter__(self) -> Missions:
        self._ensure_open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    @_admitted_mission_operation
    async def submit(
        self,
        *,
        repository: str,
        branch: str,
        tasks: Sequence[AgentTask],
        name: str = "agent-mission",
        base_ref: str = "main",
    ) -> SubmittedMission:
        """Persist one coding mission and return its durable identity."""

        return await self._dispatcher.apply(
            SubmitMission(
                owner_id=self._owner_id,
                name=self._name,
                config=self._config,
                storage=self._storage,
                submission=MissionSubmission(
                    repository=repository,
                    branch=branch,
                    tasks=tuple(tasks),
                    name=name,
                    base_ref=base_ref,
                ),
            )
        )

    @_admitted_mission_operation
    async def run(
        self,
        mission: SubmittedMission,
        *,
        max_ticks: int | None = None,
    ) -> MissionResult:
        """Run a submitted mission to a terminal result."""

        return await self._dispatcher.apply(
            RunMission(
                owner_id=self._owner_id,
                name=self._name,
                config=self._config,
                storage=self._storage,
                mission=mission,
                max_ticks=max_ticks,
            )
        )

    @_admitted_mission_operation
    async def restore_sandbox(
        self,
        mission: SubmittedMission,
        checkpoint: CheckpointRef,
    ) -> SandboxIdentity:
        """Fail explicitly until checkpoint identity is bound into Activity admission."""

        return await self._dispatcher.apply(
            RestoreMissionSandbox(
                owner_id=self._owner_id,
                mission=mission,
                checkpoint=checkpoint,
            )
        )

    @_admitted_mission_operation
    async def query(self, *components: type[Component]) -> DataFrame:
        """Query persisted mission state through the underlying world read path."""

        service = self._resources.owner(self._owner_id).require_bound()
        return await service.query(*components)

    @property
    def world_id(self):
        """Return the activated world's durable identity."""

        self._ensure_open()
        service = self._resources.owner(self._owner_id).require_bound()
        return service.world_id

    async def close(self) -> None:
        """Release this mission workflow handle and its world reservation."""

        if self._reservation.operation_admitted():
            raise RuntimeError("Agent Missions handle cannot close from an admitted operation")
        self._reservation.ensure_close_allowed()
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        del from_runtime
        self._reservation.request_operation_stop()
        async with self._close_lock:
            if self._public_closed or self._reservation_released():
                self._public_closed = True
                return
            self._public_closing = True
            await self._reservation.aclose()
            self._public_closed = True

    def _ensure_open(self, *, continuation: bool | None = None) -> None:
        self._runtime._ensure_open()
        if continuation is None:
            continuation = self._reservation.operation_admitted()
        if (
            self._public_closing or self._public_closed or self._reservation_released()
        ) and not continuation:
            raise RuntimeError("Agent Missions handle is closed")

    def _reservation_released(self) -> bool:
        return self._reservation.released


class MissionWorld:
    """Mission transcript and trajectory capabilities over one generic world."""

    def __init__(self, world: Any) -> None:
        self._world = world

    @property
    def world(self) -> Any:
        """Return the wrapped generic world handle."""

        return self._world

    async def ingest_claude_transcript(
        self,
        source: ClaudeTranscriptSource,
    ) -> TranscriptIngestionResult:
        """Redact and index one Claude JSONL session as mission evidence."""

        async def dispatch(world_id: Any, storage_config: Any, dispatcher: Any) -> Any:
            assert storage_config is not None
            return await dispatcher.apply(
                IngestClaudeTranscript(
                    world_id=world_id,
                    source=source,
                    storage_config=storage_config,
                )
            )

        return await self._world._call_library(
            dispatch,
            capability="ingest_claude_transcript",
            require_storage=True,
        )

    async def transcript_rows(self) -> DataFrame:
        """Return normalized coding-agent transcript rows for this run."""

        async def dispatch(world_id: Any, storage_config: Any, dispatcher: Any) -> Any:
            assert storage_config is not None
            return await dispatcher.apply(
                QueryTranscriptRows(
                    world_id=world_id,
                    storage_config=storage_config,
                )
            )

        return await self._world._call_library(
            dispatch,
            capability="query_transcript_rows",
            require_storage=True,
        )

    async def query_trajectory(
        self,
        component_type: type[Component],
        *,
        selection: TrajectorySelection | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """Return one trajectory table filtered by its typed index fields."""

        async def dispatch(world_id: Any, storage_config: Any, dispatcher: Any) -> Any:
            info = await _world_info(dispatcher, world_id, storage_config)
            return await dispatcher.apply(
                QueryTrajectory(
                    component=component_type,
                    world_id=world_id,
                    run_id=info.run_id,
                    storage_config=storage_config,
                    selection=selection,
                    ticks=tuple(ticks) if ticks is not None else None,
                    entity_ids=tuple(entity_ids) if entity_ids is not None else None,
                )
            )

        return await self._world._call_library(
            dispatch,
            capability="query_trajectory",
        )

    async def grade_trajectory(
        self,
        component_type: type[Component],
        *,
        graders: Sequence[TrajectoryGrader],
        selection: TrajectorySelection | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> list[GraderOutput]:
        """Query one trajectory table and run graders over the lazy frame."""

        async def dispatch(world_id: Any, storage_config: Any, dispatcher: Any) -> Any:
            info = await _world_info(dispatcher, world_id, storage_config)
            return await dispatcher.apply(
                GradeTrajectory(
                    component=component_type,
                    world_id=world_id,
                    run_id=info.run_id,
                    graders=tuple(graders),
                    storage_config=storage_config,
                    selection=selection,
                    ticks=tuple(ticks) if ticks is not None else None,
                    entity_ids=tuple(entity_ids) if entity_ids is not None else None,
                )
            )

        return await self._world._call_library(
            dispatch,
            capability="grade_trajectory",
        )


async def _world_info(dispatcher: Any, world_id: Any, storage_config: Any) -> Any:
    """Resolve one live or storage-addressed descriptor for trajectory reads."""

    from archetype.world.models import GetWorldInfo, OpenWorldReadonly

    try:
        return await dispatcher.apply(GetWorldInfo(world_id=world_id))
    except Exception:
        if storage_config is None:
            raise
        return await dispatcher.apply(
            OpenWorldReadonly(
                storage_config=storage_config,
                world_id=world_id,
            )
        )


__all__ = ["MissionWorld", "Missions"]
