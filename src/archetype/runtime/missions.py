# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported runtime handle for batteries-included coding-agent missions."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from functools import wraps
from typing import TYPE_CHECKING, Concatenate

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
from archetype.missions.models import (
    RestoreMissionSandbox,
    RunMission,
    SubmitMission,
)
from archetype.missions.sandboxes import CheckpointRef, SandboxIdentity

if TYPE_CHECKING:
    from pathlib import Path

    from archetype.core.component import Component
    from archetype.runtime.runtime import ArchetypeRuntime
    from archetype.runtime_resources import OwnerReservation


def _admitted_mission_operation[**P, R](
    operation: Callable[Concatenate[RuntimeMissions, P], Awaitable[R]],
) -> Callable[Concatenate[RuntimeMissions, P], Awaitable[R]]:
    """Keep one complete public mission call inside process and local admission."""

    @wraps(operation)
    async def admitted(self: RuntimeMissions, *args: P.args, **kwargs: P.kwargs) -> R:
        async with self._resources.admit_operation():
            continuation = self._reservation.operation_admitted()
            # A publicly closed or released handle rejects by its own contract
            # here, before the coordinator's owner-registration guard can
            # surface an internal inventory error for the same condition
            # (#627). The recheck inside the owner admission keeps rejection
            # atomic against a close that starts after this preflight.
            self._ensure_open(continuation=continuation)
            async with self._resources.admit_owner_operation(self._reservation):
                self._ensure_open(continuation=continuation)
                return await operation(self, *args, **kwargs)

    return admitted


class RuntimeMissions:
    """Small authoring facade over one mission-capable world."""

    def __init__(
        self,
        runtime: ArchetypeRuntime,
        name: str,
        *,
        config: AgentMissionConfig,
        storage: str | Path | StorageConfig | None = None,
        owner_id: str | None = None,
        reservation: OwnerReservation | None = None,
    ) -> None:
        self._runtime = runtime
        self._resources = runtime._resources
        self._dispatcher = self._resources.dispatcher
        self._owner_id = owner_id or f"mission:{uuid7()}"
        self._name = name
        self._config = config
        self._storage = storage
        self._reservation = reservation or self._resources.reserve_owner(
            self._owner_id,
            phase="workflow-handles",
            closed_message="Agent Missions handle is closed",
        )
        self._operation_admission = self._reservation.operation_admission
        self._close_lock = asyncio.Lock()
        self._public_closing = False
        self._public_closed = False

    async def __aenter__(self) -> RuntimeMissions:
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
        self._ensure_open()
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
        self._ensure_open()
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

        self._ensure_open()
        return await self._dispatcher.apply(
            RestoreMissionSandbox(
                owner_id=self._owner_id,
                mission=mission,
                checkpoint=checkpoint,
            )
        )

    async def close(self) -> None:
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

    @_admitted_mission_operation
    async def query(self, *components: type[Component]) -> DataFrame:
        """Query persisted mission state through the underlying world read path."""

        self._ensure_open()
        service = self._resources.owner(self._owner_id).require_bound()
        return await service.query(*components)

    @property
    def world_id(self):
        """Return the activated world's durable identity."""

        self._ensure_open()
        service = self._resources.owner(self._owner_id).require_bound()
        return service.world_id

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


__all__ = ["RuntimeMissions"]
