# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported runtime handle for batteries-included coding-agent missions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

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
        )
        self._closed = False

    async def __aenter__(self) -> RuntimeMissions:
        self._ensure_open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

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
                mission=mission,
                max_ticks=max_ticks,
            )
        )

    async def restore_sandbox(
        self,
        mission: SubmittedMission,
        checkpoint: CheckpointRef,
    ) -> SandboxIdentity:
        """Explicitly restore the mission's process-local sandbox before running."""

        self._ensure_open()
        return await self._dispatcher.apply(
            RestoreMissionSandbox(
                owner_id=self._owner_id,
                mission=mission,
                checkpoint=checkpoint,
            )
        )

    async def close(self) -> None:
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        del from_runtime
        if self._closed:
            return
        await self._reservation.aclose()
        self._closed = True

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

    def _ensure_open(self) -> None:
        self._runtime._ensure_open()
        if self._closed:
            raise RuntimeError("Agent Missions handle is closed")


__all__ = ["RuntimeMissions"]
