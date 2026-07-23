# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported runtime handle for batteries-included coding-agent missions."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING

from daft import DataFrame

from archetype.core.config import StorageConfig
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    MissionResult,
    SubmittedMission,
)
from archetype.missions.sandboxes import CheckpointRef, SandboxIdentity
from archetype.runtime.world import _runtime_cleanup_scope

if TYPE_CHECKING:
    from pathlib import Path

    from archetype.core.component import Component
    from archetype.runtime.runtime import ArchetypeRuntime


class RuntimeMissions:
    """Small authoring facade over one mission-capable world."""

    def __init__(
        self,
        runtime: ArchetypeRuntime,
        name: str,
        *,
        config: AgentMissionConfig,
        storage: str | Path | StorageConfig | None = None,
    ) -> None:
        self._runtime = runtime
        self._service = runtime._agent_mission_service(
            world_factory=runtime.world,
            name=name,
            config=config,
            storage=storage,
        )
        self._shutdown_lock = asyncio.Lock()
        self._closed = False

    async def __aenter__(self) -> RuntimeMissions:
        if self._closed:
            raise RuntimeError("Agent Missions handle is closed")
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
        return await self._service.submit(
            repository=repository,
            branch=branch,
            tasks=tasks,
            name=name,
            base_ref=base_ref,
        )

    async def run(
        self,
        mission: SubmittedMission,
        *,
        max_ticks: int | None = None,
    ) -> MissionResult:
        return await self._service.run(mission, max_ticks=max_ticks)

    async def restore_sandbox(
        self,
        mission: SubmittedMission,
        checkpoint: CheckpointRef,
    ) -> SandboxIdentity:
        """Explicitly restore the mission's process-local sandbox before running."""

        return await self._service.restore_sandbox(mission, checkpoint)

    async def close(self) -> None:
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        async with self._shutdown_lock:
            if self._closed:
                return
            if from_runtime:
                await self._service.close()
            else:
                # A public close admitted before runtime shutdown must retain
                # cleanup authority while a concurrent runtime close waits.
                with _runtime_cleanup_scope(self._runtime):
                    await self._service.close()
            self._closed = True
            self._runtime._unregister_mission_handle(self)

    async def query(self, *components: type[Component]) -> DataFrame:
        """Query persisted mission state through the underlying world read path."""

        return await self._service.query(*components)

    @property
    def world_id(self):
        """Return the activated world's durable identity."""

        return self._service.world_id


__all__ = ["RuntimeMissions"]
