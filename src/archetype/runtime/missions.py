# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported runtime handle for batteries-included coding-agent missions."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from daft import DataFrame

from archetype.core.config import StorageConfig
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    MissionResult,
    SubmittedMission,
)
from archetype.missions.sandboxes import CheckpointRef, SandboxIdentity
from archetype.runtime.world import RuntimeWorld, _runtime_cleanup_scope

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

        mission_world: RuntimeWorld | None = None

        def world_factory(*args: Any, **kwargs: Any) -> RuntimeWorld:
            nonlocal mission_world
            mission_world = runtime.world(*args, **kwargs)
            return mission_world

        self._service = runtime._agent_mission_service(
            world_factory=world_factory,
            name=name,
            config=config,
            storage=storage,
        )
        if mission_world is None:
            raise RuntimeError("Agent Missions service did not create its mission world")
        self._world = mission_world
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
            # Public and runtime callers share one exact cleanup capability.
            # Keeping the parameter distinguishes the internal protocol while
            # preventing that capability from admitting unrelated worlds.
            _ = from_runtime
            with _runtime_cleanup_scope(self._world._state):
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
