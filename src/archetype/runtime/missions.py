# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported runtime handle for batteries-included coding-agent missions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from daft import DataFrame

from archetype.app.missions.agent_service import AgentMissionService
from archetype.core.config import StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import GraphView
from archetype.missions.coding_agents import (
    AgentMissionSandboxResource,
    TaskExecutionOutbox,
    agent_mission_processors,
)
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    MissionResult,
    SubmittedMission,
)

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
        view = GraphView()
        outbox = TaskExecutionOutbox()
        world = runtime.world(
            name,
            storage=storage,
            processors=list(agent_mission_processors()),
            resources=[
                view,
                outbox,
                AgentMissionSandboxResource(config.sandbox),
            ],
            hooks=[
                (PostTick, view.on_post_tick),
                (PostTick, outbox.on_post_tick),
            ],
        )
        self._world = world
        self._service = AgentMissionService(
            world=world,
            view=view,
            outbox=outbox,
            sandbox=config.sandbox,
            max_ticks=config.max_ticks,
        )
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

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures: list[BaseException] = []
        for close in (self._service.close, self._world.shutdown):
            try:
                await close()
            except BaseException as exc:
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup(
                f"Agent Missions shutdown failed for {len(failures)} operation(s)",
                failures,
            )

    async def query(self, *components: type[Component]) -> DataFrame:
        """Query persisted mission state through the underlying world read path."""

        return await self._world.query(*components)

    @property
    def world_id(self):
        """Return the activated world's durable identity."""

        return self._world.world_id


__all__ = ["RuntimeMissions"]
