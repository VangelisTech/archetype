# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Internal application ports owned by the missions family."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.app.evaluation.interfaces import GraderOutput, TrajectoryGrader
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.missions.contracts import AgentTask, MissionResult, SubmittedMission
from archetype.missions.trajectories import TrajectorySelection


@runtime_checkable
class iMissionService(Protocol):
    """Materialize and drive one persisted coding-agent task graph."""

    async def submit(
        self,
        *,
        repository: str,
        branch: str,
        tasks: Sequence[AgentTask],
        name: str = "agent-mission",
        base_ref: str = "main",
    ) -> SubmittedMission: ...

    async def run(
        self,
        mission: SubmittedMission,
        *,
        max_ticks: int | None = None,
    ) -> MissionResult: ...

    async def close(self) -> None: ...

    async def query(self, *components: type[Component]) -> DataFrame: ...

    @property
    def world_id(self) -> object: ...


@runtime_checkable
class iTrajectoryService(Protocol):
    """Query and optionally grade one persisted trajectory table."""

    async def query(
        self,
        component: type[Component],
        *,
        world_id: str,
        run_id: str,
        selection: TrajectorySelection | None = None,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...

    async def grade(
        self,
        component: type[Component],
        *,
        world_id: str,
        run_id: str,
        graders: Sequence[TrajectoryGrader],
        selection: TrajectorySelection | None = None,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> list[GraderOutput]: ...


__all__ = ["iMissionService", "iTrajectoryService"]
