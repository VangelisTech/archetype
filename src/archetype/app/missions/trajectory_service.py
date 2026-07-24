# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application composition for querying and grading mission trajectories."""

from __future__ import annotations

from collections.abc import Sequence

from daft import DataFrame

from archetype.app.evaluation.interfaces import iEvaluationService
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.evaluation.contracts import GraderOutput, TrajectoryGrader
from archetype.missions.trajectories import TrajectorySelection, filter_trajectory_rows
from archetype.storage.interfaces import iStorageService
from archetype.world import query


class TrajectoryService:
    """Compose persisted query access with optional evaluation graders."""

    def __init__(
        self,
        storage_service: iStorageService,
        evaluation_service: iEvaluationService,
    ) -> None:
        self._storage = storage_service
        self._evaluation_service = evaluation_service

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
    ) -> DataFrame:
        """Return a lazily filtered persisted trajectory component table."""
        frame = await query.query_components(
            self._storage,
            [component],
            world_id=world_id,
            run_id=run_id,
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=lineage,
        )
        return filter_trajectory_rows(frame, component, selection or TrajectorySelection())

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
    ) -> list[GraderOutput]:
        """Query one trajectory table, then delegate grader execution."""
        frame = await self.query(
            component,
            world_id=world_id,
            run_id=run_id,
            selection=selection,
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=lineage,
        )
        return await self._evaluation_service.run_graders(frame, graders)
