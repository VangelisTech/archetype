# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dataframe-first evaluation service.

EvalService does not own simulation, experiment lifecycle, or durable scoring
schema. It finds persisted rows through QueryService and runs caller-provided
graders over Daft DataFrames.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from inspect import isawaitable
from typing import Any

from daft import DataFrame, col
from uuid_utils import UUID

from archetype.app.models import EpisodeResult
from archetype.app.query_service import QueryService
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.experiments.trajectories import Trajectory

GraderOutput = object
GraderReturn = GraderOutput | Sequence[GraderOutput]
TrajectoryGrader = Callable[[DataFrame], GraderReturn | Awaitable[GraderReturn]]


class EvalService:
    """Query persisted rows and execute graders over DataFrames.

    The returned analysis surface is a Daft DataFrame. Durable scores remain
    components written by the caller when a score should persist.
    """

    def __init__(self, query_service: QueryService) -> None:
        self._query_service = query_service

    async def query_components(
        self,
        components: Sequence[type[Component]],
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame:
        """Return persisted component rows as a Daft DataFrame."""
        return await self._query_service.query_components(
            list(components),
            world_id=str(world_id),
            run_id=str(run_id),
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=lineage,
        )

    async def query_episode(
        self,
        episode: EpisodeResult,
        *,
        components: Sequence[type[Component]],
        run_id: str | UUID | None = None,
        storage_config: StorageConfig | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame:
        """Return component rows produced during one episode."""
        active_run_id = run_id or episode.run_id
        if active_run_id is None:
            raise ValueError("query_episode requires episode.run_id or run_id")
        return await self.query_components(
            components,
            world_id=episode.world_id,
            run_id=active_run_id,
            storage_config=storage_config,
            ticks=list(range(int(episode.start_tick), int(episode.final_tick))),
            entity_ids=entity_ids,
            lineage=lineage,
        )

    async def query_trajectory_component(
        self,
        component: type[Component] = Trajectory,
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
        trajectory_ids: Sequence[str] | None = None,
        episode_ids: Sequence[str] | None = None,
        rollout_ids: Sequence[str] | None = None,
        task_ids: Sequence[str] | None = None,
        trial_idxs: Sequence[int] | None = None,
    ) -> DataFrame:
        """Return one typed trajectory component filtered by suite target.

        Components such as ``Trajectory``, ``TrajectoryReward``, and
        ``TrajectoryObservation`` each have their own table shape. A grader that
        needs multiple trajectory tables should request each DataFrame it needs.
        """
        df = await self.query_components(
            [component],
            world_id=world_id,
            run_id=run_id,
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=lineage,
        )
        filters = {
            "trajectory_id": trajectory_ids,
            "episode_id": episode_ids,
            "rollout_id": rollout_ids,
            "task_id": task_ids,
            "trial_idx": trial_idxs,
        }
        return _filter_component_rows(df, component, filters)

    async def run_graders(
        self,
        df: DataFrame,
        graders: Sequence[TrajectoryGrader],
    ) -> list[GraderOutput]:
        """Execute graders over a DataFrame and flatten their outputs."""
        results: list[GraderOutput] = []
        for grader in graders:
            raw = grader(df)
            output = await raw if isawaitable(raw) else raw
            if isinstance(output, Sequence) and not isinstance(output, str | bytes):
                results.extend(output)
            else:
                results.append(output)
        return results

    async def grade_trajectory_component(
        self,
        component: type[Component] = Trajectory,
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        graders: Sequence[TrajectoryGrader],
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
        trajectory_ids: Sequence[str] | None = None,
        episode_ids: Sequence[str] | None = None,
        rollout_ids: Sequence[str] | None = None,
        task_ids: Sequence[str] | None = None,
        trial_idxs: Sequence[int] | None = None,
    ) -> list[GraderOutput]:
        """Query one trajectory component and execute graders over it."""
        df = await self.query_trajectory_component(
            component,
            world_id=world_id,
            run_id=run_id,
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=lineage,
            trajectory_ids=trajectory_ids,
            episode_ids=episode_ids,
            rollout_ids=rollout_ids,
            task_ids=task_ids,
            trial_idxs=trial_idxs,
        )
        return await self.run_graders(df, graders)


def _filter_component_rows(
    df: DataFrame,
    component: type[Component],
    filters: dict[str, Sequence[Any] | None],
) -> DataFrame:
    prefix = component.get_prefix()
    model_fields = getattr(component, "model_fields", {})
    for field_name, values in filters.items():
        if values is None or field_name not in model_fields:
            continue
        df = df.where(col(f"{prefix}{field_name}").is_in(list(values)))
    return df
