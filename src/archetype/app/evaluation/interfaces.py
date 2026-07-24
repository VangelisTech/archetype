# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the evaluation family."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from daft import DataFrame
from uuid_utils import UUID

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.evaluation.contracts import (
    GraderOutput,
    GraderReturn,
    TrajectoryGrader,
)
from archetype.world.models import EpisodeResult

# PR-5 deletes these compatibility exports after application consumers repoint.


@runtime_checkable
class iEvaluationService(Protocol):
    """Pin, grade, validate, and publish evaluation evidence."""

    async def evaluate(
        self,
        world_id: str,
        components: Sequence[type[Component]],
        *,
        contract: Any,
        grader: TrajectoryGrader,
        evaluation_id: str,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> Any: ...
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
    ) -> DataFrame: ...
    async def query_episode(
        self,
        episode: EpisodeResult,
        *,
        components: Sequence[type[Component]],
        run_id: str | UUID | None = None,
        storage_config: StorageConfig | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...
    async def run_graders(
        self, df: DataFrame, graders: Sequence[TrajectoryGrader]
    ) -> list[GraderOutput]: ...


__all__ = [
    "GraderOutput",
    "GraderReturn",
    "TrajectoryGrader",
    "iEvaluationService",
]
