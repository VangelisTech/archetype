# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed Research operations over one Archetype world handle."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from uuid_utils import UUID

from archetype.research.models import (
    AutoResearch,
    AutoResearchConfig,
    AutoResearchResult,
    CandidatePreparer,
    Evaluator,
    IterationResult,
)


class Research:
    """Expose AutoResearch workflows for one world.

    Construct this adapter directly or through ``world.library("research")``.
    Research is intentionally a small world-library workflow, not a second
    process host or an application facade.
    """

    def __init__(self, world: Any) -> None:
        self._world = world

    async def autoresearch(
        self,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        """Run or resume an AutoResearch loop from this base world."""

        async def dispatch(world_id: object, _storage: Any, dispatcher: Any) -> Any:
            return await dispatcher.apply(
                AutoResearch(
                    world_id=cast(str | UUID, world_id),
                    config=config,
                    evaluator=evaluator,
                    prepare_candidate=prepare_candidate,
                    lab_world_id=lab_world_id,
                    on_iteration=on_iteration,
                )
            )

        return await self._world._call_library(
            dispatch,
            capability="autoresearch",
        )


__all__ = ["Research"]
