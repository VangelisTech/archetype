# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed Research operations over one Archetype world handle."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any, cast, overload

from uuid_utils import UUID

from archetype.research.models import (
    AutoResearch,
    AutoResearchConfig,
    AutoResearchResult,
    CandidatePreparer,
    Evaluator,
    IterationResult,
)


@overload
def _in_thread(callback: None) -> None: ...


@overload
def _in_thread[CallbackT: Callable[..., Any]](callback: CallbackT) -> CallbackT: ...


def _in_thread[CallbackT: Callable[..., Any]](
    callback: CallbackT | None,
) -> CallbackT | None:
    """Move synchronous callbacks off-loop while preserving their protocol."""

    if callback is None or inspect.iscoroutinefunction(callback):
        return callback

    async def invoke(*args: Any, **kwargs: Any) -> Any:
        result = await asyncio.to_thread(callback, *args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    return cast(CallbackT, invoke)


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

    async def _sync_autoresearch(
        self,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        """Preserve sync-handle callback re-entry through worker threads."""

        return await self.autoresearch(
            config,
            _in_thread(evaluator),
            prepare_candidate=_in_thread(prepare_candidate),
            lab_world_id=lab_world_id,
            on_iteration=_in_thread(on_iteration),
        )


__all__ = ["Research"]
