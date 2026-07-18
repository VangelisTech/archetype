# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the research family."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from uuid_utils import UUID

from archetype.app.research.contracts import (
    AutoResearchConfig,
    AutoResearchResult,
    CandidatePreparer,
    Evaluator,
    IterationResult,
)


@runtime_checkable
class iResearchService(Protocol):
    """Own autoresearch workflow and durable ledger transitions."""

    async def run(
        self,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult: ...
    async def sweep(
        self,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        param_grid: dict[str, list[Any]],
        *,
        setup_fn: Callable | None = None,
    ) -> dict[tuple, AutoResearchResult]: ...
