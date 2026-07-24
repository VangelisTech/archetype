# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Snapshot-pinned subject and durable receipt reads."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from daft import DataFrame

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.evaluation.components import EvalReceipt
from archetype.storage.interfaces import iStorageService
from archetype.world import query

EVALUATION_RESULTS_TABLE = "evaluation_results"


@dataclass(frozen=True)
class PinnedEvaluationSnapshot:
    """Immutable simulation visibility captured for one evaluation."""

    run_id: str
    tick: int
    head_tokens: tuple[str, ...]
    visibility_tokens: tuple[str, ...]
    storage_config: StorageConfig


async def pin_snapshot(
    storage: iStorageService,
    *,
    world_id: str,
    storage_config: StorageConfig,
) -> PinnedEvaluationSnapshot:
    """Capture the exact published visibility used by one grader."""

    visibility = await storage.pin_visibility(storage_config, world_id)
    if visibility.head_tick is None:
        raise RuntimeError(
            f"world {world_id} has no published visibility to evaluate "
            "(step it at least once first)"
        )
    return PinnedEvaluationSnapshot(
        run_id=visibility.run_id,
        tick=visibility.head_tick,
        head_tokens=visibility.head_tokens,
        visibility_tokens=visibility.visibility_tokens or (),
        storage_config=storage_config,
    )


async def read_pinned_subject(
    storage: iStorageService,
    snapshot: PinnedEvaluationSnapshot,
    *,
    world_id: str,
    components: Sequence[type[Component]],
    ticks: Sequence[int] | None = None,
    entity_ids: Sequence[int] | None = None,
) -> DataFrame:
    """Build the lazy component frame against the captured manifest tokens."""

    selected_ticks = (
        [int(tick) for tick in ticks if int(tick) <= snapshot.tick]
        if ticks is not None
        else list(range(snapshot.tick + 1))
    )
    return await query.query_components(
        storage,
        list(components),
        world_id,
        snapshot.run_id,
        snapshot.storage_config,
        ticks=selected_ticks,
        entity_ids=(
            [int(entity_id) for entity_id in entity_ids] if entity_ids is not None else None
        ),
        visibility_tokens=list(snapshot.visibility_tokens),
    )


async def read_result(
    storage: iStorageService,
    *,
    world_id: str,
    evaluation_id: str,
    storage_config: StorageConfig,
) -> EvalReceipt | None:
    """Return one persisted evaluation receipt without widening its world/run."""

    try:
        rows = await storage.read_world_rows(
            storage_config,
            world_id,
            EVALUATION_RESULTS_TABLE,
        )
    except KeyError:
        return None
    matched = rows.where(
        rows["evaluation_id"] == evaluation_id  # ty: ignore[invalid-argument-type]
    ).limit(1)
    values = (await storage.materialize(matched)).to_pydict()
    if not values.get("evaluation_id"):
        return None
    return EvalReceipt(**{name: values[name][0] for name in EvalReceipt.model_fields})


__all__ = [
    "EVALUATION_RESULTS_TABLE",
    "PinnedEvaluationSnapshot",
    "pin_snapshot",
    "read_pinned_subject",
    "read_result",
]
