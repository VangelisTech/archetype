# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Handle-level projection sugar, sync flavor (runtime.md R5 parity).

Blocking counterpart of :mod:`archetype.projections.worlds` for
``SyncRuntimeWorld``-shaped handles. Passing an async handle here raises
before any query.
"""

from __future__ import annotations

import inspect
from typing import Protocol, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.graph.components import Relation
from archetype.projections.frames import overview
from archetype.projections.possession import possession
from archetype.projections.worlds import require_unique_labels


class SyncQueriesLike(Protocol):
    """Structural surface the sync projection helpers need from a handle."""

    def query(self, *components: type[Component]) -> DataFrame: ...


def _require_sync(world: SyncQueriesLike) -> None:
    if inspect.iscoroutinefunction(world.query):
        raise TypeError(
            "world.query is async; for async world handles use archetype.projections.worlds"
        )


def world_overview(world: SyncQueriesLike, *components: type[Component]) -> DataFrame:
    """Per-tick population series for each component type, labeled by name.

    Label collisions raise, mirroring the async flavor; use
    :func:`archetype.projections.overview` with explicit labels instead.
    """
    if not components:
        raise ValueError("world_overview requires at least one component type")
    _require_sync(world)
    require_unique_labels(components)
    frames = {comp.__name__.lower(): world.query(comp) for comp in components}
    return overview(**frames)


def possession_view(
    world: SyncQueriesLike,
    entity: int,
    *relations: type[Relation],
    depth: int = 1,
    at: int | None = None,
) -> DataFrame:
    """Blocking counterpart of :func:`archetype.projections.possession.possession_view`."""
    if not relations:
        raise ValueError("possession_view requires at least one relation type")
    _require_sync(world)
    pairs: list[tuple[type[Relation], DataFrame]] = []
    for rel in relations:
        frame = world.query(rel)
        if at is not None:
            frame = frame.where(cast(Expression, col("tick") == at))
        pairs.append((rel, frame))
    return possession(pairs, entity, depth)
