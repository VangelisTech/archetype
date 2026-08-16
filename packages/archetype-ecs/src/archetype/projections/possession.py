# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The possession read model: one entity's relational field of view (stage 3).

The FPS half of the RTS/FPS operating model: ``possession`` computes what one
entity can reach through its relations — both directions, labeled, with hop
distances — as a single lazy frame. Fog of war is a ``where`` clause on this
frame; stepping into an agent's seat is rendering it.

This module declares the projections family's first family-to-family edge:
it consumes :mod:`archetype.graph` traversal (granted in
``quality/architecture.d/projections.toml``; acyclic — graph does not import
projections).
"""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Protocol, cast

from daft import DataFrame, Expression, col, lit

from archetype.core.component import Component
from archetype.graph.components import Relation
from archetype.graph.traverse import neighborhood


class QueriesLike(Protocol):
    """Structural surface the async possession helper needs from a handle."""

    async def query(self, *components: type[Component]) -> DataFrame: ...


def possession(
    edges_by_relation: Sequence[tuple[type[Relation], DataFrame]],
    entity: int,
    depth: int = 1,
) -> DataFrame:
    """What ``entity`` reaches through each relation, in both directions.

    Returns a lazy frame of ``entity_id``, ``hops``, ``relation``, and
    ``direction`` (``"out"`` follows source→target, ``"in"`` the reverse),
    excluding the possessed entity itself. Historical points of view are the
    caller passing historical edge slices.
    """
    if not edges_by_relation:
        raise ValueError("possession requires at least one (relation, edges) pair")
    parts: list[DataFrame] = []
    for rel, edges in edges_by_relation:
        for direction in ("out", "in"):
            reached = neighborhood(edges, rel, [entity], depth, direction=direction)
            beyond_root = cast(Expression, col("hops") > 0)  # ty: ignore[unsupported-operator]
            parts.append(
                reached.where(beyond_root)
                .with_column("relation", lit(rel.get_prefix().rstrip("_")))
                .with_column("direction", lit(direction))
            )
    out = parts[0]
    for part in parts[1:]:
        out = out.concat(part)
    return out.sort(["relation", "direction", "hops"])


def _require_async(world: QueriesLike) -> None:
    if not inspect.iscoroutinefunction(world.query):
        raise TypeError(
            "world.query is not async; for SyncRuntimeWorld-shaped handles "
            "use archetype.projections.sync"
        )


async def possession_view(
    world: QueriesLike,
    entity: int,
    *relations: type[Relation],
    depth: int = 1,
    at: int | None = None,
) -> DataFrame:
    """Handle sugar: query each relation's edges and compose ``possession``.

    ``at`` slices every relation to one tick; the default reads full history,
    which callers typically narrow first. A relation with no committed table
    surfaces the engine's missing-table error, matching ``graph.edges``.
    """
    if not relations:
        raise ValueError("possession_view requires at least one relation type")
    _require_async(world)
    pairs: list[tuple[type[Relation], DataFrame]] = []
    for rel in relations:
        frame = await world.query(rel)
        if at is not None:
            frame = frame.where(cast(Expression, col("tick") == at))
        pairs.append((rel, frame))
    return possession(pairs, entity, depth)
