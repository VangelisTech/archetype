# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Hierarchy depth as stored data (stage 6, docs/design/graph-system.md).

Sander's breadth-first trick — sorting query caches by hierarchy depth —
lands here as a column. Entities that carry :class:`Depth` get their level
recomputed every tick by :class:`DepthProcessor` from the live ``ChildOf``
edges: a root is depth 0, a child is its parent's depth plus one. The
processor reads the previous tick through :class:`GraphView`, so depth
propagates one level per tick and a D-level hierarchy converges in D ticks;
re-parenting reconverges the same way. Top-down iteration is then
``frame.sort("depth__value")`` — no traversal engine, no cache to
invalidate.

Wiring::

    view = GraphView()
    world = runtime.world(
        "sim",
        processors=[DepthProcessor()],
        resources=[view],
        hooks=[(PostTick, view.on_post_tick)],
    )
    await world.spawn(Node(), Depth())   # participate in depth ordering
"""

from __future__ import annotations

from daft import DataFrame, col, lit

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources
from archetype.graph.components import ChildOf
from archetype.graph.view import GraphView


class Depth(Component):
    """Hierarchy level: 0 for roots, parent's depth + 1 otherwise."""

    value: int = 0


class DepthProcessor(AsyncProcessor):
    """Recompute ``Depth`` from live ``ChildOf`` edges, one level per tick.

    Fully lazy: two left joins against the previous tick's edge and depth
    frames, no materialization. Before the first tick (or with no edges) the
    frame passes through unchanged.
    """

    components = (Depth,)
    priority = 20

    async def process(
        self, df: DataFrame, resources: Resources | None = None, **kwargs
    ) -> DataFrame:
        view = resources.get(GraphView) if resources is not None else None
        if view is None or view.tick < 0:
            return df
        edges = view.frame(ChildOf)
        parents = view.frame(Depth)
        if edges is None or parents is None:
            # No edges (or no prior depth) at the captured tick: every entity
            # is a root. Rewrite to 0 so seeded or stale values cannot
            # persist — the root contract holds even before any hierarchy.
            return df.with_column("depth__value", lit(0))

        prefix = ChildOf.get_prefix()
        # Exclusive ChildOf guarantees one live parent per child at persisted
        # state; the max() collapse also absorbs the documented same-batch race.
        child_parent = (
            edges.select(
                col(f"{prefix}source").alias("__child"),
                col(f"{prefix}target").alias("__parent"),
            )
            .groupby("__child")
            .agg(col("__parent").max().alias("__parent"))
        )
        parent_depth = parents.select(
            col("entity_id").alias("__parent_id"),
            col("depth__value").alias("__parent_depth"),
        )
        return (
            df.join(child_parent, left_on=col("entity_id"), right_on=col("__child"), how="left")
            .join(parent_depth, left_on=col("__parent"), right_on=col("__parent_id"), how="left")
            .with_column("depth__value", (col("__parent_depth") + 1).fill_null(0))
            .exclude("__child", "__parent", "__parent_id", "__parent_depth")
        )


def toposorted(frame: DataFrame) -> DataFrame:
    """Order a Depth-carrying frame top-down: parents before children."""
    return frame.sort("depth__value")
