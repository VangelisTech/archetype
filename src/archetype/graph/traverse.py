# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Frame-pure traversal: bounded-depth reachability as iterated joins.

Stage 2 (docs/design/graph-system.md). No traversal engine, no caches to
invalidate: each hop is a join in the lazy plan, and the result is a frame
of reached entity ids with their minimum hop distance. Cycles are harmless —
reachability is bounded by ``depth`` and duplicates collapse to the shortest
hop count.

Direction is stated from the edge's point of view: ``"out"`` follows
``source → target`` (for ``ChildOf``, child to parent — ancestors), ``"in"``
follows ``target → source`` (parent to children — descendants).
:func:`ancestors` and :func:`descendants` name those two readings.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

import daft
from daft import DataFrame, col, lit

from archetype.graph.components import ChildOf, Relation

Direction = Literal["out", "in"]


def neighborhood(
    edges: DataFrame,
    rel: type[Relation],
    roots: Iterable[int],
    depth: int,
    *,
    direction: Direction = "out",
) -> DataFrame:
    """Entities reachable from ``roots`` in at most ``depth`` hops.

    Returns a frame with ``entity_id`` and ``hops`` (minimum distance,
    ``0`` for the roots themselves). ``edges`` is the relation's frame at
    whatever tick slice the caller chose — traversal over history is the
    caller passing a historical slice.
    """
    if depth < 1:
        raise ValueError("depth must be at least 1")
    root_ids = list(roots)
    if not root_ids:
        raise ValueError("neighborhood requires at least one root")

    prefix = rel.get_prefix()
    from_col, to_col = (f"{prefix}source", f"{prefix}target")
    if direction == "in":
        from_col, to_col = to_col, from_col
    hop_edges = edges.select(col(from_col).alias("__from"), col(to_col).alias("__to")).distinct()

    frontier = daft.from_pydict({"entity_id": root_ids})
    reached = frontier.with_column("hops", lit(0))
    for hop in range(1, depth + 1):
        frontier = (
            frontier.join(hop_edges, left_on=col("entity_id"), right_on=col("__from"))
            .select(col("__to").alias("entity_id"))
            .distinct()
        )
        reached = reached.concat(frontier.with_column("hops", lit(hop)))
    return reached.groupby("entity_id").agg(col("hops").min().alias("hops"))


def ancestors(edges: DataFrame, roots: Iterable[int], depth: int) -> DataFrame:
    """``ChildOf`` reachability child → parent: the chain of command above."""
    return neighborhood(edges, ChildOf, roots, depth, direction="out")


def descendants(edges: DataFrame, roots: Iterable[int], depth: int) -> DataFrame:
    """``ChildOf`` reachability parent → children: the subtree below."""
    return neighborhood(edges, ChildOf, roots, depth, direction="in")
