# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Frame-pure edge filters (design D1, docs/design/graph-system.md).

These functions take and return lazy DataFrames and import nothing above
core, so any layer — scripts, runtime consumers, app services — may use them.
Wildcard queries from the Flecs vocabulary are plain ``where`` filters here:
``(rel, *)`` is :func:`with_source`, ``(*, target)`` is :func:`with_target`.
"""

from __future__ import annotations

from typing import cast

from daft import DataFrame, Expression, col

from archetype.graph.components import Relation

# Daft stubs type Expression.__eq__ as bool; cast records the real type.


def with_source(edges: DataFrame, rel: type[Relation], source: int) -> DataFrame:
    """Edges of ``rel`` outgoing from ``source`` — the ``(rel, *)`` wildcard."""
    return edges.where(cast(Expression, col(f"{rel.get_prefix()}source") == source))


def with_target(edges: DataFrame, rel: type[Relation], target: int) -> DataFrame:
    """Edges of ``rel`` incoming to ``target`` — the ``(*, target)`` wildcard."""
    return edges.where(cast(Expression, col(f"{rel.get_prefix()}target") == target))


def between(edges: DataFrame, rel: type[Relation], source: int, target: int) -> DataFrame:
    """Edges of ``rel`` from ``source`` to ``target``."""
    prefix = rel.get_prefix()
    matches_source = cast(Expression, col(f"{prefix}source") == source)
    matches_target = cast(Expression, col(f"{prefix}target") == target)
    return edges.where(matches_source & matches_target)


def live_edge_ids(
    edges: DataFrame, rel: type[Relation], source: int, target: int, latest: int
) -> set[int]:
    """Entity ids of the ``rel`` edges from ``source`` to ``target`` live at
    tick ``latest``.

    This is the mutation-planning boundary shared by the async and sync
    ``unlink``: despawn takes concrete ids, so the matching ids — and only
    the ids — cross into Python. The filters run in the lazy plan.
    """
    at_latest = cast(Expression, col("tick") == latest)
    rows = between(edges, rel, source, target).where(at_latest).select("entity_id").to_pylist()
    return {row["entity_id"] for row in rows}
