# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Frame-pure edge filters (design D1, docs/design/graph-system.md).

These functions take and return lazy DataFrames and import nothing above
core, so any layer — scripts, runtime consumers, app services — may use them.
Wildcard queries from the Flecs vocabulary are plain ``where`` filters here:
``(rel, *)`` is :func:`with_source`, ``(*, target)`` is :func:`with_target`.
"""

from __future__ import annotations

from daft import DataFrame, col

from archetype.graph.components import Relation


def with_source(edges: DataFrame, rel: type[Relation], source: int) -> DataFrame:
    """Edges of ``rel`` outgoing from ``source`` — the ``(rel, *)`` wildcard."""
    return edges.where(col(f"{rel.get_prefix()}source") == source)


def with_target(edges: DataFrame, rel: type[Relation], target: int) -> DataFrame:
    """Edges of ``rel`` incoming to ``target`` — the ``(*, target)`` wildcard."""
    return edges.where(col(f"{rel.get_prefix()}target") == target)


def between(edges: DataFrame, rel: type[Relation], source: int, target: int) -> DataFrame:
    """Edges of ``rel`` from ``source`` to ``target``."""
    prefix = rel.get_prefix()
    return edges.where((col(f"{prefix}source") == source) & (col(f"{prefix}target") == target))
