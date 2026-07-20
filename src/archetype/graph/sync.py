# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Handle-level edge operations, sync flavor (runtime.md R5 parity).

Blocking counterparts of :mod:`archetype.graph.edges` for
``SyncRuntimeWorld``-shaped handles: same semantics, no ``await``. Passing an
async handle here raises immediately, before any mutation.
"""

from __future__ import annotations

import inspect
from typing import Protocol, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.graph.cascade import CascadeResult, _plan
from archetype.graph.components import Policy, Relation, require_relation
from archetype.graph.frames import live_edge_ids, live_edge_ids_from
from archetype.graph.view import GraphView


class _InfoLike(Protocol):
    tick: int


class SyncWorldLike(Protocol):
    """Structural surface the sync edge helpers need from a world handle."""

    def spawn(self, *components: Component) -> int: ...

    def despawn(self, entity_id: int) -> None: ...

    def query(self, *components: type[Component]) -> DataFrame: ...

    def info(self) -> _InfoLike: ...


def _require_sync(world: SyncWorldLike, method: str) -> None:
    """Fail loud on async handles, instead of returning a never-awaited coroutine."""
    if inspect.iscoroutinefunction(getattr(world, method)):
        raise TypeError(
            f"world.{method} is async; for async world handles use archetype.graph.edges"
        )


def link(world: SyncWorldLike, rel: Relation) -> int:
    """Spawn an edge entity for ``rel``. See :func:`archetype.graph.edges.link`.

    Exclusive relations replace the live edges from the same source in the
    same batch, mirroring the async flavor.
    """
    require_relation(rel)
    _require_sync(world, "spawn")
    rel_type = type(rel)
    if not rel_type.exclusive:
        return world.spawn(rel)

    latest = world.info().tick - 1
    try:
        frame = world.query(rel_type)
    except KeyError:
        frame = None  # first edge of this relation: nothing to replace
    replaced = set() if frame is None else live_edge_ids_from(frame, rel_type, rel.source, latest)
    # Spawn before despawn: a failure between the two degrades to the
    # documented two-live-edges race, never to zero live edges.
    edge_id = world.spawn(rel)
    for old_id in replaced:
        world.despawn(old_id)
    return edge_id


def edges(world: SyncWorldLike, rel: type[Relation], *, at: int | None = None) -> DataFrame:
    """The relation's EdgeTable as a lazy frame. See :func:`archetype.graph.edges.edges`."""
    _require_sync(world, "query")
    frame = world.query(rel)
    if at is not None:
        frame = frame.where(cast(Expression, col("tick") == at))
    return frame


def unlink(world: SyncWorldLike, rel: type[Relation], source: int, target: int) -> int:
    """Despawn the live ``rel`` edges from ``source`` to ``target``.

    Same semantics as :func:`archetype.graph.edges.unlink`: idempotent
    removal, an uncommitted relation is an empty edge set.
    """
    _require_sync(world, "query")
    latest = world.info().tick - 1
    try:
        frame = world.query(rel)
    except KeyError:
        return 0
    edge_ids = live_edge_ids(frame, rel, source, target, latest)
    for edge_id in edge_ids:
        world.despawn(edge_id)
    return len(edge_ids)


def cascade(world: SyncWorldLike, rel: type[Relation], view: GraphView) -> CascadeResult:
    """Apply ``rel.on_delete_target`` to the dangling edges, one generation.

    Blocking counterpart of :func:`archetype.graph.cascade.cascade`.
    """
    _require_sync(world, "despawn")
    rows = _plan(view, rel)
    policy = rel.on_delete_target
    if not rows:
        return CascadeResult(policy=policy)
    edge_ids = tuple(row["entity_id"] for row in rows)
    if policy is Policy.FLAG:
        return CascadeResult(policy=policy, flagged_edges=edge_ids)
    for edge_id in edge_ids:
        world.despawn(edge_id)
    if policy is Policy.REMOVE:
        return CascadeResult(policy=policy, removed_edges=edge_ids)
    sources = tuple({row["source"] for row in rows})
    for source in sources:
        world.despawn(source)
    return CascadeResult(policy=policy, removed_edges=edge_ids, deleted_entities=sources)
