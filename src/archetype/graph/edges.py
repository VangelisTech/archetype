# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Handle-level edge operations, async flavor (design D2, docs/design/graph-system.md).

The helpers accept any object that structurally matches :class:`WorldLike` —
the async runtime world handle satisfies it. The graph family imports only
core (design D1), so the coupling is a protocol, not an import.

Sync-parity counterparts for ``SyncRuntimeWorld``-shaped handles live in
:mod:`archetype.graph.sync` (runtime.md R5). Passing a sync handle here
raises immediately, before any mutation.
"""

from __future__ import annotations

import inspect
from typing import Protocol, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.graph.components import Relation, require_relation
from archetype.graph.frames import live_edge_ids, live_edge_ids_from


class _InfoLike(Protocol):
    tick: int


class WorldLike(Protocol):
    """Structural surface the async edge helpers need from a world handle."""

    async def spawn(self, *components: Component) -> int: ...

    async def despawn(self, entity_id: int) -> None: ...

    async def query(self, *components: type[Component]) -> DataFrame: ...

    async def info(self) -> _InfoLike: ...


def _require_async(world: WorldLike, method: str) -> None:
    """Fail loud on sync handles before any call, instead of `await <int>`."""
    if not inspect.iscoroutinefunction(getattr(world, method)):
        raise TypeError(
            f"world.{method} is not async; for SyncRuntimeWorld-shaped handles "
            "use archetype.graph.sync"
        )


async def link(world: WorldLike, rel: Relation) -> int:
    """Spawn an edge entity for ``rel``.

    The edge lands as raw initial conditions at the next persisted tick, like
    every staged spawn. For an ``exclusive`` relation, the live edges from the
    same source are staged for despawn in the same batch: old edge out, new
    edge in, both landing at the next persisted tick. Replacement reads
    persisted state, so the step boundary is the consistency unit.
    """
    require_relation(rel)
    _require_async(world, "spawn")
    rel_type = type(rel)
    if rel_type.exclusive:
        latest = (await world.info()).tick - 1
        try:
            frame = await world.query(rel_type)
        except KeyError:
            frame = None  # first edge of this relation: nothing to replace
        if frame is not None:
            for edge_id in live_edge_ids_from(frame, rel_type, rel.source, latest):
                await world.despawn(edge_id)
    return await world.spawn(rel)


async def edges(world: WorldLike, rel: type[Relation], *, at: int | None = None) -> DataFrame:
    """The relation's EdgeTable as a lazy frame.

    Returns the full append-only history; ``at`` filters to the edges live at
    one tick. Temporal reads are filters, never a feature (design D2). A
    relation that has never been committed surfaces the engine's missing-table
    error: reading a table that does not exist is a caller error.
    """
    _require_async(world, "query")
    frame = await world.query(rel)
    if at is not None:
        frame = frame.where(cast(Expression, col("tick") == at))
    return frame


async def unlink(world: WorldLike, rel: type[Relation], source: int, target: int) -> int:
    """Despawn the ``rel`` edges from ``source`` to ``target``.

    "Live" means: has a row at the latest persisted tick. Edges staged by
    ``link`` but not yet persisted by a step are not found. Unlink is
    idempotent removal, so a relation that has never been committed is an
    empty edge set, not an error. Returns the number of edges despawned.
    """
    _require_async(world, "query")
    latest = (await world.info()).tick - 1
    try:
        frame = await world.query(rel)
    except KeyError:
        # The component-query path raises KeyError when other signatures
        # exist but none contain this relation: nothing to remove.
        return 0
    edge_ids = live_edge_ids(frame, rel, source, target, latest)
    for edge_id in edge_ids:
        await world.despawn(edge_id)
    return len(edge_ids)
