# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Handle-level edge operations (design D2, docs/design/graph-system.md).

The helpers accept any object that structurally matches :class:`WorldLike` —
the runtime world handle satisfies it. The graph family imports only core
(design D1), so the coupling is a protocol, not an import.
"""

from __future__ import annotations

from typing import Protocol

from daft import DataFrame, col

from archetype.core.component import Component
from archetype.graph.components import Relation
from archetype.graph.frames import between


class _InfoLike(Protocol):
    tick: int


class WorldLike(Protocol):
    """Structural surface the edge helpers need from a world handle."""

    async def spawn(self, *components: Component) -> int: ...

    async def despawn(self, entity_id: int) -> None: ...

    async def query(self, *components: type[Component]) -> DataFrame: ...

    async def info(self) -> _InfoLike: ...


async def link(world: WorldLike, rel: Relation) -> int:
    """Spawn an edge entity for ``rel``.

    The edge lands as raw initial conditions at the next persisted tick, like
    every staged spawn. Exclusive-relation replacement arrives in stage 5a;
    today ``link`` is deliberately a thin seam over ``spawn``.
    """
    if type(rel) is Relation:
        raise TypeError("spawn a Relation subclass, not Relation itself")
    return await world.spawn(rel)


async def edges(world: WorldLike, rel: type[Relation], *, at: int | None = None) -> DataFrame:
    """The relation's EdgeTable as a lazy frame.

    Returns the full append-only history; ``at`` filters to the edges live at
    one tick. Temporal reads are filters, never a feature (design D2).
    """
    frame = await world.query(rel)
    if at is not None:
        frame = frame.where(col("tick") == at)
    return frame


async def unlink(world: WorldLike, rel: type[Relation], source: int, target: int) -> int:
    """Despawn the ``rel`` edges from ``source`` to ``target``.

    "Live" means: has a row at the latest persisted tick. Edges staged by
    ``link`` but not yet persisted by a step are not found. Returns the number
    of edges despawned.
    """
    latest = (await world.info()).tick - 1
    matches = between(await world.query(rel), rel, source, target)
    rows = matches.where(col("tick") == latest).select("entity_id").to_pylist()
    edge_ids = {row["entity_id"] for row in rows}
    for edge_id in edge_ids:
        await world.despawn(edge_id)
    return len(edge_ids)
