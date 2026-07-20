# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Driver-level cascade: apply a relation's cleanup policy, one generation
per invocation.

Design D4 as amended (docs/design/graph-system.md; decision recorded on
issue #552, option (a)): processors are pure ``DataFrame → DataFrame`` and
cannot stage mutations, so cleanup runs at the driver level. ``cascade``
reads liveness from a :class:`GraphView`, finds the relation's live edges
whose target is no longer alive, and applies the relation's
``on_delete_target`` policy through the world API. Despawns are staged like
any mutation and land at the next persisted tick — calling ``cascade`` once
per step yields exactly the one-generation-per-tick propagation the design
promises, with every generation on the ledger.

Typical driver loop::

    view = GraphView()
    world = runtime.world("sim", resources=[view], hooks=[(PostTick, view.on_post_tick)])
    ...
    await world.despawn(parent)
    await world.step()
    await cascade(world, ChildOf, view)   # children staged for despawn
    await world.step()                    # ... landing here
"""

from __future__ import annotations

from dataclasses import dataclass, field

from typing import cast

from daft import DataFrame, Expression, col

from archetype.graph.components import Policy, Relation
from archetype.graph.edges import WorldLike, _require_async
from archetype.graph.view import GraphView


@dataclass(frozen=True, slots=True)
class CascadeResult:
    """What one cascade pass did (or, for ``FLAG``, found)."""

    policy: Policy
    removed_edges: tuple[int, ...] = field(default=())
    deleted_entities: tuple[int, ...] = field(default=())
    flagged_edges: tuple[int, ...] = field(default=())

    @property
    def total(self) -> int:
        return len(self.removed_edges) + len(self.deleted_entities) + len(self.flagged_edges)


def dangling_edges(edges: DataFrame, rel: type[Relation], population: DataFrame) -> DataFrame:
    """Frame-pure: the local-target edges whose target is absent from
    ``population``.

    Liveness as an anti-join — no per-row Python, no lookups. Local liveness
    is only decidable for local targets: a relation carrying a non-empty
    ``world`` payload (cross-world lineage such as ``IsA``) points at a
    foreign world's entity id, which is out of this anti-join's jurisdiction
    and never dangles here. Cross-world reconciliation is a world-aware
    process, not the same-world cascade helper.
    """
    prefix = rel.get_prefix()
    world_col = f"{prefix}world"
    if world_col in edges.column_names:
        local_scope = cast(Expression, col(world_col) == "")
        edges = edges.where(local_scope)
    return edges.join(
        population,
        left_on=col(f"{prefix}target"),
        right_on=col("entity_id"),
        how="anti",
    )


def _plan(view: GraphView, rel: type[Relation]) -> list[dict]:
    """Materialize the (edge id, source) pairs of the dangling edges.

    Mutation-planning boundary: despawn takes concrete ids. Everything up to
    this point — liveness union, anti-join, tick filter — runs lazily.
    """
    population = view.population()
    edges = view.frame(rel)
    if population is None or edges is None:
        return []
    prefix = rel.get_prefix()
    return (
        dangling_edges(edges, rel, population)
        .select(col("entity_id"), col(f"{prefix}source").alias("source"))
        .to_pylist()
    )


async def cascade(world: WorldLike, rel: type[Relation], view: GraphView) -> CascadeResult:
    """Apply ``rel.on_delete_target`` to the dangling edges, one generation.

    Reads liveness at the view's captured tick; stages mutations through the
    world handle, landing at the next persisted tick. Deleting a generation
    of sources creates the next generation's dangling edges — the following
    ``cascade`` call collects them.
    """
    _require_async(world, "despawn")
    rows = _plan(view, rel)
    policy = rel.on_delete_target
    if not rows:
        return CascadeResult(policy=policy)
    edge_ids = tuple(row["entity_id"] for row in rows)
    if policy is Policy.FLAG:
        return CascadeResult(policy=policy, flagged_edges=edge_ids)
    for edge_id in edge_ids:
        await world.despawn(edge_id)
    if policy is Policy.REMOVE:
        return CascadeResult(policy=policy, removed_edges=edge_ids)
    sources = tuple({row["source"] for row in rows})
    for source in sources:
        await world.despawn(source)
    return CascadeResult(policy=policy, removed_edges=edge_ids, deleted_entities=sources)
