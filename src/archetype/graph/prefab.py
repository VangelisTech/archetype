# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""PreFabs: template entities instantiated by copy, with lineage (stage 7, D5).

A prefab is an ordinary entity carrying the :class:`Prefab` marker, its
component values, and optionally a ``ChildOf`` subtree of child prefabs.
``instantiate`` copies the template: component values are materialized onto
fresh entities, overrides applied, the subtree rebuilt with new ids, and an
``IsA`` edge recorded from every new entity to the prefab entity it copies.

There is no runtime resolution (design D5): editing a prefab does not mutate
instances. Re-instantiation under the edited prefab is the upgrade path, and
both generations stay on the ledger — which is what makes prefab populations
gradeable. The library is world content: prefabs version, fork, and grade
like everything else.

Reads come from a :class:`GraphView` snapshot, so one ``instantiate`` call is
internally consistent at the view's captured tick. Spawned copies land as raw
initial conditions at the next persisted tick, like every staged spawn.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.graph.components import ChildOf, Relation
from archetype.graph.edges import WorldLike, _require_async, link
from archetype.graph.sync import SyncWorldLike, _require_sync
from archetype.graph.sync import link as link_sync
from archetype.graph.view import GraphSnapshot, GraphView, _same_component


class Prefab(Component):
    """Marks an entity as a template. The marker is never copied to instances."""

    name: str = ""


class IsA(Relation):
    """Lineage: ``source`` (the instance) was instantiated from ``target``.

    ``world`` and ``at_tick`` make the lineage globally unambiguous and
    version-pinned (registry design R2): ``world`` is the source world id
    when the template lives in a different world (empty for same-world), and
    ``at_tick`` is the captured tick the copy was taken from.
    """

    world: str = ""
    at_tick: int = -1


def _rows(frame: DataFrame) -> list[dict]:
    """Materialize planning rows for instantiation.

    Mutation-planning boundary: spawning copies requires concrete component
    values and child ids; the entity filters run in the lazy plan.
    """
    return frame.to_pylist()


def _entity_components(view: GraphView | GraphSnapshot, entity_id: int) -> list[Component]:
    """Rebuild the entity's component instances from the captured frames.

    ``Prefab`` markers and ``Relation`` components are never copied: the
    marker is what makes a template a template, and edges are their own
    entities.
    """
    for signature, frame in view.frames():
        live = frame
        if "is_active" in live.column_names:
            live = live.where(col("is_active"))
        matched = _rows(live.where(cast(Expression, col("entity_id") == entity_id)).limit(1))
        if not matched:
            continue
        row = matched[0]
        components: list[Component] = []
        for cls in signature:
            # Exclusion honors schema identity for the marker: a resumed
            # world's twin Prefab class is not an issubclass of ours, and
            # copying it would silently turn instances into templates.
            # Relation exclusion stays hierarchical — relation subclasses
            # have distinct schemas by design, and edges are their own
            # entities, never components on a template root.
            if issubclass(cls, Prefab | Relation) or _same_component(cls, Prefab):
                continue
            prefix = cls.get_prefix()
            components.append(cls(**{f: row[f"{prefix}{f}"] for f in cls.model_fields}))
        return components
    raise LookupError(f"entity {entity_id} not found at the view's captured tick")


def _child_prefabs(view: GraphView | GraphSnapshot, parent: int) -> list[int]:
    """Direct ``ChildOf`` children of ``parent`` at the captured tick."""
    edges = view.frame(ChildOf)
    if edges is None:
        return []
    prefix = ChildOf.get_prefix()
    rows = _rows(
        edges.where(cast(Expression, col(f"{prefix}target") == parent)).select(f"{prefix}source")
    )
    return [row[f"{prefix}source"] for row in rows]


def _overlay(components: list[Component], overrides: Sequence[Component]) -> list[Component]:
    """Overlay ``overrides`` onto ``components`` by schema identity.

    Class-object identity under-matches on resumed durable worlds whose
    signatures hold schema-identical twin classes; the same identity rule as
    :meth:`GraphView.frame` applies here so an override always replaces its
    counterpart instead of colliding with it at spawn.
    """
    for override in overrides:
        if issubclass(type(override), Prefab | Relation) or _same_component(type(override), Prefab):
            raise ValueError(
                f"override {type(override).__name__} is not copyable: markers and "
                "relations never attach to instances"
            )
    remaining = list(overrides)
    out: list[Component] = []
    for comp in components:
        match = next((o for o in remaining if _same_component(type(o), type(comp))), None)
        if match is not None:
            remaining.remove(match)
            out.append(match)
        else:
            out.append(comp)
    return out + remaining


_BOUNDARY = """The relation-copy boundary (registry design R7): instantiate
copies component values, recursively copies the ``ChildOf`` subtree, rebuilds
only ``ChildOf`` edges, and records ``IsA`` provenance. No other relation —
catalog structure, assignments, sockets, supply lines — is copied, and no
source-to-instance id map is exposed. Domain wiring belongs in rule entities
interpreted by a driver after instantiation; broadening this function is a
deliberate future API (``InstantiationResult(root_id, id_map)``), never a
silent change."""


async def instantiate(
    world: WorldLike,
    view: GraphView,
    prefab: int,
    overrides: Sequence[Component] = (),
    *,
    max_depth: int = 8,
) -> int:
    """Copy ``prefab`` (and its ``ChildOf`` subtree) onto fresh entities.

    Returns the new root's entity id. ``overrides`` overlay the root's
    components by type; subtree copies are verbatim. Every new entity gets an
    ``IsA`` edge to the prefab entity it was copied from — carrying the
    source world (when it differs) and the captured tick — and the subtree's
    ``ChildOf`` wiring is rebuilt between the new ids.
    """
    _require_async(world, "spawn")
    target_world = str((await world.info()).world_id)
    # One frozen snapshot for the whole recursion: a source world ticking
    # mid-instantiation must not split the copy across versions.
    snap = view.snapshot()
    source_world = "" if snap.world_id in ("", target_world) else snap.world_id
    return await _instantiate(world, snap, prefab, overrides, max_depth, source_world)


instantiate.__doc__ = (instantiate.__doc__ or "") + "\n\n    " + _BOUNDARY


async def _instantiate(
    world: WorldLike,
    view: GraphSnapshot,
    prefab: int,
    overrides: Sequence[Component],
    max_depth: int,
    source_world: str,
) -> int:
    lineage = IsA(target=prefab, world=source_world, at_tick=view.tick)
    new_id = await world.spawn(*_overlay(_entity_components(view, prefab), overrides))
    lineage.source = new_id
    await link(world, lineage)
    if max_depth > 0:
        for child in _child_prefabs(view, prefab):
            new_child = await _instantiate(world, view, child, (), max_depth - 1, source_world)
            await link(world, ChildOf(source=new_child, target=new_id))
    return new_id


def instantiate_sync(
    world: SyncWorldLike,
    view: GraphView,
    prefab: int,
    overrides: Sequence[Component] = (),
    *,
    max_depth: int = 8,
) -> int:
    """Blocking counterpart of :func:`instantiate` (runtime.md R5 parity)."""
    _require_sync(world, "spawn")
    target_world = str(world.info().world_id)
    snap = view.snapshot()
    source_world = "" if snap.world_id in ("", target_world) else snap.world_id
    return _instantiate_sync(world, snap, prefab, overrides, max_depth, source_world)


instantiate_sync.__doc__ = (instantiate_sync.__doc__ or "") + "\n\n    " + _BOUNDARY


def _instantiate_sync(
    world: SyncWorldLike,
    view: GraphSnapshot,
    prefab: int,
    overrides: Sequence[Component],
    max_depth: int,
    source_world: str,
) -> int:
    lineage = IsA(target=prefab, world=source_world, at_tick=view.tick)
    new_id = world.spawn(*_overlay(_entity_components(view, prefab), overrides))
    lineage.source = new_id
    link_sync(world, lineage)
    if max_depth > 0:
        for child in _child_prefabs(view, prefab):
            new_child = _instantiate_sync(world, view, child, (), max_depth - 1, source_world)
            link_sync(world, ChildOf(source=new_child, target=new_id))
    return new_id
