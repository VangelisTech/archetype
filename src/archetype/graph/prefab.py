# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Prefabs: non-executing entity graphs and their instantiation.

Design: ``docs/design/prefab-library.md`` (the Stage 7 slice of
``docs/design/graph-system.md``). A prefab is a tagged entity graph that
defines inheritable component state, nested structure, and per-component
instantiation behavior for a class of runtime entity graphs. A prefab library
is a queryable ``ChildOf`` hierarchy of such graphs.

This module ships the authoring surface (``PrefabTemplate``/``PrefabNode``),
the ``define`` reflection step that spawns a template as ``Prefab``-tagged
asset entities, and ``instantiate`` — the graph operation that reserves the
whole instance graph's ids, remaps internal edges onto them, preserves edges
into shared library assets, applies per-component ``InstantiationPolicy`` plus
overrides, and records ``IsA`` lineage. It never mutates the prefab
(design D5): re-instantiation under a new prefab version is the upgrade path,
and both generations stay on the ledger.

Imports only core and the graph family (design D1); no core or app changes.
Per-component policy is read from an optional ``on_instantiate`` class var, so
components opt in without touching ``archetype.core``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from daft import DataFrame, col

from archetype.core.component import Component
from archetype.graph.components import ChildOf, Relation
from archetype.graph.edges import WorldLike, link


class InstantiationPolicy(StrEnum):
    """How a component crosses the asset -> instance boundary (design PD6).

    ``INHERIT`` marks the value as belonging to the asset; in v1 it materializes
    as a copy (honoring the graph-system non-goal "no runtime inheritance
    resolution"), and the ``IsA`` lineage edge plus this tag are exactly what a
    later stage needs to switch it to resolve-at-query. ``COPY`` (the default)
    duplicates the value onto the instance, ``RESET`` gives the instance the
    component's field defaults, and ``OMIT`` leaves the component off entirely.
    """

    INHERIT = "inherit"
    COPY = "copy"
    RESET = "reset"
    OMIT = "omit"


class Prefab(Component):
    """Asset marker: this entity is a prefab node, not a live entity.

    The marker is the only thing that distinguishes an asset from an instance;
    both otherwise share representation (design D2/PD1). Live queries exclude
    assets with :func:`without_prefabs`; authoring queries select them with
    :func:`only_prefabs`.
    """


class PrefabNodeKey(Component):
    """Stable per-lineage identity of an authored node (design PD4).

    Copied onto instance children so an instance node's correspondence to its
    authored node is answerable through ``IsA``. Carrying the key does not make
    an entity an asset — only :class:`Prefab` does.
    """

    key: str = ""


class IsA(Relation):
    """Inheritance-and-instancing lineage: ``source`` is an instance/variant of ``target``.

    Non-exclusive (design PD3): a prefab may derive from several bases, and each
    instance child carries its own ``IsA`` back to the authored node. Edges are
    lineage records, never resolved at runtime in v1.
    """


# --- authoring surface (plain dataclasses; NOT components) ------------------


@dataclass(frozen=True)
class NodeRef:
    """An edge target inside the prefab graph — remapped at instantiation."""

    key: str


@dataclass(frozen=True)
class AssetRef:
    """An edge target that is a shared library asset — preserved at instantiation."""

    entity_id: int


@dataclass(frozen=True)
class PrefabEdge:
    """A relation authored from a node to a :class:`NodeRef` or :class:`AssetRef`."""

    relation: type[Relation]
    target: NodeRef | AssetRef


@dataclass
class PrefabNode:
    """A child node: stable key, component values, and outgoing edges."""

    key: str
    components: list[Component] = field(default_factory=list)
    edges: list[PrefabEdge] = field(default_factory=list)


@dataclass
class PrefabTemplate:
    """The root of a prefab: its own state plus authored children (design PD7)."""

    key: str
    components: list[Component] = field(default_factory=list)
    edges: list[PrefabEdge] = field(default_factory=list)
    children: list[PrefabNode] = field(default_factory=list)


@dataclass
class DefinedPrefab:
    """A template reflected into the world as ``Prefab``-tagged asset entities."""

    template: PrefabTemplate
    asset_ids: dict[str, int]  # node key -> asset entity id

    @property
    def root_id(self) -> int:
        return self.asset_ids[self.template.key]


@dataclass
class Instance:
    """The result of instantiation: the instance root and every instance node id."""

    root_id: int
    node_ids: dict[str, int]  # node key -> instance entity id


class PrefabWorld(WorldLike, Protocol):
    """The world surface :func:`instantiate` needs beyond :class:`WorldLike`."""

    async def reserve_ids(self, n: int) -> list[int]: ...

    async def spawn_reserved(self, entity_id: int, *components: Component) -> None: ...


# --- frame-pure helpers ------------------------------------------------------


def _all_nodes(template: PrefabTemplate) -> list[PrefabNode]:
    """Root first (as a node), then authored children — the instantiation order.

    Raises ``ValueError`` on a duplicate node key: keys are the stable identity
    edges remap against (design PD4), so a collision would silently corrupt the
    instance graph. Fail loud before any id is reserved.
    """
    root = PrefabNode(template.key, template.components, template.edges)
    nodes = [root, *template.children]
    keys = [node.key for node in nodes]
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    if duplicates:
        raise ValueError(f"prefab {template.key!r} has duplicate node keys: {duplicates}")
    return nodes


def _copies(components: list[Component]) -> list[Component]:
    """Deep-copy so asset/instance rows never alias the caller's template objects."""
    return [c.model_copy(deep=True) for c in components]


def _apply_policy(components: list[Component]) -> list[Component]:
    """Transform a node's components by each type's ``on_instantiate`` policy (PD6)."""
    out: list[Component] = []
    for component in components:
        policy = getattr(type(component), "on_instantiate", InstantiationPolicy.COPY)
        if policy is InstantiationPolicy.OMIT:
            continue
        if policy is InstantiationPolicy.RESET:
            out.append(type(component)())
        else:  # COPY, or INHERIT materialized as a copy in v1
            out.append(component.model_copy(deep=True))
    return out


def _apply_overrides(
    components: list[Component], overrides: dict[type[Component], Component]
) -> list[Component]:
    """Override wins by component type; unmentioned components are kept, new ones added."""
    if not overrides:
        return components
    by_type: dict[type[Component], Component] = {type(c): c for c in components}
    by_type.update(overrides)
    return list(by_type.values())


def _resolve_target(edge: PrefabEdge, node_ids: dict[str, int]) -> int:
    """Internal edge (``NodeRef``) remaps to the instance id; shared edge is preserved."""
    target = edge.target
    if isinstance(target, NodeRef):
        return node_ids[target.key]
    return target.entity_id


# --- query helpers (PD1) -----------------------------------------------------


async def prefab_frame(world: WorldLike) -> DataFrame | None:
    """The ``Prefab`` asset table (lazy), or ``None`` if no assets exist yet.

    Feed the result to :func:`without_prefabs` / :func:`only_prefabs`. Returning
    the frame rather than a Python id list keeps the exclusion an anti-join, so
    nothing materializes until the caller's terminal collect.
    """
    try:
        return await world.query(Prefab)
    except KeyError:
        return None


def without_prefabs(frame: DataFrame, prefabs: DataFrame | None) -> DataFrame:
    """Drop asset rows from a live query as an anti-join on ``entity_id`` (design PD1).

    ``prefabs`` comes from :func:`prefab_frame`; ``None`` (no assets yet) leaves
    the frame untouched. Lazy in, lazy out — mirrors ``graph.dangling_edges``.
    """
    if prefabs is None:
        return frame
    return frame.join(prefabs, left_on=col("entity_id"), right_on=col("entity_id"), how="anti")


def only_prefabs(frame: DataFrame, prefabs: DataFrame | None) -> DataFrame:
    """Keep only asset rows — the authoring query, a semi-join on ``entity_id`` (PD1)."""
    if prefabs is None:
        return frame.limit(0)
    return frame.join(prefabs, left_on=col("entity_id"), right_on=col("entity_id"), how="semi")


# --- define / instantiate ----------------------------------------------------


async def define(world: PrefabWorld, template: PrefabTemplate) -> DefinedPrefab:
    """Reflect a template into the world as ``Prefab``-tagged asset entities.

    Spawns the root and each child as an asset carrying :class:`Prefab`, its
    :class:`PrefabNodeKey`, and its authored components; wires the asset
    ``ChildOf`` namespace and the authored relations among assets. Staged like
    any spawn — the caller steps to persist. Returns the id map instantiation
    needs for ``IsA`` lineage and shared-asset references.
    """
    asset_ids: dict[str, int] = {}
    for node in _all_nodes(template):
        asset_ids[node.key] = await world.spawn(
            Prefab(), PrefabNodeKey(key=node.key), *_copies(node.components)
        )
    root = asset_ids[template.key]
    for child in template.children:
        await link(world, ChildOf(source=asset_ids[child.key], target=root))
    for node in _all_nodes(template):
        source = asset_ids[node.key]
        for edge in node.edges:
            target = (
                asset_ids[edge.target.key]
                if isinstance(edge.target, NodeRef)
                else edge.target.entity_id
            )
            await link(world, edge.relation(source=source, target=target))
    return DefinedPrefab(template, asset_ids)


async def instantiate(
    world: PrefabWorld,
    prefab: DefinedPrefab,
    *,
    overrides: dict[type[Component], Component] | None = None,
    node_overrides: dict[str, dict[type[Component], Component]] | None = None,
) -> Instance:
    """Materialize a fresh instance graph from a defined prefab (design PD7).

    Reserves ids for the whole graph up front, so internal edges are remapped
    onto the instance's own nodes before any row is written; edges into shared
    library assets keep their target. Applies per-component policy (PD6) then
    ``overrides`` (root) / ``node_overrides`` (per node key). Records the scene
    ``ChildOf`` hierarchy among instance nodes and one ``IsA`` lineage edge per
    node back to its authored asset. Staged in one batch; the caller steps.

    The prefab is never mutated.
    """
    template = prefab.template
    all_nodes = _all_nodes(template)
    reserved = await world.reserve_ids(len(all_nodes))
    node_ids = {node.key: reserved[index] for index, node in enumerate(all_nodes)}
    root_key = template.key

    for node in all_nodes:
        components = _apply_policy(node.components)
        node_ov: dict[type[Component], Component] = {}
        if node_overrides and node.key in node_overrides:
            node_ov.update(node_overrides[node.key])
        if node.key == root_key and overrides:
            node_ov.update(overrides)
        components = _apply_overrides(components, node_ov)
        # Instance nodes carry the node key for correspondence, never the Prefab marker.
        await world.spawn_reserved(node_ids[node.key], PrefabNodeKey(key=node.key), *components)

    root_id = node_ids[root_key]
    for child in template.children:  # scene hierarchy: instance owns its children
        await link(world, ChildOf(source=node_ids[child.key], target=root_id))
    for node in all_nodes:  # lineage / correspondence
        await link(world, IsA(source=node_ids[node.key], target=prefab.asset_ids[node.key]))
    for node in all_nodes:  # authored edges, internal remapped and shared preserved
        source = node_ids[node.key]
        for edge in node.edges:
            target = _resolve_target(edge, node_ids)
            await link(world, edge.relation(source=source, target=target))

    return Instance(root_id, node_ids)
