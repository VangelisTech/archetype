# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Graph family library: relationships as edge entities over the ECS ledger.

Design: ``docs/design/graph-system.md`` (stage 1 — EdgeTable foundation).
Imports only core and third-party packages (design D1); every layer may
import it. Async helpers are canonical; :mod:`archetype.graph.sync` holds
the R5 sync-parity counterparts.
"""

from archetype.graph import sync
from archetype.graph.cascade import CascadeResult, cascade, dangling_edges
from archetype.graph.components import ChildOf, Policy, Relation, require_relation
from archetype.graph.edges import WorldLike, edges, link, unlink
from archetype.graph.frames import between, live_edge_ids, with_source, with_target
from archetype.graph.prefab import (
    AssetRef,
    DefinedPrefab,
    Instance,
    InstantiationPolicy,
    IsA,
    NodeRef,
    Prefab,
    PrefabEdge,
    PrefabNode,
    PrefabNodeKey,
    PrefabTemplate,
    define,
    instantiate,
    only_prefabs,
    prefab_frame,
    without_prefabs,
)
from archetype.graph.view import GraphView

__all__ = [
    "AssetRef",
    "CascadeResult",
    "ChildOf",
    "DefinedPrefab",
    "GraphView",
    "Instance",
    "InstantiationPolicy",
    "IsA",
    "NodeRef",
    "Policy",
    "Prefab",
    "PrefabEdge",
    "PrefabNode",
    "PrefabNodeKey",
    "PrefabTemplate",
    "Relation",
    "cascade",
    "dangling_edges",
    "define",
    "instantiate",
    "only_prefabs",
    "prefab_frame",
    "WorldLike",
    "between",
    "edges",
    "link",
    "live_edge_ids",
    "require_relation",
    "sync",
    "unlink",
    "with_source",
    "with_target",
    "without_prefabs",
]
