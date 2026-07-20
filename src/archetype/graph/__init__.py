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
from archetype.graph.depth import Depth, DepthProcessor, toposorted
from archetype.graph.edges import WorldLike, edges, link, unlink
from archetype.graph.frames import between, live_edge_ids, with_source, with_target
from archetype.graph.prefab import IsA, Prefab, instantiate, instantiate_sync
from archetype.graph.traverse import ancestors, descendants, neighborhood
from archetype.graph.view import GraphView

__all__ = [
    "CascadeResult",
    "ChildOf",
    "Depth",
    "DepthProcessor",
    "GraphView",
    "IsA",
    "Policy",
    "Prefab",
    "Relation",
    "WorldLike",
    "ancestors",
    "between",
    "cascade",
    "dangling_edges",
    "descendants",
    "edges",
    "instantiate",
    "instantiate_sync",
    "link",
    "live_edge_ids",
    "neighborhood",
    "require_relation",
    "sync",
    "toposorted",
    "unlink",
    "with_source",
    "with_target",
]
