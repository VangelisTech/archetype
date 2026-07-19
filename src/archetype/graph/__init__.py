# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Graph family library: relationships as edge entities over the ECS ledger.

Design: ``docs/design/graph-system.md`` (stage 1 — EdgeTable foundation).
Imports only core and third-party packages (design D1); every layer may
import it.
"""

from archetype.graph.components import Relation
from archetype.graph.edges import WorldLike, edges, link, unlink
from archetype.graph.frames import between, with_source, with_target

__all__ = [
    "Relation",
    "WorldLike",
    "between",
    "edges",
    "link",
    "unlink",
    "with_source",
    "with_target",
]
