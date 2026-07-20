# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example relations: catalog structure and live-world semantics stay distinct."""

from __future__ import annotations

from archetype.graph import Policy, Relation


class AssetChildOf(Relation):
    """Catalog hierarchy: asset or collection ``source`` belongs under ``target``.

    This is deliberately not :class:`archetype.graph.ChildOf`.  ``ChildOf``
    means instantiated composition and runtime lifetime; using a separate
    relation keeps catalog folders from being copied into every unit.
    """

    exclusive = True
    on_delete_target = Policy.DELETE


class AssignedTo(Relation):
    """Operational assignment: one live assignment per source unit."""

    exclusive = True


class CommandedBy(Relation):
    """Command authority: one live commander per source formation."""

    exclusive = True


class SupplyLine(Relation):
    """A directed supply edge with domain payload."""

    throughput: int = 0


class Targets(Relation):
    """A unit's current target; acquiring another target replaces the first."""

    exclusive = True


class VisibleTo(Relation):
    """``source`` currently has visibility of ``target``."""
