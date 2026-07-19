# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Relation components: edges as entities (design D2, docs/design/graph-system.md).

A relation is a ``Component`` subclass with ``source`` and ``target`` entity-id
fields. An edge is an ordinary entity carrying exactly one relation instance,
so the relation subclass's archetype table is that relation's EdgeTable and
edges inherit ticks, ``is_active``, persistence, and fork lineage. Pairs never
enter the archetype signature: this is the non-fragmenting representation.

Subclasses define concrete relations::

    class ChildOf(Relation):
        pass

Payload is ordinary component fields on the subclass, subject to the same
Arrow-serialization rules as any component.
"""

from __future__ import annotations

from archetype.core.component import Component


class Relation(Component):
    """Base class for edge components.

    ``Relation`` itself is abstract by convention: spawn subclasses, never the
    base (``link`` enforces this). ``source`` and ``target`` are entity ids as
    stamped by the engine (``BASE_SCHEMA.entity_id``).
    """

    source: int = 0
    target: int = 0


def require_relation(rel: Relation) -> None:
    """Reject non-edge components and the abstract base before they spawn."""
    if not isinstance(rel, Relation):
        raise TypeError(f"link requires a Relation instance, got {type(rel).__name__}")
    if type(rel) is Relation:
        raise TypeError("spawn a Relation subclass, not Relation itself")
