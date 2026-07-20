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

from enum import StrEnum
from typing import ClassVar

from archetype.core.component import Component


class Policy(StrEnum):
    """What ``cascade`` does to a relation's edges when their target dies.

    Design D4 (as amended for the driver-level cascade): ``REMOVE`` despawns
    the dangling edge and leaves the source entity alone; ``DELETE`` despawns
    the edge and its source entity (the hierarchy cascade); ``FLAG`` mutates
    nothing and surfaces the dangling edges to the caller — the ledger-world
    replacement for a panic.
    """

    REMOVE = "remove"
    DELETE = "delete"
    FLAG = "flag"


class Relation(Component):
    """Base class for edge components.

    ``Relation`` itself is abstract by convention: spawn subclasses, never the
    base (``link`` enforces this). ``source`` and ``target`` are entity ids as
    stamped by the engine (``BASE_SCHEMA.entity_id``).

    ``exclusive`` declares that an entity holds at most one live edge of this
    relation as ``source`` (design stage 5a): ``link`` stages the replacement
    despawn of the previous edge in the same batch. Enforcement reads
    persisted state — linking the same exclusive source twice before a step
    stages both edges, so the step boundary is the consistency unit.
    """

    exclusive: ClassVar[bool] = False
    on_delete_target: ClassVar[Policy] = Policy.REMOVE

    #: Name of the payload field carrying a foreign-world scope, or ``None``.
    #: Only a declared scope changes semantics (cascade excludes non-empty
    #: scoped rows from local liveness); a payload field that merely happens
    #: to be named ``world`` has no special meaning.
    scope_field: ClassVar[str | None] = None

    source: int = 0
    target: int = 0


class ChildOf(Relation):
    """The blessed hierarchy relation: ``source`` is a child of ``target``.

    One parent per child (``exclusive``); when the parent dies, the child
    dies with it on the next ``cascade`` pass (``Policy.DELETE``), one
    generation per invocation, every step recorded in history.
    """

    exclusive = True
    on_delete_target = Policy.DELETE


def require_relation(rel: Relation) -> None:
    """Reject non-edge components and the abstract base before they spawn."""
    if not isinstance(rel, Relation):
        raise TypeError(f"link requires a Relation instance, got {type(rel).__name__}")
    if type(rel) is Relation:
        raise TypeError("spawn a Relation subclass, not Relation itself")
