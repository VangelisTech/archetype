# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local schemas for the Biome-inspired RTS prefab library.

The component split is intentional:

* description components (``UnitSpec``, ``Mobility``, ``Harvester``, and
  ``Weapon``) are static capabilities authored on prefab entities;
* state components (``Position``, ``Heading``, ``Health``, and ``Cargo``) are
  supplied when a prefab becomes a live unit.

Processors require both a capability and its state.  A prefab therefore
describes what an instance can do without itself entering the simulation.
"""

from __future__ import annotations

from archetype.core.component import Component


class AssetNode(Component):
    """One named node in the asset-library hierarchy."""

    name: str = ""
    kind: str = "asset"


class UnitSpec(Component):
    """Static identity and limits shared by one kind of unit."""

    role: str = "unit"
    max_health: int = 1
    sight: float = 0.0


class Mobility(Component):
    """Static movement capability; absence means the asset is stationary."""

    speed: float = 0.0


class Harvester(Component):
    """Static resource-gathering capability."""

    rate: int = 0
    capacity: int = 0


class Weapon(Component):
    """Static combat capability."""

    damage: int = 0
    range: float = 0.0


class ToolMount(Component):
    """A nested prefab node copied with its owning asset."""

    tool: str = ""


class CommandNode(Component):
    """Runtime label for an army, squad, commander, or unit."""

    name: str = ""
    kind: str = "unit"


class Position(Component):
    """Runtime two-dimensional position."""

    x: float = 0.0
    y: float = 0.0


class Heading(Component):
    """Runtime movement direction; multiplied by ``Mobility.speed``."""

    x: float = 0.0
    y: float = 0.0


class Health(Component):
    """Runtime hit points."""

    current: int = 1


class Cargo(Component):
    """Runtime amount gathered by a harvester."""

    amount: int = 0
