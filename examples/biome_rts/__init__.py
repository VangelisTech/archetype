# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local Biome-inspired assets composed from ECS components and relations.

This reference package dogfoods the graph/PreFab stack; it is not a shipped
``archetype`` family or a supported generic registration API. Static
capability components live on prefab entities; dynamic state components are
added when an asset is instantiated.  Processors match the dynamic state, so
the live asset catalog remains queryable without being simulated.
"""

from .components import (
    AssetNode,
    Cargo,
    CommandNode,
    Harvester,
    Heading,
    Health,
    Mobility,
    Position,
    ToolMount,
    UnitSpec,
    Weapon,
)
from .library import BiomeRTSPrefabLibrary, author_prefab_library
from .processors import (
    HarvestProcessor,
    MovementProcessor,
    biome_rts_processors,
)
from .projections import fog_of_war, minimap, minimap_overview, unit_view
from .registration import BiomeRTSRegistration, register_biome_rts
from .relations import (
    AssetChildOf,
    AssignedTo,
    CommandedBy,
    SupplyLine,
    Targets,
    VisibleTo,
)

__all__ = [
    "AssetChildOf",
    "AssetNode",
    "AssignedTo",
    "Cargo",
    "CommandNode",
    "CommandedBy",
    "HarvestProcessor",
    "Harvester",
    "Heading",
    "Health",
    "Mobility",
    "MovementProcessor",
    "Position",
    "BiomeRTSPrefabLibrary",
    "BiomeRTSRegistration",
    "SupplyLine",
    "Targets",
    "ToolMount",
    "UnitSpec",
    "VisibleTo",
    "Weapon",
    "author_prefab_library",
    "fog_of_war",
    "minimap",
    "minimap_overview",
    "biome_rts_processors",
    "register_biome_rts",
    "unit_view",
]
