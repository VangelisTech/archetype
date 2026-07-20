# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Author the Biome-inspired RTS prefab catalog as ordinary ECS content."""

from __future__ import annotations

from dataclasses import dataclass

from archetype.graph import ChildOf, Prefab, WorldLike, link

from .components import (
    AssetNode,
    Harvester,
    Mobility,
    ToolMount,
    UnitSpec,
    Weapon,
)
from .relations import AssetChildOf


@dataclass(frozen=True, slots=True)
class BiomeRTSPrefabLibrary:
    """Stable entity ids for one authored catalog."""

    root: int
    units: int
    structures: int
    harvester: int
    turret: int
    mining_tool: int


async def author_prefab_library(world: WorldLike) -> BiomeRTSPrefabLibrary:
    """Stage a small, queryable prefab hierarchy in ``world``.

    ``AssetChildOf`` provides catalog/namespace structure.  The mining tool's
    additional ``ChildOf`` edge makes it part of the harvester prefab's copied
    runtime subtree.  The caller owns the step that publishes these staged
    entities and edges.
    """

    root = await world.spawn(AssetNode(name="biome-rts", kind="library"))
    units = await world.spawn(AssetNode(name="units", kind="collection"))
    structures = await world.spawn(AssetNode(name="structures", kind="collection"))

    harvester = await world.spawn(
        Prefab(name="harvester"),
        AssetNode(name="harvester", kind="prefab"),
        UnitSpec(role="harvester", max_health=80, sight=4.0),
        Mobility(speed=1.0),
        Harvester(rate=3, capacity=24),
    )
    turret = await world.spawn(
        Prefab(name="turret"),
        AssetNode(name="turret", kind="prefab"),
        UnitSpec(role="turret", max_health=150, sight=6.0),
        Weapon(damage=20, range=5.0),
    )
    mining_tool = await world.spawn(
        Prefab(name="mining-tool"),
        AssetNode(name="mining-tool", kind="prefab-child"),
        ToolMount(tool="drill"),
    )

    for child, parent in (
        (units, root),
        (structures, root),
        (harvester, units),
        (turret, structures),
        (mining_tool, harvester),
    ):
        await link(world, AssetChildOf(source=child, target=parent))

    # Runtime composition: instantiating a harvester also copies its tool.
    await link(world, ChildOf(source=mining_tool, target=harvester))

    return BiomeRTSPrefabLibrary(
        root=root,
        units=units,
        structures=structures,
        harvester=harvester,
        turret=turret,
        mining_tool=mining_tool,
    )
