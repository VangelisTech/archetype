# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
PreFabs
========

A prefab is a template entity: the Prefab marker, component values, and
optionally a ChildOf subtree of child prefabs. instantiate() copies the
template onto fresh entities, applies overrides, rebuilds the subtree with
new ids, and records an IsA lineage edge from every copy to its template.
Editing a prefab never mutates instances — re-instantiation is the upgrade
path, and both generations stay on the ledger.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/12_prefabs.py
"""

import asyncio

from daft import col

from archetype import ArchetypeRuntime, Component
from archetype.core.config import StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import ChildOf, GraphView, IsA, Prefab, edges, instantiate, link


class Chassis(Component):
    armor: int = 10
    color: str = "grey"


class Turret(Component):
    caliber: int = 30


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="prefabs")
    view = GraphView()

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "factory",
            storage=storage,
            resources=[view],
            hooks=[(PostTick, view.on_post_tick)],
        )

        # ── 1. AUTHOR A PREFAB ────────────────────────────────────────────────
        tank = await world.spawn(Prefab(name="tank"), Chassis(armor=42))
        gun = await world.spawn(Prefab(name="gun"), Turret(caliber=88))
        await link(world, ChildOf(source=gun, target=tank))
        await world.step()
        print(f"1. prefab authored: tank={tank} with gun={gun}")

        # ── 2. INSTANTIATE, WITH AN OVERRIDE ──────────────────────────────────
        red = await instantiate(world, view, tank, overrides=[Chassis(armor=42, color="red")])
        await world.step()
        latest = (await world.info()).tick - 1
        lineage = (await edges(world, IsA, at=latest)).to_pylist()
        print(f"2. instantiated {red} (red): {len(lineage)} IsA lineage edges recorded")

        # ── 3. EDIT THE PREFAB; INSTANCES DO NOT MOVE ─────────────────────────
        await world.update(tank, Chassis(armor=99))
        await world.run(steps=2)  # the edit persists, then the view captures it
        mk2 = await instantiate(world, view, tank)
        await world.step()

        latest = (await world.info()).tick - 1
        rows = (await world.query(Chassis)).where(col("tick") == latest).to_pylist()
        armor = {row["entity_id"]: row["chassis__armor"] for row in rows}
        print(
            f"3. after the edit: first instance armor={armor[red]}, new instance armor={armor[mk2]}"
        )
        print("   copy-on-instantiate: the old generation is history, not collateral")


if __name__ == "__main__":
    asyncio.run(main())
