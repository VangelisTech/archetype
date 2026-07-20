# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Biome-inspired RTS prefab library
=================================

Static component combinations define asset capabilities.  Instantiation adds
dynamic state, which activates matching processors; relations then assemble a
live army from those assets.  The asset catalog and runtime scene are both ECS
graphs, but use distinct hierarchy relations.  Registration wires executable
behavior first; authoring then publishes the durable asset layer.

No external dependencies or credentials are required.

Usage:
    uv run python examples/13_biome_rts.py
"""

import asyncio

from biome_rts import (
    AssetChildOf,
    AssignedTo,
    Cargo,
    CommandedBy,
    CommandNode,
    Harvester,
    Heading,
    Health,
    Position,
    SupplyLine,
    Targets,
    UnitSpec,
    VisibleTo,
    author_prefab_library,
    fog_of_war,
    minimap,
    minimap_overview,
    register_biome_rts,
    unit_view,
)
from daft import col

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.graph import (
    ChildOf,
    Depth,
    IsA,
    cascade,
    edges,
    instantiate,
    link,
    neighborhood,
    toposorted,
)


async def main():
    registration = register_biome_rts()
    view = registration.view
    storage = StorageConfig(uri="./archetype_data", namespace="biome_rts")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "biome-rts",
            storage=storage,
            **registration.world_options(),
        )

        # 1. The library is ECS content, organized independently from runtime
        # ChildOf composition.
        assets = await author_prefab_library(world)
        await world.step()
        catalog = neighborhood(
            await edges(world, AssetChildOf, at=0),
            AssetChildOf,
            [assets.root],
            depth=3,
            direction="in",
        ).to_pylist()
        print(f"1. asset library: {len(catalog) - 1} descendants")

        army = await world.spawn(CommandNode(name="first-army", kind="army"), Depth())
        squad = await world.spawn(CommandNode(name="alpha", kind="squad"), Depth())
        commander = await world.spawn(CommandNode(name="ada", kind="commander"), Depth())
        harvester = await instantiate(
            world,
            view,
            assets.harvester,
            overrides=[
                CommandNode(name="harvester-1", kind="unit"),
                Position(),
                Heading(x=1.0),
                Health(current=80),
                Cargo(),
                Depth(),
            ],
        )
        turret = await instantiate(
            world,
            view,
            assets.turret,
            overrides=[
                CommandNode(name="turret-1", kind="unit"),
                Position(x=4.0),
                Health(current=150),
                Depth(),
            ],
        )

        for child, parent in (
            (squad, army),
            (commander, squad),
            (harvester, squad),
            (turret, squad),
        ):
            await link(world, ChildOf(source=child, target=parent))
        await link(world, AssignedTo(source=harvester, target=squad))
        await link(world, CommandedBy(source=squad, target=commander))
        await link(world, SupplyLine(source=harvester, target=turret, throughput=3))
        await link(world, Targets(source=turret, target=harvester))
        await link(world, VisibleTo(source=harvester, target=turret))
        # The first tick persists initial state. The second applies the live
        # capability processors and the prefab edit in one commit.
        await world.step()

        # Copy-on-instantiate upgrades only future generations.
        await world.update(assets.harvester, Harvester(rate=5, capacity=32))
        await world.step()
        upgraded = await instantiate(
            world,
            view,
            assets.harvester,
            overrides=[
                CommandNode(name="harvester-2", kind="unit"),
                Position(x=6.0),
                Heading(),
                Health(current=80),
                Cargo(),
                Depth(),
            ],
        )
        await link(world, ChildOf(source=upgraded, target=squad))
        await link(world, AssignedTo(source=upgraded, target=squad))
        await world.step()

        latest = (await world.info()).tick - 1
        command_order = toposorted(
            (await world.query(CommandNode, Depth))
            .where(col("tick") == latest)
            .where(col("entity_id").is_in([army, squad, commander, harvester, turret]))
        ).to_pylist()
        unit_history = await world.query(UnitSpec, Position, Health)
        live_map = minimap(unit_history)
        map_series = minimap_overview(units=unit_history).to_pylist()
        visible = fog_of_war(
            live_map,
            await edges(world, VisibleTo, at=latest),
            harvester,
        ).to_pylist()
        print(
            "2. live instances:",
            [(row["role"], row["x"], row["health"]) for row in live_map.to_pylist()],
        )
        print(
            "3. minimap population:",
            [(row["tick"], row["population"]) for row in map_series],
        )
        print("4. harvester fog of war:", [row["entity_id"] for row in visible])

        possessed = unit_view(
            [
                (ChildOf, await edges(world, ChildOf, at=latest)),
                (AssignedTo, await edges(world, AssignedTo, at=latest)),
                (SupplyLine, await edges(world, SupplyLine, at=latest)),
                (Targets, await edges(world, Targets, at=latest)),
                (VisibleTo, await edges(world, VisibleTo, at=latest)),
            ],
            harvester,
        ).to_pylist()
        print(
            "5. possessed unit view:",
            [(row["relation"], row["direction"], row["entity_id"]) for row in possessed],
        )

        print(
            "6. command order:",
            [(row["commandnode__name"], row["depth__value"]) for row in command_order],
        )

        rates = {
            row["entity_id"]: row["harvester__rate"]
            for row in (
                (await world.query(Harvester, Position)).where(col("tick") == latest).to_pylist()
            )
        }
        lineage = {
            (row["isa__source"], row["isa__target"])
            for row in (await edges(world, IsA, at=latest)).to_pylist()
        }
        print(
            "7. re-instantiated upgrade:",
            f"old rate={rates[harvester]}, new rate={rates[upgraded]},",
            f"IsA={((upgraded, assets.harvester) in lineage)}",
        )

        # The live hierarchy owns lifetime. This pass stages the direct
        # generation; another step/pass would collect nested prefab children.
        await world.despawn(squad)
        await world.step()
        direct = await cascade(world, ChildOf, view)
        print(
            "8. cascade generation staged:",
            f"direct children={len(direct.deleted_entities)}",
        )


if __name__ == "__main__":
    asyncio.run(main())
