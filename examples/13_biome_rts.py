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


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Run the RTS composition and return normalized semantic evidence."""
    registration = register_biome_rts()
    view = registration.view
    storage = StorageConfig(uri=storage_uri, namespace="biome_rts")

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
        visibility_edges = await edges(world, VisibleTo, at=latest)
        visible = fog_of_war(
            live_map,
            visibility_edges,
            harvester,
        ).to_pylist()

        child_edges = await edges(world, ChildOf, at=latest)
        assigned_edges = await edges(world, AssignedTo, at=latest)
        command_edges = await edges(world, CommandedBy, at=latest)
        supply_edges = await edges(world, SupplyLine, at=latest)
        target_edges = await edges(world, Targets, at=latest)
        possessed = unit_view(
            [
                (ChildOf, child_edges),
                (AssignedTo, assigned_edges),
                (SupplyLine, supply_edges),
                (Targets, target_edges),
                (VisibleTo, visibility_edges),
            ],
            harvester,
        ).to_pylist()

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

        # The live hierarchy owns lifetime. This pass stages the direct
        # generation; another step/pass would collect nested prefab children.
        await world.despawn(squad)
        await world.step()
        direct = await cascade(world, ChildOf, view)
        live_units = [
            {
                "role": row["role"],
                "x": row["x"],
                "health": row["health"],
            }
            for row in sorted(live_map.to_pylist(), key=lambda item: (item["role"], item["x"]))
        ]
        population = [
            {"tick": row["tick"], "population": row["population"]}
            for row in sorted(map_series, key=lambda item: item["tick"])
        ]
        possessed_relations = [
            {"relation": row["relation"], "direction": row["direction"]}
            for row in sorted(
                possessed,
                key=lambda item: (item["relation"], item["direction"]),
            )
        ]
        order = [
            {
                "name": row["commandnode__name"],
                "depth": row["depth__value"],
            }
            for row in sorted(
                command_order,
                key=lambda item: (item["depth__value"], item["commandnode__name"]),
            )
        ]
        return {
            "asset_descendant_count": len(catalog) - 1,
            "live_units": live_units,
            "minimap_population": population,
            "visible_roles": sorted(row["role"] for row in visible),
            "possessed_relations": possessed_relations,
            "command_order": order,
            "edge_counts": {
                "asset_child_of": (await edges(world, AssetChildOf, at=latest)).count_rows(),
                "child_of": child_edges.count_rows(),
                "assigned_to": assigned_edges.count_rows(),
                "commanded_by": command_edges.count_rows(),
                "supply_line": supply_edges.count_rows(),
                "targets": target_edges.count_rows(),
                "visible_to": visibility_edges.count_rows(),
                "is_a": len(lineage),
            },
            "upgrade": {
                "old_rate": rates[harvester],
                "new_rate": rates[upgraded],
                "lineage_recorded": (upgraded, assets.harvester) in lineage,
            },
            "cascade_deleted_count": len(direct.deleted_entities),
        }


async def main() -> None:
    result = await run_demo()
    print(f"1. asset library: {result['asset_descendant_count']} descendants")
    print(
        "2. live instances:",
        [(row["role"], row["x"], row["health"]) for row in result["live_units"]],
    )
    print(
        "3. minimap population:",
        [(row["tick"], row["population"]) for row in result["minimap_population"]],
    )
    print("4. harvester fog of war:", result["visible_roles"])
    print(
        "5. possessed unit view:",
        [(row["relation"], row["direction"]) for row in result["possessed_relations"]],
    )
    print(
        "6. command order:",
        [(row["name"], row["depth"]) for row in result["command_order"]],
    )
    print(
        "7. re-instantiated upgrade:",
        f"old rate={result['upgrade']['old_rate']}, "
        f"new rate={result['upgrade']['new_rate']}, "
        f"IsA={result['upgrade']['lineage_recorded']}",
    )
    print(
        "8. cascade generation staged:",
        f"direct children={result['cascade_deleted_count']}",
    )


if __name__ == "__main__":
    asyncio.run(main())
