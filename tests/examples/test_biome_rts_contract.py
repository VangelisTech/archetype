# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the example-local Biome-inspired prefab library (issue #603)."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from biome_rts import (  # noqa: E402
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
from daft import col  # noqa: E402

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.core.hooks import PostTick  # noqa: E402
from archetype.graph import (  # noqa: E402
    ChildOf,
    Depth,
    GraphView,
    IsA,
    Prefab,
    cascade,
    edges,
    instantiate,
    link,
    neighborhood,
    toposorted,
)


def _run(coro):
    return asyncio.run(coro)


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "rts_data"), namespace="rts_tests")


def test_example_registration_is_fresh_world_local_code_wiring():
    first = register_biome_rts()
    second = register_biome_rts()

    assert first.view is not second.view
    assert all(
        left is not right for left, right in zip(first.processors, second.processors, strict=True)
    )

    options = first.world_options()
    assert options["processors"] == list(first.processors)
    assert options["resources"] == [first.view]
    event_type, handler = options["hooks"][0]
    assert event_type is PostTick
    assert handler.__self__ is first.view


def test_asset_graph_drives_live_scene_without_executing_prefabs(tmp_path):
    async def go():
        registration = register_biome_rts()
        view = registration.view
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "biome-rts",
                storage=_storage(tmp_path),
                **registration.world_options(),
            )

            assets = await author_prefab_library(world)
            await world.step()

            catalog_edges = await edges(world, AssetChildOf, at=0)
            catalog = neighborhood(
                catalog_edges,
                AssetChildOf,
                [assets.root],
                depth=3,
                direction="in",
            ).to_pylist()
            assert {row["entity_id"] for row in catalog} == {
                assets.root,
                assets.units,
                assets.structures,
                assets.harvester,
                assets.turret,
                assets.mining_tool,
            }

            army = await world.spawn(CommandNode(name="first-army", kind="army"), Depth())
            squad = await world.spawn(CommandNode(name="alpha", kind="squad"), Depth())
            commander = await world.spawn(CommandNode(name="ada", kind="commander"), Depth())

            harvester = await instantiate(
                world,
                view,
                assets.harvester,
                overrides=[
                    CommandNode(name="harvester-1", kind="unit"),
                    Position(x=0.0, y=0.0),
                    Heading(x=1.0, y=0.0),
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
                    Position(x=2.0, y=0.0),
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
            await link(world, AssignedTo(source=turret, target=squad))
            await link(world, CommandedBy(source=squad, target=commander))
            await link(world, SupplyLine(source=harvester, target=turret, throughput=3))
            await link(world, Targets(source=turret, target=harvester))
            await link(world, VisibleTo(source=harvester, target=turret))

            # Tick 1 persists raw initial conditions. Later ticks run the
            # capability processors and converge the two-level hierarchy.
            await world.run(steps=3)
            latest_tick = (await world.info()).tick - 1

            # Capability composition is the interface: only the harvester has
            # both Cargo and Harvester, and only it has Heading + Mobility.
            harvest_rows = (
                (await world.query(Harvester, Cargo, Position))
                .where(col("tick") == latest_tick)
                .to_pylist()
            )
            assert len(harvest_rows) == 1
            assert harvest_rows[0]["entity_id"] == harvester
            assert harvest_rows[0]["cargo__amount"] == 6
            assert harvest_rows[0]["position__x"] == 2.0

            # Prefabs have descriptions but no dynamic state, so no runtime
            # processor can match their archetypes.
            assert view.frame(Prefab, Position) is None

            ordered = toposorted(
                (await world.query(CommandNode, Depth)).where(col("tick") == latest_tick)
            ).to_pylist()
            depths = [row["depth__value"] for row in ordered]
            assert depths == sorted(depths)
            by_id = {row["entity_id"]: row["depth__value"] for row in ordered}
            assert by_id[army] == 0
            assert by_id[squad] == 1
            assert by_id[harvester] == 2

            map_frame = minimap(await world.query(UnitSpec, Position, Health))
            map_rows = map_frame.to_pylist()
            assert {row["entity_id"] for row in map_rows} == {harvester, turret}
            overview_rows = minimap_overview(
                units=await world.query(UnitSpec, Position, Health)
            ).to_pylist()
            assert overview_rows[-1] == {
                "table": "units",
                "tick": latest_tick,
                "population": 2,
            }

            visible_edges = await edges(world, VisibleTo, at=latest_tick)
            visible = fog_of_war(map_frame, visible_edges, harvester).to_pylist()
            assert {row["entity_id"] for row in visible} == {harvester, turret}

            possessed = unit_view(
                [
                    (ChildOf, await edges(world, ChildOf, at=latest_tick)),
                    (AssignedTo, await edges(world, AssignedTo, at=latest_tick)),
                    (SupplyLine, await edges(world, SupplyLine, at=latest_tick)),
                    (Targets, await edges(world, Targets, at=latest_tick)),
                    (VisibleTo, visible_edges),
                ],
                harvester,
            ).to_pylist()
            assert {(row["relation"], row["entity_id"]) for row in possessed} >= {
                ("assignedto", squad),
                ("childof", squad),
                ("supplyline", turret),
                ("targets", turret),
                ("visibleto", turret),
            }

            command_edges = (await edges(world, CommandedBy, at=latest_tick)).to_pylist()
            assert [
                (row["commandedby__source"], row["commandedby__target"]) for row in command_edges
            ] == [(squad, commander)]
            supply_edges = (await edges(world, SupplyLine, at=latest_tick)).to_pylist()
            assert supply_edges[0]["supplyline__throughput"] == 3

            assert AssignedTo.exclusive
            assert CommandedBy.exclusive
            assert Targets.exclusive

            # Every instance, including the nested tool, keeps IsA lineage.
            lineage = (await edges(world, IsA, at=latest_tick)).to_pylist()
            assert {(row["isa__source"], row["isa__target"]) for row in lineage} >= {
                (harvester, assets.harvester),
                (turret, assets.turret),
            }
            child_edges = (await edges(world, ChildOf, at=latest_tick)).to_pylist()
            tool_instance = next(
                row["childof__source"] for row in child_edges if row["childof__target"] == harvester
            )
            assert any(
                row["isa__source"] == tool_instance and row["isa__target"] == assets.mining_tool
                for row in lineage
            )

            # The first cascade pass applies ChildOf's DELETE policy to the
            # squad's direct children. The graph contract separately pins
            # later-generation propagation and persistence at each tick.
            await world.despawn(squad)
            await world.step()
            first = await cascade(world, ChildOf, view)
            assert {harvester, turret, commander}.issubset(first.deleted_entities)
            assert tool_instance not in first.deleted_entities
            return True

    assert _run(go())


def test_harvester_upgrade_is_reinstantiation_not_live_inheritance(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "biome-rts-upgrade",
                storage=_storage(tmp_path),
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            assets = await author_prefab_library(world)
            await world.step()

            original = await instantiate(
                world,
                view,
                assets.harvester,
                overrides=[Position()],
            )
            await world.step()

            await world.update(assets.harvester, Harvester(rate=5, capacity=32))
            await world.step()
            upgraded = await instantiate(
                world,
                view,
                assets.harvester,
                overrides=[Position()],
            )
            await world.step()

            latest_tick = (await world.info()).tick - 1
            generations = (
                (await world.query(Harvester, Position))
                .where(col("tick") == latest_tick)
                .to_pylist()
            )
            rates = {row["entity_id"]: row["harvester__rate"] for row in generations}
            assert rates == {original: 3, upgraded: 5}

            lineage = (await edges(world, IsA, at=latest_tick)).to_pylist()
            pairs = {(row["isa__source"], row["isa__target"]) for row in lineage}
            assert (original, assets.harvester) in pairs
            assert (upgraded, assets.harvester) in pairs
            return True

    assert _run(go())


def test_nested_prefab_cascade_records_each_generation(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "biome-rts-cascade",
                storage=_storage(tmp_path),
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            assets = await author_prefab_library(world)
            await world.step()

            squad = await world.spawn(CommandNode(name="alpha", kind="squad"))
            unit = await instantiate(world, view, assets.harvester)
            await link(world, ChildOf(source=unit, target=squad))
            await world.step()

            live_edges = (await edges(world, ChildOf, at=1)).to_pylist()
            tool = next(
                row["childof__source"] for row in live_edges if row["childof__target"] == unit
            )

            await world.despawn(squad)
            await world.step()
            first = await cascade(world, ChildOf, view)
            assert unit in first.deleted_entities
            assert tool not in first.deleted_entities
            await world.step()

            second = await cascade(world, ChildOf, view)
            assert tool in second.deleted_entities
            await world.step()
            assert (await cascade(world, ChildOf, view)).total == 0

            history = (await edges(world, ChildOf)).to_pylist()
            direct_ticks = {
                row["tick"]
                for row in history
                if (row["childof__source"], row["childof__target"]) == (unit, squad)
            }
            nested_ticks = {
                row["tick"]
                for row in history
                if (row["childof__source"], row["childof__target"]) == (tool, unit)
            }
            assert direct_ticks
            assert nested_ticks
            assert max(nested_ticks) == max(direct_ticks) + 1
            return True

    assert _run(go())
