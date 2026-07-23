# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Graph Relationships
====================

Relationships are edge entities: a relation is a component with source and
target ids, its archetype table is an EdgeTable, and every edge inherits
ticks, history, and fork lineage. This example builds a small command
hierarchy, walks it with bounded traversal, reads the graph at an earlier
tick, and lets a cascade clean up after a despawn.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/11_graph_relationships.py
"""

import asyncio

from archetype import ArchetypeRuntime, Component
from archetype.core.config import StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import ChildOf, GraphView, cascade, descendants, edges, link


class Unit(Component):
    name: str = ""


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Exercise traversal, temporal reads, and cascade with semantic evidence."""
    storage = StorageConfig(uri=storage_uri, namespace="graph_relationships")
    view = GraphView()

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "hierarchy",
            storage=storage,
            resources=[view],
            hooks=[(PostTick, view.on_post_tick)],
        )

        # ── 1. BUILD THE HIERARCHY ────────────────────────────────────────────
        hq = await world.spawn(Unit(name="hq"))
        squad = await world.spawn(Unit(name="squad"))
        scout = await world.spawn(Unit(name="scout"))
        await world.step()  # tick 0: units exist, no edges yet
        await link(world, ChildOf(source=squad, target=hq))
        await link(world, ChildOf(source=scout, target=squad))
        await world.step()  # tick 1: the hierarchy lands

        # ── 2. TRAVERSE ───────────────────────────────────────────────────────
        latest = (await world.info()).tick - 1
        live = await edges(world, ChildOf, at=latest)
        subtree = descendants(live, [hq], depth=2).sort("hops").to_pylist()

        # ── 3. TIME TRAVEL ────────────────────────────────────────────────────
        # The EdgeTable keeps every tick; the graph at any moment is a filter.
        before = (await edges(world, ChildOf, at=0)).count_rows()

        # ── 4. CASCADE ────────────────────────────────────────────────────────
        # ChildOf deletes children when the parent dies, one generation per pass.
        await world.despawn(squad)
        await world.step()
        first = await cascade(world, ChildOf, view)
        await world.step()
        second = await cascade(world, ChildOf, view)
        await world.step()
        final_tick = (await world.info()).tick - 1
        after = (await edges(world, ChildOf, at=final_tick)).count_rows()
        return {
            "subtree_hops": [row["hops"] for row in subtree],
            "edge_count_before_build": before,
            "edge_count_after_build": live.count_rows(),
            "cascade_deleted_counts": [
                len(first.deleted_entities),
                len(second.deleted_entities),
            ],
            "edge_count_after_cascade": after,
        }


async def main() -> None:
    result = await run_demo()
    print("1. hierarchy: hq <- squad <- scout")
    print(f"2. subtree hop distances: {result['subtree_hops']}")
    print(
        f"3. edges at tick 0: {result['edge_count_before_build']} (the graph before it was built)"
    )
    print(
        "4. cascade after squad despawn: "
        f"deleted per pass={result['cascade_deleted_counts']}, "
        f"remaining edges={result['edge_count_after_cascade']}"
    )


if __name__ == "__main__":
    asyncio.run(main())
