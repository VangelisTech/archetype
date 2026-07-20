# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for hierarchy depth (stage 6, design stage table).

Each test pins a claim: depth converges one level per tick from ChildOf
edges, roots stay 0, re-parenting reconverges, and toposorted() orders
parents before children.
"""

from __future__ import annotations

import asyncio
import os

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from daft import col  # noqa: E402

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.core.hooks import PostTick  # noqa: E402
from archetype.graph import (  # noqa: E402
    ChildOf,
    Depth,
    DepthProcessor,
    GraphView,
    link,
    toposorted,
)


class Node(Component):
    name: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "depth_data"), namespace="depth_tests")


def _run(coro):
    return asyncio.run(coro)


async def _depths(world, at: int) -> dict[int, int]:
    rows = (await world.query(Depth)).where(col("tick") == at).to_pylist()
    return {row["entity_id"]: row["depth__value"] for row in rows}


def test_depth_converges_one_level_per_tick(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "converge",
                storage=_storage(tmp_path),
                processors=[DepthProcessor()],
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            root = await world.spawn(Node(name="root"), Depth())
            mid = await world.spawn(Node(name="mid"), Depth())
            leaf = await world.spawn(Node(name="leaf"), Depth())
            await link(world, ChildOf(source=mid, target=root))
            await link(world, ChildOf(source=leaf, target=mid))
            await world.run(steps=4)  # tick 0 raw + >= depth ticks to converge

            latest = (await world.info()).tick - 1
            depths = await _depths(world, latest)
            assert depths[root] == 0
            assert depths[mid] == 1
            assert depths[leaf] == 2
            return True

    assert _run(go())


def test_reparenting_reconverges(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "reparent",
                storage=_storage(tmp_path),
                processors=[DepthProcessor()],
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            a = await world.spawn(Node(name="a"), Depth())
            b = await world.spawn(Node(name="b"), Depth())
            c = await world.spawn(Node(name="c"), Depth())
            await link(world, ChildOf(source=c, target=b))  # a  b -> c
            await world.run(steps=3)

            # Re-parent c under a chain a -> b -> c: exclusive link replaces.
            await link(world, ChildOf(source=b, target=a))
            await world.run(steps=4)

            latest = (await world.info()).tick - 1
            depths = await _depths(world, latest)
            assert depths[a] == 0
            assert depths[b] == 1
            assert depths[c] == 2
            return True

    assert _run(go())


def test_roots_stay_zero_without_view_or_edges(tmp_path):
    async def go():
        # No GraphView resource at all: processor passes frames through.
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "noview", storage=_storage(tmp_path), processors=[DepthProcessor()]
            )
            lone = await world.spawn(Node(name="lone"), Depth())
            await world.run(steps=2)
            latest = (await world.info()).tick - 1
            depths = await _depths(world, latest)
            assert depths[lone] == 0
            return True

    assert _run(go())


def test_toposorted_orders_parents_first(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "topo",
                storage=_storage(tmp_path),
                processors=[DepthProcessor()],
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            root = await world.spawn(Node(name="root"), Depth())
            mid = await world.spawn(Node(name="mid"), Depth())
            leaf = await world.spawn(Node(name="leaf"), Depth())
            await link(world, ChildOf(source=mid, target=root))
            await link(world, ChildOf(source=leaf, target=mid))
            await world.run(steps=4)

            latest = (await world.info()).tick - 1
            ordered = [
                row["entity_id"]
                for row in toposorted(
                    (await world.query(Depth)).where(col("tick") == latest)
                ).to_pylist()
            ]
            assert ordered == [root, mid, leaf]
            return True

    assert _run(go())


def test_seeded_depth_normalizes_without_edges(tmp_path):
    """A caller-supplied Depth value cannot survive: no edges means root, 0."""

    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "seeded",
                storage=_storage(tmp_path),
                processors=[DepthProcessor()],
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            seeded = await world.spawn(Node(name="seeded"), Depth(value=5))
            await world.run(steps=3)
            latest = (await world.info()).tick - 1
            depths = await _depths(world, latest)
            assert depths[seeded] == 0
            return True

    assert _run(go())
