# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for frame-pure traversal (stage 2).

Traversal is iterated joins over an edge frame, so most contracts are pure
frame tests with no world at all; one integration test proves the same
calls compose with live EdgeTables.
"""

from __future__ import annotations

import asyncio
import os

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

import daft  # noqa: E402
import pytest  # noqa: E402

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.graph import (  # noqa: E402
    ChildOf,
    ancestors,
    descendants,
    edges,
    link,
    neighborhood,
)


class Node(Component):
    name: str = ""


def _edges(pairs: list[tuple[int, int]]) -> daft.DataFrame:
    """A ChildOf edge frame: (source=child, target=parent) pairs."""
    return daft.from_pydict(
        {
            "childof__source": [p[0] for p in pairs],
            "childof__target": [p[1] for p in pairs],
        }
    )


def _reached(frame: daft.DataFrame) -> dict[int, int]:
    return {row["entity_id"]: row["hops"] for row in frame.to_pylist()}


def test_descendants_walk_down_with_hop_counts():
    # 1 -> 2 -> 3 -> 4 (chain), plus 2 -> 5 (branch)
    frame = _edges([(2, 1), (3, 2), (4, 3), (5, 2)])
    got = _reached(descendants(frame, [1], depth=3))
    assert got == {1: 0, 2: 1, 3: 2, 5: 2, 4: 3}


def test_depth_bounds_the_walk():
    frame = _edges([(2, 1), (3, 2), (4, 3)])
    got = _reached(descendants(frame, [1], depth=1))
    assert got == {1: 0, 2: 1}


def test_ancestors_walk_up():
    frame = _edges([(2, 1), (3, 2), (4, 3)])
    got = _reached(ancestors(frame, [4], depth=2))
    assert got == {4: 0, 3: 1, 2: 2}


def test_diamond_collapses_to_min_hops():
    # 1 -> {2, 3} -> 4: two paths to 4, min hops wins
    frame = _edges([(2, 1), (3, 1), (4, 2), (4, 3)])
    got = _reached(descendants(frame, [1], depth=3))
    assert got[4] == 2


def test_cycle_is_bounded():
    # 1 -> 2 -> 1 cycle: traversal terminates at depth, roots keep hops 0
    frame = _edges([(2, 1), (1, 2)])
    got = _reached(descendants(frame, [1], depth=5))
    assert got == {1: 0, 2: 1}


def test_neighborhood_validates_inputs():
    frame = _edges([(2, 1)])
    with pytest.raises(ValueError):
        neighborhood(frame, ChildOf, [1], depth=0)
    with pytest.raises(ValueError):
        neighborhood(frame, ChildOf, [], depth=1)
    with pytest.raises(ValueError, match="direction"):
        neighborhood(frame, ChildOf, [1], depth=1, direction="incoming")  # type: ignore[arg-type]


def test_traversal_composes_with_live_edgetables(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "traverse",
                storage=StorageConfig(uri=str(tmp_path / "t"), namespace="traverse_tests"),
            )
            root = await world.spawn(Node(name="root"))
            mid = await world.spawn(Node(name="mid"))
            leaf = await world.spawn(Node(name="leaf"))
            await link(world, ChildOf(source=mid, target=root))
            await link(world, ChildOf(source=leaf, target=mid))
            await world.step()

            latest = (await world.info()).tick - 1
            live = await edges(world, ChildOf, at=latest)
            got = _reached(descendants(live, [root], depth=2))
            assert got == {root: 0, mid: 1, leaf: 2}
            return True

    assert asyncio.run(go())
