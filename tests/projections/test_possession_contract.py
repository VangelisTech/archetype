# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the possession read model (stage 3).

Each test pins a claim: possession labels reachability per relation and
direction with hop distances, excludes the possessed entity, bounds by
depth, and mirrors in sync.
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
from archetype.graph import ChildOf, Relation, link  # noqa: E402
from archetype.projections import possession, possession_view  # noqa: E402
from archetype.projections import sync as projections_sync  # noqa: E402


class Sees(Relation):
    pass


class Node(Component):
    name: str = ""


def _edges(prefix: str, pairs: list[tuple[int, int]]) -> daft.DataFrame:
    return daft.from_pydict(
        {
            f"{prefix}__source": [p[0] for p in pairs],
            f"{prefix}__target": [p[1] for p in pairs],
        }
    )


def _tuples(frame: daft.DataFrame) -> set[tuple[int, str, str, int]]:
    return {
        (row["entity_id"], row["relation"], row["direction"], row["hops"])
        for row in frame.to_pylist()
    }


def test_possession_labels_relations_and_directions():
    childof = _edges("childof", [(2, 1), (3, 2)])  # 1 <- 2 <- 3
    sees = _edges("sees", [(2, 9)])  # 2 watches 9
    got = _tuples(possession([(ChildOf, childof), (Sees, sees)], entity=2, depth=1))
    assert got == {
        (1, "childof", "out", 1),  # my parent
        (3, "childof", "in", 1),  # my child
        (9, "sees", "out", 1),  # what I watch
    }


def test_possession_excludes_self_and_bounds_depth():
    childof = _edges("childof", [(2, 1), (3, 2), (4, 3)])
    got = _tuples(possession([(ChildOf, childof)], entity=1, depth=2))
    assert all(entity != 1 for entity, *_ in got)
    assert got == {(2, "childof", "in", 1), (3, "childof", "in", 2)}


def test_possession_requires_relations():
    with pytest.raises(ValueError):
        possession([], entity=1)


def test_possession_view_roundtrip(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "pov",
                storage=StorageConfig(uri=str(tmp_path / "p"), namespace="possession_tests"),
            )
            hq = await world.spawn(Node(name="hq"))
            squad = await world.spawn(Node(name="squad"))
            await link(world, ChildOf(source=squad, target=hq))
            await world.step()

            latest = (await world.info()).tick - 1
            got = _tuples(await possession_view(world, squad, ChildOf, at=latest))
            assert got == {(hq, "childof", "out", 1)}
            return True

    assert asyncio.run(go())


def test_possession_view_sync_parity(tmp_path):
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world(
            "pov-sync",
            storage=StorageConfig(uri=str(tmp_path / "s"), namespace="possession_tests"),
        )
        hq = world.spawn(Node(name="hq"))
        squad = world.spawn(Node(name="squad"))
        from archetype.graph import sync as graph_sync

        graph_sync.link(world, ChildOf(source=squad, target=hq))
        world.step()

        latest = world.info().tick - 1
        got = _tuples(projections_sync.possession_view(world, squad, ChildOf, at=latest))
        assert got == {(hq, "childof", "out", 1)}
