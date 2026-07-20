# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for driver-level cascade (stage 5b, design D4 as amended).

Each test pins a D4 claim: DELETE propagates one generation per invocation
with every step on the ledger, REMOVE drops the edge but spares the source,
FLAG mutates nothing, and the sync flavor mirrors the async one.
"""

from __future__ import annotations

import asyncio
import os

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.core.hooks import PostTick  # noqa: E402
from archetype.graph import (  # noqa: E402
    ChildOf,
    GraphView,
    Policy,
    Relation,
    cascade,
    edges,
    link,
)
from archetype.graph import sync as graph_sync  # noqa: E402


class Node(Component):
    name: str = ""


class Watches(Relation):
    on_delete_target = Policy.FLAG


class Cites(Relation):
    pass  # Policy.REMOVE default


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "cascade_data"), namespace="cascade_tests")


def _run(coro):
    return asyncio.run(coro)


def _world(runtime, name, tmp_path, view):
    return runtime.world(
        name,
        storage=_storage(tmp_path),
        resources=[view],
        hooks=[(PostTick, view.on_post_tick)],
    )


def test_delete_cascades_one_generation_per_call(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "generations", tmp_path, view)
            parent = await world.spawn(Node(name="parent"))
            child = await world.spawn(Node(name="child"))
            grandchild = await world.spawn(Node(name="grandchild"))
            await link(world, ChildOf(source=child, target=parent))
            await link(world, ChildOf(source=grandchild, target=child))
            await world.step()

            await world.despawn(parent)
            await world.step()

            first = await cascade(world, ChildOf, view)
            await world.step()
            assert first.policy is Policy.DELETE
            assert child in first.deleted_entities
            assert grandchild not in first.deleted_entities  # one generation only

            second = await cascade(world, ChildOf, view)
            await world.step()
            assert grandchild in second.deleted_entities

            third = await cascade(world, ChildOf, view)
            assert third.total == 0  # converged

            # Every generation is history: the edges' pre-cascade rows remain.
            history = (await edges(world, ChildOf)).to_pylist()
            assert len(history) > 0
            return True

    assert _run(go())


def test_remove_drops_edge_and_spares_source(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "remove", tmp_path, view)
            paper = await world.spawn(Node(name="paper"))
            citer = await world.spawn(Node(name="citer"))
            await link(world, Cites(source=citer, target=paper))
            await world.step()

            await world.despawn(paper)
            await world.step()

            result = await cascade(world, Cites, view)
            await world.step()
            assert result.policy is Policy.REMOVE
            assert len(result.removed_edges) == 1
            assert result.deleted_entities == ()

            population = view.population()
            assert population is not None
            alive = {row["entity_id"] for row in population.to_pylist()}
            assert citer in alive  # the source survives
            return True

    assert _run(go())


def test_flag_mutates_nothing(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "flag", tmp_path, view)
            star = await world.spawn(Node(name="star"))
            watcher = await world.spawn(Node(name="watcher"))
            await link(world, Watches(source=watcher, target=star))
            await world.step()

            await world.despawn(star)
            await world.step()

            result = await cascade(world, Watches, view)
            await world.step()
            assert result.policy is Policy.FLAG
            assert len(result.flagged_edges) == 1
            assert result.removed_edges == ()

            # Nothing was despawned: the dangling edge is still flagged next pass.
            again = await cascade(world, Watches, view)
            assert len(again.flagged_edges) == 1
            return True

    assert _run(go())


def test_cascade_empty_view_and_absent_relation(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "empty", tmp_path, view)
            assert (await cascade(world, ChildOf, view)).total == 0  # pre-first-tick
            await world.spawn(Node(name="only"))
            await world.step()
            assert (await cascade(world, ChildOf, view)).total == 0  # no edge table
            return True

    assert _run(go())


def test_sync_cascade_parity(tmp_path):
    view = GraphView()
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world(
            "sync-cascade",
            storage=_storage(tmp_path),
            resources=[view],
            hooks=[(PostTick, view.on_post_tick_sync)],
        )
        parent = world.spawn(Node(name="parent"))
        child = world.spawn(Node(name="child"))
        graph_sync.link(world, ChildOf(source=child, target=parent))
        world.step()

        world.despawn(parent)
        world.step()

        result = graph_sync.cascade(world, ChildOf, view)
        world.step()
        assert result.policy is Policy.DELETE
        assert child in result.deleted_entities
