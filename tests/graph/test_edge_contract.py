# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the graph family's EdgeTable foundation (stage 1).

Each test pins a claim from docs/design/graph-system.md D2: edges are
ordinary entities, so EdgeTables inherit ticks, wildcard filters are plain
``where`` clauses, temporal reads are tick filters, and forks inherit edges
through lineage.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from daft import col

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.graph import (  # noqa: E402
    Relation,
    edges,
    link,
    unlink,
    with_source,
    with_target,
)
from archetype.graph import sync as graph_sync  # noqa: E402


class Node(Component):
    name: str = ""


class ChildOf(Relation):
    pass


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "graph_data"), namespace="graph_tests")


def _run(coro):
    return asyncio.run(coro)


def test_link_creates_edge_rows(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("edges", storage=_storage(tmp_path))
            parent = await world.spawn(Node(name="parent"))
            child = await world.spawn(Node(name="child"))
            await link(world, ChildOf(source=parent, target=child))
            await world.step()

            rows = (await edges(world, ChildOf)).to_pylist()
            assert len(rows) == 1
            assert rows[0]["childof__source"] == parent
            assert rows[0]["childof__target"] == child
            return True

    assert _run(go())


def test_link_rejects_base_relation(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("reject", storage=_storage(tmp_path))
            a = await world.spawn(Node(name="a"))
            b = await world.spawn(Node(name="b"))
            with pytest.raises(TypeError):
                await link(world, Relation(source=a, target=b))
            return True

    assert _run(go())


def test_wildcard_filters(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("wildcards", storage=_storage(tmp_path))
            a = await world.spawn(Node(name="a"))
            b = await world.spawn(Node(name="b"))
            c = await world.spawn(Node(name="c"))
            await link(world, ChildOf(source=a, target=b))
            await link(world, ChildOf(source=a, target=c))
            await world.step()

            frame = await edges(world, ChildOf)
            outgoing = with_source(frame, ChildOf, a).to_pylist()
            incoming = with_target(frame, ChildOf, b).to_pylist()
            assert {row["childof__target"] for row in outgoing} == {b, c}
            assert len(incoming) == 1
            assert incoming[0]["childof__source"] == a
            return True

    assert _run(go())


def test_temporal_reads_are_tick_filters(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("temporal", storage=_storage(tmp_path))
            a = await world.spawn(Node(name="a"))
            b = await world.spawn(Node(name="b"))
            await world.step()  # tick 0: vertices only
            await link(world, ChildOf(source=a, target=b))
            await world.run(steps=2)  # edge lands raw, then persists again

            before = (await edges(world, ChildOf, at=0)).to_pylist()
            latest = (await world.info()).tick - 1
            after = (await edges(world, ChildOf, at=latest)).to_pylist()
            assert before == []
            assert len(after) == 1
            return True

    assert _run(go())


def test_unlink_despawns_live_edge(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("unlink", storage=_storage(tmp_path))
            a = await world.spawn(Node(name="a"))
            b = await world.spawn(Node(name="b"))
            await link(world, ChildOf(source=a, target=b))
            await world.step()

            removed = await unlink(world, ChildOf, a, b)
            await world.step()
            assert removed == 1

            latest = (await world.info()).tick - 1
            live = (await edges(world, ChildOf, at=latest)).to_pylist()
            assert live == []

            # The edge's pre-despawn history remains readable.
            history = (await edges(world, ChildOf)).to_pylist()
            assert len(history) >= 1

            # A second unlink finds nothing live.
            assert await unlink(world, ChildOf, a, b) == 0
            return True

    assert _run(go())


def test_fork_inherits_edges(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("lineage", storage=_storage(tmp_path))
            a = await world.spawn(Node(name="a"))
            b = await world.spawn(Node(name="b"))
            await link(world, ChildOf(source=a, target=b))
            await world.step()
            fork_tick = (await world.info()).tick

            fork = await world.fork("branch")
            await fork.run(steps=1)

            pre_fork = (await edges(fork, ChildOf)).where(col("tick") < fork_tick).to_pylist()
            assert len(pre_fork) == 1
            assert pre_fork[0]["childof__source"] == a
            return True

    assert _run(go())


def test_unlink_absent_relation_returns_zero(tmp_path):
    """A relation that has never been committed is an empty edge set."""

    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("absent", storage=_storage(tmp_path))
            a = await world.spawn(Node(name="a"))
            b = await world.spawn(Node(name="b"))
            await world.step()  # unrelated signature persisted; no ChildOf table
            assert await unlink(world, ChildOf, a, b) == 0
            return True

    assert _run(go())


def test_link_rejects_non_relation_component(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("nonrel", storage=_storage(tmp_path))
            with pytest.raises(TypeError):
                await link(world, Node(name="not-an-edge"))  # type: ignore[arg-type]
            return True

    assert _run(go())


def test_async_helpers_reject_sync_world(tmp_path):
    """A sync handle fails loud and early — before any mutation."""
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("syncguard", storage=_storage(tmp_path))
        a = world.spawn(Node(name="a"))
        b = world.spawn(Node(name="b"))
        with pytest.raises(TypeError, match="archetype.graph.sync"):
            _run(link(world, ChildOf(source=a, target=b)))
        world.step()
        # The guard fired before any mutation: no ChildOf edge was created.
        assert graph_sync.unlink(world, ChildOf, a, b) == 0


def test_sync_parity_roundtrip(tmp_path):
    """graph.sync mirrors link/edges/unlink without await (runtime.md R5)."""
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("syncpar", storage=_storage(tmp_path))
        a = world.spawn(Node(name="a"))
        b = world.spawn(Node(name="b"))
        graph_sync.link(world, ChildOf(source=a, target=b))
        world.step()

        rows = graph_sync.edges(world, ChildOf).to_pylist()
        assert len(rows) == 1
        assert rows[0]["childof__source"] == a

        assert graph_sync.unlink(world, ChildOf, a, b) == 1
        world.step()
        latest = world.info().tick - 1
        assert graph_sync.edges(world, ChildOf, at=latest).to_pylist() == []
