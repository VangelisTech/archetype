# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the projections family's world read models (stage 0).

Each test pins a claim from docs/design/graph-system.md stage 0: projections
are frame-pure functions over base columns, populations track the ledger
tick-by-tick, and handle sugar has async and sync parity.
"""

from __future__ import annotations

import asyncio
import os

import pytest

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.projections import latest, overview, world_overview  # noqa: E402
from archetype.projections import sync as projections_sync  # noqa: E402


class Crew(Component):
    name: str = ""


class Cargo(Component):
    kind: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "proj_data"), namespace="projection_tests")


def _run(coro):
    return asyncio.run(coro)


def test_activity_tracks_population_over_ticks(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("activity", storage=_storage(tmp_path))
            a = await world.spawn(Crew(name="a"))
            await world.spawn(Crew(name="b"))
            await world.run(steps=2)
            await world.despawn(a)
            await world.run(steps=2)

            series = (await world_overview(world, Crew)).to_pylist()
            populations = [row["population"] for row in series]
            assert populations[0] == 2  # both spawned at tick 0
            assert populations[-1] == 1  # one despawned
            assert all(row["table"] == "crew" for row in series)
            return True

    assert _run(go())


def test_overview_labels_multiple_tables(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("labels", storage=_storage(tmp_path))
            await world.spawn(Crew(name="a"))
            await world.spawn(Cargo(kind="ore"))
            await world.spawn(Cargo(kind="ice"))
            await world.step()

            rows = (await world_overview(world, Crew, Cargo)).to_pylist()
            by_table = {row["table"]: row["population"] for row in rows}
            assert by_table == {"crew": 1, "cargo": 2}
            return True

    assert _run(go())


def test_overview_requires_a_frame():
    with pytest.raises(ValueError):
        overview()


def test_world_overview_requires_components(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("nocomp", storage=_storage(tmp_path))
            with pytest.raises(ValueError):
                await world_overview(world)
            return True

    assert _run(go())


def test_latest_returns_max_tick_rows(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("latest", storage=_storage(tmp_path))
            await world.spawn(Crew(name="a"))
            await world.spawn(Crew(name="b"))
            await world.run(steps=3)

            info = await world.info()
            rows = latest(await world.query(Crew)).to_pylist()
            assert len(rows) == 2
            assert all(row["tick"] == info.tick - 1 for row in rows)
            return True

    assert _run(go())


def test_async_helper_rejects_sync_world(tmp_path):
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("syncguard", storage=_storage(tmp_path))
        world.spawn(Crew(name="a"))
        world.step()
        with pytest.raises(TypeError, match="archetype.projections.sync"):
            _run(world_overview(world, Crew))


def test_sync_parity_roundtrip(tmp_path):
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("syncpar", storage=_storage(tmp_path))
        world.spawn(Crew(name="a"))
        world.spawn(Cargo(kind="ore"))
        world.spawn(Cargo(kind="ice"))
        world.step()

        rows = projections_sync.world_overview(world, Crew, Cargo).to_pylist()
        by_table = {row["table"]: row["population"] for row in rows}
        assert by_table == {"crew": 1, "cargo": 2}


def test_sync_helper_rejects_async_world(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("asyncguard", storage=_storage(tmp_path))
            await world.spawn(Crew(name="a"))
            await world.step()
            with pytest.raises(TypeError, match="archetype.projections.worlds"):
                projections_sync.world_overview(world, Crew)
            return True

    assert _run(go())
