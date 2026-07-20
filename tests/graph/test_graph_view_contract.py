# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for GraphView (stage 4, design D3).

Each test pins a D3 claim: processors at tick N read tick N−1 frames, the
view is empty before the first tick, matching is by signature membership
with clean cross-archetype concat, despawned rows are filtered, and sync
worlds get hook parity.
"""

from __future__ import annotations

import asyncio
import os

import pytest

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from daft import DataFrame, lit  # noqa: E402

from archetype import ArchetypeRuntime, AsyncProcessor  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.core.hooks import PostTick  # noqa: E402
from archetype.core.resources import Resources  # noqa: E402
from archetype.graph import GraphView  # noqa: E402


class Beacon(Component):
    strength: float = 1.0


class Extra(Component):
    note: str = ""


class Observer(Component):
    seen_tick: int = -99
    seen_count: int = -99


class RecordView(AsyncProcessor):
    """Writes what the GraphView shows into the Observer row each tick."""

    components = (Observer,)
    priority = 10

    async def process(self, df: DataFrame, resources: Resources | None = None, **_) -> DataFrame:
        assert resources is not None
        view = resources.require(GraphView)
        beacons = view.frame(Beacon)
        count = -1 if beacons is None else beacons.count_rows()
        df = df.with_column("observer__seen_tick", lit(view.tick))
        return df.with_column("observer__seen_count", lit(count))


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "view_data"), namespace="view_tests")


def _run(coro):
    return asyncio.run(coro)


def test_processors_see_previous_tick(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "nminus1",
                storage=_storage(tmp_path),
                processors=[RecordView()],
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            await world.spawn(Beacon(strength=1.0))
            await world.spawn(Beacon(strength=2.0))
            observer = await world.spawn(Observer())
            await world.run(steps=3)

            rows = sorted((await world.query(Observer)).to_pylist(), key=lambda r: r["tick"])
            # Tick 0 rows are raw initial conditions; the processor first runs
            # at tick 1, seeing the view captured after tick 0 persisted.
            by_tick = {r["tick"]: r for r in rows if r["entity_id"] == observer}
            assert by_tick[0]["observer__seen_tick"] == -99  # raw, untransformed
            assert by_tick[1]["observer__seen_tick"] == 0  # N-1 visibility
            assert by_tick[1]["observer__seen_count"] == 2
            assert by_tick[2]["observer__seen_tick"] == 1
            return True

    assert _run(go())


def test_view_empty_before_first_tick():
    view = GraphView()
    assert view.tick == -1
    assert view.frame(Beacon) is None


def test_frame_requires_components():
    with pytest.raises(ValueError):
        GraphView().frame()


def test_frame_matches_signature_membership(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "membership",
                storage=_storage(tmp_path),
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            await world.spawn(Beacon(strength=1.0))
            await world.spawn(Beacon(strength=2.0), Extra(note="both"))
            await world.step()

            beacons = view.frame(Beacon)
            assert beacons is not None
            assert beacons.count_rows() == 2  # concat across both archetypes
            both = view.frame(Beacon, Extra)
            assert both is not None
            assert both.count_rows() == 1
            assert view.frame(Observer) is None
            return True

    assert _run(go())


def _make_beacon_twin() -> type[Component]:
    class Beacon(Component):
        strength: float = 1.0

    return Beacon


def test_frame_matches_schema_identical_twin(tmp_path):
    """A different class object with the same name and prefixed schema must
    match — the cold-resume scenario, where the world holds its own class."""

    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "twin",
                storage=_storage(tmp_path),
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            await world.spawn(Beacon(strength=1.0))
            await world.step()

            twin = _make_beacon_twin()
            assert twin is not Beacon
            frame = view.frame(twin)
            assert frame is not None
            assert frame.count_rows() == 1
            return True

    assert _run(go())


def test_despawned_rows_are_filtered(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "liveness",
                storage=_storage(tmp_path),
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            keep = await world.spawn(Beacon(strength=1.0))
            drop = await world.spawn(Beacon(strength=2.0))
            await world.step()
            await world.despawn(drop)
            await world.step()

            beacons = view.frame(Beacon)
            assert beacons is not None
            rows = beacons.to_pylist()
            assert {r["entity_id"] for r in rows} == {keep}
            return True

    assert _run(go())


def test_sync_hook_parity(tmp_path):
    view = GraphView()
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world(
            "syncview",
            storage=_storage(tmp_path),
            resources=[view],
            hooks=[(PostTick, view.on_post_tick_sync)],
        )
        world.spawn(Beacon(strength=1.0))
        world.step()
        assert view.tick == 0
        frame = view.frame(Beacon)
        assert frame is not None
        assert frame.count_rows() == 1
