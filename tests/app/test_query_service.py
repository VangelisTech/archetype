# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for QueryService wired to real AsyncWorld data (issue #75)."""

import pytest
import pytest_asyncio

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

# --- Test components (unique names to avoid clashes with other tests) ---


class QSPosition(Component):
    x: int
    y: int


class QSVelocity(Component):
    dx: int
    dy: int


class QSTag(Component):
    label: str


@pytest_asyncio.fixture
async def container():
    c = ServiceContainer()
    try:
        yield c
    finally:
        await c.shutdown()


@pytest_asyncio.fixture
async def populated_world(container, tmp_path):
    """Create a world, spawn a few entities, step twice so we have history."""
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="qs-world"), storage)

    # Spawn: 2 entities with (QSPosition, QSVelocity), 1 with just (QSPosition),
    # 1 with (QSTag).
    e1 = await world.create_entity([QSPosition(x=1, y=2), QSVelocity(dx=10, dy=20)])
    e2 = await world.create_entity([QSPosition(x=3, y=4), QSVelocity(dx=30, dy=40)])
    e3 = await world.create_entity([QSPosition(x=5, y=6)])
    e4 = await world.create_entity([QSTag(label="hello")])

    # Tick 0 -> tick 1: materializes spawns. world.tick is now 1.
    await world.run(RunConfig(num_steps=1, prefer_live_reads=True))

    return {
        "world": world,
        "e_pos_vel": [e1, e2],
        "e_pos_only": e3,
        "e_tag": e4,
        "tick_after_spawn": world.tick,  # 1
    }


# -----------------------------------------------------------------------------
# get_world_state
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_world_state_current_tick_returns_real_entities(container, populated_world):
    world = populated_world["world"]
    qs = container.query_service

    snap = await qs.get_world_state(world.world_id)

    assert snap.world_id == world.world_id
    assert snap.tick == world.tick
    # Four entities across three archetypes.
    assert len(snap.entities) == 4
    for eid in [
        *populated_world["e_pos_vel"],
        populated_world["e_pos_only"],
        populated_world["e_tag"],
    ]:
        assert eid in snap.entities
    # One archetype per signature; counts should be 2, 1, 1.
    assert sorted(snap.archetype_counts.values()) == [1, 1, 2]

    # Entity -> component-type names mapping is correct.
    pos_vel_components = sorted(snap.entities[populated_world["e_pos_vel"][0]])
    assert pos_vel_components == ["QSPosition", "QSVelocity"]
    assert snap.entities[populated_world["e_pos_only"]] == ["QSPosition"]
    assert snap.entities[populated_world["e_tag"]] == ["QSTag"]


@pytest.mark.asyncio
async def test_get_world_state_echoes_tick_for_no_data_case(container, tmp_path):
    """An empty world: tick echoes, entities/archetype_counts empty."""
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="empty"), storage)
    snap = await container.query_service.get_world_state(world.world_id, tick=5)
    assert snap.tick == 5
    assert snap.entities == {}
    assert snap.archetype_counts == {}


# -----------------------------------------------------------------------------
# get_entity
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_entity_current_tick_returns_real_components(container, populated_world):
    world = populated_world["world"]
    e1 = populated_world["e_pos_vel"][0]

    entity = await container.query_service.get_entity(world.world_id, entity_id=e1)

    assert entity["entity_id"] == e1
    assert entity["world_id"] == str(world.world_id)
    assert entity["tick"] == world.tick
    assert entity["archetype"] is not None
    # Real component values present.
    assert entity["components"]["QSPosition"] == {"x": 1, "y": 2}
    assert entity["components"]["QSVelocity"] == {"dx": 10, "dy": 20}


@pytest.mark.asyncio
async def test_get_entity_missing_entity_returns_empty_components(container, populated_world):
    world = populated_world["world"]
    entity = await container.query_service.get_entity(world.world_id, entity_id=9999)
    assert entity["components"] == {}
    assert entity["archetype"] is None


# -----------------------------------------------------------------------------
# get_components
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_components_projects_real_data(container, populated_world):
    world = populated_world["world"]

    result = await container.query_service.get_components(world.world_id, ["QSPosition"])

    # Three entities have QSPosition (two with Velocity, one standalone).
    assert len(result["rows"]) == 3
    eids = {r["entity_id"] for r in result["rows"]}
    expected = {
        populated_world["e_pos_only"],
        *populated_world["e_pos_vel"],
    }
    assert eids == expected
    # Each row has a nested QSPosition dict with real fields.
    for row in result["rows"]:
        assert set(row["QSPosition"].keys()) == {"x", "y"}


@pytest.mark.asyncio
async def test_get_components_entity_id_filter(container, populated_world):
    world = populated_world["world"]
    e1 = populated_world["e_pos_vel"][0]

    result = await container.query_service.get_components(
        world.world_id, ["QSPosition"], entity_ids=[e1]
    )
    assert len(result["rows"]) == 1
    assert result["rows"][0]["entity_id"] == e1


@pytest.mark.asyncio
async def test_get_components_union_across_archetypes(container, populated_world):
    """QSPosition projects across both archetypes containing it."""
    world = populated_world["world"]
    # QSPosition+QSVelocity archetype has 2 entities; stand-alone has 1.
    result = await container.query_service.get_components(
        world.world_id, ["QSPosition", "QSVelocity"]
    )
    # Only the (QSPosition, QSVelocity) archetype satisfies both.
    assert len(result["rows"]) == 2
    for row in result["rows"]:
        assert "QSPosition" in row and "QSVelocity" in row


@pytest.mark.asyncio
async def test_get_components_unknown_type_returns_empty(container, populated_world):
    world = populated_world["world"]
    result = await container.query_service.get_components(
        world.world_id, ["ThisComponentDoesNotExist"]
    )
    assert result["rows"] == []


# -----------------------------------------------------------------------------
# Time-travel
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_time_travel_historical_tick_reads_from_store(container, tmp_path):
    """Snapshot at an older tick should read from the store, not the live cache.

    Uses a single ``RunConfig`` for multiple ticks so all ticks share a
    ``run_id`` (store reads filter by run_id — mirrors the comment in
    ``AsyncWorld._run_archetype``)."""
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="tt"), storage)

    await world.create_entity([QSPosition(x=1, y=1)])
    await world.create_entity([QSPosition(x=2, y=2)])

    # Run 3 ticks under a single run_id so historical store reads can find
    # rows stamped with earlier ticks.
    await world.run(RunConfig(num_steps=3, prefer_live_reads=True))
    assert world.tick == 3

    qs = container.query_service

    # Current tick: live read — 2 entities present.
    snap_now = await qs.get_world_state(world.world_id)
    assert snap_now.tick == 3
    assert len(snap_now.entities) == 2

    # Historical: tick 0 is the first persisted tick under this run_id.
    snap_t0 = await qs.get_world_state(world.world_id, tick=0)
    assert snap_t0.tick == 0
    assert len(snap_t0.entities) == 2


@pytest.mark.asyncio
async def test_time_travel_get_components_historical_tick(container, tmp_path):
    """Historical-tick get_components returns data from the store."""
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="tt2"), storage)

    e1 = await world.create_entity([QSPosition(x=1, y=1)])
    # Single run_id spanning both ticks so tick=0 rows are accessible.
    await world.run(RunConfig(num_steps=2, prefer_live_reads=True))

    result = await container.query_service.get_components(world.world_id, ["QSPosition"], tick=0)
    assert result["tick"] == 0
    assert any(r["entity_id"] == e1 for r in result["rows"])
