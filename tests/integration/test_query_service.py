# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests: QueryService wired to real AsyncWorld data."""

import pytest
import pytest_asyncio
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

# ---------------------------------------------------------------------------
# Test components
# ---------------------------------------------------------------------------


class QSPosition(Component):
    x: float = 0.0
    y: float = 0.0


class QSVelocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class QSHealth(Component):
    hp: int = 100


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest_asyncio.fixture
async def container_with_world(tmp_path):
    """Create a container with a world and spawn two entities, then step once."""
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="query-test"), storage)

    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    # Spawn entity 1: Position + Velocity
    cmd1 = Command(
        type=CommandType.SPAWN,
        tick=0,
        payload={
            "components": [
                QSPosition(x=1.0, y=2.0).to_payload(),
                QSVelocity(vx=3.0, vy=4.0).to_payload(),
            ]
        },
    )
    await container.command_service.submit(str(world.world_id), cmd1, ctx)

    # Spawn entity 2: Position + Velocity (different values)
    cmd2 = Command(
        type=CommandType.SPAWN,
        tick=0,
        payload={
            "components": [
                QSPosition(x=10.0, y=20.0).to_payload(),
                QSVelocity(vx=30.0, vy=40.0).to_payload(),
            ]
        },
    )
    await container.command_service.submit(str(world.world_id), cmd2, ctx)

    # Step to materialize the spawns
    await container.simulation_service.step(world.world_id)

    yield container, world

    await container.shutdown()


# ---------------------------------------------------------------------------
# Tests: get_world_state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_world_state_returns_entity_counts(container_with_world):
    """get_world_state should return real entity counts from _live."""
    container, world = container_with_world

    snapshot = await container.query_service.get_world_state(world.world_id)

    # Should have 2 entities
    assert len(snapshot.entities) == 2
    # All entities should list both component types
    for _entity_id, comp_names in snapshot.entities.items():
        assert "QSPosition" in comp_names
        assert "QSVelocity" in comp_names
    # Exactly one archetype with count 2
    assert sum(snapshot.archetype_counts.values()) == 2


@pytest.mark.asyncio
async def test_get_world_state_tick_reflects_current(container_with_world):
    """When tick is None, snapshot.tick should match the world's current tick."""
    container, world = container_with_world

    snapshot = await container.query_service.get_world_state(world.world_id)

    # After one step, tick is 1; _live rows are from tick 0
    assert snapshot.tick == world.tick


@pytest.mark.asyncio
async def test_get_world_state_nonexistent_world_raises():
    """Querying a nonexistent world should raise KeyError."""
    container = ServiceContainer()
    try:
        with pytest.raises(KeyError):
            await container.query_service.get_world_state(uuid7())
    finally:
        await container.shutdown()


# ---------------------------------------------------------------------------
# Tests: get_entity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_entity_returns_component_data(container_with_world):
    """get_entity should return actual component field values."""
    container, world = container_with_world

    # Entity 1 was spawned first
    result = await container.query_service.get_entity(world.world_id, entity_id=1)

    assert result["entity_id"] == 1
    assert result["world_id"] == str(world.world_id)
    assert "QSPosition" in result["components"]
    assert result["components"]["QSPosition"]["x"] == 1.0
    assert result["components"]["QSPosition"]["y"] == 2.0
    assert "QSVelocity" in result["components"]
    assert result["components"]["QSVelocity"]["vx"] == 3.0
    assert result["components"]["QSVelocity"]["vy"] == 4.0


@pytest.mark.asyncio
async def test_get_entity_second_entity(container_with_world):
    """get_entity for entity 2 should return its own values, not entity 1's."""
    container, world = container_with_world

    result = await container.query_service.get_entity(world.world_id, entity_id=2)

    assert result["entity_id"] == 2
    assert result["components"]["QSPosition"]["x"] == 10.0
    assert result["components"]["QSPosition"]["y"] == 20.0


@pytest.mark.asyncio
async def test_get_entity_missing_returns_empty_components(container_with_world):
    """get_entity for a nonexistent entity_id returns empty components."""
    container, world = container_with_world

    result = await container.query_service.get_entity(world.world_id, entity_id=999)

    assert result["entity_id"] == 999
    assert result["components"] == {}


# ---------------------------------------------------------------------------
# Tests: get_components
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_components_filters_by_type(container_with_world):
    """get_components with a single type should return data for all matching entities."""
    container, world = container_with_world

    result = await container.query_service.get_components(
        world.world_id, component_types=["QSPosition"]
    )

    assert result["component_types"] == ["QSPosition"]
    assert len(result["data"]) == 2
    # Each entry should have Position data
    xs = sorted(row["QSPosition"]["x"] for row in result["data"])
    assert xs == [1.0, 10.0]


@pytest.mark.asyncio
async def test_get_components_filters_by_entity_ids(container_with_world):
    """get_components with entity_ids should only return those entities."""
    container, world = container_with_world

    result = await container.query_service.get_components(
        world.world_id, component_types=["QSVelocity"], entity_ids=[2]
    )

    assert len(result["data"]) == 1
    assert result["data"][0]["entity_id"] == 2
    assert result["data"][0]["QSVelocity"]["vx"] == 30.0


@pytest.mark.asyncio
async def test_get_components_unknown_type_returns_empty(container_with_world):
    """get_components with an unknown component type returns empty data."""
    container, world = container_with_world

    result = await container.query_service.get_components(
        world.world_id, component_types=["NonexistentComponent"]
    )

    assert result["data"] == []


@pytest.mark.asyncio
async def test_get_components_multiple_types(container_with_world):
    """get_components with multiple types returns all requested component data."""
    container, world = container_with_world

    result = await container.query_service.get_components(
        world.world_id, component_types=["QSPosition", "QSVelocity"]
    )

    assert len(result["data"]) == 2
    for row in result["data"]:
        assert "QSPosition" in row
        assert "QSVelocity" in row


# ---------------------------------------------------------------------------
# Tests: multi-archetype worlds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_world_state_multiple_archetypes(tmp_path):
    """get_world_state counts entities across different archetypes correctly."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="multi-arch"), storage)

        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        # Spawn entity with Position+Velocity
        cmd1 = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={
                "components": [
                    QSPosition(x=1.0, y=2.0).to_payload(),
                    QSVelocity(vx=3.0, vy=4.0).to_payload(),
                ]
            },
        )
        await container.command_service.submit(str(world.world_id), cmd1, ctx)

        # Spawn entity with Position+Health (different archetype)
        cmd2 = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={
                "components": [
                    QSPosition(x=5.0, y=6.0).to_payload(),
                    QSHealth(hp=50).to_payload(),
                ]
            },
        )
        await container.command_service.submit(str(world.world_id), cmd2, ctx)

        await container.simulation_service.step(world.world_id)

        snapshot = await container.query_service.get_world_state(world.world_id)

        # Two archetypes, each with one entity
        assert len(snapshot.archetype_counts) == 2
        assert sum(snapshot.archetype_counts.values()) == 2
        assert len(snapshot.entities) == 2

        # get_components for Position should find both entities (both archetypes have Position)
        result = await container.query_service.get_components(
            world.world_id, component_types=["QSPosition"]
        )
        assert len(result["data"]) == 2

        # get_components for Health should only find entity from second archetype
        result = await container.query_service.get_components(
            world.world_id, component_types=["QSHealth"]
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["QSHealth"]["hp"] == 50
    finally:
        await container.shutdown()


# ---------------------------------------------------------------------------
# Tests: after multiple steps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_query_after_multiple_steps(tmp_path):
    """Entities should still be queryable after multiple simulation steps."""
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="multi-step"), storage)

        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={
                "components": [
                    QSPosition(x=1.0, y=2.0).to_payload(),
                    QSVelocity(vx=3.0, vy=4.0).to_payload(),
                ]
            },
        )
        await container.command_service.submit(str(world.world_id), cmd, ctx)

        # Step multiple times
        run_config = RunConfig(num_steps=3)
        await container.simulation_service.run(world.world_id, run_config)

        # Entity should still be queryable
        snapshot = await container.query_service.get_world_state(world.world_id)
        assert len(snapshot.entities) == 1

        result = await container.query_service.get_entity(world.world_id, entity_id=1)
        assert result["components"]["QSPosition"]["x"] == 1.0
    finally:
        await container.shutdown()
