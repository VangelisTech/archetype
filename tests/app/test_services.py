# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for service container, command service, simulation service, query service."""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


class TestServiceContainer:
    def test_container_wires_services(self):
        container = ServiceContainer()
        assert isinstance(container.storage_service, StorageService)
        assert isinstance(container.broker, CommandBroker)
        assert isinstance(container.world_service, WorldService)
        assert isinstance(container.command_service, CommandService)
        assert isinstance(container.simulation_service, SimulationService)
        assert isinstance(container.query_service, QueryService)

    @pytest.mark.asyncio
    async def test_container_shutdown(self):
        container = ServiceContainer()
        await container.shutdown()  # should not raise


class TestCommandService:
    @pytest.mark.asyncio
    async def test_submit_enqueues(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            config = WorldConfig(name="test")
            world = await container.world_service.create_world(config, storage)

            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            cmd = Command(type=CommandType.SPAWN, payload={"components": []})
            cmd_id = await container.command_service.submit(str(world.world_id), cmd, ctx)

            assert cmd_id == cmd.id
            count = await container.broker.get_pending_count(str(world.world_id))
            assert count == 1
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_submit_batch(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            cmds = [
                Command(type=CommandType.SPAWN, payload={"components": []}),
                Command(type=CommandType.SPAWN, payload={"components": []}),
            ]
            ids = await container.command_service.submit_batch(str(world.world_id), cmds, ctx)
            assert len(ids) == 2
        finally:
            await container.shutdown()


class TestSimulationService:
    @pytest.mark.asyncio
    async def test_step(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            cmds_applied = await container.simulation_service.step(world.world_id)
            assert cmds_applied == 0  # no commands queued
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_run(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            result = await container.simulation_service.run(world.world_id, RunConfig(num_steps=3))
            assert result.ticks_completed == 3
            assert result.world_id == world.world_id
        finally:
            await container.shutdown()


class TestQueryService:
    @pytest.mark.asyncio
    async def test_get_world_state_empty_world(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            snapshot = await container.query_service.get_world_state(world.world_id)
            assert snapshot.world_id == world.world_id
            assert snapshot.entities == {}
            assert snapshot.archetype_counts == {}
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_world_state_with_entities(self, tmp_path):
        """After creating entities, get_world_state returns real entity data."""
        from archetype.core.component import Component

        class Agent(Component):
            name: str = ""

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            await world.create_entity([Agent(name="Alice")])
            await world.create_entity([Agent(name="Bob")])
            # Step to materialize the entities into live state
            await container.simulation_service.step(world.world_id)

            snapshot = await container.query_service.get_world_state(world.world_id)
            assert snapshot.world_id == world.world_id
            assert len(snapshot.entities) == 2
            # Each entity should list Agent as a component type
            for _eid, comp_names in snapshot.entities.items():
                assert "Agent" in comp_names
            # Archetype counts should reflect 2 entities in the Agent archetype
            assert sum(snapshot.archetype_counts.values()) == 2
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_entity_returns_component_data(self, tmp_path):
        """get_entity returns actual component field values."""
        from archetype.core.component import Component

        class Position(Component):
            x: float = 0.0
            y: float = 0.0

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            eid = await world.create_entity([Position(x=10.0, y=20.0)])
            await container.simulation_service.step(world.world_id)

            result = await container.query_service.get_entity(world.world_id, eid)
            assert result["entity_id"] == eid
            assert result["world_id"] == str(world.world_id)
            assert "Position" in result["component_types"]
            assert "Position" in result["components"]
            assert result["components"]["Position"]["x"] == 10.0
            assert result["components"]["Position"]["y"] == 20.0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_entity_not_found(self, tmp_path):
        """get_entity raises KeyError for non-existent entity."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            with pytest.raises(KeyError):
                await container.query_service.get_entity(world.world_id, 9999)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_components_returns_rows(self, tmp_path):
        """get_components resolves type names and returns real DataFrame rows."""
        from archetype.core.component import Component

        class Score(Component):
            val: float = 0.0

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            await world.create_entity([Score(val=0.5)])
            await world.create_entity([Score(val=0.9)])
            await container.simulation_service.step(world.world_id)

            result = await container.query_service.get_components(world.world_id, ["Score"])
            assert result["component_types"] == ["Score"]
            assert len(result["entities"]) == 2
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_components_unknown_type(self, tmp_path):
        """get_components raises KeyError for unknown component type names."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            with pytest.raises(KeyError, match="Unknown component type"):
                await container.query_service.get_components(
                    world.world_id, ["NonExistentComponent"]
                )
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_components_empty_types(self, tmp_path):
        """get_components with empty type list returns empty entities."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            result = await container.query_service.get_components(world.world_id, [])
            assert result["entities"] == []
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_world_state_stub_returns_empty(self, tmp_path):
        """QueryService.get_world_state is a stub: entities and archetype_counts are empty."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="stub"), storage)

            snapshot = await container.query_service.get_world_state(world.world_id)
            assert snapshot.entities == {}
            assert snapshot.archetype_counts == {}
            assert snapshot.tick == 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_world_state_with_tick(self, tmp_path):
        """When tick is specified, the stub echoes it back."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="t"), storage)

            snapshot = await container.query_service.get_world_state(world.world_id, tick=42)
            assert snapshot.tick == 42
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_entity_stub(self, tmp_path):
        """QueryService.get_entity is a stub: returns dict echoing inputs."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="e"), storage)

            entity = await container.query_service.get_entity(world.world_id, entity_id=7)
            assert entity["entity_id"] == 7
            assert entity["world_id"] == str(world.world_id)
            assert entity["tick"] == 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_entity_stub_with_tick(self, tmp_path):
        """When tick is specified, the stub echoes it back."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="et"), storage)

            entity = await container.query_service.get_entity(world.world_id, entity_id=3, tick=10)
            assert entity["tick"] == 10
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_components_stub(self, tmp_path):
        """QueryService.get_components is a stub: returns dict echoing inputs."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="c"), storage)

            result = await container.query_service.get_components(
                world.world_id, ["Position", "Velocity"]
            )
            assert result["world_id"] == str(world.world_id)
            assert result["component_types"] == ["Position", "Velocity"]
            assert result["entity_ids"] is None
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_components_stub_with_entity_ids(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="ce"), storage)

            result = await container.query_service.get_components(
                world.world_id, ["Position"], entity_ids=[1, 2]
            )
            assert result["entity_ids"] == [1, 2]
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_command_history_empty(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            history = await container.query_service.get_command_history(world.world_id)
            assert history == []
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_command_history_after_submit(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            cmd = Command(type=CommandType.SPAWN, payload={})
            await container.command_service.submit(str(world.world_id), cmd, ctx)

            history = await container.query_service.get_command_history(world.world_id)
            assert len(history) == 1
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_command_history_without_broker(self, tmp_path):
        """QueryService without broker returns empty history."""
        ws = WorldService(StorageService())
        qs = QueryService(ws, broker=None)

        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="nb"), storage)
        try:
            history = await qs.get_command_history(world.world_id)
            assert history == []
        finally:
            await ws.storage_service.shutdown()

    @pytest.mark.asyncio
    async def test_get_world_state_not_found(self):
        """QueryService raises KeyError for unknown world."""
        container = ServiceContainer()
        try:
            with pytest.raises(KeyError):
                await container.query_service.get_world_state(uuid7())
        finally:
            await container.shutdown()


class TestWorldService:
    @pytest.mark.asyncio
    async def test_create_and_list(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="w1"), storage)
            worlds = container.world_service.list_worlds()
            assert len(worlds) == 1
            assert worlds[0].world_id == world.world_id
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_remove_world(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="w1"), storage)
            container.world_service.remove_world(world.world_id)
            assert len(container.world_service.list_worlds()) == 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_world_by_name(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="alpha"), storage)
            found = container.world_service.get_world_by_name("alpha")
            assert found.world_id == world.world_id
        finally:
            await container.shutdown()
