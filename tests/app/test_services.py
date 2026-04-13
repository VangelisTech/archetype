# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for service container, command service, simulation service, query service."""

from unittest.mock import AsyncMock, Mock

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

    @pytest.mark.asyncio
    async def test_run_all_empty(self):
        container = ServiceContainer()
        try:
            results = await container.simulation_service.run_all(RunConfig(num_steps=2))
            assert results == []
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_run_all_multiple_worlds(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            w1 = await container.world_service.create_world(WorldConfig(name="w1"), storage)
            w2 = await container.world_service.create_world(WorldConfig(name="w2"), storage)

            results = await container.simulation_service.run_all(RunConfig(num_steps=2))

            assert {result.world_id for result in results} == {w1.world_id, w2.world_id}
            assert all(result.ticks_completed == 2 for result in results)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_step_uses_default_run_config_when_missing(self, tmp_path, monkeypatch):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            applied = [Command(type=CommandType.SPAWN, payload={})]
            seen = {}

            async def fake_step(run_config, **kwargs):
                seen["run_config"] = run_config
                seen["kwargs"] = kwargs

            monkeypatch.setattr(
                container.command_service, "drain_and_apply", AsyncMock(return_value=applied)
            )
            monkeypatch.setattr(world, "step", fake_step)

            count = await container.simulation_service.step(world.world_id, bonus=3)

            assert count == 1
            assert seen["run_config"].num_steps == 1
            assert seen["kwargs"] == {"bonus": 3}
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_run_accumulates_commands_from_each_tick(self):
        world_id = uuid7()
        world = Mock(tick=9)
        world_service = Mock()
        world_service.get_world.return_value = world
        command_service = Mock()
        sim = SimulationService(world_service=world_service, command_service=command_service)
        sim.step = AsyncMock(side_effect=[1, 2, 3])  # type: ignore[method-assign]
        rc = RunConfig(num_steps=3)

        result = await sim.run(world_id, rc, bonus=7)

        assert result.ticks_completed == 3
        assert result.commands_applied == 6
        assert result.final_tick == 9
        assert sim.step.await_args_list[0].args[0] == world_id
        assert sim.step.await_args_list[0].args[1].num_steps == 1
        assert sim.step.await_args_list[0].kwargs == {"bonus": 7}

    @pytest.mark.asyncio
    async def test_step_skips_non_async_world_but_still_applies_commands(self):
        world = Mock(tick=4)
        world_service = Mock()
        world_service.get_world.return_value = world
        command_service = Mock()
        command_service.drain_and_apply = AsyncMock(return_value=["a", "b"])
        sim = SimulationService(world_service=world_service, command_service=command_service)

        count = await sim.step(uuid7(), RunConfig(num_steps=2))

        assert count == 2
        world.step.assert_not_called()

    def test_add_and_remove_processor_delegate_to_world(self):
        world = Mock()
        world_service = Mock()
        world_service.get_world.return_value = world
        sim = SimulationService(world_service=world_service, command_service=Mock())
        proc = object()

        sim.add_processor(uuid7(), proc)
        sim.remove_processor(uuid7(), type(proc))

        assert world.add_processor.call_count == 1
        assert world.remove_processor.call_count == 1

    def test_list_processors_returns_metadata(self):
        class Position:
            __name__ = "Position"

        processor = Mock()
        processor.priority = 10
        processor.components = (Position,)
        world = Mock()
        world.system.processors = [processor]
        sim = SimulationService.__new__(SimulationService)
        sim._world_service = Mock()
        sim._world_service.get_world.return_value = world

        infos = sim.list_processors(uuid7())

        assert len(infos) == 1
        assert infos[0].name == "Mock"
        assert infos[0].priority == 10
        assert infos[0].components == ["Position"]

    def test_list_processors_empty_when_world_has_no_system(self):
        class _World:
            pass

        sim = SimulationService.__new__(SimulationService)
        sim._world_service = Mock()
        sim._world_service.get_world.return_value = _World()

        assert sim.list_processors(uuid7()) == []


class TestQueryService:
    @pytest.mark.asyncio
    async def test_get_world_state(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            snapshot = await container.query_service.get_world_state(world.world_id)
            assert snapshot.world_id == world.world_id
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
