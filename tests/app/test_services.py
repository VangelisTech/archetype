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
    async def test_get_world_state_with_entities(self, tmp_path):
        from archetype.core.component import Component

        class Pos(Component):
            x: int = 0
            y: int = 0

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            e1 = await world.create_entity([Pos(x=1, y=2)])
            e2 = await world.create_entity([Pos(x=3, y=4)])
            await world.step(RunConfig())

            snapshot = await container.query_service.get_world_state(world.world_id)
            assert snapshot.world_id == world.world_id
            assert e1 in snapshot.entities
            assert e2 in snapshot.entities
            assert "Pos" in snapshot.entities[e1]
            assert len(snapshot.archetype_counts) == 1
            # Both entities share the same archetype → count should be 2
            counts = list(snapshot.archetype_counts.values())
            assert counts[0] == 2
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_entity_returns_components(self, tmp_path):
        from archetype.core.component import Component

        class Marker(Component):
            label: str = "default"
            score: float = 0.0

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            eid = await world.create_entity([Marker(label="hello", score=3.14)])
            await world.step(RunConfig())

            result = await container.query_service.get_entity(world.world_id, eid)
            assert result["entity_id"] == eid
            assert result["world_id"] == str(world.world_id)
            assert "marker" in result["components"]
            assert result["components"]["marker"]["label"] == "hello"
            assert result["components"]["marker"]["score"] == pytest.approx(3.14)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_entity_not_found(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            with pytest.raises(KeyError, match="Entity 999 not found"):
                await container.query_service.get_entity(world.world_id, 999)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_get_components_filters_types(self, tmp_path):
        from archetype.core.component import Component

        class Alpha(Component):
            a: int = 0

        class Beta(Component):
            b: int = 0

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            await world.create_entity([Alpha(a=1)])
            await world.create_entity([Alpha(a=2), Beta(b=10)])
            await world.step(RunConfig())

            # Query Alpha — should find both entities
            result = await container.query_service.get_components(world.world_id, ["Alpha"])
            assert len(result["rows"]) == 2
            assert result["component_types"] == ["Alpha"]
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_time_travel_query(self, tmp_path):
        from archetype.core.component import Component

        class Counter(Component):
            val: int = 0

        from archetype.core.aio.async_processor import AsyncProcessor

        class IncrementProcessor(AsyncProcessor):
            components = (Counter,)
            priority = 10

            async def process(self, df, tick: int = 0, **kwargs):
                from daft import col, lit

                return df.with_column("counter__val", col("counter__val") + lit(1))

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await world.add_processor(IncrementProcessor())

            eid = await world.create_entity([Counter(val=0)])

            # Step 3 times: tick 0 → val=1, tick 1 → val=2, tick 2 → val=3
            rc = RunConfig(num_steps=3)
            await world.run(rc)

            # Current state should be val=3 (after 3 increments)
            current = await container.query_service.get_entity(world.world_id, eid)
            assert current["components"]["counter"]["val"] == 3

            # Historical: tick 0 should be val=1 (first increment)
            tick0 = await container.query_service.get_entity(world.world_id, eid, tick=0)
            assert tick0["components"]["counter"]["val"] == 1
            assert tick0["tick"] == 0

            # Historical: tick 1 should be val=2
            tick1 = await container.query_service.get_entity(world.world_id, eid, tick=1)
            assert tick1["components"]["counter"]["val"] == 2
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
