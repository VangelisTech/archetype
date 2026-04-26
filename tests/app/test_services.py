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
from archetype.app.world_service import WorldOrchestrator
from archetype.core.component import Component
from tests.conftest import make_world_orchestrator
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class _ListWorldsPos(Component):
    x: int = 0


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
        assert isinstance(container.world_service, WorldOrchestrator)
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

    @pytest.mark.asyncio
    async def test_apply_world_lifecycle_does_not_leak_broker_state(self, tmp_path):
        """apply_world_lifecycle drains __global__ after each lifecycle op."""
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            broker = container.broker
            assert len(broker._pending) == 0
            assert len(broker._queues.get("__global__", [])) == 0

            create_cmd = Command(
                type=CommandType.CREATE_WORLD,
                tick=0,
                payload={
                    "config": {"name": "leak_check"},
                    "storage_uri": str(tmp_path / "store"),
                    "namespace": "archetypes",
                },
            )
            await container.command_service.submit("__global__", create_cmd, ctx)
            world = await container.command_service.apply_world_lifecycle(create_cmd)

            assert len(broker._pending) == 0, (
                f"CREATE_WORLD leaked into broker._pending ({len(broker._pending)} zombies)"
            )
            assert len(broker._queues.get("__global__", [])) == 0, (
                f"CREATE_WORLD leaked into broker._queues['__global__'] "
                f"({len(broker._queues['__global__'])} zombies)"
            )

            destroy_cmd = Command(
                type=CommandType.DESTROY_WORLD,
                tick=0,
                payload={"world_id": str(world.world_id)},
            )
            await container.command_service.submit("__global__", destroy_cmd, ctx)
            await container.command_service.apply_world_lifecycle(destroy_cmd)

            assert len(broker._pending) == 0, (
                f"DESTROY_WORLD leaked into broker._pending ({len(broker._pending)} zombies)"
            )
            assert len(broker._queues.get("__global__", [])) == 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_lifecycle_round_trip_does_not_leak_at_volume(self, tmp_path):
        """50 CREATE/DESTROY round-trips leave zero broker zombies."""
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            broker = container.broker

            for i in range(50):
                create = Command(
                    type=CommandType.CREATE_WORLD,
                    tick=0,
                    payload={
                        "config": {"name": f"w{i}"},
                        "storage_uri": str(tmp_path / "store"),
                        "namespace": "archetypes",
                    },
                )
                await container.command_service.submit("__global__", create, ctx)
                world = await container.command_service.apply_world_lifecycle(create)

                destroy = Command(
                    type=CommandType.DESTROY_WORLD,
                    tick=0,
                    payload={"world_id": str(world.world_id)},
                )
                await container.command_service.submit("__global__", destroy, ctx)
                await container.command_service.apply_world_lifecycle(destroy)

            assert len(broker._pending) == 0, (
                f"50 CREATE/DESTROY pairs leaked {len(broker._pending)} "
                f"zombies into broker._pending"
            )
            assert len(broker._queues.get("__global__", [])) == 0
            # History is the audit trail; it should still record everything.
            history = await broker.get_history("__global__", limit=200)
            assert len(history) == 100
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_apply_world_lifecycle_drains_on_failure(self, tmp_path):
        """Failed lifecycle commands are still drained from the broker."""
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            broker = container.broker

            # Missing world_id triggers KeyError inside apply_world_lifecycle
            bad_destroy = Command(
                type=CommandType.DESTROY_WORLD,
                tick=0,
                payload={},  # missing "world_id"
            )
            await container.command_service.submit("__global__", bad_destroy, ctx)
            with pytest.raises(KeyError):
                await container.command_service.apply_world_lifecycle(bad_destroy)

            assert len(broker._pending) == 0, "Failed lifecycle command leaked into broker._pending"
            assert len(broker._queues.get("__global__", [])) == 0
        finally:
            await container.shutdown()


class TestSimulationService:
    @pytest.mark.asyncio
    async def test_step(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            cmds_applied = await container.simulation_service.step(world.world_id, RunConfig())
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
    async def test_step_requires_run_config(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            with pytest.raises(TypeError):
                await container.simulation_service.step(world.world_id)  # type: ignore[call-arg]
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_step_forwards_caller_run_config(self, tmp_path, monkeypatch):
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

            rc = RunConfig()
            count = await container.simulation_service.step(world.world_id, rc, bonus=3)

            assert count == 1
            assert seen["run_config"] is rc
            assert seen["kwargs"] == {"bonus": 3}
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_run_accumulates_commands_from_each_tick(self):
        world_id = uuid7()
        world = Mock(tick=9, run_id=None)
        world_service = Mock()
        world_service.get_world.return_value = world
        command_service = Mock()
        sim = SimulationService(worlds=world_service, command_drain=command_service)
        sim.step = AsyncMock(side_effect=[1, 2, 3])  # type: ignore[method-assign]
        rc = RunConfig(num_steps=3)

        result = await sim.run(world_id, rc, bonus=7)

        assert result.ticks_completed == 3
        assert result.commands_applied == 6
        assert result.final_tick == 9
        # Each per-tick step must receive the user's RunConfig unchanged so
        # run_id / debug / metadata reach world.step.
        assert sim.step.await_args_list[0].args[0] == world_id
        assert sim.step.await_args_list[0].args[1] is rc
        assert sim.step.await_args_list[0].kwargs == {"bonus": 7}
        # World's run_id pointer must be pinned to the user's run_id up-front.
        assert world.run_id == str(rc.run_id)

    @pytest.mark.asyncio
    async def test_run_threads_user_run_config_into_every_step(self, tmp_path):
        """Regression for bug-simulation-service-run-discards-runconfig.

        ``SimulationService.run`` used to construct ``RunConfig(num_steps=1)``
        inside its loop, dropping the user's run_id, debug, suite, trial,
        metadata, etc. Every per-tick step must now receive the same RunConfig
        the caller passed in, and the world's run_id pointer must be set to
        the user's run_id.
        """
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="t"), storage)
            seen: list[RunConfig] = []
            original_step = container.simulation_service.step

            async def capturing_step(wid, rc=None, **kwargs):
                seen.append(rc)
                return await original_step(wid, rc, **kwargs)

            container.simulation_service.step = capturing_step  # type: ignore[method-assign]

            user_rc = RunConfig(
                num_steps=3,
                debug=True,
                metadata={"source": "test"},
            )
            result = await container.simulation_service.run(world.world_id, user_rc)

            assert result.run_id == user_rc.run_id
            assert result.ticks_completed == 3
            # Every per-tick step received the user's RunConfig, not a fresh one.
            assert len(seen) == 3
            for rc in seen:
                assert rc is user_rc
                assert rc.run_id == user_rc.run_id
                assert rc.debug is True
                assert rc.metadata == {"source": "test"}
            # World's run_id pointer is pinned to the user's run_id.
            assert str(world.run_id) == str(user_rc.run_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_step_skips_non_async_world_but_still_applies_commands(self):
        world = Mock(tick=4)
        world_service = Mock()
        world_service.get_world.return_value = world
        command_service = Mock()
        command_service.drain_and_apply = AsyncMock(return_value=["a", "b"])
        sim = SimulationService(worlds=world_service, command_drain=command_service)

        count = await sim.step(uuid7(), RunConfig(num_steps=2))

        assert count == 2
        world.step.assert_not_called()

    @pytest.mark.asyncio
    async def test_add_and_remove_processor_delegate_to_world(self):
        world = AsyncMock()
        world_service = Mock()
        world_service.get_world.return_value = world
        sim = SimulationService(worlds=world_service, command_drain=Mock())
        proc = object()

        await sim.add_processor(uuid7(), proc)
        await sim.remove_processor(uuid7(), type(proc))

        assert world.add_processor.call_count == 1
        assert world.remove_processor.call_count == 1

    @pytest.mark.asyncio
    async def test_add_processor_appends_to_world_system(self, tmp_path):
        from archetype.core.aio.async_processor import AsyncProcessor

        class _Marker(AsyncProcessor):
            components = (_ListWorldsPos,)
            priority = 99

            async def process(self, df, **kwargs):
                return df

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(
                WorldConfig(name="add_proc_test"), storage
            )
            await container.simulation_service.add_processor(world.world_id, _Marker())
            names = [type(p).__name__ for p in world.system.processors]
            assert "_Marker" in names
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_remove_processor_deletes_from_world_system(self, tmp_path):
        from archetype.core.aio.async_processor import AsyncProcessor

        class _Removable(AsyncProcessor):
            components = (_ListWorldsPos,)
            priority = 99

            async def process(self, df, **kwargs):
                return df

        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(
                WorldConfig(name="rm_proc_test"), storage
            )
            await world.add_processor(_Removable())
            assert any(type(p).__name__ == "_Removable" for p in world.system.processors)

            await container.simulation_service.remove_processor(world.world_id, _Removable)
            assert all(type(p).__name__ != "_Removable" for p in world.system.processors)
        finally:
            await container.shutdown()

    def test_list_processors_returns_metadata(self):
        class Position:
            __name__ = "Position"

        processor = Mock()
        processor.priority = 10
        processor.components = (Position,)
        world = Mock()
        world.system.processors = [processor]
        sim = SimulationService.__new__(SimulationService)
        sim._worlds = Mock()
        sim._worlds.get_world.return_value = world

        infos = sim.list_processors(uuid7())

        assert len(infos) == 1
        assert infos[0].name == "Mock"
        assert infos[0].priority == 10
        assert infos[0].components == ["Position"]

    def test_list_processors_empty_when_world_has_no_system(self):
        class _World:
            pass

        sim = SimulationService.__new__(SimulationService)
        sim._worlds = Mock()
        sim._worlds.get_world.return_value = _World()

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
        ws = make_world_orchestrator()
        qs = QueryService(ws, broker=None)

        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="nb"), storage)
        try:
            history = await qs.get_command_history(world.world_id)
            assert history == []
        finally:
            await ws._storage_service.shutdown()

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
            await container.world_service.remove_world(world.world_id)
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

    @pytest.mark.asyncio
    async def test_remove_world_clears_broker_state(self, tmp_path):
        """remove_world clears the world's broker state."""
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="doomed"), storage)
            wid = world.world_id
            wid_str = str(wid)

            # Enqueue a few commands so the broker has non-trivial state.
            for i in range(3):
                cmd = Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_ListWorldsPos(x=i).to_payload()]},
                )
                await container.command_service.submit(wid, cmd, ctx)

            broker = container.broker
            assert len(broker._queues.get(wid_str, [])) == 3
            assert len(broker._history.get(wid_str, [])) == 3
            assert await broker.get_pending_count() >= 3

            await container.world_service.remove_world(wid)

            assert broker._queues.get(wid_str, []) == []
            assert broker._history.get(wid_str, []) == []
            assert await broker.get_pending_count() == 0
            assert all(cmd_id not in broker._pending for cmd_id in list(broker._pending))
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_destroy_world_command_clears_broker_state(self, tmp_path):
        """DESTROY_WORLD command clears broker state for the destroyed world."""
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(
                WorldConfig(name="ephemeral"), storage
            )
            wid = world.world_id
            wid_str = str(wid)

            spawn_cmd = Command(
                type=CommandType.SPAWN,
                tick=0,
                payload={"components": [_ListWorldsPos(x=0).to_payload()]},
            )
            await container.command_service.submit(wid, spawn_cmd, ctx)

            broker = container.broker
            assert len(broker._queues.get(wid_str, [])) == 1

            destroy_cmd = Command(
                type=CommandType.DESTROY_WORLD,
                tick=0,
                payload={"world_id": str(wid)},
            )
            await container.command_service.apply_world_lifecycle(destroy_cmd)

            assert broker._queues.get(wid_str, []) == []
            assert broker._history.get(wid_str, []) == []
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_list_worlds_reports_actual_entity_count(self, tmp_path):
        """list_worlds reports the actual entity count, not zero."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="counted"), storage)
            for i in range(5):
                await world.create_entity([_ListWorldsPos(x=i)])

            worlds = container.world_service.list_worlds()
            assert len(worlds) == 1
            assert worlds[0].entity_count == 5
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_create_world_with_explicit_none_world_id_generates_uuid(self, tmp_path):
        """create_world with explicit world_id=None produces a real UUID."""
        ws = make_world_orchestrator()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await ws.create_world(WorldConfig(name="t", world_id=None), storage)

            assert world.world_id is not None, (
                "WorldConfig(world_id=None) produced world_id=None — "
                "create_world's local fresh uuid7 was dead code"
            )
            # Round-trip lookup by the returned UUID must succeed.
            assert ws.get_world(world.world_id) is world
            assert None not in ws._worlds
        finally:
            await ws.shutdown()

    @pytest.mark.asyncio
    async def test_two_worlds_with_explicit_none_ids_do_not_collide(self, tmp_path):
        """Two creates with world_id=None produce distinct worlds."""
        ws = make_world_orchestrator()
        try:
            w1 = await ws.create_world(
                WorldConfig(name="a", world_id=None),
                StorageConfig(uri=str(tmp_path / "s1"), namespace="ns"),
            )
            w2 = await ws.create_world(
                WorldConfig(name="b", world_id=None),
                StorageConfig(uri=str(tmp_path / "s2"), namespace="ns"),
            )

            assert w1.world_id is not None
            assert w2.world_id is not None
            assert w1.world_id != w2.world_id, (
                "two WorldConfig(world_id=None) calls collapsed to the same id"
            )
            assert len(ws._worlds) == 2, (
                f"expected two distinct worlds, got {len(ws._worlds)} entries"
            )
            assert ws.get_world(w1.world_id) is w1
            assert ws.get_world(w2.world_id) is w2
        finally:
            await ws.shutdown()

    @pytest.mark.asyncio
    async def test_create_world_does_not_mutate_caller_config(self, tmp_path):
        """``create_world`` must not mutate the caller's ``WorldConfig``.

        The fix resolves ``world_id`` locally and threads it to the factory
        via ``model_copy``, leaving the caller's config object untouched.
        """
        ws = make_world_orchestrator()
        try:
            original = WorldConfig(name="immutable", world_id=None)
            assert original.world_id is None

            await ws.create_world(
                original,
                StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            )

            # The caller's config must still reflect what they passed in.
            assert original.world_id is None
        finally:
            await ws.shutdown()
