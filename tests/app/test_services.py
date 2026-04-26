# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for service container, command service, simulation service, world service."""

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
from archetype.core.component import Component
from tests.conftest import make_world_service
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

            result = await container.simulation_service.step(world.world_id, RunConfig())
            assert result is None  # step returns None
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
            assert len(worlds[0].entity2sig) == 5
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_create_world_with_explicit_none_world_id_generates_uuid(self, tmp_path):
        """create_world with explicit world_id=None produces a real UUID."""
        ws = make_world_service()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await ws.create_world(WorldConfig(name="t", world_id=None), storage)

            assert world.world_id is not None, (
                "WorldConfig(world_id=None) produced world_id=None — "
                "create_world's local fresh uuid7 was dead code"
            )
            # Round-trip lookup by the returned UUID must succeed.
            assert ws.get_world(world.world_id) is world
            assert None not in ws._orchestrator._registry._worlds
        finally:
            await ws.shutdown()

    @pytest.mark.asyncio
    async def test_two_worlds_with_explicit_none_ids_do_not_collide(self, tmp_path):
        """Two creates with world_id=None produce distinct worlds."""
        ws = make_world_service()
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
            assert len(ws._orchestrator._registry._worlds) == 2, (
                f"expected two distinct worlds, got {len(ws._orchestrator._registry._worlds)} entries"
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
        ws = make_world_service()
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
