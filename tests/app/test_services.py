# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for service container, command service, simulation service, world service."""

import asyncio

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_service


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
    async def test_containers_borrow_shared_storage_service(self):
        class TrackedStorageService(StorageService):
            def __init__(self):
                super().__init__()
                self.shutdown_calls = 0

            async def shutdown(self):
                self.shutdown_calls += 1
                await super().shutdown()

        storage_service = TrackedStorageService()

        first = ServiceContainer(storage_service=storage_service)
        second = ServiceContainer(storage_service=storage_service)

        assert first.storage_service is storage_service
        assert second.storage_service is storage_service
        await first.shutdown()
        await second.shutdown()
        assert storage_service.shutdown_calls == 0

        await storage_service.shutdown()
        assert storage_service.shutdown_calls == 1

    @pytest.mark.asyncio
    async def test_container_shutdown(self):
        container = ServiceContainer()
        await container.shutdown()  # should not raise


class TestSimulationService:
    @pytest.mark.asyncio
    async def test_step(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)

            result = await container.simulation_service.step(world.world_id, RunConfig())
            assert result == 0
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
    async def test_idempotent_create_preserves_original_storage(self, tmp_path):
        container = ServiceContainer()
        try:
            world_id = uuid7()
            original = StorageConfig(uri=str(tmp_path / "original"), namespace="first")
            replacement = StorageConfig(uri=str(tmp_path / "replacement"), namespace="second")

            first = await container.world_service.create_world(
                WorldConfig(world_id=world_id, name="original"), original
            )
            repeated = await container.world_service.create_world(
                WorldConfig(world_id=world_id, name="replacement"), replacement
            )

            assert repeated is first
            assert container.world_service.storage_record(world_id) == (original, None)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_idempotent_create_waits_for_registration_and_fencing(
        self, tmp_path, monkeypatch
    ):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="create_race")
        catalog = container.storage_service.get_control_catalog(storage)
        register_started = asyncio.Event()
        allow_registration = asyncio.Event()
        real_register = catalog.register_world

        async def blocked_register(record):
            register_started.set()
            await allow_registration.wait()
            await real_register(record)

        monkeypatch.setattr(catalog, "register_world", blocked_register)
        config = WorldConfig(world_id=uuid7(), name="single-flight-create")
        try:
            first = asyncio.create_task(container.world_service.create_world(config, storage))
            await asyncio.wait_for(register_started.wait(), timeout=2)

            retry = asyncio.create_task(container.world_service.create_world(config, storage))
            await asyncio.sleep(0)
            returned_before_registration = retry.done()

            allow_registration.set()
            first_world, retry_world = await asyncio.gather(first, retry)

            assert not returned_before_registration
            assert retry_world is first_world
            assert retry_world.commit_coordinator is not None
        finally:
            allow_registration.set()
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
