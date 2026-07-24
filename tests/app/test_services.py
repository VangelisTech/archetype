# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the application composition root and managed-world capabilities."""

import asyncio

import pytest
from uuid_utils import uuid7

import archetype.app.gateway.auth.guard as guard
from archetype.app.application.service import RuntimeApplication
from archetype.app.commands.service import CommandScheduler
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.guard import reset_daily_tokens
from archetype.app.gateway.service import CommandGateway
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.storage.service import StorageService
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry
from tests.conftest import make_world_harness


class _ListWorldsPos(Component):
    x: int = 0


@pytest.fixture(autouse=True)
def _reset_quotas():
    guard._tick_counters.clear()
    reset_daily_tokens()
    yield
    guard._tick_counters.clear()
    reset_daily_tokens()


class TestServiceContainer:
    def test_container_wires_services(self):
        container = ServiceContainer()
        assert isinstance(container.storage_service, StorageService)
        assert isinstance(container.command_scheduler, CommandScheduler)
        assert isinstance(container.world_registry, WorldRegistry)
        assert isinstance(container.world_lifecycle, WorldLifecycle)
        assert isinstance(container.command_gateway, CommandGateway)
        assert isinstance(container.application, RuntimeApplication)

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

    @pytest.mark.asyncio
    async def test_container_shutdown_drains_later_services_after_admission_failure(
        self, monkeypatch
    ):
        container = ServiceContainer()
        calls: list[str] = []

        async def fail_admission():
            calls.append("admission")
            raise RuntimeError("admission close failed")

        async def shutdown_audit():
            calls.append("audit")

        async def shutdown_storage():
            calls.append("storage")

        monkeypatch.setattr(container.application, "stop_admission", fail_admission)
        monkeypatch.setattr(container.audit_log, "shutdown", shutdown_audit)
        monkeypatch.setattr(container.storage_service, "shutdown", shutdown_storage)

        with pytest.raises(ExceptionGroup, match="failed for 1 step") as captured:
            await container.shutdown()

        assert calls == ["admission", "audit", "storage"]
        assert len(captured.value.exceptions) == 1
        assert "admission close failed" in str(captured.value.exceptions[0])

    @pytest.mark.asyncio
    async def test_container_shutdown_drains_later_services_after_cancellation(self, monkeypatch):
        container = ServiceContainer()
        calls: list[str] = []

        async def cancel_admission():
            calls.append("admission")
            raise asyncio.CancelledError("admission close cancelled")

        async def shutdown_audit():
            calls.append("audit")

        async def shutdown_storage():
            calls.append("storage")

        monkeypatch.setattr(container.application, "stop_admission", cancel_admission)
        monkeypatch.setattr(container.audit_log, "shutdown", shutdown_audit)
        monkeypatch.setattr(container.storage_service, "shutdown", shutdown_storage)

        with pytest.raises(BaseExceptionGroup, match="failed for 1 step") as captured:
            await container.shutdown()

        assert calls == ["admission", "audit", "storage"]
        assert len(captured.value.exceptions) == 1
        assert isinstance(captured.value.exceptions[0], asyncio.CancelledError)


class TestApplicationSimulation:
    @pytest.mark.asyncio
    async def test_step(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_lifecycle.create_world(WorldConfig(name="test"), storage)

            result = await container.application.step(world.world_id, RunConfig())
            assert result == 0
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_run(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_lifecycle.create_world(WorldConfig(name="test"), storage)

            result = await container.application.run(world.world_id, RunConfig(num_steps=3))
            assert result.ticks_completed == 3
            assert result.world_id == world.world_id
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_step_requires_run_config(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_lifecycle.create_world(WorldConfig(name="test"), storage)

            with pytest.raises(TypeError):
                await container.application.step(world.world_id)  # ty: ignore[missing-argument]
        finally:
            await container.shutdown()


class TestWorldLifecycleAndRegistry:
    @pytest.mark.asyncio
    async def test_create_and_list(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_lifecycle.create_world(WorldConfig(name="w1"), storage)
            worlds = await container.world_registry.list_worlds()
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

            first = await container.world_lifecycle.create_world(
                WorldConfig(world_id=world_id, name="original"), original
            )
            repeated = await container.world_lifecycle.create_world(
                WorldConfig(world_id=world_id, name="replacement"), replacement
            )

            assert repeated is first
            assert await container.world_registry.storage_record(str(world_id)) == (
                original,
                None,
            )
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
            first = asyncio.create_task(container.world_lifecycle.create_world(config, storage))
            await asyncio.wait_for(register_started.wait(), timeout=2)

            retry = asyncio.create_task(container.world_lifecycle.create_world(config, storage))
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
            world = await container.world_lifecycle.create_world(WorldConfig(name="alpha"), storage)
            found = await container.world_registry.live_world(
                await container.world_registry.world_id_for_name("alpha")
            )
            assert found is not None
            assert found.world_id == world.world_id
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_fork_rejects_duplicate_live_name_without_corrupting_index(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            base = await container.world_lifecycle.create_world(
                WorldConfig(name="duplicate"), storage
            )

            with pytest.raises(ValueError, match="duplicate"):
                await container.world_lifecycle.fork_world(
                    base.world_id,
                    name="duplicate",
                    storage_config=storage,
                )

            duplicate_id = await container.world_registry.world_id_for_name("duplicate")
            assert await container.world_registry.live_world(duplicate_id) is base
            assert await container.world_registry.list_worlds() == [base]
            assert len(await container.world_lifecycle.discover_worlds(storage)) == 1
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_list_worlds_reports_actual_entity_count(self, tmp_path):
        """list_worlds reports the actual entity count, not zero."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await container.world_lifecycle.create_world(
                WorldConfig(name="counted"), storage
            )
            for i in range(5):
                await world.create_entity([_ListWorldsPos(x=i)])

            worlds = await container.world_registry.list_worlds()
            assert len(worlds) == 1
            assert len(worlds[0].entity2sig) == 5
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_create_world_with_explicit_none_world_id_generates_uuid(self, tmp_path):
        """create_world with explicit world_id=None produces a real UUID."""
        ws = make_world_harness()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await ws.lifecycle.create_world(WorldConfig(name="t", world_id=None), storage)

            assert world.world_id is not None, (
                "WorldConfig(world_id=None) produced world_id=None — "
                "create_world's local fresh uuid7 was dead code"
            )
            # Round-trip lookup by the returned UUID must succeed.
            assert await ws.registry.live_world(str(world.world_id)) is world
            assert all(item.world_id is not None for item in await ws.registry.list_worlds())
        finally:
            await ws.close()

    @pytest.mark.asyncio
    async def test_two_worlds_with_explicit_none_ids_do_not_collide(self, tmp_path):
        """Two creates with world_id=None produce distinct worlds."""
        ws = make_world_harness()
        try:
            w1 = await ws.lifecycle.create_world(
                WorldConfig(name="a", world_id=None),
                StorageConfig(uri=str(tmp_path / "s1"), namespace="ns"),
            )
            w2 = await ws.lifecycle.create_world(
                WorldConfig(name="b", world_id=None),
                StorageConfig(uri=str(tmp_path / "s2"), namespace="ns"),
            )

            assert w1.world_id is not None
            assert w2.world_id is not None
            assert w1.world_id != w2.world_id, (
                "two WorldConfig(world_id=None) calls collapsed to the same id"
            )
            worlds = await ws.registry.list_worlds()
            assert len(worlds) == 2, f"expected two distinct worlds, got {len(worlds)} entries"
            assert await ws.registry.live_world(str(w1.world_id)) is w1
            assert await ws.registry.live_world(str(w2.world_id)) is w2
        finally:
            await ws.close()

    @pytest.mark.asyncio
    async def test_create_world_does_not_mutate_caller_config(self, tmp_path):
        """``create_world`` must not mutate the caller's ``WorldConfig``.

        The fix resolves ``world_id`` locally and threads it to the factory
        via ``model_copy``, leaving the caller's config object untouched.
        """
        ws = make_world_harness()
        try:
            original = WorldConfig(name="immutable", world_id=None)
            assert original.world_id is None

            await ws.lifecycle.create_world(
                original,
                StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            )

            # The caller's config must still reflect what they passed in.
            assert original.world_id is None
        finally:
            await ws.close()
