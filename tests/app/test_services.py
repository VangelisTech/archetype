# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for explicit runtime resources and managed-world capabilities."""

import asyncio

import pytest
from uuid_utils import uuid7

from archetype.commands.dispatch import CommandDispatcher
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.runtime_resources import RuntimeCloseState, RuntimeResources
from archetype.storage.service import StorageService
from archetype.world.models import CreateWorld, Run, Step
from tests._runtime import build_test_runtime
from tests.conftest import make_world_harness


class _ListWorldsPos(Component):
    x: int = 0


class TestRuntimeResources:
    @pytest.mark.asyncio
    async def test_resources_expose_only_the_supported_dispatch_surface(self, tmp_path):
        resources = build_test_runtime(tmp_path)
        try:
            assert isinstance(resources, RuntimeResources)
            assert isinstance(resources.dispatcher, CommandDispatcher)
            assert {
                "application",
                "command_gateway",
                "world_registry",
                "world_lifecycle",
                "storage_service",
                "command_scheduler",
                "audit_log",
            }.isdisjoint(vars(resources))
        finally:
            await resources.aclose()

    @pytest.mark.asyncio
    async def test_resource_owners_borrow_shared_storage_service(self, tmp_path):
        class TrackedStorageService(StorageService):
            def __init__(self):
                super().__init__()
                self.shutdown_calls = 0

            async def shutdown(self):
                self.shutdown_calls += 1
                await super().shutdown()

        storage_service = TrackedStorageService()

        first = build_test_runtime(tmp_path, storage_service=storage_service)
        second = build_test_runtime(tmp_path, storage_service=storage_service)
        await first.aclose()
        await second.aclose()
        assert storage_service.shutdown_calls == 0

        await storage_service.shutdown()
        assert storage_service.shutdown_calls == 1

    @pytest.mark.asyncio
    async def test_resource_owner_close_is_idempotent(self, tmp_path):
        resources = build_test_runtime(tmp_path)

        await resources.aclose()
        await resources.aclose()

        assert resources.close_state is RuntimeCloseState.CLOSED


class TestApplicationSimulation:
    @pytest.mark.asyncio
    async def test_step(self, tmp_path):
        resources = build_test_runtime(tmp_path)
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await resources.dispatcher.apply(
                CreateWorld(config=WorldConfig(name="test"), storage_config=storage)
            )

            result = await resources.dispatcher.apply(
                Step(world_id=world.world_id, run_config=RunConfig())
            )
            assert result == 0
        finally:
            await resources.aclose()

    @pytest.mark.asyncio
    async def test_run(self, tmp_path):
        resources = build_test_runtime(tmp_path)
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await resources.dispatcher.apply(
                CreateWorld(config=WorldConfig(name="test"), storage_config=storage)
            )

            result = await resources.dispatcher.apply(
                Run(world_id=world.world_id, run_config=RunConfig(num_steps=3))
            )
            assert result.ticks_completed == 3
            assert result.world_id == world.world_id
        finally:
            await resources.aclose()

    def test_step_operation_has_an_explicit_default_run_config(self):
        operation = Step(world_id="world")
        assert operation.run_config == RunConfig()


class TestWorldLifecycleAndRegistry:
    @pytest.mark.asyncio
    async def test_create_and_list(self, tmp_path):
        harness = make_world_harness()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await harness.lifecycle.create_world(WorldConfig(name="w1"), storage)
            worlds = await harness.registry.list_worlds()
            assert len(worlds) == 1
            assert worlds[0].world_id == world.world_id
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_idempotent_create_preserves_original_storage(self, tmp_path):
        harness = make_world_harness()
        try:
            world_id = uuid7()
            original = StorageConfig(uri=str(tmp_path / "original"), namespace="first")
            replacement = StorageConfig(uri=str(tmp_path / "replacement"), namespace="second")

            first = await harness.lifecycle.create_world(
                WorldConfig(world_id=world_id, name="original"), original
            )
            repeated = await harness.lifecycle.create_world(
                WorldConfig(world_id=world_id, name="replacement"), replacement
            )

            assert repeated is first
            assert await harness.registry.storage_record(str(world_id)) == (
                original,
                None,
            )
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_idempotent_create_waits_for_registration_and_fencing(
        self, tmp_path, monkeypatch
    ):
        harness = make_world_harness()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="create_race")
        catalog = harness.storage.get_control_catalog(storage)
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
            first = asyncio.create_task(harness.lifecycle.create_world(config, storage))
            await asyncio.wait_for(register_started.wait(), timeout=2)

            retry = asyncio.create_task(harness.lifecycle.create_world(config, storage))
            await asyncio.sleep(0)
            returned_before_registration = retry.done()

            allow_registration.set()
            first_world, retry_world = await asyncio.gather(first, retry)

            assert not returned_before_registration
            assert retry_world is first_world
            assert retry_world.commit_coordinator is not None
        finally:
            allow_registration.set()
            await harness.close()

    @pytest.mark.asyncio
    async def test_get_world_by_name(self, tmp_path):
        harness = make_world_harness()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await harness.lifecycle.create_world(WorldConfig(name="alpha"), storage)
            found = await harness.registry.live_world(
                await harness.registry.world_id_for_name("alpha")
            )
            assert found is not None
            assert found.world_id == world.world_id
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_fork_rejects_duplicate_live_name_without_corrupting_index(self, tmp_path):
        harness = make_world_harness()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            base = await harness.lifecycle.create_world(WorldConfig(name="duplicate"), storage)

            with pytest.raises(ValueError, match="duplicate"):
                await harness.lifecycle.fork_world(
                    base.world_id,
                    name="duplicate",
                    storage_config=storage,
                )

            duplicate_id = await harness.registry.world_id_for_name("duplicate")
            assert await harness.registry.live_world(duplicate_id) is base
            assert await harness.registry.list_worlds() == [base]
            assert len(await harness.lifecycle.discover_worlds(storage)) == 1
        finally:
            await harness.close()

    @pytest.mark.asyncio
    async def test_list_worlds_reports_actual_entity_count(self, tmp_path):
        """list_worlds reports the actual entity count, not zero."""
        harness = make_world_harness()
        try:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = await harness.lifecycle.create_world(WorldConfig(name="counted"), storage)
            for i in range(5):
                await world.create_entity([_ListWorldsPos(x=i)])

            worlds = await harness.registry.list_worlds()
            assert len(worlds) == 1
            assert len(worlds[0].entity2sig) == 5
        finally:
            await harness.close()

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
