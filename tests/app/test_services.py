# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for service container, command service, simulation service, world service."""

import asyncio

import pytest
from daft import DataFrame, col
from uuid_utils import uuid7

from archetype.app.artifact_service import ArtifactService
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.errors import WorldNotFoundError
from archetype.app.models import EpisodeConfig, RolloutConfig
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_service


class _ListWorldsPos(Component):
    x: int = 0


class _SerializedCounter(Component):
    value: int = 0


class _BlockingIncrement(AsyncProcessor):
    components = (_SerializedCounter,)

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        self.entered.set()
        await self.release.wait()
        return df.with_column("_serializedcounter__value", col("_serializedcounter__value") + 1)


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
        assert isinstance(container.artifact_service, ArtifactService)

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
    async def test_concurrent_same_world_steps_publish_distinct_ticks(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="serialized_steps")
        try:
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await container.mutation_service.create_entity(world.world_id, [_SerializedCounter()])
            await container.simulation_service.step(world.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.mutation_service.add_processor(world.world_id, processor)

            first = asyncio.create_task(
                container.simulation_service.step(world.world_id, RunConfig())
            )
            await processor.entered.wait()
            second = asyncio.create_task(
                container.simulation_service.step(world.world_id, RunConfig())
            )
            processor.release.set()
            await asyncio.gather(first, second)

            catalog = container.storage_service.get_control_catalog(storage)
            manifest_tick = await catalog.max_manifest_tick(str(world.world_id), str(world.run_id))
            rows = (
                await container.query_service.query_components(
                    [_SerializedCounter],
                    str(world.world_id),
                    str(world.run_id),
                    storage,
                    ticks=[0, 1, 2],
                )
            ).to_pylist()

            assert world.tick == manifest_tick + 1 == 3
            assert sorted((row["tick"], row["_serializedcounter__value"]) for row in rows) == [
                (0, 0),
                (1, 1),
                (2, 2),
            ]
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_destroy_waits_for_admitted_step_and_closes_admission(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="destroy_waits")
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        try:
            info = await container.command_service.create_world(
                ctx, WorldConfig(name="test"), storage
            )
            await container.command_service.create_entity(
                ctx, info.world_id, [_SerializedCounter()]
            )
            await container.command_service.step(ctx, info.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.command_service.add_processor(ctx, info.world_id, processor)

            step = asyncio.create_task(
                container.command_service.step(ctx, info.world_id, RunConfig())
            )
            await processor.entered.wait()
            destroy = asyncio.create_task(
                container.command_service.destroy_world(ctx, info.world_id)
            )
            await asyncio.sleep(0)
            assert not destroy.done()

            processor.release.set()
            await asyncio.gather(step, destroy)

            with pytest.raises(WorldNotFoundError):
                await container.command_service.step(ctx, info.world_id, RunConfig())
            record = await container.storage_service.get_control_catalog(storage).get_world(
                str(info.world_id)
            )
            assert record is not None
            assert record.status == "destroyed"
            assert record.tick_head == 1
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_fork_waits_for_admitted_step_snapshot(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="fork_waits")
        try:
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await container.mutation_service.create_entity(world.world_id, [_SerializedCounter()])
            await container.simulation_service.step(world.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.mutation_service.add_processor(world.world_id, processor)

            step = asyncio.create_task(
                container.simulation_service.step(world.world_id, RunConfig())
            )
            await processor.entered.wait()
            fork_task = asyncio.create_task(
                container.world_service.fork_world(world.world_id, name="after-step")
            )
            await asyncio.sleep(0)
            assert not fork_task.done()

            processor.release.set()
            async with asyncio.timeout(5):
                _commands_applied, fork = await asyncio.gather(step, fork_task)

            assert world.tick == fork.tick == 2
            assert fork.lineage[-1] == (str(world.world_id), str(world.run_id), 1)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_cancelled_destroy_reopens_live_world_admission(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="cancel_destroy")
        try:
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await container.mutation_service.create_entity(world.world_id, [_SerializedCounter()])
            await container.simulation_service.step(world.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.mutation_service.add_processor(world.world_id, processor)

            step = asyncio.create_task(
                container.simulation_service.step(world.world_id, RunConfig())
            )
            await processor.entered.wait()
            destroy = asyncio.create_task(container.world_service.destroy_world(world.world_id))
            await asyncio.sleep(0)
            assert not destroy.done()

            destroy.cancel()
            with pytest.raises(asyncio.CancelledError):
                await destroy
            processor.release.set()
            await step

            assert container.world_service.has_world(world.world_id)
            await container.simulation_service.step(world.world_id, RunConfig())
            assert world.tick == 3
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_destroy_prelude_failure_reopens_live_world_admission(
        self, tmp_path, monkeypatch
    ):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="failed_destroy")
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        try:
            info = await container.command_service.create_world(
                ctx, WorldConfig(name="test"), storage
            )

            async def fail_flush():
                raise RuntimeError("audit storage unavailable")

            real_flush = container.audit_log.flush
            monkeypatch.setattr(container.audit_log, "flush", fail_flush)
            with pytest.raises(RuntimeError, match="audit storage unavailable"):
                await container.command_service.destroy_world(ctx, info.world_id)
            monkeypatch.setattr(container.audit_log, "flush", real_flush)

            assert container.world_service.has_world(info.world_id)
            await container.simulation_service.step(info.world_id, RunConfig())
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_run_holds_serial_order_for_its_full_tick_sequence(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="serialized_run")
        try:
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await container.mutation_service.create_entity(world.world_id, [_SerializedCounter()])
            await container.simulation_service.step(world.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.mutation_service.add_processor(world.world_id, processor)

            run = asyncio.create_task(
                container.simulation_service.run(world.world_id, RunConfig(num_steps=2))
            )
            await processor.entered.wait()
            step = asyncio.create_task(
                container.simulation_service.step(world.world_id, RunConfig())
            )
            await asyncio.sleep(0)
            assert not step.done()

            processor.release.set()
            run_result, _commands_applied = await asyncio.gather(run, step)

            assert run_result.final_tick == 3
            assert world.tick == 4
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_destroy_waits_for_admitted_episode(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="serialized_episode")
        try:
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await container.mutation_service.create_entity(world.world_id, [_SerializedCounter()])
            await container.simulation_service.step(world.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.mutation_service.add_processor(world.world_id, processor)

            episode = asyncio.create_task(
                container.simulation_service.run_episode(
                    world.world_id,
                    EpisodeConfig(max_steps=2),
                )
            )
            await processor.entered.wait()
            destroy = asyncio.create_task(container.world_service.destroy_world(world.world_id))
            await asyncio.sleep(0)
            assert not destroy.done()

            processor.release.set()
            result, _destroyed = await asyncio.gather(episode, destroy)

            assert result.duration_steps == 2
            assert not container.world_service.has_world(world.world_id)
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_destroy_waits_for_admitted_rollout(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="serialized_rollout")
        try:
            world = await container.world_service.create_world(WorldConfig(name="test"), storage)
            await container.mutation_service.create_entity(world.world_id, [_SerializedCounter()])
            await container.simulation_service.step(world.world_id, RunConfig())
            processor = _BlockingIncrement()
            await container.mutation_service.add_processor(world.world_id, processor)

            rollout = asyncio.create_task(
                container.simulation_service.run_rollout(
                    world.world_id,
                    RolloutConfig(
                        num_episodes=1,
                        episode_config=EpisodeConfig(max_steps=1),
                        destroy_forks_on_complete=True,
                    ),
                )
            )
            await processor.entered.wait()
            destroy = asyncio.create_task(container.world_service.destroy_world(world.world_id))
            await asyncio.sleep(0)
            assert not destroy.done()

            processor.release.set()
            result, _destroyed = await asyncio.gather(rollout, destroy)

            assert result.num_episodes == 1
            assert not container.world_service.has_world(world.world_id)
            assert all(
                not container.world_service.has_world(episode.world_id)
                for episode in result.episodes
            )
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_steps_on_different_worlds_remain_concurrent(self, tmp_path):
        container = ServiceContainer()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="independent_worlds")
        try:
            worlds = [
                await container.world_service.create_world(WorldConfig(name=f"world-{i}"), storage)
                for i in range(2)
            ]
            processors = [_BlockingIncrement(), _BlockingIncrement()]
            for world, processor in zip(worlds, processors, strict=True):
                await container.mutation_service.create_entity(
                    world.world_id, [_SerializedCounter()]
                )
                await container.simulation_service.step(world.world_id, RunConfig())
                await container.mutation_service.add_processor(world.world_id, processor)

            steps = [
                asyncio.create_task(container.simulation_service.step(world.world_id, RunConfig()))
                for world in worlds
            ]
            async with asyncio.timeout(1):
                await asyncio.gather(*(processor.entered.wait() for processor in processors))
            for processor in processors:
                processor.release.set()
            await asyncio.gather(*steps)

            assert [world.tick for world in worlds] == [2, 2]
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
