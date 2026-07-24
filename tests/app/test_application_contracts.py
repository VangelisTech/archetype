# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the actor-free application boundary."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from daft import DataFrame

from archetype import AsyncProcessor, Component
from archetype.app.application.interfaces import iRuntimeApplication
from archetype.app.application.service import RuntimeApplication
from archetype.app.commands.interfaces import iCommandScheduler
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import CommittedTickReceipt
from archetype.storage.service import StorageService
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import WorldInfo
from archetype.world.registry import WorldRegistry
from archetype.world.simulation import PostCommitProjectionError, RequiredProjector

pytestmark = [
    pytest.mark.contract("runtime.lifecycle.single_flight_and_drain"),
    pytest.mark.integration,
    pytest.mark.race,
]


class Value(Component):
    number: int = 0


class BlockingProcessor(AsyncProcessor):
    components = (Value,)

    def __init__(self, entered: asyncio.Event, release: asyncio.Event) -> None:
        self.entered = entered
        self.release = release

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        self.entered.set()
        await self.release.wait()
        return df


@dataclass
class _Commands:
    """Narrow scheduler spy for facade lifecycle contracts."""

    registry: WorldRegistry
    cancellations: list[str] = field(default_factory=list)

    async def require_world(self, world_id: object) -> None:
        if not await self.registry.contains(str(world_id)):
            raise KeyError(str(world_id))

    async def cancel_world(self, world_id: object, **kwargs: object) -> int:
        del kwargs
        self.cancellations.append(str(world_id))
        return 0

    @staticmethod
    def validate_deferred(command: object) -> None:
        del command

    async def admit(self, *args: object, **kwargs: object) -> Any:
        raise AssertionError((args, kwargs))

    async def admit_batch(self, *args: object, **kwargs: object) -> Any:
        raise AssertionError((args, kwargs))

    async def admit_spawn(self, *args: object, **kwargs: object) -> Any:
        raise AssertionError((args, kwargs))


@dataclass
class _ApplicationHarness:
    application: RuntimeApplication
    lifecycle: WorldLifecycle
    registry: WorldRegistry
    storage: StorageService
    commands: _Commands


@asynccontextmanager
async def _application_harness():
    storage = StorageService()
    registry = WorldRegistry()
    lifecycle = WorldLifecycle(storage, registry)
    commands = _Commands(registry)
    application = RuntimeApplication(
        registry=registry,
        lifecycle=lifecycle,
        storage=storage,
        commands=cast(iCommandScheduler, commands),
    )
    assert isinstance(application, iRuntimeApplication)
    harness = _ApplicationHarness(
        application=application,
        lifecycle=lifecycle,
        registry=registry,
        storage=storage,
        commands=commands,
    )
    try:
        yield harness
    finally:
        for world in await registry.list_worlds():
            await lifecycle.destroy_world(world.world_id)
        await application.stop_admission()
        await storage.shutdown()


@pytest.mark.asyncio
async def test_same_world_steps_serialize_and_publish_distinct_manifests(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-serial")
    async with _application_harness() as harness:
        app = harness.application
        info = await app.create_world(WorldConfig(name="serial"), storage)
        await app.create_entity(info.world_id, [Value(number=1)])
        await asyncio.gather(
            app.step(info.world_id, RunConfig()),
            app.step(info.world_id, RunConfig()),
        )

        world = await harness.registry.live_world(str(info.world_id))
        assert world is not None
        active_run_id = str(world.run_id)
        rows = (
            await app.query_components([Value], str(info.world_id), active_run_id, storage)
        ).to_pylist()
        visible = await harness.storage.get_control_catalog(storage).visible_tokens(
            str(info.world_id), active_run_id, [0, 1]
        )

        assert world.tick == 2
        assert sorted(row["tick"] for row in rows) == [0, 1]
        assert visible is not None
        assert sorted(visible) == [0, 1]
        assert all(len(visible[tick]) == 1 for tick in (0, 1))


@pytest.mark.asyncio
async def test_destroy_waits_for_an_admitted_same_world_step(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-destroy")
    entered = asyncio.Event()
    release = asyncio.Event()
    async with _application_harness() as harness:
        app = harness.application
        info = await app.create_world(WorldConfig(name="destroy-order"), storage)
        await app.create_entity(info.world_id, [Value(number=1)])
        await app.add_processor(info.world_id, BlockingProcessor(entered, release))

        step = asyncio.create_task(app.step(info.world_id, RunConfig()))
        await entered.wait()
        destroy = asyncio.create_task(app.destroy_world(info.world_id))
        await asyncio.sleep(0)

        assert not destroy.done()
        assert harness.commands.cancellations == []
        with pytest.raises(RuntimeError, match="closing"):
            await app.get_world_info(info.world_id)
        release.set()
        await step
        await destroy
        assert not await harness.registry.contains(str(info.world_id))
        assert harness.commands.cancellations == [str(info.world_id)]


@pytest.mark.asyncio
async def test_public_destroy_projects_pending_receipt_before_command_cancellation(
    tmp_path,
) -> None:
    events: list[str] = []
    attempts = 0

    async def fail_twice(receipt) -> None:
        nonlocal attempts
        attempts += 1
        events.append(f"project:{receipt.committed_tick}")
        if attempts < 3:
            raise RuntimeError("required projection unavailable")

    storage_config = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="application-pending-destroy",
    )
    storage = StorageService()
    registry = WorldRegistry()
    projector = RequiredProjector(
        consumer_name="test.application-pending-destroy",
        project=fail_twice,
    )
    lifecycle = WorldLifecycle(
        storage,
        registry,
        required_projector_factory=lambda _world_id: projector,
    )
    commands = _Commands(registry)
    application = RuntimeApplication(
        registry=registry,
        lifecycle=lifecycle,
        storage=storage,
        commands=cast(iCommandScheduler, commands),
    )
    try:
        info = await application.create_world(
            WorldConfig(name="pending-destroy"),
            storage_config,
        )
        await application.create_entity(info.world_id, [Value(number=7)])

        with pytest.raises(PostCommitProjectionError):
            await application.step(info.world_id, RunConfig())

        pending = registry.pending_receipt(info.world_id)
        assert pending is not None
        with pytest.raises(PostCommitProjectionError):
            await application.destroy_world(info.world_id)

        assert registry.pending_receipt(info.world_id) is pending
        assert commands.cancellations == []
        assert events == ["project:0", "project:0"]
        catalog = storage.get_control_catalog(storage_config)
        record = await catalog.get_world(str(info.world_id))
        assert record is not None and record.status == "active"

        await application.destroy_world(info.world_id)

        assert events == ["project:0", "project:0", "project:0"]
        assert commands.cancellations == [str(info.world_id)]
        assert not await registry.contains(str(info.world_id))
        record = await catalog.get_world(str(info.world_id))
        assert record is not None and record.status == "destroyed"
    finally:
        await application.stop_admission()
        await storage.shutdown()


@pytest.mark.asyncio
async def test_world_info_entrypoints_reconcile_every_locked_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-info")
    async with _application_harness() as harness:
        first = await harness.application.create_world(WorldConfig(name="info-first"), storage)
        second = await harness.application.create_world(WorldConfig(name="info-second"), storage)
        recovered_ticks = {
            str(first.world_id): 3,
            str(second.world_id): 5,
        }
        reconciled: list[str] = []

        async def reconcile(registry, world_id, world) -> bool:
            assert registry is harness.registry
            reconciled.append(str(world_id))
            world.tick = recovered_ticks[str(world_id)]
            return True

        monkeypatch.setattr(
            "archetype.world.simulation.reconcile_committed_work_locked",
            reconcile,
        )

        info = await harness.application.get_world_info(first.world_id)
        listed = await harness.application.list_worlds()

        assert info.tick == 3
        assert [(str(item.world_id), item.tick) for item in listed] == [
            (str(first.world_id), 3),
            (str(second.world_id), 5),
        ]
        assert reconciled == [
            str(first.world_id),
            str(first.world_id),
            str(second.world_id),
        ]

        # Restore synthetic ticks before lifecycle teardown.
        for world in await harness.registry.list_worlds():
            world.tick = 0


@pytest.mark.asyncio
async def test_list_worlds_recovery_callback_can_enter_a_sibling_world(tmp_path) -> None:
    storage_config = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="application-info-cross-world",
    )
    storage = StorageService()
    registry = WorldRegistry()
    application: RuntimeApplication | None = None
    callback_info: list[WorldInfo] = []
    sibling_id: str | None = None

    async def project(receipt: CommittedTickReceipt) -> None:
        del receipt
        assert application is not None
        assert sibling_id is not None
        callback_info.append(await application.get_world_info(sibling_id))

    lifecycle = WorldLifecycle(
        storage,
        registry,
        required_projector_factory=lambda _world_id: RequiredProjector(
            consumer_name="test.list-worlds-cross-world",
            project=project,
        ),
    )
    commands = _Commands(registry)
    application = RuntimeApplication(
        registry=registry,
        lifecycle=lifecycle,
        storage=storage,
        commands=cast(iCommandScheduler, commands),
    )
    try:
        first = await application.create_world(
            WorldConfig(name="info-callback-source"),
            storage_config,
        )
        second = await application.create_world(
            WorldConfig(name="info-callback-target"),
            storage_config,
        )
        sibling_id = str(second.world_id)
        first_world = await registry.live_world(first.world_id)
        assert first_world is not None
        first_world.tick = 1
        registry.retain_receipt(
            first.world_id,
            CommittedTickReceipt(
                world_id=str(first.world_id),
                run_id=str(first.run_id),
                committed_tick=0,
                visibility_token="manifest-0",
                commands_applied=0,
            ),
        )

        listed = await asyncio.wait_for(application.list_worlds(), timeout=1)

        assert [(str(item.world_id), item.tick) for item in callback_info] == [
            (str(second.world_id), 0)
        ]
        assert {str(item.world_id): item.tick for item in listed} == {
            str(first.world_id): 1,
            str(second.world_id): 0,
        }
        assert registry.pending_receipt(first.world_id) is None
        first_world.tick = 0
    finally:
        for world in await registry.list_worlds():
            await lifecycle.destroy_world(world.world_id)
        await application.stop_admission()
        await storage.shutdown()


@pytest.mark.asyncio
async def test_different_registry_world_operations_execute_concurrently(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-parallel")
    release = asyncio.Event()
    entered_a = asyncio.Event()
    entered_b = asyncio.Event()
    async with _application_harness() as harness:
        app = harness.application
        first = await app.create_world(WorldConfig(name="first"), storage)
        second = await app.create_world(WorldConfig(name="second"), storage)
        for info, entered in ((first, entered_a), (second, entered_b)):
            await app.create_entity(info.world_id, [Value(number=1)])
            await app.add_processor(info.world_id, BlockingProcessor(entered, release))

        first_step = asyncio.create_task(app.step(first.world_id, RunConfig()))
        second_step = asyncio.create_task(app.step(second.world_id, RunConfig()))
        await asyncio.wait_for(asyncio.gather(entered_a.wait(), entered_b.wait()), timeout=2)

        release.set()
        await asyncio.gather(first_step, second_step)


@pytest.mark.asyncio
async def test_reserve_ids_uses_registry_operation_and_admission(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-reserve")
    entered = asyncio.Event()
    release = asyncio.Event()
    async with _application_harness() as harness:
        app = harness.application
        info = await app.create_world(WorldConfig(name="reserve-order"), storage)
        await app.create_entity(info.world_id, [Value(number=1)])
        await app.add_processor(info.world_id, BlockingProcessor(entered, release))

        step = asyncio.create_task(app.step(info.world_id, RunConfig()))
        await entered.wait()
        reservation = asyncio.create_task(app.reserve_entity_ids(info.world_id, 2))
        await asyncio.sleep(0)

        assert not reservation.done()
        release.set()
        await step
        assert await reservation == [2, 3]

        await app.stop_admission()
        with pytest.raises(RuntimeError, match="shutting down"):
            await app.reserve_entity_ids(info.world_id, 1)


@pytest.mark.asyncio
async def test_inherited_admission_context_cannot_bypass_registry_lock_or_close(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-inherited")
    async with _application_harness() as harness:
        app = harness.application
        info = await app.create_world(WorldConfig(name="inherited-context"), storage)

        async with app._admit(), harness.registry.operation(str(info.world_id)):
            reservation = asyncio.create_task(app.reserve_entity_ids(info.world_id, 1))
            await asyncio.sleep(0)
            assert not reservation.done()
            await harness.registry.begin_close(str(info.world_id))

        with pytest.raises(RuntimeError, match="closing"):
            await reservation
