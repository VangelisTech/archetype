# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the trusted exact-operation boundary."""

from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, cast

import pytest
from daft import DataFrame

from archetype import AsyncProcessor, Component
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.scheduler import CommandScheduler
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import CommittedTickReceipt
from archetype.runtime_resources import RuntimeResources
from archetype.storage.service import StorageService
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    AddProcessor,
    ComponentTypeRef,
    CreateWorld,
    DestroyWorld,
    GetWorldInfo,
    ListWorlds,
    QueryComponents,
    ReserveEntityIds,
    Spawn,
    Step,
    WorldInfo,
)
from archetype.world.registry import WorldRegistry
from archetype.world.simulation import PostCommitProjectionError, RequiredProjector
from tests._runtime import build_test_runtime

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


def _wiring_parts(
    dispatcher: CommandDispatcher,
) -> tuple[WorldRegistry, WorldLifecycle, CommandScheduler]:
    step_handler = dispatcher._registry.resolve_name("step").handler  # noqa: SLF001
    assert isinstance(step_handler, partial)
    registry = step_handler.args[0]
    assert isinstance(registry, WorldRegistry)

    create_handler = dispatcher._registry.resolve_name("create_world").handler  # noqa: SLF001
    assert isinstance(create_handler, partial)
    create_world = create_handler.args[0]
    lifecycle = getattr(create_world, "__self__", None)
    assert isinstance(lifecycle, WorldLifecycle)

    destroy_handler = dispatcher._registry.resolve_name("destroy_world").handler  # noqa: SLF001
    assert isinstance(destroy_handler, partial)
    destroy_world = destroy_handler.args[0]
    scheduler = inspect.getclosurevars(destroy_world).nonlocals["scheduler"]
    assert isinstance(scheduler, CommandScheduler)
    return registry, lifecycle, scheduler


def _record_scheduler_cancellations(scheduler: CommandScheduler) -> list[str]:
    """Observe teardown while preserving the concrete scheduler behavior."""

    cancellations: list[str] = []
    cancel_world = scheduler.cancel_world

    async def record(
        world_id: object,
        *,
        reason: str = "world destroyed",
    ) -> int:
        cancellations.append(str(world_id))
        return await cancel_world(world_id, reason=reason)

    cast(Any, scheduler).cancel_world = record
    return cancellations


@dataclass
class _ApplicationHarness:
    resources: RuntimeResources
    dispatcher: CommandDispatcher
    lifecycle: WorldLifecycle
    registry: WorldRegistry
    storage: StorageService
    cancellations: list[str]


@asynccontextmanager
async def _application_harness(tmp_path: Path, **overrides: Any):
    storage = StorageService()
    resources = build_test_runtime(
        tmp_path,
        storage_service=storage,
        **overrides,
    )
    dispatcher = resources.dispatcher
    registry, lifecycle, scheduler = _wiring_parts(dispatcher)
    harness = _ApplicationHarness(
        resources=resources,
        dispatcher=dispatcher,
        lifecycle=lifecycle,
        registry=registry,
        storage=storage,
        cancellations=_record_scheduler_cancellations(scheduler),
    )
    try:
        yield harness
    finally:
        for world in await registry.list_worlds():
            await lifecycle.destroy_world(world.world_id)
        await resources.aclose()
        await storage.shutdown()


@pytest.mark.asyncio
async def test_same_world_steps_serialize_and_publish_distinct_manifests(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-serial")
    async with _application_harness(tmp_path) as harness:
        dispatcher = harness.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="serial"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Value(number=1)],
            )
        )
        await asyncio.gather(
            dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig())),
            dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig())),
        )

        world = await harness.registry.live_world(str(info.world_id))
        assert world is not None
        active_run_id = str(world.run_id)
        rows = (
            await dispatcher.apply(
                QueryComponents(
                    components=(ComponentTypeRef.from_type(Value),),
                    world_id=info.world_id,
                    run_id=active_run_id,
                    storage_config=storage,
                )
            )
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
    async with _application_harness(tmp_path) as harness:
        dispatcher = harness.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="destroy-order"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Value(number=1)],
            )
        )
        await dispatcher.apply(
            AddProcessor(
                world_id=info.world_id,
                processor=BlockingProcessor(entered, release),
            )
        )

        step = asyncio.create_task(
            dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        )
        await entered.wait()
        destroy = asyncio.create_task(
            dispatcher.apply(DestroyWorld(world_id=info.world_id))
        )
        await asyncio.sleep(0)

        assert not destroy.done()
        assert harness.cancellations == []
        with pytest.raises(RuntimeError, match="closing"):
            await dispatcher.apply(GetWorldInfo(world_id=info.world_id))
        release.set()
        await step
        await destroy
        assert not await harness.registry.contains(str(info.world_id))
        assert harness.cancellations == [str(info.world_id)]


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
    projector = RequiredProjector(
        consumer_name="test.application-pending-destroy",
        project=fail_twice,
    )
    async with _application_harness(
        tmp_path,
        required_projector_factory=lambda _world_id: projector,
    ) as harness:
        dispatcher = harness.dispatcher
        registry = harness.registry
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="pending-destroy"),
                storage_config=storage_config,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Value(number=7)],
            )
        )

        with pytest.raises(PostCommitProjectionError):
            await dispatcher.apply(
                Step(world_id=info.world_id, run_config=RunConfig())
            )

        pending = registry.pending_receipt(info.world_id)
        assert pending is not None
        with pytest.raises(PostCommitProjectionError):
            await dispatcher.apply(DestroyWorld(world_id=info.world_id))

        assert registry.pending_receipt(info.world_id) is pending
        assert harness.cancellations == []
        assert events == ["project:0", "project:0"]
        catalog = harness.storage.get_control_catalog(storage_config)
        record = await catalog.get_world(str(info.world_id))
        assert record is not None and record.status == "active"

        await dispatcher.apply(DestroyWorld(world_id=info.world_id))

        assert events == ["project:0", "project:0", "project:0"]
        assert harness.cancellations == [str(info.world_id)]
        assert not await registry.contains(str(info.world_id))
        record = await catalog.get_world(str(info.world_id))
        assert record is not None and record.status == "destroyed"


@pytest.mark.asyncio
async def test_world_info_entrypoints_reconcile_every_locked_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-info")
    async with _application_harness(tmp_path) as harness:
        first = await harness.dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="info-first"),
                storage_config=storage,
            )
        )
        second = await harness.dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="info-second"),
                storage_config=storage,
            )
        )
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

        info = await harness.dispatcher.apply(GetWorldInfo(world_id=first.world_id))
        listed = await harness.dispatcher.apply(ListWorlds())

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
    dispatcher: CommandDispatcher | None = None
    callback_info: list[WorldInfo] = []
    sibling_id: str | None = None

    async def project(receipt: CommittedTickReceipt) -> None:
        del receipt
        assert dispatcher is not None
        assert sibling_id is not None
        callback_info.append(
            await dispatcher.apply(GetWorldInfo(world_id=sibling_id))
        )

    async with _application_harness(
        tmp_path,
        required_projector_factory=lambda _world_id: RequiredProjector(
            consumer_name="test.list-worlds-cross-world",
            project=project,
        ),
    ) as harness:
        dispatcher = harness.dispatcher
        registry = harness.registry
        first = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="info-callback-source"),
                storage_config=storage_config,
            )
        )
        second = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="info-callback-target"),
                storage_config=storage_config,
            )
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

        listed = await asyncio.wait_for(dispatcher.apply(ListWorlds()), timeout=1)

        assert [(str(item.world_id), item.tick) for item in callback_info] == [
            (str(second.world_id), 0)
        ]
        assert {str(item.world_id): item.tick for item in listed} == {
            str(first.world_id): 1,
            str(second.world_id): 0,
        }
        assert registry.pending_receipt(first.world_id) is None
        first_world.tick = 0


@pytest.mark.asyncio
async def test_different_registry_world_operations_execute_concurrently(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-parallel")
    release = asyncio.Event()
    entered_a = asyncio.Event()
    entered_b = asyncio.Event()
    async with _application_harness(tmp_path) as harness:
        dispatcher = harness.dispatcher
        first = await dispatcher.apply(
            CreateWorld(config=WorldConfig(name="first"), storage_config=storage)
        )
        second = await dispatcher.apply(
            CreateWorld(config=WorldConfig(name="second"), storage_config=storage)
        )
        for info, entered in ((first, entered_a), (second, entered_b)):
            await dispatcher.apply(
                Spawn.from_components(
                    world_id=info.world_id,
                    components=[Value(number=1)],
                )
            )
            await dispatcher.apply(
                AddProcessor(
                    world_id=info.world_id,
                    processor=BlockingProcessor(entered, release),
                )
            )

        first_step = asyncio.create_task(
            dispatcher.apply(Step(world_id=first.world_id))
        )
        second_step = asyncio.create_task(
            dispatcher.apply(Step(world_id=second.world_id))
        )
        await asyncio.wait_for(asyncio.gather(entered_a.wait(), entered_b.wait()), timeout=2)

        release.set()
        await asyncio.gather(first_step, second_step)


@pytest.mark.asyncio
async def test_reserve_ids_uses_registry_operation_and_admission(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-reserve")
    entered = asyncio.Event()
    release = asyncio.Event()
    async with _application_harness(tmp_path) as harness:
        dispatcher = harness.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="reserve-order"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[Value(number=1)],
            )
        )
        await dispatcher.apply(
            AddProcessor(
                world_id=info.world_id,
                processor=BlockingProcessor(entered, release),
            )
        )

        step = asyncio.create_task(
            dispatcher.apply(Step(world_id=info.world_id))
        )
        await entered.wait()
        reservation = asyncio.create_task(
            dispatcher.apply(
                ReserveEntityIds(world_id=info.world_id, count=2)
            )
        )
        await asyncio.sleep(0)

        assert not reservation.done()
        release.set()
        await step
        assert await reservation == [2, 3]

        await dispatcher.stop_admission()
        with pytest.raises(RuntimeError, match="not accepting work"):
            await dispatcher.apply(
                ReserveEntityIds(world_id=info.world_id, count=1)
            )


@pytest.mark.asyncio
async def test_inherited_admission_context_cannot_bypass_registry_lock_or_close(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="application-inherited")
    async with _application_harness(tmp_path) as harness:
        dispatcher = harness.dispatcher
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="inherited-context"),
                storage_config=storage,
            )
        )

        async with (
            dispatcher._admitted(),  # noqa: SLF001 - inherited admission oracle
            harness.registry.operation(str(info.world_id)),
        ):
            reservation = asyncio.create_task(
                dispatcher.apply(
                    ReserveEntityIds(world_id=info.world_id, count=1)
                )
            )
            await asyncio.sleep(0)
            assert not reservation.done()
            await harness.registry.begin_close(str(info.world_id))

        with pytest.raises(RuntimeError, match="closing"):
            await reservation
