# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration contracts for exact direct/deferred command flow."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from uuid_utils import uuid7

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx, DeferredItem, DurableOptions
from archetype.commands.scheduler import CommandScheduler
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.errors import WorldNotFoundError
from archetype.runtime_resources import RuntimeResources
from archetype.storage.service import StorageService
from archetype.world.errors import WorldClosingError
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    WORLD_OPERATION_TYPES,
    AddComponents,
    ComponentTypeRef,
    ComponentValue,
    CreateWorld,
    Despawn,
    DestroyWorld,
    QueryComponents,
    ReserveEntityIds,
    Run,
    Spawn,
    SpawnReserved,
    Step,
    Update,
    WorldInfo,
)
from archetype.world.registry import WorldRegistry
from tests._runtime import build_test_runtime

_PORTABLE_TYPES = frozenset(PORTABLE_TICK_OPERATION_TYPES)
_DIRECT_OPERATION_TYPES = tuple(
    sorted(
        (
            operation_type
            for operation_type in WORLD_OPERATION_TYPES
            if operation_type not in _PORTABLE_TYPES
        ),
        key=lambda operation_type: str(
            operation_type.model_fields["operation"].default
        ),
    )
)


class CommandFlowMarker(Component):
    tag: str = ""


@dataclass
class _FlowRuntime:
    resources: RuntimeResources
    dispatcher: CommandDispatcher
    worlds: WorldRegistry
    lifecycle: WorldLifecycle
    scheduler: CommandScheduler
    storage: StorageService


@asynccontextmanager
async def _flow_runtime(tmp_path: Path):
    storage = StorageService()
    resources = build_test_runtime(
        tmp_path,
        storage_service=storage,
    )
    dispatcher = resources.dispatcher
    step_handler = dispatcher._registry.resolve_name("step").handler  # noqa: SLF001
    create_handler = dispatcher._registry.resolve_name("create_world").handler  # noqa: SLF001
    assert isinstance(step_handler, partial)
    assert isinstance(create_handler, partial)
    worlds = step_handler.args[0]
    lifecycle = getattr(create_handler.args[0], "__self__", None)
    assert isinstance(worlds, WorldRegistry)
    assert isinstance(lifecycle, WorldLifecycle)
    scheduler = dispatcher._scheduler  # noqa: SLF001 - exact composed-owner oracle
    assert isinstance(scheduler, CommandScheduler)
    harness = _FlowRuntime(
        resources=resources,
        dispatcher=dispatcher,
        worlds=worlds,
        lifecycle=lifecycle,
        scheduler=scheduler,
        storage=storage,
    )
    try:
        yield harness
    finally:
        for world in await worlds.list_worlds():
            await lifecycle.destroy_world(world.world_id)
        await resources.aclose()
        await storage.shutdown()


async def _create_world(
    harness: _FlowRuntime,
    *,
    name: str,
    storage: StorageConfig,
) -> WorldInfo:
    return await harness.dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name=name),
            storage_config=storage,
        )
    )


async def _marker_rows(
    harness: _FlowRuntime,
    info: WorldInfo,
    storage: StorageConfig,
    *,
    ticks: tuple[int, ...] | None = None,
) -> list[dict[str, Any]]:
    frame = await harness.dispatcher.apply(
        QueryComponents(
            components=(ComponentTypeRef.from_type(CommandFlowMarker),),
            world_id=info.world_id,
            run_id=info.run_id,
            storage_config=storage,
            ticks=ticks,
        )
    )
    return frame.to_pylist()


@pytest.mark.asyncio
async def test_deferred_spawn_reserved_id_survives_drain(tmp_path: Path) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="flow")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="flow", storage=storage)

        reserved_id, _command_id = await harness.dispatcher.defer_spawn_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandFlowMarker(tag="reserved")],
            ),
            DurableOptions(target_tick=0),
        )
        applied = await harness.dispatcher.apply(
            Step(world_id=info.world_id, run_config=RunConfig())
        )

        world = await harness.worlds.live_world(str(info.world_id))
        assert world is not None
        assert applied == 1
        assert reserved_id in world.entity2sig
        rows = await _marker_rows(harness, info, storage)
        assert [(row["entity_id"], row["tick"]) for row in rows] == [
            (reserved_id, 0)
        ]
        (record,) = await harness.scheduler.records(info.world_id)
        assert record.status == "APPLIED"


@pytest.mark.asyncio
async def test_materializer_failure_fails_tick_before_settlement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="materializer")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(
            harness,
            name="materializer-failure",
            storage=storage,
        )
        await harness.dispatcher.defer_spawn_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandFlowMarker(tag="retry")],
            ),
            DurableOptions(target_tick=0),
        )
        world = await harness.worlds.live_world(str(info.world_id))
        assert world is not None
        real_materialize = world._materialize_commands

        async def unavailable_materializer(_world: object, _tick: int) -> None:
            raise RuntimeError("command materializer unavailable")

        monkeypatch.setattr(
            world,
            "_materialize_commands",
            unavailable_materializer,
        )
        with pytest.raises(RuntimeError, match="command materializer unavailable"):
            await harness.dispatcher.apply(Step(world_id=info.world_id))

        assert world.tick == 0
        (pending,) = await harness.scheduler.records(info.world_id)
        assert pending.status == "PENDING"
        assert world.entity2sig == {}

        monkeypatch.setattr(world, "_materialize_commands", real_materialize)
        assert await harness.dispatcher.apply(Step(world_id=info.world_id)) == 1
        (applied,) = await harness.scheduler.records(info.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0


@pytest.mark.asyncio
async def test_replayed_reserved_spawn_is_not_applied_twice(tmp_path: Path) -> None:
    """The drain path enforces the direct mutation double-spawn guard."""

    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="spawn-replay")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="spawn-replay", storage=storage)
        (entity_id,) = await harness.dispatcher.apply(
            ReserveEntityIds(world_id=info.world_id, count=1)
        )
        first = SpawnReserved(
            world_id=info.world_id,
            entity_id=entity_id,
            components=(
                ComponentValue.from_component(CommandFlowMarker(tag="first")),
            ),
        )
        replay = SpawnReserved(
            world_id=info.world_id,
            entity_id=entity_id,
            components=(
                ComponentValue.from_component(CommandFlowMarker(tag="replay")),
            ),
        )
        await harness.dispatcher.defer_batch(
            (
                DeferredItem(
                    operation=first,
                    options=DurableOptions(target_tick=0),
                ),
                DeferredItem(
                    operation=replay,
                    options=DurableOptions(target_tick=0),
                ),
            )
        )

        applied = await harness.dispatcher.apply(Step(world_id=info.world_id))

        assert applied == 1
        records = await harness.scheduler.records(info.world_id)
        assert [record.status for record in records] == ["APPLIED", "REJECTED"]
        rows = await _marker_rows(harness, info, storage)
        assert len(rows) == 1
        assert rows[0][f"{CommandFlowMarker.get_prefix()}tag"] == "first"


@pytest.mark.asyncio
async def test_queued_update_is_applied_during_drain(tmp_path: Path) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="updates")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="updates", storage=storage)
        entity_id = await harness.dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandFlowMarker(tag="before")],
            )
        )
        await harness.dispatcher.apply(Step(world_id=info.world_id))

        await harness.dispatcher.defer_as(
            actor,
            Update(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(
                    ComponentValue.from_component(
                        CommandFlowMarker(tag="after")
                    ),
                ),
            ),
            DurableOptions(target_tick=1),
        )
        applied = await harness.dispatcher.apply(Step(world_id=info.world_id))

        assert applied == 1
        rows = await _marker_rows(harness, info, storage, ticks=(1,))
        assert rows[0][f"{CommandFlowMarker.get_prefix()}tag"] == "after"


@pytest.mark.asyncio
async def test_defer_to_unknown_world_is_rejected(tmp_path: Path) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    phantom = uuid7()
    async with _flow_runtime(tmp_path) as harness:
        operation = Despawn(world_id=phantom, entity_id=1)
        options = DurableOptions(target_tick=0)

        with pytest.raises(WorldNotFoundError):
            await harness.dispatcher.defer_as(actor, operation, options)

        with pytest.raises(WorldNotFoundError):
            await harness.dispatcher.defer_batch_as(
                actor,
                (DeferredItem(operation=operation, options=options),),
            )

        with pytest.raises(WorldNotFoundError):
            await harness.dispatcher.defer_spawn_as(
                actor,
                Spawn.from_components(
                    world_id=phantom,
                    components=[CommandFlowMarker(tag="x")],
                ),
                options,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_type", _DIRECT_OPERATION_TYPES)
async def test_direct_only_operations_cannot_enter_deferred_scheduler(
    tmp_path: Path,
    operation_type: type[BaseModel],
) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="direct-only")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="direct-only", storage=storage)
        operation = operation_type.model_construct()

        with pytest.raises(ValueError, match="direct-only"):
            await harness.dispatcher.defer(
                operation,
                DurableOptions(target_tick=0),
            )

        assert await harness.scheduler.pending_count(info.world_id) == 0
        assert await harness.scheduler.history(info.world_id) == []


@pytest.mark.asyncio
async def test_direct_only_operation_rejects_entire_deferred_batch(
    tmp_path: Path,
) -> None:
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="batch-direct")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="batch-direct", storage=storage)
        items = (
            DeferredItem(
                operation=Despawn(world_id=info.world_id, entity_id=1),
                options=DurableOptions(target_tick=0),
            ),
            DeferredItem(
                operation=CreateWorld(config=WorldConfig(name="not-admitted")),
                options=DurableOptions(target_tick=0),
            ),
        )

        with pytest.raises(ValueError, match="direct-only"):
            await harness.dispatcher.defer_batch(items)

        assert await harness.scheduler.pending_count(info.world_id) == 0
        assert await harness.scheduler.history(info.world_id) == []


@pytest.mark.asyncio
async def test_rejected_deferred_batch_does_not_debit_quota(
    tmp_path: Path,
) -> None:
    actor = ActorCtx(id=uuid7(), roles={"player"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="batch-quota")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="batch-quota", storage=storage)
        items = (
            DeferredItem(
                operation=Despawn(world_id=info.world_id, entity_id=1),
                options=DurableOptions(target_tick=0),
            ),
            DeferredItem(
                operation=AddComponents(
                    world_id=info.world_id,
                    entity_id=1,
                    components=(
                        ComponentValue.from_component(
                            CommandFlowMarker(tag="denied")
                        ),
                    ),
                ),
                options=DurableOptions(target_tick=0),
            ),
        )

        with pytest.raises(PermissionError):
            await harness.dispatcher.defer_batch_as(actor, items)

        policy = harness.dispatcher._policy  # noqa: SLF001 - atomic debit oracle
        assert policy._tick_debits == {}
        assert policy._daily_token_debits == {}
        assert await harness.scheduler.pending_count(info.world_id) == 0
        assert await harness.scheduler.history(info.world_id) == []


@pytest.mark.asyncio
async def test_admission_racing_destroy_is_cancelled_without_orphaning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="admit-destroy-race",
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(
            harness,
            name="admit-destroy-race",
            storage=storage,
        )
        catalog = harness.storage.get_control_catalog(storage)
        admit_commands = catalog.admit_commands

        async def blocked_admit(world_id: str, admissions: object) -> object:
            entered.set()
            await release.wait()
            return await admit_commands(world_id, admissions)

        monkeypatch.setattr(catalog, "admit_commands", blocked_admit)
        submit = asyncio.create_task(
            harness.dispatcher.defer_as(
                actor,
                Despawn(world_id=info.world_id, entity_id=9_999),
                DurableOptions(target_tick=10_000),
            )
        )
        await entered.wait()

        destroy = asyncio.create_task(
            harness.dispatcher.apply(DestroyWorld(world_id=info.world_id))
        )
        await asyncio.sleep(0)
        assert not destroy.done()

        release.set()
        command_id = await submit
        await destroy

        (record,) = await harness.scheduler.records(info.world_id)
        assert str(command_id) == record.command_id
        assert record.status == "REJECTED"
        assert not await harness.worlds.contains(str(info.world_id))

        with pytest.raises((WorldClosingError, WorldNotFoundError)):
            await harness.dispatcher.defer_as(
                actor,
                Despawn(world_id=info.world_id, entity_id=1),
                DurableOptions(target_tick=0),
            )


@pytest.mark.asyncio
async def test_run_result_run_id_round_trips_to_query(tmp_path: Path) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="run-id")
    async with _flow_runtime(tmp_path) as harness:
        info = await harness.dispatcher.apply_as(
            actor,
            CreateWorld(
                config=WorldConfig(name="run-id"),
                storage_config=storage,
            ),
        )
        await harness.dispatcher.apply_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandFlowMarker(tag="x")],
            ),
        )

        result = await harness.dispatcher.apply_as(
            actor,
            Run(
                world_id=info.world_id,
                run_config=RunConfig(num_steps=1),
            ),
        )
        world = await harness.worlds.live_world(str(info.world_id))
        assert world is not None
        assert str(result.run_id) == str(world.run_id)

        frame = await harness.dispatcher.apply_as(
            actor,
            QueryComponents(
                components=(ComponentTypeRef.from_type(CommandFlowMarker),),
                world_id=info.world_id,
                run_id=result.run_id,
                storage_config=storage,
            ),
        )
        assert frame.count_rows() >= 1


@pytest.mark.asyncio
async def test_defer_to_destroyed_world_is_rejected(tmp_path: Path) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="destroyed")
    async with _flow_runtime(tmp_path) as harness:
        info = await _create_world(harness, name="ephemeral", storage=storage)
        await harness.dispatcher.apply(DestroyWorld(world_id=info.world_id))

        with pytest.raises(WorldNotFoundError):
            await harness.dispatcher.defer_as(
                actor,
                Despawn(world_id=info.world_id, entity_id=1),
                DurableOptions(target_tick=0),
            )


@pytest.mark.asyncio
async def test_consecutive_runs_share_world_run_id(tmp_path: Path) -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runs")
    async with _flow_runtime(tmp_path) as harness:
        info = await harness.dispatcher.apply_as(
            actor,
            CreateWorld(
                config=WorldConfig(name="runs"),
                storage_config=storage,
            ),
        )
        await harness.dispatcher.apply_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandFlowMarker(tag="x")],
            ),
        )

        result_a = await harness.dispatcher.apply_as(
            actor,
            Run(
                world_id=info.world_id,
                run_config=RunConfig(num_steps=1),
            ),
        )
        result_b = await harness.dispatcher.apply_as(
            actor,
            Run(
                world_id=info.world_id,
                run_config=RunConfig(num_steps=1),
            ),
        )

        assert str(result_a.run_id) == str(result_b.run_id)
        world = await harness.worlds.live_world(str(info.world_id))
        assert world is not None
        frame = await harness.dispatcher.apply_as(
            actor,
            QueryComponents(
                components=(ComponentTypeRef.from_type(CommandFlowMarker),),
                world_id=info.world_id,
                run_id=world.run_id,
                storage_config=storage,
            ),
        )
        assert frame.count_rows() >= 1
