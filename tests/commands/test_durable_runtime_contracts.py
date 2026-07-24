# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the durable command ledger and tick settlement."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, create_model
from uuid_utils import uuid7

from archetype.commands import (
    ActorCtx,
    CommandScheduler,
    DeferredItem,
    DurableOperation,
    DurableOptions,
    GetAuditHistory,
    OperationRegistry,
    OperationSpec,
)
from archetype.commands.audit import AuditLog
from archetype.commands.dispatch import CommandDispatcher
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.errors import AmbiguousTickCommitError
from archetype.core.hooks import HookRegistry, OnDestroy
from archetype.core.interfaces import StaleWriterError
from archetype.core.resources import Resources
from archetype.runtime_resources import RuntimeResources
from archetype.storage.catalog import (
    CommandConflictError,
    SqliteControlCatalog,
    WorldRecord,
    catalog_path_for,
)
from archetype.storage.commit import CatalogCommitCoordinator
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.handlers import materialize_locked
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    AddHook,
    ComponentTypeRef,
    ComponentValue,
    CreateWorld,
    DestroyWorld,
    OpenWorldReadonly,
    RemoveComponents,
    ReserveEntityIds,
    ResumeWorld,
    Spawn,
    SpawnReserved,
    Step,
    Update,
)
from archetype.world.registry import WorldRegistry

pytestmark = [
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
    pytest.mark.integration,
]


class DurableMarker(Component):
    value: int = 0


@dataclass
class _RuntimeProcess:
    resources: RuntimeResources
    dispatcher: CommandDispatcher
    scheduler: CommandScheduler
    worlds: WorldRegistry
    lifecycle: WorldLifecycle
    storage: StorageService
    audit: AuditLog

    async def shutdown(self) -> None:
        await self.resources.aclose()
        await self.storage.shutdown()


def _runtime_process(
    tmp_path: Path,
    *,
    audit_storage_config: StorageConfig,
) -> _RuntimeProcess:
    control_config = ControlCatalogConfig(catalog_dir=tmp_path / "control-catalogs")
    storage = StorageService(control_catalog_config=control_config)
    resources = build_runtime_resources(
        RuntimeBootstrapConfig(
            control_catalog_config=control_config,
            storage_service=storage,
            audit_storage_config=audit_storage_config,
        )
    )
    dispatcher = resources.dispatcher
    step_handler = dispatcher._registry.resolve_name("step").handler  # noqa: SLF001
    create_handler = dispatcher._registry.resolve_name("create_world").handler  # noqa: SLF001
    audit_handler = dispatcher._registry.resolve_name("get_audit_history").handler  # noqa: SLF001
    assert isinstance(step_handler, partial)
    assert isinstance(create_handler, partial)
    assert isinstance(audit_handler, partial)
    worlds = step_handler.args[0]
    lifecycle = getattr(create_handler.args[0], "__self__", None)
    audit = audit_handler.args[0]
    assert isinstance(worlds, WorldRegistry)
    assert isinstance(lifecycle, WorldLifecycle)
    assert isinstance(audit, AuditLog)
    return _RuntimeProcess(
        resources=resources,
        dispatcher=dispatcher,
        scheduler=dispatcher._scheduler,  # noqa: SLF001 - exact owner oracle
        worlds=worlds,
        lifecycle=lifecycle,
        storage=storage,
        audit=audit,
    )


async def _create_world(
    process: _RuntimeProcess,
    config: WorldConfig,
    storage: StorageConfig,
) -> AsyncWorld:
    info = await process.dispatcher.apply(
        CreateWorld(config=config, storage_config=storage)
    )
    world = await process.worlds.live_world(str(info.world_id))
    assert isinstance(world, AsyncWorld)
    return world


async def _materializer_harness(tmp_path, namespace: str):
    """Build the canonical registry/scheduler seam without process wiring."""
    world_id = str(uuid7())
    run_id = uuid7()
    catalog = SqliteControlCatalog(tmp_path / f"{namespace}.db")
    await catalog.register_world(
        WorldRecord(
            world_id=world_id,
            name=namespace,
            run_id=str(run_id),
            parent_world_id=None,
            status="active",
            tick_head=0,
        )
    )
    epoch = await catalog.acquire_fence(world_id, "materializer-test")
    coordinator = CatalogCommitCoordinator.bound(
        catalog,
        world_id,
        str(run_id),
        writer_epoch=epoch,
    )
    world = AsyncWorld(
        world_id=world_id,
        name=namespace,
        querier=MagicMock(),
        updater=MagicMock(),
        system=MagicMock(),
        resources=Resources(),
        hooks=HookRegistry(),
        run_id=run_id,
        commit_coordinator=coordinator,
    )

    async def resolve_control_catalog(candidate: str):
        assert str(candidate) == world_id
        return catalog

    async def reserve_entity_ids(candidate, count: int) -> list[int]:
        assert str(candidate) == world_id
        return world.reserve_entity_ids(count)

    async def direct_handler(_operation: BaseModel) -> None:
        raise AssertionError("durable materialization must not call the direct handler")

    def summarize(operation: BaseModel) -> dict[str, Any]:
        return {
            "operation": cast("Any", operation).operation,
            "world_id": str(cast("Any", operation).world_id),
        }

    registry = OperationRegistry()
    for model in PORTABLE_TICK_OPERATION_TYPES:
        operation_name = model.model_fields["operation"].default
        assert isinstance(operation_name, str)
        registry.register(
            OperationSpec(
                name=operation_name,
                model=model,
                handler=direct_handler,
                permission=operation_name,
                summarize=summarize,
                quota_scope="live_world",
                world_key=lambda operation: cast("Any", operation).world_id,
                durable=DurableOperation(
                    decode=model.model_validate_json,
                    materialize=cast("Any", materialize_locked),
                ),
            )
        )

    scheduler = CommandScheduler(
        registry=registry,
        catalog_for_world=resolve_control_catalog,
        reserve_entity_ids=reserve_entity_ids,
        owner="materializer-test",
    )
    return scheduler, world, catalog, coordinator


def _components(*values: Component) -> tuple[ComponentValue, ...]:
    return tuple(ComponentValue.from_component(value) for value in values)


def _spawn_reserved(
    world_id: object,
    entity_id: int,
    *components: Component,
) -> SpawnReserved:
    return SpawnReserved(
        world_id=cast("Any", world_id),
        entity_id=entity_id,
        components=_components(*components),
    )


def _update(
    world_id: object,
    entity_id: int,
    *components: Component,
) -> Update:
    return Update(
        world_id=cast("Any", world_id),
        entity_id=entity_id,
        components=_components(*components),
    )


def _item(
    operation: BaseModel,
    *,
    target_tick: int = 0,
    priority: int = 0,
    max_attempts: int = 3,
    command_id=None,
) -> DeferredItem:
    return DeferredItem(
        operation=operation,
        options=DurableOptions(
            target_tick=target_tick,
            priority=priority,
            max_attempts=max_attempts,
        ),
        command_id=command_id,
    )


async def _defer_reserved_spawn(
    process: _RuntimeProcess,
    world_id: object,
    marker: DurableMarker,
    *,
    target_tick: int = 0,
) -> tuple[int, object]:
    """Use the trusted path only after the world has reserved the exact ID."""
    (entity_id,) = await process.dispatcher.apply(
        ReserveEntityIds(world_id=cast("Any", world_id), count=1)
    )
    command_id = await process.dispatcher.defer(
        _spawn_reserved(world_id, entity_id, marker),
        DurableOptions(target_tick=target_tick),
    )
    return entity_id, command_id


# Same wire name, different durable schemas: command hydration must select by
# schema identity rather than fail on the process-global class-name collision.
WireCollision = create_model(
    "DurableWireCollision",
    __base__=Component,
    __module__="tests.command_wire_a",
    value=(int, 0),
)
OtherWireCollision = create_model(
    "DurableWireCollision",
    __base__=Component,
    __module__="tests.command_wire_b",
    label=(str, ""),
)


def _storage(tmp_path, namespace: str = "commands") -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace=namespace)


def _audit_storage(tmp_path, namespace: str = "audit") -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "audit"),
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
    )


@pytest.mark.asyncio
async def test_scheduler_materializes_the_exact_world_in_ledger_order_and_stages_settlement(
    tmp_path, monkeypatch
):
    scheduler, world, catalog, coordinator = await _materializer_harness(
        tmp_path,
        "exact-world",
    )
    first_entity_id, spawn_entity_id = world.reserve_entity_ids(2)
    first = _spawn_reserved(world.world_id, first_entity_id)
    spawn = _spawn_reserved(
        world.world_id,
        spawn_entity_id,
        DurableMarker(value=41),
    )
    seen: list[tuple[AsyncWorld, int]] = []
    real_spawn = world.spawn_with_reserved_id

    async def record_exact_world(entity_id, components):
        seen.append((world, entity_id))
        await real_spawn(entity_id, components)

    monkeypatch.setattr(world, "spawn_with_reserved_id", record_exact_world)
    try:
        command_ids = await scheduler.admit_batch(
            (
                _item(spawn, priority=5),
                _item(first, priority=0),
            )
        )
        spawn_id, first_id = command_ids

        assert await scheduler.materialize(world, 0) == 2
        assert seen == [
            (world, first_entity_id),
            (world, spawn_entity_id),
        ]
        assert coordinator.is_command_staged(0, str(first_id))
        assert coordinator.is_command_staged(0, str(spawn_id))
        assert spawn_entity_id in world.entity2sig

        # A retry before publication sees the same staged ledger identities
        # and reports them without replaying their mutations.
        seen.clear()
        assert await scheduler.materialize(world, 0) == 2
        assert seen == []

        context = await coordinator.begin_tick(0)
        await coordinator.publish_tick(0, context, list(world.active_signatures))
        records = {record.command_id: record for record in await scheduler.records(world.world_id)}
        assert records[str(first_id)].status == "APPLIED"
        assert records[str(spawn_id)].status == "APPLIED"
    finally:
        await catalog.close()


@pytest.mark.asyncio
async def test_scheduler_preserves_permanent_retryable_and_tail_release_classification(
    tmp_path, monkeypatch
):
    scheduler, world, catalog, coordinator = await _materializer_harness(
        tmp_path,
        "classification",
    )
    permanent_entity_id, transient_entity_id, tail_entity_id = world.reserve_entity_ids(3)
    await world.spawn_with_reserved_id(permanent_entity_id, [])
    permanent = _spawn_reserved(world.world_id, permanent_entity_id)
    transient = _spawn_reserved(world.world_id, transient_entity_id)
    tail = _spawn_reserved(world.world_id, tail_entity_id)
    real_spawn = world.spawn_with_reserved_id
    fail_transient = True

    async def classify(entity_id, components):
        nonlocal fail_transient
        if entity_id == transient_entity_id and fail_transient:
            fail_transient = False
            raise RuntimeError("temporary dispatcher outage")
        await real_spawn(entity_id, components)

    monkeypatch.setattr(world, "spawn_with_reserved_id", classify)
    try:
        permanent_id, transient_id, tail_id = await scheduler.admit_batch(
            (
                _item(permanent, priority=0),
                _item(transient, priority=1),
                _item(tail, priority=2),
            )
        )

        assert await scheduler.materialize(world, 0) == 0
        first = {record.command_id: record for record in await scheduler.records(world.world_id)}
        assert first[str(permanent_id)].status == "REJECTED"
        assert first[str(transient_id)].status == "RETRYABLE"
        assert first[str(tail_id)].status == "PENDING"

        assert await scheduler.materialize(world, 0) == 2
        context = await coordinator.begin_tick(0)
        await coordinator.publish_tick(0, context, [])
        settled = {record.command_id: record for record in await scheduler.records(world.world_id)}
        assert settled[str(permanent_id)].status == "REJECTED"
        assert settled[str(transient_id)].status == "APPLIED"
        assert settled[str(tail_id)].status == "APPLIED"
    finally:
        await catalog.close()


@pytest.mark.asyncio
async def test_command_id_is_durable_idempotency_identity(tmp_path):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await _create_world(
            process,
            WorldConfig(name="idempotency"), _storage(tmp_path)
        )
        command_id = uuid7()
        operation = Spawn.from_components(
            world_id=world.world_id,
            components=[DurableMarker(value=1)],
        )
        options = DurableOptions(target_tick=0)

        first = await process.dispatcher.defer_spawn_as(
            ctx,
            operation,
            options,
            command_id=command_id,
        )
        replay = await process.dispatcher.defer_spawn_as(
            ctx,
            operation,
            options,
            command_id=command_id,
        )

        assert replay == first == (1, command_id)
        assert await process.scheduler.pending_count(world.world_id) == 1
        assert len(await process.scheduler.records(world.world_id)) == 1

        changed = Spawn.from_components(
            world_id=world.world_id,
            components=[DurableMarker(value=2)],
        )
        with pytest.raises(CommandConflictError):
            await process.dispatcher.defer_spawn_as(
                ctx,
                changed,
                options,
                command_id=command_id,
            )
        assert await process.scheduler.pending_count(world.world_id) == 1
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_permanent_rejection_does_not_block_later_same_tick_command(tmp_path):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    try:
        world = await _create_world(
            process,
            WorldConfig(name="poison"), _storage(tmp_path)
        )
        entity_id = await process.dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[DurableMarker(value=0)],
            )
        )
        (reserved_id,) = await process.dispatcher.apply(
            ReserveEntityIds(world_id=world.world_id, count=1)
        )
        poison = _spawn_reserved(world.world_id, entity_id, DurableMarker(value=1))
        valid = _spawn_reserved(world.world_id, reserved_id, DurableMarker(value=2))
        await process.dispatcher.defer_batch(
            (
                _item(poison),
                _item(valid),
            ),
        )

        applied = await process.dispatcher.apply(
            Step(world_id=world.world_id, run_config=RunConfig())
        )
        records = await process.scheduler.records(world.world_id)

        assert applied == 1
        assert [record.status for record in records] == ["REJECTED", "APPLIED"]
        assert set(world.entity2sig) == {entity_id, reserved_id}
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_transient_failure_retries_and_preserves_tail_order(tmp_path, monkeypatch):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await _create_world(
            process,
            WorldConfig(name="retry"), _storage(tmp_path)
        )
        entity_id = await process.dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[DurableMarker(value=0)],
            )
        )
        first = _update(world.world_id, entity_id, DurableMarker(value=1))
        second = _update(world.world_id, entity_id, DurableMarker(value=2))
        await process.dispatcher.defer_batch_as(
            ctx,
            (
                _item(first),
                _item(second, priority=1),
            ),
        )
        real_update = world.update_entity
        failed = False

        async def fail_once(actual_entity_id, components):
            nonlocal failed
            if components[0].value == 1 and not failed:
                failed = True
                raise RuntimeError("temporary dispatcher outage")
            return await real_update(actual_entity_id, components)

        monkeypatch.setattr(world, "update_entity", fail_once)
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 0
        first_attempt = await process.scheduler.records(world.world_id)
        assert [record.status for record in first_attempt] == ["RETRYABLE", "PENDING"]

        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 2
        settled = await process.scheduler.records(world.world_id)
        assert [record.status for record in settled] == ["APPLIED", "APPLIED"]
        assert [record.applied_tick for record in settled] == [1, 1]
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_exhausted_transient_command_dead_letters_then_tail_continues(tmp_path, monkeypatch):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await _create_world(
            process,
            WorldConfig(name="dead-letter"), _storage(tmp_path)
        )
        poison_entity = await process.dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[DurableMarker(value=0)],
            )
        )
        valid_entity = await process.dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[DurableMarker(value=0)],
            )
        )
        poison = _update(world.world_id, poison_entity, DurableMarker(value=1))
        valid = _update(world.world_id, valid_entity, DurableMarker(value=9))
        await process.dispatcher.defer_batch_as(
            ctx,
            (
                _item(poison, max_attempts=3),
                _item(valid, priority=1),
            ),
        )
        real_update = world.update_entity

        async def fail_poison(entity_id, components):
            if entity_id == poison_entity:
                raise RuntimeError("still unavailable")
            return await real_update(entity_id, components)

        monkeypatch.setattr(world, "update_entity", fail_poison)
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 0
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 0
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1

        records = await process.scheduler.records(world.world_id)
        assert [record.status for record in records] == ["DEAD_LETTER", "APPLIED"]
        assert records[0].attempts == 3
        assert records[1].applied_tick == 2
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_manifest_failure_keeps_command_leased_and_retry_does_not_restage(
    tmp_path, monkeypatch
):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    try:
        world = await _create_world(
            process,
            WorldConfig(name="atomic"), _storage(tmp_path)
        )
        entity_id, command_id = await _defer_reserved_spawn(
            process,
            world.world_id,
            DurableMarker(value=41),
        )
        record = await process.worlds.storage_record(str(world.world_id))
        assert record is not None
        catalog = process.storage.get_control_catalog(record[0])
        real_publish = catalog.publish_manifest
        crashed = False

        async def crash_once(*args, **kwargs):
            nonlocal crashed
            if not crashed:
                crashed = True
                raise RuntimeError("crash before manifest transaction")
            return await real_publish(*args, **kwargs)

        monkeypatch.setattr(catalog, "publish_manifest", crash_once)
        with pytest.raises(RuntimeError, match="crash before manifest"):
            await process.dispatcher.apply(Step(world_id=world.world_id))

        (leased,) = await process.scheduler.records(world.world_id)
        assert leased.status == "LEASED"
        assert leased.command_id == str(command_id)
        signature = world.entity2sig[entity_id]
        assert (
            len([row for row in world.spawn_cache[signature] if row["entity_id"] == entity_id]) == 1
        )

        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1
        (applied,) = await process.scheduler.records(world.world_id)
        assert applied.status == "APPLIED" and applied.applied_tick == 0
        assert (
            len(
                [
                    row
                    for row in world.spawn_cache.get(signature, [])
                    if row["entity_id"] == entity_id
                ]
            )
            == 0
        )
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_committed_manifest_response_loss_reconciles_without_replaying_tick(
    tmp_path,
    monkeypatch,
) -> None:
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    try:
        world = await _create_world(
            process,
            WorldConfig(name="committed-response-loss"),
            _storage(tmp_path, "committed-response-loss"),
        )
        entity_id, command_id = await _defer_reserved_spawn(
            process,
            world.world_id,
            DurableMarker(value=41),
        )
        record = await process.worlds.storage_record(str(world.world_id))
        assert record is not None
        catalog = process.storage.get_control_catalog(record[0])

        materialized_ticks: list[int] = []
        real_materialize = world._materialize_commands  # noqa: SLF001 - replay oracle

        async def record_materialize(actual_world, target_tick):
            materialized_ticks.append(target_tick)
            return await real_materialize(actual_world, target_tick)

        monkeypatch.setattr(world, "_materialize_commands", record_materialize)
        append_calls = 0
        world_store = cast("Any", world.updater).store
        real_append = world_store.append

        async def record_append(sig, frame):
            nonlocal append_calls
            append_calls += 1
            return await real_append(sig, frame)

        monkeypatch.setattr(world_store, "append", record_append)
        execute_calls = 0
        real_execute = world.system.execute

        async def record_execute(*args, **kwargs):
            nonlocal execute_calls
            execute_calls += 1
            return await real_execute(*args, **kwargs)

        monkeypatch.setattr(world.system, "execute", record_execute)

        publish_calls = 0
        real_publish = catalog.publish_manifest

        async def commit_then_lose_response(*args, **kwargs):
            nonlocal publish_calls
            publish_calls += 1
            result = await real_publish(*args, **kwargs)
            if publish_calls == 1:
                raise RuntimeError("manifest committed but response was lost")
            return result

        monkeypatch.setattr(catalog, "publish_manifest", commit_then_lose_response)

        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1

        (manifest,) = await catalog.list_manifests(
            str(world.world_id),
            str(world.run_id),
        )
        (applied,) = await process.scheduler.records(world.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
        assert applied.commit_token == manifest.commit_token
        assert materialized_ticks == [0]
        assert execute_calls == 1
        assert append_calls == 1
        assert publish_calls == 1, "exact visibility must avoid a second fenced POST"
        coordinator = world.commit_coordinator
        assert coordinator is not None
        assert not cast("Any", coordinator).is_command_staged(0, str(command_id))

        audit_rows = (
            await process.dispatcher.apply(
                GetAuditHistory(world_id=world.world_id)
            )
        ).to_pylist()
        command_rows = [row for row in audit_rows if row["command_id"] == str(command_id)]
        assert [row["status"] for row in command_rows] == ["queued", "applied"]
        assert world.tick == 1
        assert entity_id in world.entity2sig
        signature = world.entity2sig[entity_id]
        physical_rows = (
            await world_store.get_archetype_df(
                signature,
                str(world.world_id),
                str(world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(physical_rows) == 1
        assert physical_rows[0]["commit_token"] == manifest.commit_token
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_exact_visible_commit_finalizes_after_fence_handoff_without_second_post(
    tmp_path,
    monkeypatch,
) -> None:
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    try:
        world = await _create_world(
            process,
            WorldConfig(name="response-loss-fence-handoff"),
            _storage(tmp_path, "response-loss-fence-handoff"),
        )
        _entity_id, command_id = await _defer_reserved_spawn(
            process,
            world.world_id,
            DurableMarker(value=45),
        )
        record = await process.worlds.storage_record(str(world.world_id))
        assert record is not None
        catalog = process.storage.get_control_catalog(record[0])
        coordinator = world.commit_coordinator
        assert coordinator is not None
        original_epoch = cast("Any", coordinator).writer_epoch
        publish_calls = 0
        replacement_epoch = None
        real_publish = catalog.publish_manifest

        async def commit_handoff_then_lose_response(*args, **kwargs):
            nonlocal publish_calls, replacement_epoch
            publish_calls += 1
            result = await real_publish(*args, **kwargs)
            if publish_calls == 1:
                replacement_epoch = await catalog.acquire_fence(
                    str(world.world_id),
                    "replacement-writer",
                )
                raise RuntimeError("committed response lost during writer handoff")
            return result

        monkeypatch.setattr(catalog, "publish_manifest", commit_handoff_then_lose_response)

        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1

        assert replacement_epoch == original_epoch + 1
        assert publish_calls == 1
        assert world.tick == 1
        assert not world.has_prepared_tick_commit
        (manifest,) = await catalog.list_manifests(
            str(world.world_id),
            str(world.run_id),
        )
        (applied,) = await process.scheduler.records(world.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
        assert applied.commit_token == manifest.commit_token
        assert not cast("Any", coordinator).is_command_staged(0, str(command_id))

        with pytest.raises(StaleWriterError):
            await process.dispatcher.apply(Step(world_id=world.world_id))

        assert publish_calls == 2
        assert world.tick == 1
        assert world.last_committed_receipt is not None
        assert world.last_committed_receipt.identity == (
            str(world.world_id),
            str(world.run_id),
            0,
            manifest.commit_token,
        )
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_destroy_reconciles_ambiguous_prepared_command_before_cancellation(
    tmp_path,
    monkeypatch,
) -> None:
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    try:
        world = await _create_world(
            process,
            WorldConfig(name="ambiguous-destroy"),
            _storage(tmp_path, "ambiguous-destroy"),
        )
        entity_id, command_id = await _defer_reserved_spawn(
            process,
            world.world_id,
            DurableMarker(value=51),
        )
        destroy_events: list[str] = []

        async def record_destroy(_event: OnDestroy) -> None:
            destroy_events.append("destroy")

        await process.dispatcher.apply(
            AddHook(
                world_id=world.world_id,
                event_type=OnDestroy,
                handler=record_destroy,
            )
        )
        record = await process.worlds.storage_record(str(world.world_id))
        assert record is not None
        catalog = process.storage.get_control_catalog(record[0])

        publish_attempts = 0
        publish_failed = False
        real_publish = catalog.publish_manifest

        async def lose_pre_effect_response(*args, **kwargs):
            nonlocal publish_attempts, publish_failed
            publish_attempts += 1
            if publish_attempts <= 2:
                publish_failed = True
                raise RuntimeError("manifest POST outcome was lost before effect")
            return await real_publish(*args, **kwargs)

        visibility_attempts_after_publish = 0
        real_visible = catalog.visible_tokens

        async def lose_first_reconciliation_read(*args, **kwargs):
            nonlocal visibility_attempts_after_publish
            if publish_failed:
                visibility_attempts_after_publish += 1
                if visibility_attempts_after_publish <= 2:
                    raise RuntimeError("visibility response unavailable")
            return await real_visible(*args, **kwargs)

        monkeypatch.setattr(catalog, "publish_manifest", lose_pre_effect_response)
        monkeypatch.setattr(catalog, "visible_tokens", lose_first_reconciliation_read)

        with pytest.raises(AmbiguousTickCommitError):
            await process.dispatcher.apply(Step(world_id=world.world_id))

        (leased,) = await process.scheduler.records(world.world_id)
        assert leased.status == "LEASED"
        assert leased.applied_tick is None
        assert leased.commit_token is None
        assert world.has_prepared_tick_commit
        assert await catalog.list_manifests(str(world.world_id), str(world.run_id)) == []

        prepared_context = world._commit_ctx  # noqa: SLF001 - exact retry oracle
        assert prepared_context is not None
        prepared_token = prepared_context.commit_token
        with pytest.raises(AmbiguousTickCommitError):
            await process.dispatcher.apply(
                DestroyWorld(world_id=world.world_id)
            )

        (still_leased,) = await process.scheduler.records(world.world_id)
        assert still_leased.status == "LEASED"
        assert still_leased.applied_tick is None
        assert still_leased.commit_token is None
        assert world.has_prepared_tick_commit
        retry_context = world._commit_ctx  # noqa: SLF001
        assert retry_context is not None
        assert retry_context.commit_token == prepared_token
        assert await process.worlds.contains(str(world.world_id))
        catalog_world = await catalog.get_world(str(world.world_id))
        assert catalog_world is not None and catalog_world.status == "active"
        assert await catalog.list_manifests(str(world.world_id), str(world.run_id)) == []
        assert destroy_events == []

        await process.dispatcher.apply(DestroyWorld(world_id=world.world_id))

        (manifest,) = await catalog.list_manifests(
            str(world.world_id),
            str(world.run_id),
        )
        (applied,) = await process.scheduler.records(world.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
        assert applied.commit_token == manifest.commit_token
        assert publish_attempts == 3
        assert world.tick == 1
        assert not world.has_prepared_tick_commit
        assert not await process.worlds.contains(str(world.world_id))
        assert destroy_events == ["destroy"]

        audit_rows = (
            await process.dispatcher.apply(
                GetAuditHistory(world_id=world.world_id)
            )
        ).to_pylist()
        command_rows = [row for row in audit_rows if row["command_id"] == str(command_id)]
        assert [row["status"] for row in command_rows] == ["queued", "applied"]
        signature = world.entity2sig[entity_id]
        physical_rows = (
            await world.updater.store.get_archetype_df(
                signature,
                str(world.world_id),
                str(world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(physical_rows) == 1
        assert physical_rows[0]["commit_token"] == manifest.commit_token
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_pending_reserved_spawn_survives_process_restart(tmp_path):
    storage = _storage(tmp_path, "restart")
    first = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path, "audit-first"),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = None
    try:
        world = await _create_world(
            first,
            WorldConfig(name="restart"),
            storage,
        )
        world_id = str(world.world_id)
        reserved, _command_id = await first.dispatcher.defer_spawn_as(
            ctx,
            Spawn.from_components(
                world_id=world.world_id,
                components=[DurableMarker(value=7)],
            ),
            DurableOptions(target_tick=0),
        )
        assert reserved == 1
    finally:
        await first.shutdown()

    second = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path, "audit-second"),
    )
    try:
        assert world_id is not None
        await second.dispatcher.apply(
            ResumeWorld(storage_config=storage, world_id=world_id)
        )
        resumed = await second.worlds.live_world(world_id)
        assert resumed is not None
        assert resumed.next_entity_id == 2
        assert await second.dispatcher.apply(Step(world_id=world_id)) == 1
        assert 1 in resumed.entity2sig
        (record,) = await second.scheduler.records(world_id)
        assert record.status == "APPLIED"
    finally:
        await second.shutdown()


@pytest.mark.asyncio
async def test_expired_lease_is_recovered_by_another_owner_without_dequeue(tmp_path):
    storage = _storage(tmp_path, "lease")
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await _create_world(
            process,
            WorldConfig(name="lease"),
            storage,
        )
        await process.dispatcher.defer_spawn_as(
            ctx,
            Spawn.from_components(world_id=world.world_id, components=[]),
            DurableOptions(target_tick=0),
        )
        record = await process.worlds.storage_record(str(world.world_id))
        assert record is not None
        catalog = process.storage.get_control_catalog(record[0])
        first = await catalog.lease_commands(str(world.world_id), 0, "worker-a")
        assert [record.status for record in first] == ["LEASED"]

        with sqlite3.connect(catalog_path_for(storage)) as connection:
            connection.execute(
                "UPDATE commands SET lease_expires_at=0 WHERE command_id=?",
                (first[0].command_id,),
            )
        recovered = await catalog.lease_commands(str(world.world_id), 0, "worker-b")
        assert recovered[0].lease_owner == "worker-b"
        assert recovered[0].attempts == 2
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_command_outbox_projects_queued_and_applied_with_watermark(tmp_path):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await _create_world(
            process,
            WorldConfig(name="projection"), _storage(tmp_path)
        )
        await process.dispatcher.defer_spawn_as(
            ctx,
            Spawn.from_components(world_id=world.world_id, components=[]),
            DurableOptions(target_tick=0),
        )
        (command,) = await process.scheduler.records(world.world_id)
        await process.dispatcher.apply(Step(world_id=world.world_id))

        rows = (
            await process.dispatcher.apply(
                GetAuditHistory(world_id=world.world_id)
            )
        ).to_pylist()
        command_rows = [row for row in rows if row["command_id"] == command.command_id]
        assert [row["status"] for row in command_rows] == ["queued", "applied"]
        assert await process.scheduler.outbox_progress() == {
            str(world.world_id): (2, 0)
        }
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_cold_readonly_open_discovers_unprojected_command_outbox(tmp_path):
    world_storage = _storage(tmp_path, "cold-outbox")
    audit_storage = _audit_storage(tmp_path, "cold-outbox-audit")
    writer = _runtime_process(tmp_path, audit_storage_config=audit_storage)
    reader = _runtime_process(tmp_path, audit_storage_config=audit_storage)
    try:
        world = await _create_world(
            writer,
            WorldConfig(name="cold-outbox"), world_storage
        )
        _entity_id, command_id = await writer.dispatcher.defer_spawn(
            Spawn.from_components(world_id=world.world_id, components=[]),
            DurableOptions(target_tick=0),
        )

        assert await reader.scheduler.outbox_progress() == {}
        info = await reader.dispatcher.apply(
            OpenWorldReadonly(
                storage_config=world_storage,
                world_id=world.world_id,
            )
        )
        assert info.world_id == world.world_id

        rows = (
            await reader.dispatcher.apply(
                GetAuditHistory(world_id=world.world_id)
            )
        ).to_pylist()
        command_rows = [row for row in rows if row["command_id"] == str(command_id)]
        assert [row["status"] for row in command_rows] == ["queued"]
        assert await reader.scheduler.outbox_progress() == {
            str(world.world_id): (1, 0)
        }
    finally:
        await reader.shutdown()
        await writer.shutdown()


@pytest.mark.asyncio
async def test_portable_commands_wait_for_scheduled_tick_and_settle_in_ledger_order(tmp_path):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    try:
        world = await _create_world(
            process,
            WorldConfig(name="messages"), _storage(tmp_path)
        )
        reserved_ids = await process.dispatcher.apply(
            ReserveEntityIds(world_id=world.world_id, count=3)
        )
        items = tuple(
            _item(
                _spawn_reserved(
                    world.world_id,
                    entity_id,
                    DurableMarker(value=tick),
                ),
                target_tick=tick,
            )
            for entity_id, tick in zip(reserved_ids, (2, 0, 1), strict=True)
        )
        await process.dispatcher.defer_batch(items)

        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1
        assert await process.scheduler.pending_count(world.world_id) == 2
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1

        records = await process.scheduler.records(world.world_id)
        assert [(record.scheduled_tick, record.applied_tick) for record in records] == [
            (2, 2),
            (0, 0),
            (1, 1),
        ]
        assert set(world.entity2sig) == set(reserved_ids)
    finally:
        await process.shutdown()


@pytest.mark.asyncio
async def test_component_wire_identity_survives_same_named_loaded_classes(tmp_path):
    process = _runtime_process(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await _create_world(
            process,
            WorldConfig(name="component-wire"), _storage(tmp_path, "component-wire")
        )
        entity_id = await process.dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[
                    DurableMarker(value=1),
                    WireCollision(value=2),  # ty: ignore[unknown-argument]
                ],
            )
        )
        await process.dispatcher.apply(Step(world_id=world.world_id))

        await process.dispatcher.defer_as(
            ctx,
            _update(
                world.world_id,
                entity_id,
                WireCollision(value=9),  # ty: ignore[unknown-argument]
            ),
            DurableOptions(target_tick=world.tick),
        )
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1
        rows = (await world.get_components([WireCollision])).collect().to_pylist()
        assert rows[0]["durablewirecollision__value"] == 9

        await process.dispatcher.defer_as(
            ctx,
            RemoveComponents(
                world_id=world.world_id,
                entity_id=entity_id,
                component_types=(ComponentTypeRef.from_type(WireCollision),),
            ),
            DurableOptions(target_tick=world.tick),
        )
        assert await process.dispatcher.apply(Step(world_id=world.world_id)) == 1
        assert world.entity2sig[entity_id] == (DurableMarker,)
    finally:
        await process.shutdown()
