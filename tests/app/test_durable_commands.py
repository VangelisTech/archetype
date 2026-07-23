# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the durable command ledger and tick settlement."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest
from pydantic import create_model
from uuid_utils import uuid7

from archetype.app.commands.service import CommandScheduler
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.hooks import HookRegistry
from archetype.core.resources import Resources
from archetype.errors import WorldNotFoundError
from archetype.storage.catalog import (
    CommandConflictError,
    SqliteControlCatalog,
    WorldRecord,
    catalog_path_for,
)
from archetype.storage.commit import CatalogCommitCoordinator

pytestmark = [
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
    pytest.mark.integration,
]


class DurableMarker(Component):
    value: int = 0


async def _materializer_harness(tmp_path, namespace: str):
    """Build the command seam without the temporary application container."""
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

    async def require_live_world(candidate) -> None:
        if str(candidate) != world_id:
            raise WorldNotFoundError(candidate)

    async def resolve_control_catalog(candidate):
        assert str(candidate) == world_id
        return catalog

    async def list_catalog_world_ids() -> list[str]:
        return [world_id]

    async def reserve_entity_ids(candidate, count: int) -> list[int]:
        assert str(candidate) == world_id
        return world.reserve_entity_ids(count)

    scheduler = CommandScheduler(
        require_live_world=require_live_world,
        resolve_control_catalog=resolve_control_catalog,
        list_catalog_world_ids=list_catalog_world_ids,
        reserve_entity_ids=reserve_entity_ids,
        owner="materializer-test",
    )
    return scheduler, world, catalog, coordinator


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
    spawn = Command(
        type=CommandType.SPAWN,
        priority=5,
        payload={"entity_id": 41, "components": [DurableMarker(value=41)]},
    )
    noop = Command(type=CommandType.CUSTOM, priority=0)
    seen: list[tuple[AsyncWorld, CommandType]] = []
    real_apply = scheduler._apply

    async def record_exact_world(actual_world, command):
        seen.append((actual_world, command.type))
        await real_apply(actual_world, command)

    monkeypatch.setattr(scheduler, "_apply", record_exact_world)
    try:
        await scheduler.admit_batch(world.world_id, [spawn, noop])

        async def forbid_world_reentry(_candidate):
            raise AssertionError("materializer must not reacquire live-world admission")

        monkeypatch.setattr(scheduler, "_require_live_world", forbid_world_reentry)
        assert await scheduler.materialize(world, 0) == 2
        assert seen == [
            (world, CommandType.CUSTOM),
            (world, CommandType.SPAWN),
        ]
        assert coordinator.is_command_staged(0, str(noop.id))
        assert coordinator.is_command_staged(0, str(spawn.id))
        assert 41 in world.entity2sig

        # A retry before publication sees the same staged ledger identities
        # and reports them without replaying their mutations.
        seen.clear()
        assert await scheduler.materialize(world, 0) == 2
        assert seen == []

        context = await coordinator.begin_tick(0)
        await coordinator.publish_tick(0, context, list(world.active_signatures))
        records = {record.command_id: record for record in await scheduler.records(world.world_id)}
        assert records[str(noop.id)].status == "APPLIED"
        assert records[str(spawn.id)].status == "APPLIED"
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
    permanent = Command(
        type=CommandType.SPAWN,
        priority=0,
        payload={"components": [{"value": 1}]},
    )
    transient = Command(type=CommandType.CUSTOM, priority=1)
    tail = Command(type=CommandType.CUSTOM, priority=2)
    real_apply = scheduler._apply
    fail_transient = True

    async def classify(actual_world, command):
        nonlocal fail_transient
        if command.id == transient.id and fail_transient:
            fail_transient = False
            raise RuntimeError("temporary dispatcher outage")
        await real_apply(actual_world, command)

    monkeypatch.setattr(scheduler, "_apply", classify)
    try:
        await scheduler.admit_batch(world.world_id, [permanent, transient, tail])

        assert await scheduler.materialize(world, 0) == 0
        first = {record.command_id: record for record in await scheduler.records(world.world_id)}
        assert first[str(permanent.id)].status == "REJECTED"
        assert first[str(transient.id)].status == "RETRYABLE"
        assert first[str(tail.id)].status == "PENDING"

        assert await scheduler.materialize(world, 0) == 2
        context = await coordinator.begin_tick(0)
        await coordinator.publish_tick(0, context, [])
        settled = {record.command_id: record for record in await scheduler.records(world.world_id)}
        assert settled[str(permanent.id)].status == "REJECTED"
        assert settled[str(transient.id)].status == "APPLIED"
        assert settled[str(tail.id)].status == "APPLIED"
    finally:
        await catalog.close()


@pytest.mark.asyncio
async def test_command_id_is_durable_idempotency_identity(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="idempotency"), _storage(tmp_path)
        )
        command = Command(
            type=CommandType.SPAWN,
            payload={"components": [DurableMarker(value=1)]},
        )

        first = await container.command_gateway.submit(ctx, world.world_id, command)
        replay = await container.command_gateway.submit(ctx, world.world_id, command)

        assert replay == first == command.id
        assert await container.command_scheduler.pending_count(world.world_id) == 1
        assert len(await container.command_scheduler.records(world.world_id)) == 1

        changed = command.model_copy(update={"payload": {"components": [DurableMarker(value=2)]}})
        with pytest.raises(CommandConflictError):
            await container.command_gateway.submit(ctx, world.world_id, changed)
        assert await container.command_scheduler.pending_count(world.world_id) == 1
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_permanent_rejection_does_not_block_later_same_tick_command(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="poison"), _storage(tmp_path)
        )
        poison = Command(
            type=CommandType.SPAWN,
            payload={"components": [{"value": 1}]},  # missing component type
        )
        valid = Command(
            type=CommandType.SPAWN,
            payload={"components": [DurableMarker(value=2)]},
        )
        await container.command_gateway.submit_batch(ctx, world.world_id, [poison, valid])

        applied = await container.application.step(world.world_id, RunConfig())
        records = await container.command_scheduler.records(world.world_id)

        assert applied == 1
        assert [record.status for record in records] == ["REJECTED", "APPLIED"]
        assert len(world.entity2sig) == 1
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_transient_failure_retries_and_preserves_tail_order(tmp_path, monkeypatch):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="retry"), _storage(tmp_path)
        )
        first = Command(type=CommandType.CUSTOM, payload={"position": 1})
        second = Command(
            type=CommandType.SPAWN,
            payload={"components": [DurableMarker(value=2)]},
        )
        await container.command_gateway.submit_batch(ctx, world.world_id, [first, second])
        real_apply = container.command_scheduler._apply
        failed = False

        async def fail_once(world_id, command):
            nonlocal failed
            if command.id == first.id and not failed:
                failed = True
                raise RuntimeError("temporary dispatcher outage")
            return await real_apply(world_id, command)

        monkeypatch.setattr(container.command_scheduler, "_apply", fail_once)
        assert await container.application.step(world.world_id, RunConfig()) == 0
        first_attempt = await container.command_scheduler.records(world.world_id)
        assert [record.status for record in first_attempt] == ["RETRYABLE", "PENDING"]

        assert await container.application.step(world.world_id, RunConfig()) == 2
        settled = await container.command_scheduler.records(world.world_id)
        assert [record.status for record in settled] == ["APPLIED", "APPLIED"]
        assert [record.applied_tick for record in settled] == [1, 1]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_exhausted_transient_command_dead_letters_then_tail_continues(tmp_path, monkeypatch):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="dead-letter"), _storage(tmp_path)
        )
        poison = Command(type=CommandType.CUSTOM, payload={"poison": True})
        valid = Command(
            type=CommandType.SPAWN,
            payload={"components": [DurableMarker(value=9)]},
        )
        await container.command_gateway.submit_batch(ctx, world.world_id, [poison, valid])
        real_apply = container.command_scheduler._apply

        async def fail_poison(world_id, command):
            if command.id == poison.id:
                raise RuntimeError("still unavailable")
            return await real_apply(world_id, command)

        monkeypatch.setattr(container.command_scheduler, "_apply", fail_poison)
        assert await container.application.step(world.world_id, RunConfig()) == 0
        assert await container.application.step(world.world_id, RunConfig()) == 0
        assert await container.application.step(world.world_id, RunConfig()) == 1

        records = await container.command_scheduler.records(world.world_id)
        assert [record.status for record in records] == ["DEAD_LETTER", "APPLIED"]
        assert records[0].attempts == 3
        assert records[1].applied_tick == 2
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_manifest_failure_keeps_command_leased_and_retry_does_not_restage(
    tmp_path, monkeypatch
):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="atomic"), _storage(tmp_path)
        )
        command = Command(
            type=CommandType.SPAWN,
            payload={"entity_id": 41, "components": [DurableMarker(value=41)]},
        )
        await container.command_gateway.submit(ctx, world.world_id, command)
        record = await container.world_registry.storage_record(str(world.world_id))
        assert record is not None
        catalog = container.storage_service.get_control_catalog(record[0])
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
            await container.application.step(world.world_id, RunConfig())

        (leased,) = await container.command_scheduler.records(world.world_id)
        assert leased.status == "LEASED"
        signature = world.entity2sig[41]
        assert len([row for row in world.spawn_cache[signature] if row["entity_id"] == 41]) == 1

        assert await container.application.step(world.world_id, RunConfig()) == 1
        (applied,) = await container.command_scheduler.records(world.world_id)
        assert applied.status == "APPLIED" and applied.applied_tick == 0
        assert (
            len([row for row in world.spawn_cache.get(signature, []) if row["entity_id"] == 41])
            == 0
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_pending_reserved_spawn_survives_process_restart(tmp_path):
    storage = _storage(tmp_path, "restart")
    first = ServiceContainer(audit_storage_config=_audit_storage(tmp_path, "audit-first"))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = None
    try:
        world = await first.world_lifecycle.create_world(WorldConfig(name="restart"), storage)
        world_id = str(world.world_id)
        reserved = await first.command_gateway.submit_spawn(
            ctx, world.world_id, [DurableMarker(value=7)]
        )
        assert reserved == 1
    finally:
        await first.shutdown()

    second = ServiceContainer(audit_storage_config=_audit_storage(tmp_path, "audit-second"))
    try:
        assert world_id is not None
        resumed = await second.world_lifecycle.open_world_mutable(storage, world_id)
        assert resumed.next_entity_id == 2
        assert await second.application.step(world_id, RunConfig()) == 1
        assert 1 in resumed.entity2sig
        (record,) = await second.command_scheduler.records(world_id)
        assert record.status == "APPLIED"
    finally:
        await second.shutdown()


@pytest.mark.asyncio
async def test_expired_lease_is_recovered_by_another_owner_without_dequeue(tmp_path):
    storage = _storage(tmp_path, "lease")
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(WorldConfig(name="lease"), storage)
        command = Command(type=CommandType.CUSTOM)
        await container.command_gateway.submit(ctx, world.world_id, command)
        record = await container.world_registry.storage_record(str(world.world_id))
        assert record is not None
        catalog = container.storage_service.get_control_catalog(record[0])
        first = await catalog.lease_commands(str(world.world_id), 0, "worker-a")
        assert [record.status for record in first] == ["LEASED"]

        with sqlite3.connect(catalog_path_for(storage)) as connection:
            connection.execute(
                "UPDATE commands SET lease_expires_at=0 WHERE command_id=?", (str(command.id),)
            )
        recovered = await catalog.lease_commands(str(world.world_id), 0, "worker-b")
        assert recovered[0].lease_owner == "worker-b"
        assert recovered[0].attempts == 2
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_command_outbox_projects_queued_and_applied_with_watermark(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="projection"), _storage(tmp_path)
        )
        command = Command(type=CommandType.CUSTOM)
        await container.command_gateway.submit(ctx, world.world_id, command)
        await container.application.step(world.world_id, RunConfig())

        rows = (await container.audit_log.query(world_id=world.world_id)).to_pylist()
        command_rows = [row for row in rows if row["command_id"] == str(command.id)]
        assert [row["status"] for row in command_rows] == ["queued", "applied"]
        assert await container.command_scheduler.outbox_progress() == {str(world.world_id): (2, 0)}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_cold_readonly_open_discovers_unprojected_command_outbox(tmp_path):
    world_storage = _storage(tmp_path, "cold-outbox")
    audit_storage = _audit_storage(tmp_path, "cold-outbox-audit")
    writer = ServiceContainer(audit_storage_config=audit_storage)
    reader = ServiceContainer(audit_storage_config=audit_storage)
    command = Command(type=CommandType.CUSTOM)
    try:
        world = await writer.world_lifecycle.create_world(
            WorldConfig(name="cold-outbox"), world_storage
        )
        await writer.command_scheduler.admit(world.world_id, command)

        assert await reader.command_scheduler.outbox_progress() == {}
        info = await reader.application.open_world_readonly(world_storage, world.world_id)
        assert info.world_id == world.world_id

        rows = (await reader.audit_log.query(world_id=world.world_id)).to_pylist()
        command_rows = [row for row in rows if row["command_id"] == str(command.id)]
        assert [row["status"] for row in command_rows] == ["queued"]
        assert await reader.command_scheduler.outbox_progress() == {str(world.world_id): (1, 0)}
    finally:
        await reader.shutdown()
        await writer.shutdown()


@pytest.mark.asyncio
async def test_message_commands_wait_for_scheduled_tick_and_settle_as_noops(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="messages"), _storage(tmp_path)
        )
        commands = [
            Command(type=CommandType.MESSAGE, tick=tick, payload={"tick": tick})
            for tick in (2, 0, 1)
        ]
        await container.command_gateway.submit_batch(ctx, world.world_id, commands)

        assert await container.application.step(world.world_id, RunConfig()) == 1
        assert await container.command_scheduler.pending_count(world.world_id) == 2
        assert await container.application.step(world.world_id, RunConfig()) == 1
        assert await container.application.step(world.world_id, RunConfig()) == 1

        records = await container.command_scheduler.records(world.world_id)
        assert [(record.scheduled_tick, record.applied_tick) for record in records] == [
            (2, 2),
            (0, 0),
            (1, 1),
        ]
        assert world.entity2sig == {}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_component_wire_identity_survives_same_named_loaded_classes(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="component-wire"), _storage(tmp_path, "component-wire")
        )
        entity_id = await container.application.create_entity(
            world.world_id,
            [DurableMarker(value=1), WireCollision(value=2)],
        )
        await container.application.step(world.world_id, RunConfig())

        await container.command_gateway.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.UPDATE,
                payload={
                    "entity_id": entity_id,
                    "components": [WireCollision(value=9)],
                },
            ),
        )
        assert await container.application.step(world.world_id, RunConfig()) == 1
        rows = (await world.get_components([WireCollision])).collect().to_pylist()
        assert rows[0]["durablewirecollision__value"] == 9

        await container.command_gateway.submit(
            ctx,
            world.world_id,
            Command(
                type=CommandType.REMOVE_COMPONENT,
                payload={"entity_id": entity_id, "component_types": [WireCollision]},
            ),
        )
        assert await container.application.step(world.world_id, RunConfig()) == 1
        assert world.entity2sig[entity_id] == (DurableMarker,)
    finally:
        await container.shutdown()
