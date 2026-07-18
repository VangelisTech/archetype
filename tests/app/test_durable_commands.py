# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the durable command ledger and tick settlement."""

from __future__ import annotations

import sqlite3

import pytest
from pydantic import create_model
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.app.storage.catalog import CommandConflictError, catalog_path_for
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig

pytestmark = [
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
    pytest.mark.integration,
]


class DurableMarker(Component):
    value: int = 0


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
async def test_command_id_is_durable_idempotency_identity(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_service.create_world(
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
        world = await container.world_service.create_world(
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

        applied = await container.simulation_service.step(world.world_id, RunConfig())
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
        world = await container.world_service.create_world(
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
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 0
        first_attempt = await container.command_scheduler.records(world.world_id)
        assert [record.status for record in first_attempt] == ["RETRYABLE", "PENDING"]

        assert await container.simulation_service.step(world.world_id, RunConfig()) == 2
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
        world = await container.world_service.create_world(
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
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 0
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 0
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1

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
        world = await container.world_service.create_world(
            WorldConfig(name="atomic"), _storage(tmp_path)
        )
        command = Command(
            type=CommandType.SPAWN,
            payload={"entity_id": 41, "components": [DurableMarker(value=41)]},
        )
        await container.command_gateway.submit(ctx, world.world_id, command)
        catalog = container.world_service.control_catalog(world.world_id)
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
            await container.simulation_service.step(world.world_id, RunConfig())

        (leased,) = await container.command_scheduler.records(world.world_id)
        assert leased.status == "LEASED"
        signature = world.entity2sig[41]
        assert len([row for row in world.spawn_cache[signature] if row["entity_id"] == 41]) == 1

        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1
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
        world = await first.world_service.create_world(WorldConfig(name="restart"), storage)
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
        resumed = await second.world_service.open_world_mutable(storage, world_id)
        assert resumed.next_entity_id == 2
        assert await second.simulation_service.step(world_id, RunConfig()) == 1
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
        world = await container.world_service.create_world(WorldConfig(name="lease"), storage)
        command = Command(type=CommandType.CUSTOM)
        await container.command_gateway.submit(ctx, world.world_id, command)
        catalog = container.world_service.control_catalog(world.world_id)
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
        world = await container.world_service.create_world(
            WorldConfig(name="projection"), _storage(tmp_path)
        )
        command = Command(type=CommandType.CUSTOM)
        await container.command_gateway.submit(ctx, world.world_id, command)
        await container.simulation_service.step(world.world_id, RunConfig())

        rows = (await container.audit_log.query(world_id=world.world_id)).to_pylist()
        command_rows = [row for row in rows if row["command_id"] == str(command.id)]
        assert [row["status"] for row in command_rows] == ["queued", "applied"]
        assert await container.command_scheduler.outbox_progress() == {str(world.world_id): (2, 0)}
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_message_commands_wait_for_scheduled_tick_and_settle_as_noops(tmp_path):
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_service.create_world(
            WorldConfig(name="messages"), _storage(tmp_path)
        )
        commands = [
            Command(type=CommandType.MESSAGE, tick=tick, payload={"tick": tick})
            for tick in (2, 0, 1)
        ]
        await container.command_gateway.submit_batch(ctx, world.world_id, commands)

        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1
        assert await container.command_scheduler.pending_count(world.world_id) == 2
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1

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
        world = await container.world_service.create_world(
            WorldConfig(name="component-wire"), _storage(tmp_path, "component-wire")
        )
        entity_id = await container.mutation_service.create_entity(
            world.world_id,
            [DurableMarker(value=1), WireCollision(value=2)],
        )
        await container.simulation_service.step(world.world_id, RunConfig())

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
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1
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
        assert await container.simulation_service.step(world.world_id, RunConfig()) == 1
        assert world.entity2sig[entity_id] == (DurableMarker,)
    finally:
        await container.shutdown()
