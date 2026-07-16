# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the bounded, append-only Iceberg audit table."""

import pytest
from uuid_utils import uuid7

from archetype.app.audit_log import AuditLog, make_audit_row
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import CommandType
from archetype.app.query_service import QueryService
from archetype.app.storage_service import StorageService
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.runtime.session import configure_session


class APos(Component):
    x: float = 0.0


def _storage(tmp_path, namespace: str = "audit") -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "audit_store"),
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
    )


def test_audit_configuration_fails_closed(tmp_path):
    with pytest.raises(ValueError, match="flush_rows"):
        AuditLog(storage_config=_storage(tmp_path), flush_rows=0)
    with pytest.raises(ValueError, match="backend=iceberg"):
        AuditLog(storage_config=StorageConfig(uri=str(tmp_path / "lance")))


@pytest.mark.asyncio
async def test_injected_session_requires_and_enforces_audit_identity(tmp_path):
    storage = _storage(tmp_path, namespace="managed")
    storage_service = StorageService(session=configure_session(storage))
    container = None
    try:
        with pytest.raises(ValueError, match="audit_storage_config is required"):
            ServiceContainer(storage_service=storage_service)

        container = ServiceContainer(
            storage_service=storage_service,
            audit_storage_config=storage,
        )
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        world = await container.command_service.create_world(
            ctx,
            WorldConfig(name="managed"),
            storage,
        )
        rows = (await container.audit_log.query(world_id=world.world_id)).to_pylist()
        assert [row["command_type"] for row in rows] == ["create_world"]

        different = storage.model_copy(update={"uri": str(tmp_path / "other")})
        with pytest.raises(ValueError, match="configured for a different storage identity"):
            await container.world_service.create_world(WorldConfig(name="other"), different)
    finally:
        if container is not None:
            await container.shutdown()
        await storage_service.shutdown()


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.asyncio
async def test_gated_mutations_emit_exactly_one_audit_row(tmp_path):
    c = ServiceContainer(audit_storage_config=_storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="audit"), StorageConfig(uri=str(tmp_path / "world"))
        )
        wid = world.world_id

        before = (await c.audit_log.query()).count_rows()
        entity_id = await c.command_service.create_entity(ctx, wid, [APos(x=1)])
        rows = (await c.audit_log.query()).to_pylist()
        assert len(rows) == before + 1
        assert rows[-1]["command_type"] == "spawn"

        before = len(rows)
        await c.command_service.step(ctx, wid, RunConfig())
        rows = (await c.audit_log.query()).to_pylist()
        assert len(rows) == before + 1
        assert rows[-1]["command_type"] == "step"

        before = len(rows)
        await c.command_service.remove_entity(ctx, wid, entity_id)
        rows = (await c.audit_log.query()).to_pylist()
        assert len(rows) == before + 1
        assert rows[-1]["command_type"] == "despawn"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_audit_log_persists_rows_across_instances(tmp_path):
    storage = _storage(tmp_path, "audit_ns")
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = str(uuid7())
    first_storage = StorageService()
    second_storage = StorageService()
    first = AuditLog(first_storage, storage)
    second = AuditLog(second_storage, storage)
    try:
        await first.record(make_audit_row(ctx, "create_world", world_id))
        await first.shutdown()

        rows = (await second.query(world_id)).to_pylist()
        assert len(rows) == 1
        assert rows[0]["world_id"] == world_id
        assert rows[0]["command_type"] == "create_world"
    finally:
        await second.shutdown()
        await first_storage.shutdown()
        await second_storage.shutdown()


@pytest.mark.asyncio
async def test_audit_query_filters_orders_and_limits_in_daft(tmp_path):
    storage_service = StorageService()
    audit = AuditLog(storage_service, _storage(tmp_path))
    actor_a = ActorCtx(id=uuid7(), roles={"admin"})
    actor_b = ActorCtx(id=uuid7(), roles={"admin"})
    world_a = str(uuid7())
    world_b = str(uuid7())
    try:
        first = make_audit_row(actor_a, "first", world_a, status="queued")
        second = make_audit_row(actor_b, "second", world_b)
        await audit.record(first)
        await audit.record(second)

        actor_rows = (await audit.query(world_a, actor_id=actor_a.id, status="queued")).to_pylist()
        assert [row["audit_id"] for row in actor_rows] == [str(first.audit_id)]

        latest = (await audit.query(limit=1)).to_pylist()
        assert [row["audit_id"] for row in latest] == [str(second.audit_id)]
        assert (await audit.query(limit=0)).count_rows() == 0
        with pytest.raises(ValueError, match="non-negative"):
            await audit.query(limit=-1)
        assert (await audit.query(tick_range=(0, 1))).count_rows() == 2
    finally:
        await audit.shutdown()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_queued_history_restores_command_uuid_from_iceberg(tmp_path):
    storage_service = StorageService()
    audit = AuditLog(storage_service, _storage(tmp_path))
    query = QueryService(storage_service, audit)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = str(uuid7())
    command_id = uuid7()
    try:
        await audit.record(
            make_audit_row(
                ctx,
                CommandType.SPAWN.value,
                world_id,
                command_id=command_id,
                status="queued",
            )
        )

        history = await query.get_command_history(world_id)

        assert [(command.id, command.type) for command in history] == [
            (command_id, CommandType.SPAWN)
        ]
    finally:
        await audit.shutdown()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_batch_threshold_creates_one_snapshot_per_batch(tmp_path):
    storage = _storage(tmp_path)
    session = configure_session(storage)
    storage_service = StorageService(session=session)
    audit = AuditLog(storage_service, storage, flush_rows=3)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        for index in range(7):
            await audit.record(make_audit_row(ctx, f"op-{index}"))
        await audit.shutdown()

        native_table = session.get_table("audit_rows")._inner
        assert len(native_table.snapshots()) == 3
    finally:
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_failed_flush_cannot_grow_pending_rows_beyond_one_batch(tmp_path):
    class FailingContext:
        table = object()

        def create_table_if_not_exists(self, _name, _schema):
            return self.table

        async def append(self, _table, _frame):
            raise RuntimeError("storage unavailable")

    class FailingStorage:
        context = FailingContext()

        async def get_iceberg_context(self, _config):
            return self.context

    audit = AuditLog(FailingStorage(), _storage(tmp_path), flush_rows=2)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    await audit.record(make_audit_row(ctx, "first"))
    with pytest.raises(RuntimeError, match="storage unavailable"):
        await audit.record(make_audit_row(ctx, "second"))
    assert len(audit._pending) == 2

    with pytest.raises(RuntimeError, match="storage unavailable"):
        await audit.record(make_audit_row(ctx, "never-buffered"))
    assert len(audit._pending) == 2
