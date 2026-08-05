# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the bounded, append-only Iceberg audit table."""

from datetime import UTC, datetime
from typing import Any, cast

import pytest
from uuid_utils import uuid7

from archetype.commands.audit import AuditBackpressureError, AuditLog
from archetype.commands.models import ActorCtx, AuditRow
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.storage.service import StorageService
from archetype.storage.session import configure_session
from archetype.world.models import CreateWorld, Despawn, Spawn, Step
from tests._runtime import build_test_runtime


class APos(Component):
    x: float = 0.0


def _storage(tmp_path, namespace: str = "audit") -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "audit_store"),
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
    )


async def _read_no_outbox(**_kwargs):
    return []


async def _ack_no_outbox(_events) -> None:
    return None


def _audit_log(storage_service, storage_config, *, flush_rows: int = 128) -> AuditLog:
    return AuditLog(
        storage_service,
        storage_config,
        read_outbox=_read_no_outbox,
        acknowledge_outbox=_ack_no_outbox,
        flush_rows=flush_rows,
    )


def _audit_row(
    ctx,
    command_type: str,
    world_id=None,
    *,
    command_id=None,
    status: str = "applied",
    payload_json: str = "{}",
) -> AuditRow:
    now = datetime.now(UTC).isoformat()
    return AuditRow(
        command_id=command_id,
        world_id=world_id,
        actor_id=ctx.id,
        command_type=command_type,
        status=status,
        payload_json=payload_json,
        accepted_at=now,
        applied_at=now,
    )


def test_audit_configuration_fails_closed(tmp_path):
    with pytest.raises(ValueError, match="flush_rows"):
        _audit_log(StorageService(), _storage(tmp_path), flush_rows=0)
    with pytest.raises(ValueError, match="backend=iceberg"):
        _audit_log(
            StorageService(),
            StorageConfig(uri=str(tmp_path / "lance")),
        )


@pytest.mark.asyncio
async def test_command_gate_keeps_audit_backpressure_advisory(
    tmp_path,
    monkeypatch,
):
    resources = build_test_runtime(
        tmp_path,
        audit_storage_config=_storage(tmp_path, "advisory"),
    )
    dispatcher = resources.dispatcher
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    async def reject_record(_row):
        raise AuditBackpressureError("bounded audit batch is full")

    monkeypatch.setattr(dispatcher, "_record_access", reject_record)
    try:
        world = await dispatcher.apply_as(
            ctx,
            CreateWorld(
                config=WorldConfig(name="applied-despite-audit-backpressure"),
                storage_config=StorageConfig(uri=str(tmp_path / "world")),
            ),
        )

        assert world.name == "applied-despite-audit-backpressure"
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_injected_session_requires_and_enforces_audit_identity(tmp_path):
    storage = _storage(tmp_path, namespace="managed")
    storage_service = StorageService(session=configure_session(storage))
    resources = None
    try:
        with pytest.raises(ValueError, match="audit_storage_config is required"):
            build_test_runtime(tmp_path, storage_service=storage_service)

        resources = build_test_runtime(
            tmp_path,
            storage_service=storage_service,
            audit_storage_config=storage,
        )
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        await resources.dispatcher.apply_as(
            ctx,
            CreateWorld(
                config=WorldConfig(name="managed"),
                storage_config=storage,
            ),
        )
        audit = resources.dispatcher._record_access.__self__
        rows = (await audit.query()).to_pylist()
        assert [row["command_type"] for row in rows] == ["create_world"]
        assert rows[0]["world_id"] is None

        different = storage.model_copy(update={"uri": str(tmp_path / "other")})
        with pytest.raises(ValueError, match="configured for a different storage identity"):
            await resources.dispatcher.apply(
                CreateWorld(
                    config=WorldConfig(name="other"),
                    storage_config=different,
                )
            )
    finally:
        if resources is not None:
            await resources.aclose()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_gated_mutations_emit_exactly_one_audit_row(tmp_path):
    resources = build_test_runtime(
        tmp_path,
        audit_storage_config=_storage(tmp_path),
    )
    dispatcher = resources.dispatcher
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="audit"),
                storage_config=StorageConfig(uri=str(tmp_path / "world")),
            )
        )
        wid = world.world_id
        audit = dispatcher._record_access.__self__

        before = (await audit.query()).count_rows()
        entity_id = await dispatcher.apply_as(
            ctx,
            Spawn.from_components(
                world_id=wid,
                components=[APos(x=1)],
            ),
        )
        rows = (await audit.query()).to_pylist()
        assert len(rows) == before + 1
        assert rows[-1]["command_type"] == "spawn"

        before = len(rows)
        await dispatcher.apply_as(
            ctx,
            Step(world_id=wid, run_config=RunConfig()),
        )
        rows = (await audit.query()).to_pylist()
        assert len(rows) == before + 1
        assert rows[-1]["command_type"] == "step"

        before = len(rows)
        await dispatcher.apply_as(
            ctx,
            Despawn(world_id=wid, entity_id=entity_id),
        )
        rows = (await audit.query()).to_pylist()
        assert len(rows) == before + 1
        assert rows[-1]["command_type"] == "despawn"
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_audit_log_persists_rows_across_instances(tmp_path):
    storage = _storage(tmp_path, "audit_ns")
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = str(uuid7())
    first_storage = StorageService()
    second_storage = StorageService()
    first = _audit_log(first_storage, storage)
    second = _audit_log(second_storage, storage)
    try:
        await first.record(_audit_row(ctx, "create_world", world_id))
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
    audit = _audit_log(storage_service, _storage(tmp_path))
    actor_a = ActorCtx(id=uuid7(), roles={"admin"})
    actor_b = ActorCtx(id=uuid7(), roles={"admin"})
    world_a = str(uuid7())
    world_b = str(uuid7())
    try:
        first = _audit_row(actor_a, "first", world_a, status="queued")
        second = _audit_row(actor_b, "second", world_b)
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
    audit = _audit_log(storage_service, _storage(tmp_path))
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = str(uuid7())
    command_id = uuid7()
    try:
        await audit.record(
            _audit_row(
                ctx,
                "spawn",
                world_id,
                command_id=command_id,
                status="queued",
            )
        )

        history = (await audit.query(world_id)).to_pylist()
        assert [(row["command_id"], row["command_type"]) for row in history] == [
            (str(command_id), "spawn")
        ]
    finally:
        await audit.shutdown()
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_batch_threshold_creates_one_snapshot_per_batch(tmp_path):
    storage = _storage(tmp_path)
    session = configure_session(storage)
    storage_service = StorageService(session=session)
    audit = _audit_log(storage_service, storage, flush_rows=3)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        for index in range(7):
            await audit.record(_audit_row(ctx, f"op-{index}"))
        await audit.shutdown()

        native_table = cast(Any, session.get_table("audit_rows"))._inner
        assert len(native_table.snapshots()) == 3
    finally:
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_failed_flush_rejects_new_rows_without_unbounded_growth(tmp_path):
    class FailingStorage:
        async def append_table(self, _config, _table_name, _frame):
            raise RuntimeError("storage unavailable")

    audit = _audit_log(FailingStorage(), _storage(tmp_path), flush_rows=2)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    await audit.record(_audit_row(ctx, "first"))
    with pytest.raises(RuntimeError, match="storage unavailable"):
        await audit.record(_audit_row(ctx, "second"))
    assert len(audit._pending) == 2

    with pytest.raises(AuditBackpressureError, match="bounded pending batch"):
        await audit.record(_audit_row(ctx, "rejected"))
    assert len(audit._pending) == 2
    assert audit.rejected_rows == 1
