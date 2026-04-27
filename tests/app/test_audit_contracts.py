# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Audit contract tests.

For each public gated method on CommandService (except drain_and_apply),
call it and verify exactly one new audit row was emitted.
"""

import pytest
from uuid_utils import uuid7

from archetype.app.audit_log import AuditLog, make_audit_row
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.storage_service import StorageService
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class APos(Component):
    x: float = 0.0


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.asyncio
async def test_gated_mutations_emit_audit_rows(tmp_path):
    c = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await c.world_service.create_world(
            WorldConfig(name="audit"), StorageConfig(uri=str(tmp_path / "store"))
        )
        wid = world.world_id
        audit = c.audit_log

        before = len(audit._rows)
        eid = await c.command_service.create_entity(ctx, wid, [APos(x=1)])
        assert len(audit._rows) == before + 1
        assert audit._rows[-1].command_type == "spawn"

        before = len(audit._rows)
        await c.command_service.step(ctx, wid, RunConfig())
        assert len(audit._rows) == before + 1
        assert audit._rows[-1].command_type == "step"

        before = len(audit._rows)
        await c.command_service.remove_entity(ctx, wid, eid)
        assert len(audit._rows) == before + 1
        assert audit._rows[-1].command_type == "despawn"

    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_audit_log_persists_rows_in_dedicated_namespace(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="audit_ns")
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    world_id = str(uuid7())
    first_storage = StorageService()
    second_storage = StorageService()
    first = AuditLog(first_storage, storage)
    second = AuditLog(second_storage, storage)
    try:
        await first.record(make_audit_row(ctx, "create_world", world_id))
        await first.shutdown()

        df = await second.query(world_id)
        rows = df.collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["world_id"] == world_id
        assert rows[0]["command_type"] == "create_world"
    finally:
        await second.shutdown()
        await first_storage.shutdown()
        await second_storage.shutdown()


@pytest.mark.asyncio
async def test_audit_log_filters_persisted_rows(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="audit_ns")
    actor_a = ActorCtx(id=uuid7(), roles={"admin"})
    actor_b = ActorCtx(id=uuid7(), roles={"admin"})
    world_a = str(uuid7())
    world_b = str(uuid7())
    storage_service = StorageService()
    audit = AuditLog(storage_service, storage)
    try:
        await audit.record(make_audit_row(actor_a, "create_world", world_a))
        await audit.record(make_audit_row(actor_b, "create_world", world_b))

        rows = (await audit.query(world_a, actor_id=actor_a.id)).collect().to_pylist()
        assert [row["world_id"] for row in rows] == [world_a]
        assert [row["actor_id"] for row in rows] == [str(actor_a.id)]
    finally:
        await audit.shutdown()
        await storage_service.shutdown()
