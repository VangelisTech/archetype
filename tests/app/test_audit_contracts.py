# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Audit contract tests.

For each public gated method on CommandService (except drain_and_apply),
call it and verify exactly one new audit row was emitted.
"""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
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
