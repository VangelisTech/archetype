# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for service-layer gate, quota, and lifecycle bugs."""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import (
    _daily_tokens,
    _tick_counters,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class GatePos(Component):
    x: float = 0.0
    y: float = 0.0


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.fixture
def admin_ctx():
    return ActorCtx(id=uuid7(), roles={"admin"})


async def _make_world(container, tmp_path, name="gate-regress"):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    return await container.world_service.create_world(WorldConfig(name=name), storage)


class TestPerTickQuotaReset:
    @pytest.mark.asyncio
    async def test_tick_counters_reset_at_tick_boundary(self, tmp_path, admin_ctx):
        """Per docs/guide/token-quotas.md the per-tick counter resets at the
        tick boundary. It used to be reset only by tests, so 500 commands
        locked an actor out for the lifetime of the process."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)
            await container.command_service.create_entity(admin_ctx, world.world_id, [GatePos()])
            assert _tick_counters.get(admin_ctx.id, 0) > 0

            await container.simulation_service.step(world.world_id, RunConfig())

            assert _tick_counters.get(admin_ctx.id, 0) == 0
        finally:
            await container.shutdown()


class TestSubmitBatchQuota:
    @pytest.mark.asyncio
    async def test_mid_batch_rbac_failure_does_not_debit_quota(self, tmp_path):
        """submit_batch is documented all-or-nothing; gating per command used
        to burn quota for commands that never got enqueued."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)
            player = ActorCtx(id=uuid7(), roles={"player"})

            bulk = [
                Command(type=CommandType.SPAWN, payload={"components": []}),
                Command(type=CommandType.ADD_PROCESSOR, payload={}),  # forbidden for player
            ]
            with pytest.raises(PermissionError):
                await container.command_service.submit_batch(player, world.world_id, bulk)

            assert _tick_counters.get(player.id, 0) == 0
            assert _daily_tokens.get(player.id, 0) == 0
            assert await container.broker.get_pending_count(world.world_id) == 0
        finally:
            await container.shutdown()


class TestQueuedUpdateApplies:
    @pytest.mark.asyncio
    async def test_queued_update_command_is_applied_at_drain(self, tmp_path, admin_ctx):
        """A queued UPDATE used to be dequeued, dropped with a warning, and
        still reported as applied."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path)
            gate = container.command_service

            eid = await gate.create_entity(admin_ctx, world.world_id, [GatePos(x=1.0)])
            await container.simulation_service.step(world.world_id, RunConfig())

            cmd = Command(
                type=CommandType.UPDATE,
                tick=0,
                payload={"entity_id": eid, "components": [GatePos(x=42.0, y=7.0).to_payload()]},
            )
            await gate.submit(admin_ctx, world.world_id, cmd)
            await container.simulation_service.step(world.world_id, RunConfig())

            from archetype.core.archetype import Archetype

            sig = Archetype.sig_from_components([GatePos()])
            rows = (await world.query_archetype(sig, ticks=[1])).to_pylist()
            active = [r for r in rows if r["entity_id"] == eid and r["is_active"]]
            assert len(active) == 1
            assert active[0]["gatepos__x"] == 42.0
        finally:
            await container.shutdown()


class TestForkNameUniqueness:
    @pytest.mark.asyncio
    async def test_fork_with_duplicate_name_raises(self, tmp_path):
        """Fork used to silently rebind an existing name in the registry,
        corrupting name lookup for the live world."""
        container = ServiceContainer()
        try:
            world = await _make_world(container, tmp_path, name="base-world")
            await container.world_service.fork_world(world.world_id, name="the-fork")
            with pytest.raises(ValueError, match="already exists"):
                await container.world_service.fork_world(world.world_id, name="the-fork")
            # Original mapping intact
            assert container.world_service.get_world_by_name("base-world") is world
        finally:
            await container.shutdown()
