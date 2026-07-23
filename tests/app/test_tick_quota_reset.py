# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Target-tick-aware gateway quota contracts.

Quota scope is carried explicitly from gateway composition or from the durable
command envelope. Advancing one world therefore creates a new quota key without
clearing unrelated worlds or relying on a simulation callback.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from uuid_utils import uuid7

import archetype.app.gateway.auth.guard as guard
from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.guard import reset_daily_tokens
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.service import CommandGateway
from archetype.app.models import Command, CommandType

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _reset_quotas():
    guard._tick_counters.clear()
    reset_daily_tokens()
    yield
    guard._tick_counters.clear()
    reset_daily_tokens()


def _application() -> AsyncMock:
    application = AsyncMock()
    application.require_world = AsyncMock()
    application.validate_deferred_command = Mock()
    return application


async def test_direct_calls_are_scoped_by_resolved_world_and_target_tick(monkeypatch):
    monkeypatch.setattr(guard, "MAX_CMDS_PER_TICK", 1)
    application = _application()
    target_ticks = {"world-a": 7, "world-b": 7}
    gateway = CommandGateway(
        application,
        target_tick_for_world=lambda world_id: target_ticks[str(world_id)],
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    await gateway.create_entity(ctx, "world-a", [])
    await gateway.create_entity(ctx, "world-b", [])

    with pytest.raises(GuardrailError, match="per-tick quota"):
        await gateway.create_entity(ctx, "world-a", [])

    target_ticks["world-a"] = 8
    await gateway.create_entity(ctx, "world-a", [])

    assert guard._tick_counters == {
        (ctx.id, "world-a", 7): 1,
        (ctx.id, "world-b", 7): 1,
        (ctx.id, "world-a", 8): 1,
    }
    assert application.create_entity.await_count == 3


async def test_direct_world_call_without_target_tick_resolver_fails_closed():
    application = _application()
    gateway = CommandGateway(application)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    with pytest.raises(RuntimeError, match="target_tick_for_world"):
        await gateway.create_entity(ctx, "world-a", [])

    application.create_entity.assert_not_awaited()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


async def test_cold_durable_operations_share_explicit_world_tick_zero():
    application = _application()
    application.evaluate.return_value = SimpleNamespace(outcome="pass")

    def missing_live_world(_world_id):
        raise KeyError("not live")

    gateway = CommandGateway(
        application,
        target_tick_for_world=missing_live_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = object()
    world_id = "cold-world"

    await gateway.open_world_readonly(ctx, storage, world_id)
    await gateway.resume_world(ctx, storage, world_id)
    await gateway.get_audit_history(ctx, world_id)
    await gateway.query_artifacts(ctx, world_id, storage_config=storage)
    await gateway.evaluate(
        ctx,
        world_id,
        [],
        contract=SimpleNamespace(grader_id="grader"),
    )
    await gateway.destroy_world(ctx, world_id)

    assert guard._tick_counters == {(ctx.id, world_id, 0): 6}
    application.open_world_readonly.assert_awaited_once_with(storage, world_id)
    application.resume_world.assert_awaited_once_with(storage, world_id)
    application.get_audit_history.assert_awaited_once_with(world_id)
    application.query_artifacts.assert_awaited_once_with(
        world_id,
        storage_config=storage,
    )
    application.evaluate.assert_awaited_once()
    application.destroy_world.assert_awaited_once_with(world_id)


async def test_deferred_commands_use_their_actual_scheduled_target_tick(monkeypatch):
    monkeypatch.setattr(guard, "MAX_CMDS_PER_TICK", 1)
    application = _application()

    def unexpected_resolver(_world_id):
        pytest.fail("deferred command admission must use command.tick")

    gateway = CommandGateway(application, target_tick_for_world=unexpected_resolver)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    tick_11 = Command(type=CommandType.CUSTOM, tick=11)
    tick_12 = Command(type=CommandType.CUSTOM, tick=12)

    await gateway.submit(ctx, "world-a", tick_11)
    await gateway.submit(ctx, "world-a", tick_12)

    with pytest.raises(GuardrailError, match="per-tick quota"):
        await gateway.submit(
            ctx,
            "world-a",
            Command(type=CommandType.CUSTOM, tick=11),
        )

    assert guard._tick_counters == {
        (ctx.id, "world-a", 11): 1,
        (ctx.id, "world-a", 12): 1,
    }
    assert application.submit.await_count == 2


async def test_batch_validation_is_atomic_and_groups_each_scheduled_tick(monkeypatch):
    monkeypatch.setattr(guard, "MAX_CMDS_PER_TICK", 1)
    application = _application()
    gateway = CommandGateway(application)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    rejected = [
        Command(type=CommandType.CUSTOM, tick=20),
        Command(type=CommandType.CUSTOM, tick=20),
    ]
    with pytest.raises(GuardrailError, match="per-tick quota"):
        await gateway.submit_batch(ctx, "world-a", rejected)

    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}
    application.submit_batch.assert_not_awaited()

    accepted = [
        Command(type=CommandType.CUSTOM, tick=20),
        Command(type=CommandType.CUSTOM, tick=21),
    ]
    await gateway.submit_batch(ctx, "world-a", accepted)

    assert guard._tick_counters == {
        (ctx.id, "world-a", 20): 1,
        (ctx.id, "world-a", 21): 1,
    }
    assert guard._daily_tokens[ctx.id] == sum(
        guard.estimate_token_cost(command) for command in accepted
    )
    application.submit_batch.assert_awaited_once()
