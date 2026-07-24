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
from archetype.errors import WorldNotFoundError

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


async def test_unauthorized_live_world_call_does_not_resolve_target_tick():
    application = _application()
    target_tick_for_world = Mock(side_effect=AssertionError("resolver must not run"))
    gateway = CommandGateway(
        application,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"viewer"})

    with pytest.raises(GuardrailError, match="cannot execute 'step'"):
        await gateway.step(ctx, "secret-world", object())

    target_tick_for_world.assert_not_called()
    application.step.assert_not_awaited()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


async def test_unauthorized_durable_world_call_does_not_resolve_target_tick():
    application = _application()
    target_tick_for_world = Mock(side_effect=AssertionError("resolver must not run"))
    gateway = CommandGateway(
        application,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"viewer"})

    with pytest.raises(GuardrailError, match="cannot execute 'create_world'"):
        await gateway.resume_world(ctx, object(), "secret-world")

    target_tick_for_world.assert_not_called()
    application.resume_world.assert_not_awaited()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


async def test_authorized_world_call_resolves_and_debits_once():
    application = _application()
    target_tick_for_world = Mock(return_value=7)
    gateway = CommandGateway(
        application,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"player"})

    await gateway.create_entity(ctx, "world-a", [])

    target_tick_for_world.assert_called_once_with("world-a")
    application.create_entity.assert_awaited_once_with("world-a", [])
    assert guard._tick_counters == {(ctx.id, "world-a", 7): 1}
    assert guard._daily_tokens == {
        ctx.id: guard.estimate_token_cost(Command(type=CommandType.SPAWN))
    }


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


def _admission_gateway():
    application = _application()
    audit = AsyncMock()
    target_tick_for_world = Mock(side_effect=AssertionError("resolver must not run"))
    gateway = CommandGateway(
        application,
        audit=audit,
        target_tick_for_world=target_tick_for_world,
    )
    return gateway, application, audit, target_tick_for_world


def _assert_no_admission_effects(application, audit, target_tick_for_world):
    application.require_world.assert_not_awaited()
    application.validate_deferred_command.assert_not_called()
    application.submit.assert_not_awaited()
    application.submit_batch.assert_not_awaited()
    application.submit_spawn.assert_not_awaited()
    audit.record.assert_not_awaited()
    target_tick_for_world.assert_not_called()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


async def test_denied_submit_has_no_world_or_admission_effects():
    gateway, application, audit, target_tick_for_world = _admission_gateway()
    ctx = ActorCtx(id=uuid7(), roles={"viewer"})

    with pytest.raises(GuardrailError, match="cannot execute 'spawn'"):
        await gateway.submit(ctx, "secret-world", Command(type=CommandType.SPAWN))

    _assert_no_admission_effects(application, audit, target_tick_for_world)


async def test_later_denied_batch_member_has_no_world_or_admission_effects():
    gateway, application, audit, target_tick_for_world = _admission_gateway()
    ctx = ActorCtx(id=uuid7(), roles={"player"})
    commands = [
        Command(type=CommandType.CUSTOM),
        Command(type=CommandType.ADD_COMPONENT),
    ]

    with pytest.raises(GuardrailError, match="cannot execute 'add_component'"):
        await gateway.submit_batch(ctx, "secret-world", commands)

    _assert_no_admission_effects(application, audit, target_tick_for_world)


async def test_denied_submit_spawn_has_no_world_or_admission_effects():
    gateway, application, audit, target_tick_for_world = _admission_gateway()
    ctx = ActorCtx(id=uuid7(), roles={"viewer"})

    with pytest.raises(GuardrailError, match="cannot execute 'spawn'"):
        await gateway.submit_spawn(ctx, "secret-world", [])

    _assert_no_admission_effects(application, audit, target_tick_for_world)


async def test_empty_batch_has_no_world_or_admission_effects():
    gateway, application, audit, target_tick_for_world = _admission_gateway()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    with pytest.raises(ValueError, match="commands must not be empty"):
        await gateway.submit_batch(ctx, "secret-world", [])

    _assert_no_admission_effects(application, audit, target_tick_for_world)


@pytest.mark.parametrize("admission", ["submit", "submit_batch", "submit_spawn"])
async def test_authorized_unknown_world_has_no_admission_or_quota_effects(admission):
    gateway, application, audit, target_tick_for_world = _admission_gateway()
    application.require_world.side_effect = WorldNotFoundError("missing-world")
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    with pytest.raises(WorldNotFoundError):
        if admission == "submit":
            await gateway.submit(
                ctx,
                "missing-world",
                Command(type=CommandType.CUSTOM),
            )
        elif admission == "submit_batch":
            await gateway.submit_batch(
                ctx,
                "missing-world",
                [Command(type=CommandType.CUSTOM)],
            )
        else:
            await gateway.submit_spawn(ctx, "missing-world", [])

    application.require_world.assert_awaited_once_with("missing-world")
    application.validate_deferred_command.assert_not_called()
    application.submit.assert_not_awaited()
    application.submit_batch.assert_not_awaited()
    application.submit_spawn.assert_not_awaited()
    audit.record.assert_not_awaited()
    target_tick_for_world.assert_not_called()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


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
