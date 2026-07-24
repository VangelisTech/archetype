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
from archetype.world.registry import WorldRegistry

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


_DURABLE_WORLD_ROUTES = [
    pytest.param("destroy_world", CommandType.DESTROY_WORLD, id="destroy"),
    pytest.param("open_world_readonly", CommandType.GET_WORLD_INFO, id="readonly-open"),
    pytest.param("resume_world", CommandType.CREATE_WORLD, id="resume"),
    pytest.param("query_components", CommandType.QUERY_WORLD, id="component-query"),
    pytest.param("query_archetype", CommandType.QUERY_WORLD, id="archetype-query"),
    pytest.param("list_signatures", CommandType.LIST_SIGNATURES, id="signatures"),
    pytest.param("get_audit_history", CommandType.GET_AUDIT_HISTORY, id="audit"),
    pytest.param("query_artifacts", CommandType.QUERY_WORLD, id="artifacts"),
    pytest.param("evaluate", CommandType.EVALUATE, id="evaluation"),
]


async def _invoke_durable_world_route(
    gateway: CommandGateway,
    route: str,
    ctx: ActorCtx,
    *,
    world_id: str,
    storage: object,
) -> None:
    if route == "destroy_world":
        await gateway.destroy_world(ctx, world_id)
    elif route == "open_world_readonly":
        await gateway.open_world_readonly(ctx, storage, world_id)
    elif route == "resume_world":
        await gateway.resume_world(ctx, storage, world_id)
    elif route == "query_components":
        await gateway.query_components(ctx, [], world_id, "run-id", storage)
    elif route == "query_archetype":
        await gateway.query_archetype(ctx, "signature", world_id, "run-id", storage)
    elif route == "list_signatures":
        await gateway.list_signatures(ctx, storage, world_id=world_id)
    elif route == "get_audit_history":
        await gateway.get_audit_history(ctx, world_id)
    elif route == "query_artifacts":
        await gateway.query_artifacts(ctx, world_id, storage_config=storage)
    elif route == "evaluate":
        await gateway.evaluate(
            ctx,
            world_id,
            [],
            contract=SimpleNamespace(grader_id="grader"),
        )
    else:
        raise AssertionError(f"unknown durable route {route}")


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


@pytest.mark.parametrize(("route", "command_type"), _DURABLE_WORLD_ROUTES)
async def test_closing_durable_operations_debit_tick_zero_before_delegation(
    route,
    command_type,
):
    application = _application()
    audit = AsyncMock()
    registry = WorldRegistry()
    world = SimpleNamespace(world_id="closing-world", name="closing", tick=13)
    await registry.insert(world)
    await registry.begin_close(world.world_id)
    target_tick_for_world = Mock(wraps=registry.target_tick)
    gateway = CommandGateway(
        application,
        audit=audit,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    expected_ticks = {(ctx.id, world.world_id, 0): 1}
    expected_tokens = guard.estimate_token_cost(Command(type=command_type))

    async def assert_gated_before_delegation(*_args, **_kwargs):
        assert guard._tick_counters == expected_ticks
        assert guard._daily_tokens == {ctx.id: expected_tokens}
        return SimpleNamespace(outcome="pass")

    application_route = getattr(application, route)
    application_route.side_effect = assert_gated_before_delegation

    await _invoke_durable_world_route(
        gateway,
        route,
        ctx,
        world_id=world.world_id,
        storage=object(),
    )

    target_tick_for_world.assert_called_once_with(world.world_id)
    assert application_route.await_count == 1
    audit.record.assert_awaited_once()
    assert guard._tick_counters == expected_ticks
    assert guard._daily_tokens == {ctx.id: expected_tokens}


@pytest.mark.parametrize(("route", "command_type"), _DURABLE_WORLD_ROUTES)
async def test_cold_durable_operations_share_explicit_world_tick_zero(
    route,
    command_type,
):
    application = _application()
    audit = AsyncMock()

    def missing_live_world(_world_id):
        raise KeyError("not live")

    target_tick_for_world = Mock(side_effect=missing_live_world)
    gateway = CommandGateway(
        application,
        audit=audit,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = object()
    world_id = "cold-world"
    expected_ticks = {(ctx.id, world_id, 0): 1}
    expected_tokens = guard.estimate_token_cost(Command(type=command_type))

    async def assert_gated_before_delegation(*_args, **_kwargs):
        assert guard._tick_counters == expected_ticks
        assert guard._daily_tokens == {ctx.id: expected_tokens}
        return SimpleNamespace(outcome="pass")

    application_route = getattr(application, route)
    application_route.side_effect = assert_gated_before_delegation

    await _invoke_durable_world_route(
        gateway,
        route,
        ctx,
        world_id=world_id,
        storage=storage,
    )

    target_tick_for_world.assert_called_once_with(world_id)
    assert application_route.await_count == 1
    audit.record.assert_awaited_once()
    assert guard._tick_counters == expected_ticks
    assert guard._daily_tokens == {ctx.id: expected_tokens}


@pytest.mark.parametrize(("route", "_command_type"), _DURABLE_WORLD_ROUTES)
async def test_durable_operations_propagate_unrelated_resolver_runtime_errors(
    route,
    _command_type,
):
    application = _application()
    audit = AsyncMock()
    target_tick_for_world = Mock(side_effect=RuntimeError("resolver misconfigured"))
    gateway = CommandGateway(
        application,
        audit=audit,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    with pytest.raises(RuntimeError, match="resolver misconfigured"):
        await _invoke_durable_world_route(
            gateway,
            route,
            ctx,
            world_id="world-a",
            storage=object(),
        )

    target_tick_for_world.assert_called_once_with("world-a")
    getattr(application, route).assert_not_awaited()
    audit.record.assert_not_awaited()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


@pytest.mark.parametrize(("route", "_command_type"), _DURABLE_WORLD_ROUTES)
async def test_unauthorized_durable_operations_stop_before_resolver(
    route,
    _command_type,
):
    application = _application()
    audit = AsyncMock()
    target_tick_for_world = Mock(side_effect=AssertionError("resolver must not run"))
    gateway = CommandGateway(
        application,
        audit=audit,
        target_tick_for_world=target_tick_for_world,
    )
    ctx = ActorCtx(id=uuid7(), roles=set())

    with pytest.raises(GuardrailError, match="cannot execute"):
        await _invoke_durable_world_route(
            gateway,
            route,
            ctx,
            world_id="secret-world",
            storage=object(),
        )

    target_tick_for_world.assert_not_called()
    getattr(application, route).assert_not_awaited()
    audit.record.assert_not_awaited()
    assert guard._tick_counters == {}
    assert guard._daily_tokens == {}


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
