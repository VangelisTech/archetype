# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dispatcher policy-coordinate and guard-first migration contracts.

The historical filename is retained for test-selection compatibility. Quota
generations now belong to one injected :class:`Policy`; no tick-reset callback
or module-global counter exists.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ConfigDict
from uuid_utils import uuid7

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import (
    AccessSummary,
    ActorCtx,
    DeferredItem,
    DurableOptions,
)
from archetype.commands.policy import Policy
from archetype.commands.registry import DurableOperation, OperationRegistry, OperationSpec

pytestmark = pytest.mark.asyncio


class _LiveOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["synthetic_live"] = "synthetic_live"
    world_id: str


class _DurableOperation(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["synthetic_durable"] = "synthetic_durable"
    world_id: str


class _OperatorDurableOperation(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["synthetic_operator_durable"] = "synthetic_operator_durable"
    world_id: str


class _ApplicationOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["synthetic_application"] = "synthetic_application"


@dataclass(slots=True)
class _Harness:
    dispatcher: CommandDispatcher
    effects: list[str]
    evidence: list[AccessSummary]
    target_reads: list[str]
    scheduler: Any


def _world_key(operation: BaseModel) -> object:
    return cast("Any", operation).world_id


def _summary(operation: BaseModel) -> Mapping[str, Any]:
    return {"world_id": cast("Any", operation).world_id}


def _empty_summary(_operation: BaseModel) -> Mapping[str, Any]:
    return {}


async def _never_materialize(_world: Any, _operation: BaseModel) -> None:
    raise AssertionError("dispatcher admission must not materialize an operation")


def _durable_metadata(
    model: type[BaseModel],
) -> DurableOperation:
    return DurableOperation(
        decode=model.model_validate_json,
        materialize=_never_materialize,
    )


def _register(
    registry: OperationRegistry,
    *,
    name: str,
    model: type[BaseModel],
    handler: Callable[[BaseModel], Awaitable[Any]],
    permission: str,
    quota_scope: Literal["application", "live_world", "durable_world"],
    durable: DurableOperation | None = None,
    token_cost: int = 0,
    world_key: Callable[[BaseModel], object] | None = _world_key,
) -> None:
    registry.register(
        OperationSpec(
            name=name,
            model=model,
            handler=handler,
            permission=permission,
            summarize=(_empty_summary if quota_scope == "application" else _summary),
            quota_scope=quota_scope,
            world_key=world_key,
            durable=durable,
            token_cost=token_cost,
        )
    )


def _harness(
    *,
    policy: Policy,
    ticks: dict[str, int] | None = None,
) -> _Harness:
    registry = OperationRegistry()
    effects: list[str] = []
    evidence: list[AccessSummary] = []
    target_reads: list[str] = []
    scheduler = AsyncMock()

    async def handle(operation: BaseModel) -> str:
        world_id = str(_world_key(operation))
        effects.append(world_id)
        return world_id

    async def record_access(row: AccessSummary) -> None:
        evidence.append(row)

    resolved_ticks = ticks if ticks is not None else {}

    def target_tick_for_world(world_id: object) -> int:
        normalized = str(world_id)
        target_reads.append(normalized)
        return resolved_ticks[normalized]

    _register(
        registry,
        name="synthetic_live",
        model=_LiveOperation,
        handler=handle,
        permission="spawn",
        quota_scope="live_world",
        token_cost=1,
    )
    return _Harness(
        dispatcher=CommandDispatcher(
            registry=registry,
            policy=policy,
            scheduler=scheduler,
            record_access=record_access,
            target_tick_for_world=target_tick_for_world,
        ),
        effects=effects,
        evidence=evidence,
        target_reads=target_reads,
        scheduler=scheduler,
    )


async def test_live_dispatch_uses_actor_world_and_current_tick_generations() -> None:
    ticks = {"world-a": 7, "world-b": 7}
    harness = _harness(
        policy=Policy(max_commands_per_tick=1, max_tokens_per_day=100),
        ticks=ticks,
    )
    actor = ActorCtx(id=uuid7(), roles={"player"})

    await harness.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))
    await harness.dispatcher.apply_as(actor, _LiveOperation(world_id="world-b"))

    with pytest.raises(PermissionError, match="per-tick quota"):
        await harness.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))

    ticks["world-a"] = 8
    await harness.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))

    assert harness.effects == ["world-a", "world-b", "world-a"]
    assert harness.target_reads == ["world-a", "world-b", "world-a", "world-a"]


async def test_live_dispatch_isolates_actors_at_the_same_world_tick() -> None:
    harness = _harness(
        policy=Policy(max_commands_per_tick=1, max_tokens_per_day=100),
        ticks={"world-a": 7},
    )
    actor_a = ActorCtx(id=uuid7(), roles={"player"})
    actor_b = ActorCtx(id=uuid7(), roles={"player"})

    await harness.dispatcher.apply_as(actor_a, _LiveOperation(world_id="world-a"))
    await harness.dispatcher.apply_as(actor_b, _LiveOperation(world_id="world-a"))

    assert harness.effects == ["world-a", "world-a"]


async def test_dispatchers_with_distinct_policy_instances_do_not_share_debits() -> None:
    actor = ActorCtx(id=uuid7(), roles={"player"})
    first = _harness(
        policy=Policy(max_commands_per_tick=1),
        ticks={"world-a": 7},
    )
    second = _harness(
        policy=Policy(max_commands_per_tick=1),
        ticks={"world-a": 7},
    )

    await first.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))
    with pytest.raises(PermissionError, match="per-tick quota"):
        await first.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))

    await second.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))
    assert second.effects == ["world-a"]


async def test_role_denial_precedes_target_resolution_handler_and_evidence() -> None:
    harness = _harness(
        policy=Policy(max_commands_per_tick=1),
        ticks={},
    )
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})

    with pytest.raises(PermissionError, match="cannot execute permission 'spawn'"):
        await harness.dispatcher.apply_as(
            viewer,
            _LiveOperation(world_id="secret-world"),
        )

    assert harness.target_reads == []
    assert harness.effects == []
    assert harness.evidence == []


async def test_full_quota_denial_precedes_handler_and_records_bounded_evidence() -> None:
    harness = _harness(
        policy=Policy(max_commands_per_tick=1, max_tokens_per_day=100),
        ticks={"world-a": 3},
    )
    actor = ActorCtx(id=uuid7(), roles={"player"})

    await harness.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))
    with pytest.raises(PermissionError, match="per-tick quota"):
        await harness.dispatcher.apply_as(actor, _LiveOperation(world_id="world-a"))

    assert harness.effects == ["world-a"]
    assert [(row.decision, row.outcome) for row in harness.evidence] == [
        ("allowed", "succeeded"),
        ("denied", "denied"),
    ]
    assert all(row.metadata.keys() <= {"world_id"} for row in harness.evidence)


async def test_application_scope_never_resolves_a_tick_or_consumes_tick_quota() -> None:
    registry = OperationRegistry()
    effects: list[str] = []
    record_access = AsyncMock()
    target_tick_for_world = Mock(
        side_effect=AssertionError("application scope has no tick coordinate")
    )

    async def handle(_operation: BaseModel) -> str:
        effects.append("application")
        return "application"

    _register(
        registry,
        name="synthetic_application",
        model=_ApplicationOperation,
        handler=handle,
        permission="create_world",
        quota_scope="application",
        world_key=None,
    )
    dispatcher = CommandDispatcher(
        registry=registry,
        policy=Policy(max_commands_per_tick=1),
        scheduler=AsyncMock(),
        record_access=record_access,
        target_tick_for_world=target_tick_for_world,
    )
    admin = ActorCtx(id=uuid7(), roles={"admin"})

    await dispatcher.apply_as(admin, _ApplicationOperation())
    await dispatcher.apply_as(admin, _ApplicationOperation())

    assert effects == ["application", "application"]
    target_tick_for_world.assert_not_called()


async def test_deferred_dispatch_uses_options_tick_without_live_tick_resolution() -> None:
    registry = OperationRegistry()
    scheduler = AsyncMock()
    scheduler.admit.return_value = "queued"
    evidence: list[AccessSummary] = []
    target_tick_for_world = Mock(
        side_effect=AssertionError("deferred scope must use DurableOptions.target_tick")
    )

    async def handle(_operation: BaseModel) -> None:
        raise AssertionError("deferred admission must not invoke the direct handler")

    async def record_access(row: AccessSummary) -> None:
        evidence.append(row)

    _register(
        registry,
        name="synthetic_durable",
        model=_DurableOperation,
        handler=handle,
        permission="spawn",
        quota_scope="live_world",
        durable=_durable_metadata(_DurableOperation),
    )
    dispatcher = CommandDispatcher(
        registry=registry,
        policy=Policy(max_commands_per_tick=1),
        scheduler=scheduler,
        record_access=record_access,
        target_tick_for_world=target_tick_for_world,
    )
    actor = ActorCtx(id=uuid7(), roles={"player"})
    operation = _DurableOperation(world_id="world-a")

    await dispatcher.defer_as(actor, operation, DurableOptions(target_tick=7))
    await dispatcher.defer_as(actor, operation, DurableOptions(target_tick=8))
    with pytest.raises(PermissionError, match="per-tick quota"):
        await dispatcher.defer_as(actor, operation, DurableOptions(target_tick=7))

    assert scheduler.admit.await_count == 2
    target_tick_for_world.assert_not_called()
    assert [row.outcome for row in evidence] == ["queued", "queued", "denied"]


async def test_later_batch_role_denial_precedes_all_coordinates_and_admission() -> None:
    registry = OperationRegistry()
    scheduler = AsyncMock()
    coordinate_reads: list[str] = []
    evidence: list[AccessSummary] = []

    async def handle(_operation: BaseModel) -> None:
        raise AssertionError("deferred admission must not invoke the direct handler")

    async def record_access(row: AccessSummary) -> None:
        evidence.append(row)

    def forbidden_world_key(operation: BaseModel) -> object:
        coordinate_reads.append(type(operation).__name__)
        raise AssertionError("batch coordinates must follow all role checks")

    for name, model, permission in (
        ("synthetic_durable", _DurableOperation, "spawn"),
        (
            "synthetic_operator_durable",
            _OperatorDurableOperation,
            "add_components",
        ),
    ):
        _register(
            registry,
            name=name,
            model=model,
            handler=handle,
            permission=permission,
            quota_scope="live_world",
            durable=_durable_metadata(model),
            world_key=forbidden_world_key,
        )

    dispatcher = CommandDispatcher(
        registry=registry,
        policy=Policy(max_commands_per_tick=1),
        scheduler=scheduler,
        record_access=record_access,
        target_tick_for_world=cast(
            "Callable[[object], int]",
            lambda _world_id: pytest.fail("batch must not resolve a live tick"),
        ),
    )
    player = ActorCtx(id=uuid7(), roles={"player"})
    items = (
        DeferredItem(
            operation=_DurableOperation(world_id="secret-world"),
            options=DurableOptions(target_tick=4),
        ),
        DeferredItem(
            operation=_OperatorDurableOperation(world_id="secret-world"),
            options=DurableOptions(target_tick=4),
        ),
    )

    with pytest.raises(PermissionError, match="cannot execute permission 'add_components'"):
        await dispatcher.defer_batch_as(player, items)

    assert coordinate_reads == []
    scheduler.admit_batch.assert_not_awaited()
    assert evidence == []
