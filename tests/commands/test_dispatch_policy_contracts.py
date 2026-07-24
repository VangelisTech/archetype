# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for governed command dispatch and instance-owned policy."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from inspect import signature
from typing import Any, ClassVar, Literal, NamedTuple, cast

import pytest
from pydantic import BaseModel, ConfigDict, Field
from uuid_utils import UUID, uuid7

from archetype.storage.catalog import CommandConflictError
from archetype.world.models import Spawn

pytestmark = pytest.mark.contract("gateway.authorization.rbac")


class _CommandsApi(NamedTuple):
    CommandDispatcher: type[Any]
    DeferredItem: type[Any]
    PolicyRequest: type[Any]
    Policy: type[Any]
    policy_module: Any


def _commands_api() -> _CommandsApi:
    """Load the intentionally absent pre-PR-3 family after collection."""
    dispatch_module = import_module("archetype.commands.dispatch")
    policy_module = import_module("archetype.commands.policy")
    models_module = import_module("archetype.commands.models")
    return _CommandsApi(
        CommandDispatcher=dispatch_module.CommandDispatcher,
        DeferredItem=models_module.DeferredItem,
        PolicyRequest=models_module.PolicyRequest,
        Policy=policy_module.Policy,
        policy_module=policy_module,
    )


class _Actor(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    id: UUID = Field(default_factory=uuid7)
    roles: frozenset[str] = frozenset({"player"})


class _Operation(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["synthetic"] = "synthetic"
    world_id: str
    label: str = "operation"


class _SensitiveOperation(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    operation: Literal["sensitive"] = "sensitive"
    world_id: str
    component_values: tuple[str, ...]
    credential: str
    callback: Callable[[], None]
    storage_config: dict[str, str]
    task_base_revision: str
    repository_diff: str
    validator_output: str
    critic_findings: str
    cleanup_state: str
    arbitrary_result: str


class _LiveOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    operation: Literal["live_operation"] = "live_operation"
    world_id: str
    callback: Callable[[], None]
    credential: str


class _ApplicationOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["application_operation"] = "application_operation"
    label: str = "application"


class _DurableReadOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["durable_read"] = "durable_read"
    world_id: str


class _DurableOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    target_tick: int = Field(ge=0)
    priority: int = 0


def _world_key(operation: BaseModel) -> object:
    return cast("Any", operation).world_id


@dataclass(frozen=True, slots=True)
class _Spec:
    name: str
    model: type[BaseModel]
    handler: Callable[[Any], Any]
    permission: str
    summarize: Callable[[Any], Mapping[str, Any]]
    quota_scope: Literal["application", "live_world", "durable_world"] = "live_world"
    world_key: Callable[[BaseModel], object] | None = _world_key
    durable: object | None = None
    trusted: bool = True
    untrusted: bool = True
    token_cost: int | Callable[[BaseModel], int] = 0


class _ObservedSpec:
    """Observe or forbid effect-bearing metadata reads after resolution."""

    def __init__(
        self,
        spec: _Spec,
        events: list[str],
        *,
        forbidden_reads: frozenset[str] = frozenset(),
    ) -> None:
        self._spec = spec
        self._events = events
        self._forbidden_reads = forbidden_reads

    def __getattr__(self, name: str) -> Any:
        if name in {"untrusted", "durable", "world_key", "token_cost"}:
            self._events.append(name)
            if name in self._forbidden_reads:
                raise AssertionError(f"{name} must not be read before role preauthorization")
        return getattr(self._spec, name)


class _Registry:
    def __init__(self, specs: tuple[Any, ...], events: list[str]) -> None:
        self._specs = {spec.model: spec for spec in specs}
        self.events = events
        self.resolved: list[BaseModel] = []

    def resolve(self, operation: BaseModel) -> _Spec:
        self.events.append("resolve")
        self.resolved.append(operation)
        spec = self._specs.get(type(operation))
        if spec is None:
            raise KeyError(f"{type(operation).__name__} is not registered")
        return spec


class _PolicyPort:
    """Synthetic policy with the exact bounded dispatcher-to-policy seam."""

    def __init__(
        self,
        events: list[str],
        *,
        denial: BaseException | None = None,
        preauthorization_denial: BaseException | None = None,
        denied_permissions: frozenset[str] = frozenset(),
    ) -> None:
        self.events = events
        self.denial = denial
        self.preauthorization_denial = preauthorization_denial
        self.denied_permissions = denied_permissions
        self.preauthorization_calls: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []
        self.application_calls: list[dict[str, Any]] = []
        self.batch_calls: list[dict[str, Any]] = []

    def preauthorize(
        self,
        actor: _Actor,
        *,
        permission: str,
    ) -> None:
        self.events.append("preauthorize")
        self.preauthorization_calls.append(
            {
                "actor": actor,
                "permission": permission,
            }
        )
        if self.preauthorization_denial is not None:
            raise self.preauthorization_denial
        if permission in self.denied_permissions:
            raise PermissionError(f"role denied permission {permission!r}")

    def authorize(
        self,
        actor: _Actor,
        *,
        permission: str,
        world_id: object,
        target_tick: int,
        token_cost: int = 0,
    ) -> None:
        self.events.append("policy")
        self.calls.append(
            {
                "actor": actor,
                "permission": permission,
                "world_id": str(world_id),
                "target_tick": target_tick,
                "token_cost": token_cost,
            }
        )
        if self.denial is not None:
            raise self.denial

    def authorize_application(
        self,
        actor: _Actor,
        *,
        permission: str,
        token_cost: int = 0,
    ) -> None:
        self.events.append("policy_application")
        self.application_calls.append(
            {
                "actor": actor,
                "permission": permission,
                "token_cost": token_cost,
            }
        )
        if self.denial is not None:
            raise self.denial

    def authorize_batch(
        self,
        actor: _Actor,
        *,
        requests: tuple[object, ...],
    ) -> None:
        self.events.append("policy_batch")
        self.batch_calls.append(
            {
                "actor": actor,
                "requests": requests,
            }
        )
        if self.denial is not None:
            raise self.denial


class _SchedulerPort:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.admissions: list[dict[str, Any]] = []
        self.batch_admissions: list[dict[str, Any]] = []
        self.spawn_admissions: list[dict[str, Any]] = []
        self._replays: dict[str, tuple[object, ...]] = {}

    async def admit(
        self,
        operation: BaseModel,
        options: BaseModel,
        *,
        command_id: object | None = None,
        principal_id: object | None = None,
        origin: str = "local",
        version: int = 1,
    ) -> str:
        self.events.append("scheduler")
        resolved_command_id = str(command_id or f"command-{len(self.admissions) + 1}")
        immutable = (
            operation,
            options,
            str(principal_id) if principal_id is not None else None,
            origin,
            version,
        )
        existing = self._replays.get(resolved_command_id)
        if existing is not None and existing != immutable:
            raise CommandConflictError(
                f"command {resolved_command_id} content conflicts with its durable identity"
            )
        self._replays[resolved_command_id] = immutable
        self.admissions.append(
            {
                "operation": operation,
                "options": options,
                "command_id": command_id,
                "principal_id": principal_id,
                "origin": origin,
                "version": version,
            }
        )
        return resolved_command_id

    async def admit_batch(
        self,
        items: tuple[object, ...],
        *,
        principal_id: object | None = None,
        origin: str = "local",
    ) -> list[str]:
        self.events.append("scheduler_batch")
        self.batch_admissions.append(
            {
                "items": items,
                "principal_id": principal_id,
                "origin": origin,
            }
        )
        return [str(getattr(item, "command_id", None) or uuid7()) for item in items]

    async def admit_spawn(
        self,
        operation: Spawn,
        options: BaseModel,
        *,
        command_id: object | None = None,
        principal_id: object | None = None,
        origin: str = "local",
        version: int = 1,
    ) -> tuple[int, str]:
        self.events.append("scheduler_spawn")
        self.spawn_admissions.append(
            {
                "operation": operation,
                "options": options,
                "command_id": command_id,
                "principal_id": principal_id,
                "origin": origin,
                "version": version,
            }
        )
        return 41, str(command_id or "spawn-command-1")


class _AccessSink:
    def __init__(
        self,
        events: list[str],
        *,
        failure: Exception | None = None,
    ) -> None:
        self.events = events
        self.failure = failure
        self.rows: list[Any] = []

    async def __call__(self, evidence: Any) -> None:
        self.events.append("evidence")
        self.rows.append(evidence)
        if self.failure is not None:
            raise self.failure


class _TargetTickResolver:
    def __init__(
        self,
        ticks: Mapping[str, int],
        *,
        events: list[str] | None = None,
    ) -> None:
        self.ticks = dict(ticks)
        self.events = events
        self.calls: list[str] = []

    def __call__(self, world_id: object) -> int:
        if self.events is not None:
            self.events.append("target_tick")
        normalized_world_id = str(world_id)
        self.calls.append(normalized_world_id)
        return self.ticks[normalized_world_id]


def _evidence_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if hasattr(value, "__dict__"):
        return dict(vars(value))
    raise AssertionError(f"unsupported access-evidence value: {type(value)!r}")


def _dispatcher(
    api: _CommandsApi,
    *,
    registry: _Registry,
    policy: _PolicyPort,
    scheduler: _SchedulerPort,
    access: _AccessSink,
    target_tick_for_world: Callable[[object], int],
) -> Any:
    return api.CommandDispatcher(
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        record_access=access,
        target_tick_for_world=target_tick_for_world,
    )


@pytest.mark.asyncio
async def test_trusted_and_actor_aware_entry_share_the_exact_handler() -> None:
    api = _commands_api()
    events: list[str] = []
    handled: list[_Operation] = []

    async def handler(operation: _Operation) -> tuple[str, str]:
        events.append("handler")
        handled.append(operation)
        return operation.operation, operation.label

    def summarize(operation: _Operation) -> dict[str, str]:
        events.append("summarize")
        return {"label": operation.label}

    world_key_calls: list[_Operation] = []

    def world_key(operation: BaseModel) -> str:
        exact_operation = cast("_Operation", operation)
        world_key_calls.append(exact_operation)
        return exact_operation.world_id

    spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=handler,
        permission="spawn",
        summarize=summarize,
        world_key=world_key,
    )
    registry = _Registry((spec,), events)
    policy = _PolicyPort(events)
    scheduler = _SchedulerPort(events)
    access = _AccessSink(events)
    target_ticks = _TargetTickResolver({"world-parity": 7})
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=target_ticks,
    )
    operation = _Operation(
        world_id="world-parity",
        label="same-command",
    )
    actor = _Actor()

    trusted = await dispatcher.apply(operation)
    assert policy.preauthorization_calls == []
    assert policy.calls == []
    assert access.rows == []
    assert target_ticks.calls == []

    actor_aware = await dispatcher.apply_as(actor, operation)

    assert trusted == actor_aware == ("synthetic", "same-command")
    assert handled == [operation, operation]
    assert registry.resolved == [operation, operation]
    assert world_key_calls == [operation]
    assert target_ticks.calls == ["world-parity"]
    assert policy.preauthorization_calls == [
        {
            "actor": actor,
            "permission": "spawn",
        }
    ]
    assert policy.calls == [
        {
            "actor": actor,
            "permission": "spawn",
            "world_id": "world-parity",
            "target_tick": 7,
            "token_cost": 0,
        }
    ]
    assert len(access.rows) == 1
    evidence = _evidence_dict(access.rows[0])
    assert evidence["operation"] == "synthetic"
    assert str(evidence["actor_id"]) == str(actor.id)
    assert evidence["decision"] == "allowed"
    assert evidence["outcome"] == "succeeded"


@pytest.mark.asyncio
async def test_actor_aware_order_is_resolve_preauthorize_coordinates_policy_then_effects() -> None:
    api = _commands_api()
    events: list[str] = []

    async def handler(operation: _Operation) -> str:
        events.append("handler")
        return operation.label

    def summarize(operation: _Operation) -> dict[str, str]:
        events.append("summarize")
        return {"label": operation.label}

    def world_key(operation: BaseModel) -> object:
        events.append("world_key_call")
        return cast("_Operation", operation).world_id

    def token_cost(_operation: BaseModel) -> int:
        events.append("cost_call")
        return 3

    spec = _ObservedSpec(
        _Spec(
            name="synthetic",
            model=_Operation,
            handler=handler,
            permission="spawn",
            summarize=summarize,
            world_key=world_key,
            token_cost=token_cost,
        ),
        events,
    )
    registry = _Registry((spec,), events)
    policy = _PolicyPort(events)
    access = _AccessSink(events)
    target_ticks = _TargetTickResolver({"world-order": 19}, events=events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=_SchedulerPort(events),
        access=access,
        target_tick_for_world=target_ticks,
    )
    actor = _Actor()
    operation = _Operation(
        world_id="world-order",
        label="ordered",
    )

    assert await dispatcher.apply_as(actor, operation) == "ordered"

    assert events[:3] == ["resolve", "preauthorize", "untrusted"]
    ordered_events = (
        "world_key",
        "world_key_call",
        "target_tick",
        "token_cost",
        "cost_call",
        "policy",
        "handler",
        "summarize",
        "evidence",
    )
    assert [events.index(name) for name in ordered_events] == sorted(
        events.index(name) for name in ordered_events
    )
    assert target_ticks.calls == ["world-order"]
    assert policy.calls[0] == {
        "actor": actor,
        "permission": "spawn",
        "world_id": "world-order",
        "target_tick": 19,
        "token_cost": 3,
    }
    evidence = _evidence_dict(access.rows[0])
    assert evidence["world_id"] == "world-order"
    assert evidence["metadata"] == {"label": "ordered"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "permission", "quota_scope"),
    [
        pytest.param(
            _Operation(world_id="secret-live"),
            "spawn",
            "live_world",
            id="live-world",
        ),
        pytest.param(
            _DurableReadOperation(world_id="secret-durable"),
            "resume_world",
            "durable_world",
            id="durable-world",
        ),
    ],
)
async def test_role_denied_direct_world_operation_stops_before_any_resource_or_effect(
    operation: BaseModel,
    permission: str,
    quota_scope: Literal["live_world", "durable_world"],
) -> None:
    api = _commands_api()
    events: list[str] = []

    async def unexpected_handler(_operation: BaseModel) -> None:
        raise AssertionError("application handler must not run after role denial")

    def unexpected_world_key(_operation: BaseModel) -> object:
        raise AssertionError("world_key must not run before role preauthorization")

    def unexpected_cost(_operation: BaseModel) -> int:
        raise AssertionError("token cost must not resolve before role preauthorization")

    base_spec = _Spec(
        name=str(type(operation).model_fields["operation"].default),
        model=type(operation),
        handler=unexpected_handler,
        permission=permission,
        summarize=lambda _operation: {},
        quota_scope=quota_scope,
        world_key=unexpected_world_key,
        token_cost=unexpected_cost,
    )
    spec = _ObservedSpec(
        base_spec,
        events,
        forbidden_reads=frozenset({"untrusted", "durable", "world_key", "token_cost"}),
    )
    registry = _Registry((spec,), events)
    policy = _PolicyPort(
        events,
        preauthorization_denial=PermissionError("role denied uniformly"),
    )
    scheduler = _SchedulerPort(events)
    access = _AccessSink(events)

    def unexpected_target_tick(_world_id: object) -> int:
        raise AssertionError("target-tick resolver must not run after role denial")

    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=unexpected_target_tick,
    )
    actor = _Actor(roles=frozenset({"viewer"}))

    with pytest.raises(PermissionError, match="role denied uniformly"):
        await dispatcher.apply_as(actor, operation)

    assert events == ["resolve", "preauthorize"]
    assert policy.preauthorization_calls == [
        {
            "actor": actor,
            "permission": permission,
        }
    ]
    assert policy.calls == []
    assert policy.application_calls == []
    assert policy.batch_calls == []
    assert scheduler.admissions == []
    assert scheduler.batch_admissions == []
    assert scheduler.spawn_admissions == []
    assert access.rows == []


@pytest.mark.asyncio
async def test_denied_and_failed_access_evidence_excludes_sensitive_data() -> None:
    api = _commands_api()
    events: list[str] = []
    handler_calls = 0

    async def handler(_operation: _SensitiveOperation) -> None:
        nonlocal handler_calls
        handler_calls += 1
        events.append("handler")
        raise RuntimeError("handler exploded with DIFF_SENTINEL and RESULT_SENTINEL")

    def malicious_summary(operation: _SensitiveOperation) -> dict[str, Any]:
        events.append("summarize")
        return {
            "component_values": operation.component_values,
            "credential": operation.credential,
            "callback": operation.callback,
            "storage_config": operation.storage_config,
            "task_base_revision": operation.task_base_revision,
            "repository_diff": operation.repository_diff,
            "validator_output": operation.validator_output,
            "critic_findings": operation.critic_findings,
            "cleanup_state": operation.cleanup_state,
            "arbitrary_result": operation.arbitrary_result,
            "safe_count": len(operation.component_values),
        }

    operation = _SensitiveOperation(
        world_id="world-sensitive",
        component_values=("COMPONENT_SENTINEL",),
        credential="GITHUB_TOKEN_SENTINEL",
        callback=lambda: None,
        storage_config={"uri": "STORAGE_SENTINEL"},
        task_base_revision="TASK_BASE_SENTINEL",
        repository_diff="DIFF_SENTINEL",
        validator_output="VALIDATOR_SENTINEL",
        critic_findings="CRITIC_SENTINEL",
        cleanup_state="CLEANUP_SENTINEL",
        arbitrary_result="RESULT_SENTINEL",
    )
    spec = _Spec(
        name="sensitive",
        model=_SensitiveOperation,
        handler=handler,
        permission="spawn",
        summarize=malicious_summary,
    )
    registry = _Registry((spec,), events)
    policy = _PolicyPort(
        events,
        denial=PermissionError("denied with GITHUB_TOKEN_SENTINEL and VALIDATOR_SENTINEL"),
    )
    access = _AccessSink(events)
    target_ticks = _TargetTickResolver({"world-sensitive": 23})
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=_SchedulerPort(events),
        access=access,
        target_tick_for_world=target_ticks,
    )
    actor = _Actor()

    with pytest.raises(PermissionError, match="denied"):
        await dispatcher.apply_as(actor, operation)
    assert handler_calls == 0

    policy.denial = None
    with pytest.raises(RuntimeError, match="handler exploded"):
        await dispatcher.apply_as(actor, operation)
    assert handler_calls == 1
    assert len(access.rows) == 2
    assert target_ticks.calls == ["world-sensitive", "world-sensitive"]

    denied, failed = (_evidence_dict(row) for row in access.rows)
    assert (denied["decision"], denied["outcome"]) == ("denied", "denied")
    assert (failed["decision"], failed["outcome"]) == ("allowed", "failed")
    for row in (denied, failed):
        assert row["operation"] == "sensitive"
        assert str(row["actor_id"]) == str(actor.id)
        assert row["world_id"] == "world-sensitive"
        encoded = json.dumps(row, sort_keys=True, default=str)
        assert len(encoded) <= 4096
        for forbidden in (
            "COMPONENT_SENTINEL",
            "GITHUB_TOKEN_SENTINEL",
            "STORAGE_SENTINEL",
            "TASK_BASE_SENTINEL",
            "DIFF_SENTINEL",
            "VALIDATOR_SENTINEL",
            "CRITIC_SENTINEL",
            "CLEANUP_SENTINEL",
            "RESULT_SENTINEL",
        ):
            assert forbidden not in encoded
        for forbidden_key in (
            "component_values",
            "credential",
            "callback",
            "storage_config",
            "task_base_revision",
            "repository_diff",
            "validator_output",
            "critic_findings",
            "cleanup_state",
            "arbitrary_result",
        ):
            assert forbidden_key not in encoded


@pytest.mark.asyncio
async def test_defer_as_rejects_live_direct_only_before_scheduler_persistence() -> None:
    api = _commands_api()
    events: list[str] = []

    async def handler(_operation: _LiveOperation) -> None:
        raise AssertionError("direct handler must not run during deferred admission")

    def summarize(_operation: _LiveOperation) -> dict[str, str]:
        return {"classification": "direct_only"}

    spec = _Spec(
        name="live_operation",
        model=_LiveOperation,
        handler=handler,
        permission="add_processor",
        summarize=summarize,
        durable=None,
    )
    registry = _Registry((spec,), events)
    policy = _PolicyPort(events)
    scheduler = _SchedulerPort(events)
    access = _AccessSink(events)

    def unexpected_target_tick(_world_id: object) -> int:
        pytest.fail("deferred admission must use DurableOptions.target_tick")

    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=unexpected_target_tick,
    )
    actor = _Actor(roles=frozenset({"operator"}))
    operation = _LiveOperation(
        world_id="world-live",
        callback=lambda: None,
        credential="LIVE_CREDENTIAL_SENTINEL",
    )
    options = _DurableOptions(target_tick=29)

    with pytest.raises(ValueError, match=r"(?i)direct-only"):
        await dispatcher.defer_as(actor, operation, options)

    assert policy.calls == []
    assert policy.application_calls == []
    assert policy.batch_calls == []
    assert scheduler.admissions == []
    assert events[0] == "resolve"
    assert "policy" not in events
    assert "scheduler" not in events
    assert len(access.rows) == 1
    evidence = _evidence_dict(access.rows[0])
    assert evidence["decision"] == "denied"
    assert evidence["outcome"] == "rejected"
    assert "LIVE_CREDENTIAL_SENTINEL" not in json.dumps(
        evidence,
        sort_keys=True,
        default=str,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entrypoint", "operation"),
    [
        pytest.param(
            "defer_as",
            _Operation(world_id="secret-deferred"),
            id="generic-deferred",
        ),
        pytest.param(
            "defer_spawn_as",
            Spawn(world_id="secret-spawn", components=()),
            id="reserved-spawn",
        ),
    ],
)
async def test_role_denied_deferred_entry_stops_before_eligibility_coordinates_or_admission(
    entrypoint: str,
    operation: BaseModel,
) -> None:
    api = _commands_api()
    events: list[str] = []

    async def unexpected_handler(_operation: BaseModel) -> None:
        raise AssertionError("direct application handler must remain untouched")

    def unexpected_world_key(_operation: BaseModel) -> object:
        raise AssertionError("world_key must not run before role preauthorization")

    def unexpected_cost(_operation: BaseModel) -> int:
        raise AssertionError("token cost must not resolve before role preauthorization")

    base_spec = _Spec(
        name=str(type(operation).model_fields["operation"].default),
        model=type(operation),
        handler=unexpected_handler,
        permission="spawn",
        summarize=lambda _operation: {},
        durable=object(),
        world_key=unexpected_world_key,
        token_cost=unexpected_cost,
    )
    registry = _Registry(
        (
            _ObservedSpec(
                base_spec,
                events,
                forbidden_reads=frozenset({"untrusted", "durable", "world_key", "token_cost"}),
            ),
        ),
        events,
    )
    policy = _PolicyPort(
        events,
        preauthorization_denial=PermissionError("role denied before admission"),
    )
    scheduler = _SchedulerPort(events)
    access = _AccessSink(events)

    def unexpected_target_tick(_world_id: object) -> int:
        raise AssertionError("deferred paths must not use the live target-tick resolver")

    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=unexpected_target_tick,
    )
    actor = _Actor(roles=frozenset({"viewer"}))
    options = _DurableOptions(target_tick=31)

    with pytest.raises(PermissionError, match="role denied before admission"):
        method = getattr(dispatcher, entrypoint)
        await method(actor, operation, options)

    assert events == ["resolve", "preauthorize"]
    assert policy.preauthorization_calls == [
        {
            "actor": actor,
            "permission": "spawn",
        }
    ]
    assert policy.calls == []
    assert policy.application_calls == []
    assert policy.batch_calls == []
    assert scheduler.admissions == []
    assert scheduler.batch_admissions == []
    assert scheduler.spawn_admissions == []
    assert access.rows == []


@pytest.mark.asyncio
async def test_application_and_durable_world_policy_coordinates_are_exact_and_fail_closed() -> None:
    api = _commands_api()
    events: list[str] = []
    handled: list[str] = []

    async def application_handler(operation: _ApplicationOperation) -> str:
        handled.append(operation.operation)
        return operation.label

    async def durable_handler(operation: _DurableReadOperation) -> str:
        handled.append(operation.world_id)
        return operation.world_id

    specs = (
        _Spec(
            name="application_operation",
            model=_ApplicationOperation,
            handler=application_handler,
            permission="list_worlds",
            summarize=lambda operation: {"label": operation.label},
            quota_scope="application",
            world_key=None,
        ),
        _Spec(
            name="durable_read",
            model=_DurableReadOperation,
            handler=durable_handler,
            permission="query_components",
            summarize=lambda operation: {"world_id": operation.world_id},
            quota_scope="durable_world",
        ),
    )
    registry = _Registry(specs, events)
    policy = _PolicyPort(events)
    target_ticks = _TargetTickResolver({"world-live": 17})
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=_SchedulerPort(events),
        access=_AccessSink(events),
        target_tick_for_world=target_ticks,
    )
    actor = _Actor(roles=frozenset({"viewer"}))

    assert await dispatcher.apply_as(actor, _ApplicationOperation()) == "application"
    assert (
        await dispatcher.apply_as(
            actor,
            _DurableReadOperation(world_id="world-live"),
        )
        == "world-live"
    )
    assert (
        await dispatcher.apply_as(
            actor,
            _DurableReadOperation(world_id="world-cold"),
        )
        == "world-cold"
    )

    assert policy.application_calls == [
        {
            "actor": actor,
            "permission": "list_worlds",
            "token_cost": 0,
        }
    ]
    assert [
        (call["world_id"], call["target_tick"], call["permission"]) for call in policy.calls
    ] == [
        ("world-live", 17, "query_components"),
        ("world-cold", 0, "query_components"),
    ]
    assert target_ticks.calls == ["world-live", "world-cold"]
    assert handled == ["application_operation", "world-live", "world-cold"]

    closed_policy = _PolicyPort([])

    def resolver_failure(_world_id: object) -> int:
        raise RuntimeError("catalog resolver unavailable")

    closed_dispatcher = _dispatcher(
        api,
        registry=_Registry((specs[1],), []),
        policy=closed_policy,
        scheduler=_SchedulerPort([]),
        access=_AccessSink([]),
        target_tick_for_world=resolver_failure,
    )
    with pytest.raises(RuntimeError, match="catalog resolver unavailable"):
        await closed_dispatcher.apply_as(
            actor,
            _DurableReadOperation(world_id="world-error"),
        )
    assert closed_policy.calls == []
    assert handled == ["application_operation", "world-live", "world-cold"]


@pytest.mark.asyncio
async def test_dispatcher_preserves_command_identity_version_and_replay_conflicts() -> None:
    api = _commands_api()
    events: list[str] = []

    async def handler(operation: _Operation) -> str:
        return operation.label

    registry = _Registry(
        (
            _Spec(
                name="synthetic",
                model=_Operation,
                handler=handler,
                permission="spawn",
                summarize=lambda operation: {"label": operation.label},
                durable=object(),
            ),
        ),
        events,
    )
    scheduler = _SchedulerPort(events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=_PolicyPort(events),
        scheduler=scheduler,
        access=_AccessSink(events),
        target_tick_for_world=lambda _world_id: 99,
    )
    operation = _Operation(world_id="world-replay", label="same")
    options = _DurableOptions(target_tick=12, priority=-3)
    command_id = uuid7()

    first = await dispatcher.defer(
        operation,
        options,
        command_id=command_id,
        version=7,
    )
    replay = await dispatcher.defer(
        operation,
        options,
        command_id=command_id,
        version=7,
    )
    assert str(first) == str(replay) == str(command_id)
    assert scheduler.admissions == [
        {
            "operation": operation,
            "options": options,
            "command_id": command_id,
            "principal_id": None,
            "origin": "local",
            "version": 7,
        },
        {
            "operation": operation,
            "options": options,
            "command_id": command_id,
            "principal_id": None,
            "origin": "local",
            "version": 7,
        },
    ]

    with pytest.raises(CommandConflictError):
        await dispatcher.defer(
            operation.model_copy(update={"label": "changed"}),
            options,
            command_id=command_id,
            version=7,
        )
    with pytest.raises(CommandConflictError):
        await dispatcher.defer(
            operation,
            options,
            command_id=command_id,
            version=8,
        )


@pytest.mark.asyncio
async def test_batch_validates_all_members_then_debits_and_admits_once() -> None:
    api = _commands_api()
    events: list[str] = []

    async def handler(operation: BaseModel) -> str:
        return str(operation)

    portable_spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=handler,
        permission="spawn",
        summarize=lambda operation: {"label": operation.label},
        durable=object(),
        token_cost=3,
    )
    direct_spec = _Spec(
        name="live_operation",
        model=_LiveOperation,
        handler=handler,
        permission="add_processor",
        summarize=lambda _operation: {"classification": "direct_only"},
        durable=None,
    )
    registry = _Registry((portable_spec, direct_spec), events)
    policy = _PolicyPort(events)
    scheduler = _SchedulerPort(events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=_AccessSink(events),
        target_tick_for_world=lambda _world_id: 99,
    )
    actor = _Actor()
    command_a = uuid7()
    command_b = uuid7()
    items = (
        api.DeferredItem(
            operation=_Operation(world_id="world-batch", label="a"),
            options=_DurableOptions(target_tick=4, priority=0),
            command_id=command_a,
            version=2,
        ),
        api.DeferredItem(
            operation=_Operation(world_id="world-batch", label="b"),
            options=_DurableOptions(target_tick=4, priority=1),
            command_id=command_b,
            version=3,
        ),
    )

    admitted = await dispatcher.defer_batch_as(actor, items)

    assert [str(value) for value in admitted] == [str(command_a), str(command_b)]
    assert len(policy.batch_calls) == 1
    requests = policy.batch_calls[0]["requests"]
    assert [
        (
            request.permission,
            str(request.world_id),
            request.target_tick,
            request.token_cost,
        )
        for request in requests
    ] == [
        ("spawn", "world-batch", 4, 3),
        ("spawn", "world-batch", 4, 3),
    ]
    assert scheduler.batch_admissions == [
        {
            "items": items,
            "principal_id": actor.id,
            "origin": "gateway",
        }
    ]
    assert events.count("policy_batch") == 1
    assert events.count("scheduler_batch") == 1
    assert events.index("policy_batch") < events.index("scheduler_batch")

    rejected = (
        items[0],
        api.DeferredItem(
            operation=_LiveOperation(
                world_id="world-batch",
                callback=lambda: None,
                credential="BATCH_SECRET",
            ),
            options=_DurableOptions(target_tick=4),
        ),
    )
    policy.batch_calls.clear()
    scheduler.batch_admissions.clear()
    with pytest.raises(ValueError, match=r"(?i)direct-only"):
        await dispatcher.defer_batch_as(actor, rejected)
    assert policy.batch_calls == []
    assert scheduler.batch_admissions == []


@pytest.mark.asyncio
async def test_later_role_denied_batch_member_stops_before_any_member_resource_or_effect() -> None:
    api = _commands_api()
    events: list[str] = []

    async def unexpected_handler(_operation: BaseModel) -> None:
        raise AssertionError("batch admission must not invoke direct application handlers")

    def unexpected_world_key(_operation: BaseModel) -> object:
        raise AssertionError("no batch world_key may run before all members preauthorize")

    def unexpected_cost(_operation: BaseModel) -> int:
        raise AssertionError("no batch cost may resolve before all members preauthorize")

    specs = (
        _ObservedSpec(
            _Spec(
                name="synthetic",
                model=_Operation,
                handler=unexpected_handler,
                permission="spawn",
                summarize=lambda _operation: {},
                world_key=unexpected_world_key,
                durable=object(),
                token_cost=unexpected_cost,
            ),
            events,
            forbidden_reads=frozenset({"untrusted", "durable", "world_key", "token_cost"}),
        ),
        _ObservedSpec(
            _Spec(
                name="live_operation",
                model=_LiveOperation,
                handler=unexpected_handler,
                permission="add_processor",
                summarize=lambda _operation: {},
                world_key=unexpected_world_key,
                durable=object(),
                token_cost=unexpected_cost,
            ),
            events,
            forbidden_reads=frozenset({"untrusted", "durable", "world_key", "token_cost"}),
        ),
    )
    registry = _Registry(specs, events)
    policy = _PolicyPort(
        events,
        denied_permissions=frozenset({"add_processor"}),
    )
    scheduler = _SchedulerPort(events)
    access = _AccessSink(events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=lambda _world_id: pytest.fail(
            "deferred batch must not use the live target-tick resolver"
        ),
    )
    actor = _Actor(roles=frozenset({"player"}))
    items = (
        api.DeferredItem(
            operation=_Operation(world_id="secret-batch", label="allowed-first"),
            options=_DurableOptions(target_tick=5),
        ),
        api.DeferredItem(
            operation=_LiveOperation(
                world_id="secret-batch",
                callback=lambda: None,
                credential="BATCH_SECRET",
            ),
            options=_DurableOptions(target_tick=5),
        ),
    )

    with pytest.raises(PermissionError, match="add_processor"):
        await dispatcher.defer_batch_as(actor, items)

    assert events == ["resolve", "resolve", "preauthorize", "preauthorize"]
    assert policy.preauthorization_calls == [
        {
            "actor": actor,
            "permission": "spawn",
        },
        {
            "actor": actor,
            "permission": "add_processor",
        },
    ]
    assert policy.calls == []
    assert policy.application_calls == []
    assert policy.batch_calls == []
    assert scheduler.admissions == []
    assert scheduler.batch_admissions == []
    assert scheduler.spawn_admissions == []
    assert access.rows == []


@pytest.mark.asyncio
async def test_empty_actor_aware_batch_rejects_before_registry_policy_or_effects() -> None:
    api = _commands_api()
    events: list[str] = []
    registry = _Registry((), events)
    policy = _PolicyPort(events)
    scheduler = _SchedulerPort(events)
    access = _AccessSink(events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=lambda _world_id: pytest.fail(
            "empty deferred batch must not resolve a target tick"
        ),
    )

    with pytest.raises(ValueError, match=r"(?i)empty|at least one|must not be empty"):
        await dispatcher.defer_batch_as(_Actor(roles=frozenset({"admin"})), ())

    assert events == []
    assert registry.resolved == []
    assert policy.preauthorization_calls == []
    assert policy.calls == []
    assert policy.application_calls == []
    assert policy.batch_calls == []
    assert scheduler.admissions == []
    assert scheduler.batch_admissions == []
    assert scheduler.spawn_admissions == []
    assert access.rows == []


@pytest.mark.asyncio
async def test_reserved_spawn_authorizes_before_scheduler_and_dispatcher_never_reserves() -> None:
    api = _commands_api()
    assert "reserve_entity_ids" not in signature(api.CommandDispatcher).parameters
    events: list[str] = []

    async def handler(_operation: Spawn) -> None:
        raise AssertionError("reserved deferred spawn must not invoke the direct handler")

    operation = Spawn(world_id="world-spawn", components=())
    registry = _Registry(
        (
            _Spec(
                name="spawn",
                model=Spawn,
                handler=handler,
                permission="spawn",
                summarize=lambda item: {"world_id": str(item.world_id)},
                durable=object(),
            ),
        ),
        events,
    )
    policy = _PolicyPort(events, denial=PermissionError("reserved spawn denied"))
    scheduler = _SchedulerPort(events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        access=_AccessSink(events),
        target_tick_for_world=lambda _world_id: 99,
    )
    actor = _Actor()
    options = _DurableOptions(target_tick=9, priority=-10)
    command_id = uuid7()

    with pytest.raises(PermissionError, match="reserved spawn denied"):
        await dispatcher.defer_spawn_as(
            actor,
            operation,
            options,
            command_id=command_id,
            version=4,
        )
    assert scheduler.spawn_admissions == []

    policy.denial = None
    entity_id, admitted = await dispatcher.defer_spawn_as(
        actor,
        operation,
        options,
        command_id=command_id,
        version=4,
    )
    assert entity_id == 41
    assert str(admitted) == str(command_id)
    assert scheduler.spawn_admissions == [
        {
            "operation": operation,
            "options": options,
            "command_id": command_id,
            "principal_id": actor.id,
            "origin": "gateway",
            "version": 4,
        }
    ]
    assert events.index("policy") < events.index("scheduler_spawn")


@pytest.mark.asyncio
async def test_access_sink_failures_are_advisory_to_result_denial_and_handler_failure() -> None:
    api = _commands_api()
    actor = _Actor()
    operation = _Operation(world_id="world-evidence", label="allowed")

    async def allowed_handler(item: _Operation) -> str:
        return item.label

    allowed_spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=allowed_handler,
        permission="spawn",
        summarize=lambda item: {"label": item.label},
    )

    def build(
        *,
        spec: _Spec,
        policy_error: BaseException | None = None,
    ) -> tuple[Any, _AccessSink]:
        events: list[str] = []
        sink = _AccessSink(events, failure=RuntimeError("projection unavailable"))
        return (
            _dispatcher(
                api,
                registry=_Registry((spec,), events),
                policy=_PolicyPort(events, denial=policy_error),
                scheduler=_SchedulerPort(events),
                access=sink,
                target_tick_for_world=lambda _world_id: 1,
            ),
            sink,
        )

    allowed, allowed_sink = build(spec=allowed_spec)
    assert await allowed.apply_as(actor, operation) == "allowed"
    assert len(allowed_sink.rows) == 1

    denied, denied_sink = build(
        spec=allowed_spec,
        policy_error=PermissionError("original denial"),
    )
    with pytest.raises(PermissionError, match="original denial"):
        await denied.apply_as(actor, operation)
    assert len(denied_sink.rows) == 1

    async def failed_handler(_item: _Operation) -> None:
        raise LookupError("original handler failure")

    failed_spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=failed_handler,
        permission="spawn",
        summarize=lambda item: {"label": item.label},
    )
    failed, failed_sink = build(spec=failed_spec)
    with pytest.raises(LookupError, match="original handler failure"):
        await failed.apply_as(actor, operation)
    assert len(failed_sink.rows) == 1


class _FatalSignal(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", [asyncio.CancelledError, _FatalSignal])
async def test_process_fatal_dispatch_failures_are_never_converted(
    failure_type: type[BaseException],
) -> None:
    api = _commands_api()
    events: list[str] = []

    async def handler(_operation: _Operation) -> None:
        raise failure_type()

    access = _AccessSink(events)
    dispatcher = _dispatcher(
        api,
        registry=_Registry(
            (
                _Spec(
                    name="synthetic",
                    model=_Operation,
                    handler=handler,
                    permission="spawn",
                    summarize=lambda item: {"label": item.label},
                ),
            ),
            events,
        ),
        policy=_PolicyPort(events),
        scheduler=_SchedulerPort(events),
        access=access,
        target_tick_for_world=lambda _world_id: 1,
    )

    with pytest.raises(failure_type):
        await dispatcher.apply_as(
            _Actor(),
            _Operation(world_id="world-fatal"),
        )
    assert access.rows == []


_VIEWER_PERMISSIONS = frozenset(
    {
        "get_world_info",
        "list_worlds",
        "discover_worlds",
        "open_world_readonly",
        "query_components",
        "query_archetype",
        "list_signatures",
        "get_audit_history",
        "list_processors",
        "list_hooks",
        "list_resources",
        "query_artifacts",
    }
)
_PLAYER_PERMISSIONS = _VIEWER_PERMISSIONS | {
    "spawn",
    "create_entities",
    "despawn",
    "update",
}
_OPERATOR_PERMISSIONS = _PLAYER_PERMISSIONS | {
    "add_components",
    "remove_components",
    "add_processor",
    "remove_processor",
    "fork_world",
    "destroy_world",
    "step",
    "run",
    "run_episode",
    "run_rollout",
    "add_resource",
    "add_hook",
    "remove_hook",
    "autoresearch",
    "ingest_artifacts",
    "evaluate",
}
_ADMIN_PERMISSIONS = _OPERATOR_PERMISSIONS | {
    "create_world",
    "resume_world",
}
_EXPECTED_PERMISSIONS_BY_ROLE = {
    "viewer": _VIEWER_PERMISSIONS,
    "player": _PLAYER_PERMISSIONS,
    "operator": _OPERATOR_PERMISSIONS,
    "admin": _ADMIN_PERMISSIONS,
}


def test_policy_role_matrix_is_complete_for_world_audit_and_pr3_bridge_permissions() -> None:
    api = _commands_api()
    all_permissions = frozenset().union(*_EXPECTED_PERMISSIONS_BY_ROLE.values())

    for role, allowed_permissions in _EXPECTED_PERMISSIONS_BY_ROLE.items():
        actor = _Actor(roles=frozenset({role}))
        for permission in sorted(all_permissions):
            policy = api.Policy(
                max_commands_per_tick=10,
                max_tokens_per_day=1_000_000,
            )
            if permission in allowed_permissions:
                policy.preauthorize(
                    actor,
                    permission=permission,
                )
            else:
                with pytest.raises(PermissionError, match=r"(?i)cannot|permission|denied"):
                    policy.preauthorize(
                        actor,
                        permission=permission,
                    )

    assert "add_components" not in _EXPECTED_PERMISSIONS_BY_ROLE["player"]
    assert "remove_components" not in _EXPECTED_PERMISSIONS_BY_ROLE["player"]
    assert "create_world" not in _EXPECTED_PERMISSIONS_BY_ROLE["operator"]
    assert "resume_world" not in _EXPECTED_PERMISSIONS_BY_ROLE["operator"]


def test_policy_preauthorization_is_pure_and_full_policy_is_the_sole_debit() -> None:
    api = _commands_api()
    actor = _Actor(roles=frozenset({"player"}))
    policy = api.Policy(
        max_commands_per_tick=1,
        max_tokens_per_day=1,
    )

    for _ in range(3):
        policy.preauthorize(actor, permission="spawn")

    policy.authorize(
        actor,
        permission="spawn",
        world_id="world-a",
        target_tick=1,
        token_cost=1,
    )
    policy.preauthorize(actor, permission="spawn")

    with pytest.raises(PermissionError, match=r"(?i)daily|token"):
        policy.authorize(
            actor,
            permission="spawn",
            world_id="world-b",
            target_tick=2,
            token_cost=1,
        )

    denied_actor = _Actor(roles=frozenset({"viewer"}))
    with pytest.raises(PermissionError, match=r"(?i)cannot|permission|denied"):
        policy.preauthorize(denied_actor, permission="spawn")


@dataclass(slots=True)
class _UtcClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def test_application_quota_has_no_tick_zero_bucket_and_daily_rollover_is_instance_owned() -> None:
    api = _commands_api()
    actor = _Actor(roles=frozenset({"admin"}))
    clock = _UtcClock(datetime(2026, 7, 23, 23, 59, tzinfo=UTC))
    policy = api.Policy(
        max_commands_per_tick=1,
        max_tokens_per_day=5,
        utcnow=clock,
    )

    policy.authorize_application(actor, permission="create_world", token_cost=0)
    policy.authorize_application(actor, permission="create_world", token_cost=0)
    policy.authorize_application(actor, permission="create_world", token_cost=4)
    with pytest.raises(PermissionError, match=r"(?i)daily|token"):
        policy.authorize_application(actor, permission="create_world", token_cost=2)

    independent = api.Policy(
        max_commands_per_tick=1,
        max_tokens_per_day=5,
        utcnow=clock,
    )
    independent.authorize_application(actor, permission="create_world", token_cost=4)

    clock.value += timedelta(minutes=2)
    policy.authorize_application(actor, permission="create_world", token_cost=2)


def test_policy_batch_rejection_is_atomic_and_uses_one_debit_generation() -> None:
    api = _commands_api()
    actor = _Actor()
    policy = api.Policy(
        max_commands_per_tick=1,
        max_tokens_per_day=1_000_000,
    )
    request = api.PolicyRequest(
        permission="spawn",
        world_id="world-batch-policy",
        target_tick=6,
        token_cost=0,
    )

    with pytest.raises(PermissionError, match=r"(?i)per-tick quota"):
        policy.authorize_batch(actor, requests=(request, request))

    policy.authorize_batch(actor, requests=(request,))
    with pytest.raises(PermissionError, match=r"(?i)per-tick quota"):
        policy.authorize_batch(actor, requests=(request,))


def test_policy_quota_generations_are_actor_world_tick_and_instance_owned() -> None:
    api = _commands_api()
    policy = api.Policy(
        max_commands_per_tick=1,
        max_tokens_per_day=1_000_000,
    )
    actor_a = _Actor()
    actor_b = _Actor()

    def authorize(
        target_policy: Any,
        actor: _Actor,
        world_id: str,
        target_tick: int,
    ) -> None:
        target_policy.authorize(
            actor,
            permission="spawn",
            world_id=world_id,
            target_tick=target_tick,
            token_cost=0,
        )

    authorize(policy, actor_a, "world-a", 7)
    with pytest.raises(PermissionError, match=r"(?i)per-tick quota"):
        authorize(policy, actor_a, "world-a", 7)

    authorize(policy, actor_b, "world-a", 7)
    authorize(policy, actor_a, "world-b", 7)
    authorize(policy, actor_a, "world-a", 8)

    independent = api.Policy(
        max_commands_per_tick=1,
        max_tokens_per_day=1_000_000,
    )
    authorize(independent, actor_a, "world-a", 7)

    assert "_tick_counters" not in vars(api.policy_module)
    assert "reset_tick_quotas" not in vars(api.policy_module)
    assert "reset_quotas" not in vars(api.policy_module)
    assert "set_quota_reset" not in vars(api.policy_module)


@pytest.mark.asyncio
async def test_stop_admission_rejects_inherited_child_and_drain_waits_for_active() -> None:
    api = _commands_api()
    events: list[str] = []
    outer_entered = asyncio.Event()
    create_child = asyncio.Event()
    release_outer = asyncio.Event()
    inner_called = asyncio.Event()
    child_created: asyncio.Future[asyncio.Task[Any]] = asyncio.get_running_loop().create_future()
    dispatcher: Any = None

    def unexpected_target_tick(_world_id: object) -> int:
        pytest.fail("trusted dispatch must not resolve an untrusted quota coordinate")

    async def handler(operation: _Operation) -> str:
        if operation.label == "outer":
            outer_entered.set()
            await create_child.wait()
            child = asyncio.create_task(
                dispatcher.apply(
                    _Operation(
                        world_id=operation.world_id,
                        label="inherited-child",
                    )
                )
            )
            child_created.set_result(child)
            await release_outer.wait()
            return "outer-complete"
        inner_called.set()
        return operation.label

    spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=handler,
        permission="spawn",
        summarize=lambda operation: {"label": operation.label},
    )
    registry = _Registry((spec,), events)
    dispatcher = _dispatcher(
        api,
        registry=registry,
        policy=_PolicyPort(events),
        scheduler=_SchedulerPort(events),
        access=_AccessSink(events),
        target_tick_for_world=unexpected_target_tick,
    )
    outer_task = asyncio.create_task(
        dispatcher.apply(
            _Operation(
                world_id="world-drain",
                label="outer",
            )
        )
    )
    child_task: asyncio.Task[Any] | None = None
    drain_task: asyncio.Task[Any] | None = None
    try:
        await asyncio.wait_for(outer_entered.wait(), timeout=0.5)

        await asyncio.wait_for(dispatcher.stop_admission(), timeout=0.5)
        drain_task = asyncio.create_task(dispatcher.wait_drained())
        await asyncio.sleep(0)
        assert not drain_task.done(), "drain must retain the active outer operation"

        create_child.set()
        child_task = await asyncio.wait_for(child_created, timeout=0.5)
        with pytest.raises(RuntimeError, match=r"(?i)admission|accept|shutting down"):
            await asyncio.wait_for(child_task, timeout=0.5)
        assert not inner_called.is_set(), (
            "a child task must not inherit admission authority from its parent"
        )
        assert registry.resolved == [
            _Operation(
                world_id="world-drain",
                label="outer",
            )
        ]

        with pytest.raises(RuntimeError, match=r"(?i)admission|accept|shutting down"):
            await dispatcher.apply(
                _Operation(
                    world_id="world-drain",
                    label="new-work",
                )
            )

        release_outer.set()
        assert await asyncio.wait_for(outer_task, timeout=0.5) == "outer-complete"
        assert drain_task is not None
        await asyncio.wait_for(drain_task, timeout=0.5)
    finally:
        release_outer.set()
        tasks = [
            task
            for task in (child_task, drain_task, outer_task)
            if task is not None and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
