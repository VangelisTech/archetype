# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for governed command dispatch and instance-owned policy."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, is_dataclass
from importlib import import_module
from typing import Any, ClassVar, Literal, NamedTuple

import pytest
from pydantic import BaseModel, ConfigDict, Field
from uuid_utils import UUID, uuid7

pytestmark = pytest.mark.contract("gateway.authorization.rbac")


class _CommandsApi(NamedTuple):
    CommandDispatcher: type[Any]
    Policy: type[Any]
    policy_module: Any


def _commands_api() -> _CommandsApi:
    """Load the intentionally absent pre-PR-3 family after collection."""
    dispatch_module = import_module("archetype.commands.dispatch")
    policy_module = import_module("archetype.commands.policy")
    return _CommandsApi(
        CommandDispatcher=dispatch_module.CommandDispatcher,
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


class _DurableOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    target_tick: int = Field(ge=0)
    priority: int = 0


@dataclass(frozen=True, slots=True)
class _Spec:
    name: str
    model: type[BaseModel]
    handler: Callable[[Any], Any]
    permission: str
    summarize: Callable[[Any], Mapping[str, Any]]
    durable: object | None = None
    trusted: bool = True
    untrusted: bool = True
    token_cost: int = 0


class _Registry:
    def __init__(self, specs: tuple[_Spec, ...], events: list[str]) -> None:
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
    ) -> None:
        self.events = events
        self.denial = denial
        self.calls: list[dict[str, Any]] = []

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


class _SchedulerPort:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.admissions: list[dict[str, Any]] = []

    async def admit(
        self,
        operation: BaseModel,
        options: BaseModel,
        *,
        principal_id: object | None = None,
        origin: str = "local",
    ) -> str:
        self.events.append("scheduler")
        self.admissions.append(
            {
                "operation": operation,
                "options": options,
                "principal_id": principal_id,
                "origin": origin,
            }
        )
        return "command-1"


class _AccessSink:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.rows: list[Any] = []

    async def __call__(self, evidence: Any) -> None:
        self.events.append("evidence")
        self.rows.append(evidence)


class _TargetTickResolver:
    def __init__(self, ticks: Mapping[str, int]) -> None:
        self.ticks = dict(ticks)
        self.calls: list[str] = []

    def __call__(self, world_id: object) -> int:
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

    spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=handler,
        permission="spawn",
        summarize=summarize,
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
    assert policy.calls == []
    assert access.rows == []
    assert target_ticks.calls == []

    actor_aware = await dispatcher.apply_as(actor, operation)

    assert trusted == actor_aware == ("synthetic", "same-command")
    assert handled == [operation, operation]
    assert registry.resolved == [operation, operation]
    assert target_ticks.calls == ["world-parity"]
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
async def test_actor_aware_order_is_resolve_policy_handler_bounded_evidence() -> None:
    api = _commands_api()
    events: list[str] = []

    async def handler(operation: _Operation) -> str:
        events.append("handler")
        return operation.label

    def summarize(operation: _Operation) -> dict[str, str]:
        events.append("summarize")
        return {"label": operation.label}

    spec = _Spec(
        name="synthetic",
        model=_Operation,
        handler=handler,
        permission="spawn",
        summarize=summarize,
        token_cost=3,
    )
    registry = _Registry((spec,), events)
    policy = _PolicyPort(events)
    access = _AccessSink(events)
    target_ticks = _TargetTickResolver({"world-order": 19})
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

    assert events == ["resolve", "policy", "handler", "summarize", "evidence"]
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

    assert policy.calls == [
        {
            "actor": actor,
            "permission": "add_processor",
            "world_id": "world-live",
            "target_tick": 29,
            "token_cost": 0,
        }
    ]
    assert scheduler.admissions == []
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
