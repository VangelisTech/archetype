# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Cross-lane red contracts for command registration and family parity."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, is_dataclass
from functools import partial
from importlib import import_module
from typing import Any, ClassVar, Literal, NamedTuple, cast

import pytest
from pydantic import BaseModel, ConfigDict, Field
from uuid_utils import UUID, uuid7

from archetype.world import handlers as world_handlers
from archetype.world import mutation as world_mutation
from archetype.world.handlers import WORLD_OPERATION_HANDLERS, materialize_locked
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    WORLD_OPERATION_TYPES,
    ListWorlds,
    SpawnReserved,
)

pytestmark = [
    pytest.mark.contract("gateway.authorization.rbac"),
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
]

_APPLICATION_SCOPED_WORLD_OPERATIONS = {
    "create_world",
    "list_worlds",
    "discover_worlds",
    "list_signatures",
}
_DURABLE_WORLD_SCOPED_OPERATIONS = {
    "destroy_world",
    "open_world_readonly",
    "resume_world",
    "query_components",
    "query_archetype",
    "list_world_signatures",
}

_WORLD_PERMISSION_OVERRIDES = {
    "reserve_entity_ids": "spawn",
    "spawn_reserved": "spawn",
    "list_world_signatures": "list_signatures",
}
_INTERNAL_WORLD_OPERATIONS = {
    "reserve_entity_ids",
    "spawn_reserved",
}
_WORLD_TOKEN_COSTS = {
    "advance_world_to_tick": 50,
    "spawn": 10,
    "create_entities": 10,
    "reserve_entity_ids": 10,
    "spawn_reserved": 10,
    "despawn": 5,
    "update": 8,
    "add_components": 8,
    "remove_components": 5,
    "add_processor": 15,
    "remove_processor": 5,
    "create_world": 50,
    "fork_world": 100,
    "destroy_world": 10,
    "get_world_info": 2,
    "list_worlds": 2,
    "discover_worlds": 2,
    "open_world_readonly": 2,
    "resume_world": 50,
    "step": 10,
    "run": 50,
    "run_episode": 500,
    "run_rollout": 200,
    "query_components": 5,
    "query_archetype": 5,
    "list_signatures": 2,
    "list_world_signatures": 2,
    "add_resource": 10,
    "add_hook": 10,
    "remove_hook": 5,
    "list_processors": 2,
    "list_hooks": 2,
    "list_resources": 2,
}


def _expected_world_quota_scope(operation_name: str) -> str:
    if operation_name in _APPLICATION_SCOPED_WORLD_OPERATIONS:
        return "application"
    if operation_name in _DURABLE_WORLD_SCOPED_OPERATIONS:
        return "durable_world"
    return "live_world"


class _CommandsApi(NamedTuple):
    CommandDispatcher: type[Any]
    DurableOperation: type[Any]
    DurableOptions: type[Any]
    GetAuditHistory: type[BaseModel]
    OperationRegistry: type[Any]
    OperationSpec: type[Any]


def _commands_api() -> _CommandsApi:
    """Load the intentionally absent pre-PR-3 family after collection."""
    dispatch_module = import_module("archetype.commands.dispatch")
    models_module = import_module("archetype.commands.models")
    registry_module = import_module("archetype.commands.registry")
    return _CommandsApi(
        CommandDispatcher=dispatch_module.CommandDispatcher,
        DurableOperation=registry_module.DurableOperation,
        DurableOptions=models_module.DurableOptions,
        GetAuditHistory=models_module.GetAuditHistory,
        OperationRegistry=registry_module.OperationRegistry,
        OperationSpec=registry_module.OperationSpec,
    )


class _Actor(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    id: UUID = Field(default_factory=uuid7)
    roles: frozenset[str] = frozenset({"admin"})


class _Policy:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def preauthorize(
        self,
        actor: _Actor,
        *,
        permission: str,
    ) -> None:
        self.calls.append(
            {
                "actor": actor,
                "permission": permission,
                "phase": "preauthorize",
            }
        )

    def authorize(
        self,
        actor: _Actor,
        *,
        permission: str,
        world_id: object,
        target_tick: int,
        token_cost: int = 0,
    ) -> None:
        self.calls.append(
            {
                "actor": actor,
                "permission": permission,
                "world_id": str(world_id),
                "target_tick": target_tick,
                "token_cost": token_cost,
            }
        )

    def authorize_application(
        self,
        actor: _Actor,
        *,
        permission: str,
        token_cost: int = 0,
    ) -> None:
        self.calls.append(
            {
                "actor": actor,
                "permission": permission,
                "token_cost": token_cost,
            }
        )


class _Scheduler:
    def __init__(self) -> None:
        self.admissions: list[dict[str, Any]] = []

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
        return str(command_id or f"command-{len(self.admissions)}")


class _AccessSink:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def __call__(self, evidence: Any) -> None:
        self.rows.append(evidence)


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
    registry: Any,
    scheduler: _Scheduler,
    access: _AccessSink,
    target_tick_for_world: Callable[[object], int],
) -> Any:
    return api.CommandDispatcher(
        registry=registry,
        policy=_Policy(),
        scheduler=scheduler,
        record_access=access,
        target_tick_for_world=target_tick_for_world,
    )


@dataclass(slots=True)
class _World:
    world_id: str


class _WorldRegistry:
    def __init__(self, world: _World) -> None:
        self.world = world
        self.operation_calls: list[str] = []

    @asynccontextmanager
    async def operation(self, world_id: object) -> AsyncIterator[_World]:
        normalized = str(world_id)
        self.operation_calls.append(normalized)
        assert normalized == self.world.world_id
        yield self.world


@pytest.mark.asyncio
async def test_reserved_spawn_direct_and_deferred_share_locked_family_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two entry modes preserve the reserved ID through one world mutation."""
    api = _commands_api()
    world = _World(world_id="world-parity")
    world_registry = _WorldRegistry(world)
    calls: list[tuple[_World, int, list[Any]]] = []

    async def shared_locked_behavior(
        actual_world: _World,
        entity_id: int,
        components: list[Any],
    ) -> None:
        calls.append((actual_world, entity_id, components))

    monkeypatch.setattr(
        world_mutation,
        "_spawn_with_reserved_id_locked",
        shared_locked_behavior,
    )
    registry = api.OperationRegistry()
    spec = api.OperationSpec(
        name="spawn_reserved",
        model=SpawnReserved,
        handler=partial(
            world_handlers.spawn_reserved,
            cast("Any", world_registry),
        ),
        permission="spawn",
        summarize=lambda operation: {
            "operation": operation.operation,
            "world_id": str(operation.world_id),
        },
        quota_scope="live_world",
        world_key=lambda operation: operation.world_id,
        durable=api.DurableOperation(
            decode=SpawnReserved.model_validate_json,
            materialize=materialize_locked,
        ),
    )
    registry.register(spec)
    scheduler = _Scheduler()
    access = _AccessSink()

    def unexpected_live_tick(_world_id: object) -> int:
        pytest.fail("trusted direct/deferred parity must not fabricate policy coordinates")

    dispatcher = _dispatcher(
        api,
        registry=registry,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=unexpected_live_tick,
    )
    operation = SpawnReserved(
        world_id=world.world_id,
        entity_id=41,
        components=(),
    )
    options = api.DurableOptions(
        target_tick=7,
        priority=-10,
        max_attempts=3,
    )

    await dispatcher.apply(operation)
    command_id = await dispatcher.defer(operation, options)

    assert str(command_id) == "command-1"
    assert scheduler.admissions == [
        {
            "operation": operation,
            "options": options,
            "command_id": None,
            "principal_id": None,
            "origin": "local",
            "version": 1,
        }
    ]
    decoded = spec.durable.decode(operation.model_dump_json())
    await spec.durable.materialize(world, decoded)

    assert calls == [
        (world, 41, []),
        (world, 41, []),
    ]
    assert world_registry.operation_calls == ["world-parity"]
    assert access.rows == []
    assert {"target_tick", "priority", "max_attempts"}.isdisjoint(SpawnReserved.model_fields)


class _MissionOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    operation: str
    world_id: str
    provider: object
    task_base_revision: str
    candidate_diff: str
    validator_output: str
    critic_findings: str
    cleanup_state: str


class _SubmitMission(_MissionOperation):
    operation: Literal["submit_mission"] = "submit_mission"


class _RunMission(_MissionOperation):
    operation: Literal["run_mission"] = "run_mission"


class _RestoreMissionSandbox(_MissionOperation):
    operation: Literal["restore_mission_sandbox"] = "restore_mission_sandbox"


class _AcceptMissionRun(_MissionOperation):
    operation: Literal["accept_mission_run"] = "accept_mission_run"


class _GetMissionRun(_MissionOperation):
    operation: Literal["get_mission_run"] = "get_mission_run"


class _CancelMissionRun(_MissionOperation):
    operation: Literal["cancel_mission_run"] = "cancel_mission_run"


class _GetMissionRunEvents(_MissionOperation):
    operation: Literal["get_mission_run_events"] = "get_mission_run_events"


class _ListMissionRuns(_MissionOperation):
    operation: Literal["list_mission_runs"] = "list_mission_runs"


@pytest.mark.asyncio
async def test_future_mission_operations_are_trusted_direct_only_and_reject_all_wire_paths() -> (
    None
):
    """Configured live capabilities never execute or enter durable metadata."""
    api = _commands_api()
    registry = api.OperationRegistry()
    provider_calls: list[str] = []
    mission_models = (
        _SubmitMission,
        _RunMission,
        _RestoreMissionSandbox,
        _AcceptMissionRun,
        _GetMissionRun,
        _CancelMissionRun,
        _GetMissionRunEvents,
        _ListMissionRuns,
    )

    async def live_handler(operation: _MissionOperation) -> None:
        provider_calls.append(operation.operation)

    def adversarial_summary(operation: _MissionOperation) -> dict[str, Any]:
        return {
            "world_id": operation.world_id,
            "provider": operation.provider,
            "task_base_revision": operation.task_base_revision,
            "candidate_diff": operation.candidate_diff,
            "validator_output": operation.validator_output,
            "critic_findings": operation.critic_findings,
            "cleanup_state": operation.cleanup_state,
        }

    for model in mission_models:
        name = str(model.model_fields["operation"].default)
        registry.register(
            api.OperationSpec(
                name=name,
                model=model,
                handler=live_handler,
                permission=name,
                summarize=adversarial_summary,
                quota_scope="live_world",
                world_key=lambda operation: operation.world_id,
                trusted=True,
                untrusted=False,
                durable=None,
            )
        )

    scheduler = _Scheduler()
    access = _AccessSink()
    resolved_ticks: list[str] = []

    def target_tick_for_world(world_id: object) -> int:
        resolved_ticks.append(str(world_id))
        return 13

    dispatcher = _dispatcher(
        api,
        registry=registry,
        scheduler=scheduler,
        access=access,
        target_tick_for_world=target_tick_for_world,
    )
    actor = _Actor()
    options = api.DurableOptions(
        target_tick=29,
        priority=0,
        max_attempts=3,
    )
    sentinel_values = (
        "PROVIDER_SENTINEL",
        "TASK_BASE_SENTINEL",
        "DIFF_SENTINEL",
        "VALIDATOR_SENTINEL",
        "CRITIC_SENTINEL",
        "CLEANUP_SENTINEL",
    )

    for model in mission_models:
        operation = model(
            world_id="mission-world",
            provider=sentinel_values[0],
            task_base_revision=sentinel_values[1],
            candidate_diff=sentinel_values[2],
            validator_output=sentinel_values[3],
            critic_findings=sentinel_values[4],
            cleanup_state=sentinel_values[5],
        )
        spec = registry.resolve(operation)
        assert spec.trusted is True
        assert spec.untrusted is False
        assert spec.durable is None

        with pytest.raises(PermissionError, match=r"(?i)trusted|untrusted|not available"):
            await dispatcher.apply_as(actor, operation)
        with pytest.raises(ValueError, match=r"(?i)direct-only"):
            await dispatcher.defer(operation, options)
        with pytest.raises(
            (PermissionError, ValueError),
            match=r"(?i)trusted|untrusted|not available|direct-only",
        ):
            await dispatcher.defer_as(actor, operation, options)

    assert provider_calls == []
    assert scheduler.admissions == []
    assert {"target_tick", "priority", "max_attempts"}.isdisjoint(_MissionOperation.model_fields)
    assert len(access.rows) == 16
    assert set(resolved_ticks) <= {"mission-world"}
    for evidence_value in access.rows:
        evidence = _evidence_dict(evidence_value)
        encoded = json.dumps(evidence, sort_keys=True, default=str)
        assert len(encoded) <= 4096
        for sentinel in sentinel_values:
            assert sentinel not in encoded
        for forbidden_key in (
            "provider",
            "task_base_revision",
            "candidate_diff",
            "validator_output",
            "critic_findings",
            "cleanup_state",
        ):
            assert forbidden_key not in encoded


@pytest.mark.asyncio
async def test_runtime_wiring_composes_the_exact_world_registry(tmp_path) -> None:
    """The explicit process root registers the real world surface exactly once."""
    api = _commands_api()
    from tests._runtime import build_test_runtime

    resources = build_test_runtime(tmp_path)
    try:
        dispatcher = resources.dispatcher
        registry = cast("Any", dispatcher)._registry
        assert await dispatcher.apply(ListWorlds()) == []

        world_specs = tuple(spec for spec in registry.specs if spec.model in WORLD_OPERATION_TYPES)
        actual_models = tuple(spec.model for spec in world_specs)
        assert len(world_specs) == 33
        assert len(actual_models) == len(set(actual_models))
        assert set(actual_models) == set(WORLD_OPERATION_TYPES)
        assert {spec.name for spec in world_specs} == {
            str(model.model_fields["operation"].default) for model in WORLD_OPERATION_TYPES
        }
        assert {spec.model for spec in world_specs if spec.durable is not None} == set(
            PORTABLE_TICK_OPERATION_TYPES
        )
        for spec in world_specs:
            assert isinstance(spec.handler, partial)
            assert spec.handler.func is WORLD_OPERATION_HANDLERS[spec.model]
            assert spec.quota_scope == _expected_world_quota_scope(spec.name)
            assert spec.permission == _WORLD_PERMISSION_OVERRIDES.get(spec.name, spec.name)
            assert spec.trusted is True
            assert spec.untrusted is (spec.name not in _INTERNAL_WORLD_OPERATIONS)
            assert spec.token_cost == _WORLD_TOKEN_COSTS[spec.name]
            if spec.quota_scope != "application":
                assert spec.world_key is not None
            else:
                assert spec.world_key is None

        audit_spec = registry.resolve_name("get_audit_history")
        assert audit_spec.model is api.GetAuditHistory
        assert audit_spec.permission == "get_audit_history"
        assert audit_spec.quota_scope == "durable_world"
        assert audit_spec.world_key is not None
        assert audit_spec.trusted is True
        assert audit_spec.untrusted is True
        assert audit_spec.token_cost == 5
        assert audit_spec.durable is None
    finally:
        await resources.aclose()
