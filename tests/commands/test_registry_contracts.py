# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for the exact command-operation registry."""

from __future__ import annotations

import json
from collections.abc import Callable
from functools import partial
from importlib import import_module
from inspect import signature
from typing import Any, ClassVar, Literal, NamedTuple, cast

import pytest
from pydantic import BaseModel, ConfigDict

from archetype.world.handlers import WORLD_OPERATION_HANDLERS, materialize_locked
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    WORLD_OPERATION_TYPES,
    AddProcessor,
)

pytestmark = pytest.mark.contract("commands.identity.idempotent")


class _RegistryApi(NamedTuple):
    OperationRegistry: type[Any]
    OperationSpec: type[Any]
    DurableOperation: type[Any]


def _registry_api() -> _RegistryApi:
    """Load the intentionally absent pre-PR-3 family after collection."""
    registry_module = import_module("archetype.commands.registry")
    return _RegistryApi(
        OperationRegistry=registry_module.OperationRegistry,
        OperationSpec=registry_module.OperationSpec,
        DurableOperation=registry_module.DurableOperation,
    )


class _FrozenOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, extra="forbid")


class _AlphaOperation(_FrozenOperation):
    operation: Literal["alpha"] = "alpha"
    world_id: str = "world-alpha"


class _BetaOperation(_FrozenOperation):
    operation: Literal["beta"] = "beta"
    world_id: str = "world-beta"


class _AlphaChildOperation(_AlphaOperation):
    operation: Literal["alpha_child"] = "alpha_child"


class _CanonicalOperation(_FrozenOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["canonical"] = "canonical"
    world_id: str
    payload: dict[str, Any]


async def _alpha_handler(operation: _AlphaOperation) -> tuple[str, str]:
    return operation.operation, operation.world_id


def _alpha_summary(operation: _AlphaOperation) -> dict[str, str]:
    return {
        "operation": operation.operation,
        "world_id": operation.world_id,
    }


async def _beta_handler(operation: _BetaOperation) -> tuple[str, str]:
    return operation.operation, operation.world_id


def _beta_summary(operation: _BetaOperation) -> dict[str, str]:
    return {
        "operation": operation.operation,
        "world_id": operation.world_id,
    }


async def _canonical_materializer(
    world: object,
    operation: _CanonicalOperation,
) -> tuple[object, _CanonicalOperation]:
    return world, operation


async def _canonical_handler(
    operation: _CanonicalOperation,
) -> tuple[str, str]:
    return operation.operation, operation.world_id


def _canonical_summary(operation: _CanonicalOperation) -> dict[str, str]:
    return {
        "operation": operation.operation,
        "world_id": operation.world_id,
    }


def _canonical_json(operation: BaseModel) -> str:
    return json.dumps(
        operation.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _bind_world_handler(handler: Any) -> partial[Any]:
    dependency_count = len(signature(handler).parameters) - 1
    return partial(handler, *(object() for _ in range(dependency_count)))


def _model_world_key(operation: BaseModel) -> object:
    return cast("Any", operation).world_id


def _spec(
    api: _RegistryApi,
    *,
    name: str,
    model: type[BaseModel],
    handler: Any,
    summarize: Any,
    permission: str | None = None,
    durable: Any = None,
    quota_scope: Literal["application", "live_world", "durable_world"] = "live_world",
    world_key: Callable[[BaseModel], object] | None = _model_world_key,
    trusted: bool = True,
    untrusted: bool = True,
    token_cost: int | Callable[[BaseModel], int] = 0,
) -> Any:
    return api.OperationSpec(
        name=name,
        model=model,
        handler=handler,
        permission=permission or name,
        summarize=summarize,
        quota_scope=quota_scope,
        world_key=world_key,
        trusted=trusted,
        untrusted=untrusted,
        token_cost=token_cost,
        durable=durable,
    )


def test_duplicate_operation_name_and_exact_model_fail_independently() -> None:
    api = _registry_api()
    registry = api.OperationRegistry()
    alpha = _spec(
        api,
        name="alpha",
        model=_AlphaOperation,
        handler=_alpha_handler,
        summarize=_alpha_summary,
    )
    registry.register(alpha)

    with pytest.raises(
        ValueError,
        match=r"(?i)operation name .*alpha.*already registered",
    ):
        registry.register(
            _spec(
                api,
                name="alpha",
                model=_BetaOperation,
                handler=_beta_handler,
                summarize=_beta_summary,
            )
        )

    with pytest.raises(
        ValueError,
        match=r"(?i)operation model .*_AlphaOperation.*already registered",
    ):
        registry.register(
            _spec(
                api,
                name="alpha_alias",
                model=_AlphaOperation,
                handler=_alpha_handler,
                summarize=_alpha_summary,
            )
        )

    assert registry.resolve(_AlphaOperation()) is alpha
    assert registry.resolve_name("alpha") is alpha


def test_exact_resolution_never_falls_back_through_model_mro() -> None:
    api = _registry_api()
    registry = api.OperationRegistry()
    base_spec = _spec(
        api,
        name="alpha",
        model=_AlphaOperation,
        handler=_alpha_handler,
        summarize=_alpha_summary,
    )
    registry.register(base_spec)

    child = _AlphaChildOperation()
    with pytest.raises(
        KeyError,
        match=r"(?i)_AlphaChildOperation.*not registered",
    ):
        registry.resolve(child)

    async def child_handler(operation: _AlphaChildOperation) -> str:
        return operation.operation

    def child_summary(operation: _AlphaChildOperation) -> dict[str, str]:
        return {"operation": operation.operation}

    child_spec = _spec(
        api,
        name="alpha_child",
        model=_AlphaChildOperation,
        handler=child_handler,
        summarize=child_summary,
    )
    registry.register(child_spec)

    assert registry.resolve(child) is child_spec
    assert registry.resolve(_AlphaOperation()) is base_spec


@pytest.mark.asyncio
async def test_handler_and_summarizer_receive_the_registered_model_directly() -> None:
    api = _registry_api()
    registry = api.OperationRegistry()
    spec = _spec(
        api,
        name="alpha",
        model=_AlphaOperation,
        handler=_alpha_handler,
        summarize=_alpha_summary,
    )
    registry.register(spec)
    operation = _AlphaOperation(world_id="world-7")

    resolved = registry.resolve(operation)

    assert resolved.model is _AlphaOperation
    assert resolved.handler is _alpha_handler
    assert resolved.summarize is _alpha_summary
    assert resolved.quota_scope == "live_world"
    assert resolved.world_key(operation) == "world-7"
    assert resolved.trusted is True
    assert resolved.untrusted is True
    assert await resolved.handler(operation) == ("alpha", "world-7")
    assert resolved.summarize(operation) == {
        "operation": "alpha",
        "world_id": "world-7",
    }


@pytest.mark.asyncio
async def test_explicit_durable_operation_round_trips_canonical_json() -> None:
    api = _registry_api()
    durable = api.DurableOperation(
        decode=_CanonicalOperation.model_validate_json,
        materialize=_canonical_materializer,
    )
    spec = _spec(
        api,
        name="canonical",
        model=_CanonicalOperation,
        handler=_canonical_handler,
        summarize=_canonical_summary,
        durable=durable,
    )
    registry = api.OperationRegistry()
    registry.register(spec)
    operation = _CanonicalOperation(
        world_id="world-canonical",
        payload={
            "zeta": 0,
            "alpha": [3, {"z": 2, "a": 1}],
        },
    )

    payload_json = _canonical_json(operation)
    decoded = registry.resolve(operation).durable.decode(payload_json)

    assert payload_json == (
        '{"operation":"canonical","payload":'
        '{"alpha":[3,{"a":1,"z":2}],"zeta":0},'
        '"world_id":"world-canonical"}'
    )
    assert type(decoded) is _CanonicalOperation
    assert decoded == operation
    assert _canonical_json(decoded) == payload_json

    world = object()
    assert await durable.materialize(world, decoded) == (world, operation)


def test_direct_only_registration_exposes_rejection_metadata_without_payload() -> None:
    api = _registry_api()
    registry = api.OperationRegistry()
    secret_capability = object()
    operation = AddProcessor(
        world_id="world-direct",
        processor=secret_capability,
    )

    async def handler(command: AddProcessor) -> None:
        assert command is operation

    def summarize(command: AddProcessor) -> dict[str, str]:
        return {
            "operation": command.operation,
            "world_id": str(command.world_id),
        }

    registry.register(
        _spec(
            api,
            name="add_processor",
            model=AddProcessor,
            handler=handler,
            summarize=summarize,
        )
    )

    resolved = registry.resolve(operation)
    rejection_metadata = {
        "operation": resolved.name,
        "model": f"{resolved.model.__module__}.{resolved.model.__qualname__}",
        "reason": "direct_only",
    }

    assert type(operation).direct_only is True
    assert resolved.durable is None
    assert rejection_metadata == {
        "operation": "add_processor",
        "model": "archetype.world.models.AddProcessor",
        "reason": "direct_only",
    }
    assert repr(secret_capability) not in json.dumps(rejection_metadata)


class _ExpectedWorldRegistration(NamedTuple):
    model: str
    permission: str
    quota_scope: Literal["application", "live_world", "durable_world"]
    world_key_field: Literal["world_id", "source_world_id"] | None
    trusted: bool
    untrusted: bool
    token_cost: int
    durable: bool


def _world_registration(
    model: str,
    *,
    permission: str,
    quota_scope: Literal["application", "live_world", "durable_world"],
    world_key_field: Literal["world_id", "source_world_id"] | None,
    token_cost: int,
    durable: bool = False,
    untrusted: bool = True,
) -> _ExpectedWorldRegistration:
    return _ExpectedWorldRegistration(
        model=f"archetype.world.models.{model}",
        permission=permission,
        quota_scope=quota_scope,
        world_key_field=world_key_field,
        trusted=True,
        untrusted=untrusted,
        token_cost=token_cost,
        durable=durable,
    )


_EXPECTED_WORLD_REGISTRATIONS = {
    "spawn": _world_registration(
        "Spawn",
        permission="spawn",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
        durable=True,
    ),
    "create_entities": _world_registration(
        "CreateEntities",
        permission="create_entities",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
    ),
    "reserve_entity_ids": _world_registration(
        "ReserveEntityIds",
        permission="spawn",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
        untrusted=False,
    ),
    "spawn_reserved": _world_registration(
        "SpawnReserved",
        permission="spawn",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
        durable=True,
        untrusted=False,
    ),
    "despawn": _world_registration(
        "Despawn",
        permission="despawn",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=5,
        durable=True,
    ),
    "update": _world_registration(
        "Update",
        permission="update",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=8,
        durable=True,
    ),
    "add_components": _world_registration(
        "AddComponents",
        permission="add_components",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=8,
        durable=True,
    ),
    "remove_components": _world_registration(
        "RemoveComponents",
        permission="remove_components",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=5,
        durable=True,
    ),
    "add_processor": _world_registration(
        "AddProcessor",
        permission="add_processor",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=15,
    ),
    "remove_processor": _world_registration(
        "RemoveProcessor",
        permission="remove_processor",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=5,
    ),
    "create_world": _world_registration(
        "CreateWorld",
        permission="create_world",
        quota_scope="application",
        world_key_field=None,
        token_cost=50,
    ),
    "fork_world": _world_registration(
        "ForkWorld",
        permission="fork_world",
        quota_scope="live_world",
        world_key_field="source_world_id",
        token_cost=100,
    ),
    "destroy_world": _world_registration(
        "DestroyWorld",
        permission="destroy_world",
        quota_scope="durable_world",
        world_key_field="world_id",
        token_cost=10,
    ),
    "get_world_info": _world_registration(
        "GetWorldInfo",
        permission="get_world_info",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=2,
    ),
    "list_worlds": _world_registration(
        "ListWorlds",
        permission="list_worlds",
        quota_scope="application",
        world_key_field=None,
        token_cost=2,
    ),
    "discover_worlds": _world_registration(
        "DiscoverWorlds",
        permission="discover_worlds",
        quota_scope="application",
        world_key_field=None,
        token_cost=2,
    ),
    "open_world_readonly": _world_registration(
        "OpenWorldReadonly",
        permission="open_world_readonly",
        quota_scope="durable_world",
        world_key_field="world_id",
        token_cost=2,
    ),
    "resume_world": _world_registration(
        "ResumeWorld",
        permission="resume_world",
        quota_scope="durable_world",
        world_key_field="world_id",
        token_cost=50,
    ),
    "step": _world_registration(
        "Step",
        permission="step",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
    ),
    "run": _world_registration(
        "Run",
        permission="run",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=50,
    ),
    "advance_world_to_tick": _world_registration(
        "AdvanceWorldToTick",
        permission="advance_world_to_tick",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=50,
    ),
    "run_episode": _world_registration(
        "RunEpisode",
        permission="run_episode",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=500,
    ),
    "run_rollout": _world_registration(
        "RunRollout",
        permission="run_rollout",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=200,
    ),
    "query_components": _world_registration(
        "QueryComponents",
        permission="query_components",
        quota_scope="durable_world",
        world_key_field="world_id",
        token_cost=5,
    ),
    "query_archetype": _world_registration(
        "QueryArchetype",
        permission="query_archetype",
        quota_scope="durable_world",
        world_key_field="world_id",
        token_cost=5,
    ),
    "list_signatures": _world_registration(
        "ListSignatures",
        permission="list_signatures",
        quota_scope="application",
        world_key_field=None,
        token_cost=2,
    ),
    "list_world_signatures": _world_registration(
        "ListWorldSignatures",
        permission="list_signatures",
        quota_scope="durable_world",
        world_key_field="world_id",
        token_cost=2,
    ),
    "add_resource": _world_registration(
        "AddResource",
        permission="add_resource",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
    ),
    "add_hook": _world_registration(
        "AddHook",
        permission="add_hook",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=10,
    ),
    "remove_hook": _world_registration(
        "RemoveHook",
        permission="remove_hook",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=5,
    ),
    "list_processors": _world_registration(
        "ListProcessors",
        permission="list_processors",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=2,
    ),
    "list_hooks": _world_registration(
        "ListHooks",
        permission="list_hooks",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=2,
    ),
    "list_resources": _world_registration(
        "ListResources",
        permission="list_resources",
        quota_scope="live_world",
        world_key_field="world_id",
        token_cost=2,
    ),
}


def _registration_world_key(
    field: Literal["world_id", "source_world_id"],
) -> Callable[[BaseModel], object]:
    return lambda operation: getattr(operation, field)


def test_world_operation_registration_inventory_is_exact_and_complete() -> None:
    api = _registry_api()
    registry = api.OperationRegistry()

    def summarize(operation: Any) -> dict[str, str]:
        return {"operation": str(operation.operation)}

    for model in WORLD_OPERATION_TYPES:
        operation_name = str(model.model_fields["operation"].default)
        expected = _EXPECTED_WORLD_REGISTRATIONS[operation_name]
        durable = None
        if model in PORTABLE_TICK_OPERATION_TYPES:
            durable = api.DurableOperation(
                decode=model.model_validate_json,
                materialize=materialize_locked,
            )
        registry.register(
            _spec(
                api,
                name=operation_name,
                model=model,
                handler=_bind_world_handler(WORLD_OPERATION_HANDLERS[model]),
                summarize=summarize,
                permission=expected.permission,
                durable=durable,
                quota_scope=expected.quota_scope,
                world_key=(
                    None
                    if expected.world_key_field is None
                    else _registration_world_key(expected.world_key_field)
                ),
                trusted=expected.trusted,
                untrusted=expected.untrusted,
                token_cost=expected.token_cost,
            )
        )

    actual = {
        spec.name: f"{spec.model.__module__}.{spec.model.__qualname__}" for spec in registry.specs
    }
    missing = sorted(_EXPECTED_WORLD_REGISTRATIONS.keys() - actual.keys())
    extra = sorted(actual.keys() - _EXPECTED_WORLD_REGISTRATIONS.keys())
    mismatched = {
        name: {
            "expected": _EXPECTED_WORLD_REGISTRATIONS[name].model,
            "actual": actual[name],
        }
        for name in sorted(_EXPECTED_WORLD_REGISTRATIONS.keys() & actual.keys())
        if actual[name] != _EXPECTED_WORLD_REGISTRATIONS[name].model
    }
    assert not (missing or extra or mismatched), (
        "world operation registry mismatch: "
        f"missing={missing}, extra={extra}, mismatched={mismatched}"
    )

    model_types = frozenset(WORLD_OPERATION_TYPES)
    handler_types = frozenset(WORLD_OPERATION_HANDLERS)
    missing_handlers = sorted(model.__name__ for model in model_types - handler_types)
    extra_handlers = sorted(model.__name__ for model in handler_types - model_types)
    assert not (missing_handlers or extra_handlers), (
        f"world handler inventory mismatch: missing={missing_handlers}, extra={extra_handlers}"
    )
    for spec in registry.specs:
        expected = _EXPECTED_WORLD_REGISTRATIONS[spec.name]
        assert isinstance(spec.handler, partial)
        assert spec.handler.func is WORLD_OPERATION_HANDLERS[spec.model]
        assert tuple(signature(spec.handler).parameters) == ("operation",)
        assert spec.permission == expected.permission
        assert spec.quota_scope == expected.quota_scope
        assert spec.trusted is expected.trusted
        assert spec.untrusted is expected.untrusted
        assert spec.token_cost == expected.token_cost
        assert (spec.world_key is None) == (expected.world_key_field is None)
        assert (spec.durable is not None) is expected.durable

    assert len(registry.specs) == 33

    durable_models = frozenset(spec.model for spec in registry.specs if spec.durable is not None)
    missing_durable = sorted(
        model.__name__ for model in PORTABLE_TICK_OPERATION_TYPES - durable_models
    )
    extra_durable = sorted(
        model.__name__ for model in durable_models - PORTABLE_TICK_OPERATION_TYPES
    )
    assert not (missing_durable or extra_durable), (
        f"world durable registration mismatch: missing={missing_durable}, extra={extra_durable}"
    )

    assert all(
        registry.resolve_name(name).model is model
        for model in WORLD_OPERATION_TYPES
        if (name := str(model.model_fields["operation"].default))
    )
