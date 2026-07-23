# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for the exact command-operation registry."""

from __future__ import annotations

import json
from functools import partial
from importlib import import_module
from inspect import signature
from typing import Any, ClassVar, Literal, NamedTuple

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


def _spec(
    api: _RegistryApi,
    *,
    name: str,
    model: type[BaseModel],
    handler: Any,
    summarize: Any,
    durable: Any = None,
) -> Any:
    return api.OperationSpec(
        name=name,
        model=model,
        handler=handler,
        permission=name,
        summarize=summarize,
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


_EXPECTED_WORLD_OPERATIONS = {
    "spawn": "archetype.world.models.Spawn",
    "create_entities": "archetype.world.models.CreateEntities",
    "reserve_entity_ids": "archetype.world.models.ReserveEntityIds",
    "spawn_reserved": "archetype.world.models.SpawnReserved",
    "despawn": "archetype.world.models.Despawn",
    "update": "archetype.world.models.Update",
    "add_components": "archetype.world.models.AddComponents",
    "remove_components": "archetype.world.models.RemoveComponents",
    "add_processor": "archetype.world.models.AddProcessor",
    "remove_processor": "archetype.world.models.RemoveProcessor",
    "create_world": "archetype.world.models.CreateWorld",
    "fork_world": "archetype.world.models.ForkWorld",
    "destroy_world": "archetype.world.models.DestroyWorld",
    "get_world_info": "archetype.world.models.GetWorldInfo",
    "list_worlds": "archetype.world.models.ListWorlds",
    "discover_worlds": "archetype.world.models.DiscoverWorlds",
    "open_world_readonly": "archetype.world.models.OpenWorldReadonly",
    "resume_world": "archetype.world.models.ResumeWorld",
    "step": "archetype.world.models.Step",
    "run": "archetype.world.models.Run",
    "run_episode": "archetype.world.models.RunEpisode",
    "run_rollout": "archetype.world.models.RunRollout",
    "query_components": "archetype.world.models.QueryComponents",
    "query_archetype": "archetype.world.models.QueryArchetype",
    "list_signatures": "archetype.world.models.ListSignatures",
    "add_resource": "archetype.world.models.AddResource",
    "add_hook": "archetype.world.models.AddHook",
    "remove_hook": "archetype.world.models.RemoveHook",
    "list_processors": "archetype.world.models.ListProcessors",
    "list_hooks": "archetype.world.models.ListHooks",
    "list_resources": "archetype.world.models.ListResources",
}


def test_world_operation_registration_inventory_is_exact_and_complete() -> None:
    api = _registry_api()
    registry = api.OperationRegistry()

    def summarize(operation: Any) -> dict[str, str]:
        return {"operation": str(operation.operation)}

    for model in WORLD_OPERATION_TYPES:
        operation_name = str(model.model_fields["operation"].default)
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
                durable=durable,
            )
        )

    actual = {
        spec.name: f"{spec.model.__module__}.{spec.model.__qualname__}" for spec in registry.specs
    }
    missing = sorted(_EXPECTED_WORLD_OPERATIONS.keys() - actual.keys())
    extra = sorted(actual.keys() - _EXPECTED_WORLD_OPERATIONS.keys())
    mismatched = {
        name: {
            "expected": _EXPECTED_WORLD_OPERATIONS[name],
            "actual": actual[name],
        }
        for name in sorted(_EXPECTED_WORLD_OPERATIONS.keys() & actual.keys())
        if actual[name] != _EXPECTED_WORLD_OPERATIONS[name]
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
        assert isinstance(spec.handler, partial)
        assert spec.handler.func is WORLD_OPERATION_HANDLERS[spec.model]
        assert tuple(signature(spec.handler).parameters) == ("operation",)

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
