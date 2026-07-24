# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contracts for the PR-4 actor-aware FastAPI dispatcher boundary.

The pulled-forward family modules are imported only inside helpers invoked by
individual tests.  This keeps all eight packet nodes collectable on the PR-3
base while still producing explicit missing-seam failures before the A/B
interface seed lands.
"""

from __future__ import annotations

import ast
import inspect
from collections.abc import Awaitable, Callable, Mapping
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import BaseModel
from uuid_utils import uuid7

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx
from archetype.commands.policy import Policy
from archetype.commands.registry import OperationRegistry, OperationSpec
from archetype.core.config import RunConfig
from archetype.world.models import Step

_ACTOR_MODEL_BOUNDARIES = (
    ("archetype.research.models", "AutoResearch", "autoresearch"),
    ("archetype.artifacts.models", "IngestArtifacts", "ingest_artifacts"),
    ("archetype.artifacts.models", "QueryArtifacts", "query_artifacts"),
    ("archetype.evaluation.models", "Evaluate", "evaluate"),
)
_ACTOR_AWARE_MODEL_NAMES = frozenset(
    model_name for _module, model_name, _literal in _ACTOR_MODEL_BOUNDARIES
)
_PULL_FORWARD_LITERALS = frozenset(
    {
        "ingest_artifacts",
        "query_artifacts",
        "run_graders",
        "evaluate",
        "autoresearch",
        "evaluate_physical_task",
        "sweep_physical_instructions",
        "ingest_claude_transcript",
        "query_transcript_rows",
        "query_trajectory",
        "grade_trajectory",
        "submit_mission",
        "run_mission",
        "restore_mission_sandbox",
    }
)


class _EffectTrap:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"API/runtime reached forbidden mirror effect {name!r}")


class _ApplyProbe:
    def __init__(self, *, result: object = None) -> None:
        self.result = result
        self.trusted: list[Any] = []
        self.actor_aware: list[tuple[object, Any]] = []

    async def apply(self, operation: Any) -> Any:
        self.trusted.append(operation)
        return self.result

    async def apply_as(self, actor: object, operation: Any) -> Any:
        self.actor_aware.append((actor, operation))
        return self.result


class _ForbiddenAsyncContext:
    async def __aenter__(self) -> None:
        raise AssertionError("runtime retained a duplicate per-world operation lock")

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _RuntimeStateProbe:
    def __init__(self, dispatcher: object) -> None:
        resources = SimpleNamespace(dispatcher=dispatcher)
        self.runtime = SimpleNamespace(
            _resources=resources,
            resources=resources,
            _dispatcher=dispatcher,
            dispatcher=dispatcher,
            _application=_EffectTrap(),
            _container=_EffectTrap(),
            _shutdown_started=False,
            _closed=False,
            _ensure_open=lambda: None,
        )
        self.name = "api-runtime-parity"
        self.world_id = "world-1"
        self.initialized = True
        self.storage_config = None
        self.closing = False
        self.closed = False
        self.op_lock = _ForbiddenAsyncContext()

    async def ensure_init(self) -> str:
        return "world-1"


def _runtime_world(dispatcher: object) -> Any:
    world_type = import_module("archetype.runtime.world").RuntimeWorld
    return world_type(state=cast("Any", _RuntimeStateProbe(dispatcher)))


def _canonical_actor_models() -> dict[str, type[BaseModel]]:
    loaded: dict[str, type[BaseModel]] = {}
    errors: list[str] = []
    for module_name, model_name, _literal in _ACTOR_MODEL_BOUNDARIES:
        try:
            model = getattr(import_module(module_name), model_name)
        except (AttributeError, ImportError) as error:
            errors.append(f"{module_name}.{model_name}: {type(error).__name__}")
            continue
        if not isinstance(model, type) or not issubclass(model, BaseModel):
            errors.append(f"{module_name}.{model_name}: not a Pydantic model")
            continue
        loaded[model_name] = model
    if errors:
        pytest.fail(
            "PR-4 actor-aware operation boundary is incomplete:\n- " + "\n- ".join(errors),
            pytrace=False,
        )
    return loaded


def _actor_operation(
    model: type[BaseModel],
    *,
    world_id: str = "world-1",
) -> BaseModel:
    literal = str(model.model_fields["operation"].default)
    return model.model_construct(operation=literal, world_id=world_id)


class _SchedulerTrap:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def admit(self, *_args: object, **_kwargs: object) -> None:
        self.calls.append("admit")
        raise AssertionError("direct-only API operation reached scheduler")


class _AccessSink:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def __call__(self, row: Any) -> None:
        self.rows.append(row)


def _safe_summary(operation: BaseModel) -> Mapping[str, Any]:
    return {
        "operation": cast("Any", operation).operation,
        "world_id": cast("Any", operation).world_id,
    }


def _dispatcher(
    registry: OperationRegistry,
    *,
    access: _AccessSink | None = None,
    scheduler: _SchedulerTrap | None = None,
) -> tuple[CommandDispatcher, _AccessSink, _SchedulerTrap]:
    access = access or _AccessSink()
    scheduler = scheduler or _SchedulerTrap()
    dispatcher = CommandDispatcher(
        registry=registry,
        policy=Policy(max_tokens_per_day=1_000_000),
        scheduler=scheduler,
        record_access=access,
        target_tick_for_world=lambda _world_id: 11,
    )
    return dispatcher, access, scheduler


@pytest.mark.asyncio
async def test_routes_construct_the_same_models_as_runtime() -> None:
    """One shared request becomes the same exact Step model on both surfaces."""

    from archetype.api.models import StepRequest
    from archetype.api.routes.simulation import step_world

    config = RunConfig(num_steps=1, debug=True)
    runtime_dispatcher = _ApplyProbe(result=7)
    api_dispatcher = _ApplyProbe(result=7)
    runtime_world = _runtime_world(runtime_dispatcher)
    actor = ActorCtx(id=uuid7(), roles={"operator"})

    await runtime_world.step(config=config)
    response = await step_world(
        "world-1",
        StepRequest(run_config=config),
        cast("Any", api_dispatcher),
        actor,
    )

    assert response.commands_applied == 7
    assert len(runtime_dispatcher.trusted) == 1
    assert len(api_dispatcher.actor_aware) == 1
    runtime_operation = runtime_dispatcher.trusted[0]
    api_actor, api_operation = api_dispatcher.actor_aware[0]
    assert type(runtime_operation) is Step
    assert type(api_operation) is Step
    assert runtime_operation == api_operation
    assert runtime_operation.run_config is config
    assert api_actor is actor
    assert runtime_dispatcher.actor_aware == []
    assert api_dispatcher.trusted == []


class _ResourcesSurfaceProbe:
    def __init__(self, dispatcher: object) -> None:
        self.dispatcher = dispatcher
        self.forbidden_reads: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.forbidden_reads.append(name)
        raise AssertionError(f"request dependency read forbidden resource {name!r}")


@pytest.mark.asyncio
async def test_actor_aware_ingress_uses_apply_as_for_exact_four_pull_forward_models() -> None:
    """Programmatic untrusted ingress is the request-scoped dispatcher itself."""

    deps = import_module("archetype.api.deps")
    models = _canonical_actor_models()
    dispatcher = _ApplyProbe()
    resources = _ResourcesSurfaceProbe(dispatcher)
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(resources=resources)),
    )
    actor = ActorCtx(id=uuid7(), roles={"admin"})

    resolved = await deps.get_dispatcher(cast("Any", request))
    assert resolved is dispatcher
    for model_name in sorted(models):
        operation = _actor_operation(models[model_name])
        await resolved.apply_as(actor, operation)

    assert {
        type(operation).__name__ for _actual_actor, operation in dispatcher.actor_aware
    } == _ACTOR_AWARE_MODEL_NAMES
    assert len(dispatcher.actor_aware) == 4
    assert all(actual_actor is actor for actual_actor, _operation in dispatcher.actor_aware)
    assert dispatcher.trusted == []
    assert resources.forbidden_reads == []


@pytest.mark.asyncio
async def test_four_actor_aware_operations_preserve_existing_permission_and_audit_contracts() -> (
    None
):
    """The PR-3 role split, denial timing, and bounded evidence survive."""

    models = _canonical_actor_models()
    registry = OperationRegistry()
    effects: list[str] = []
    token_costs = {
        "AutoResearch": 200,
        "IngestArtifacts": 10,
        "QueryArtifacts": 5,
        "Evaluate": 10,
    }

    for model_name, model in models.items():
        literal = str(model.model_fields["operation"].default)

        def handler_for(operation_name: str) -> Callable[[BaseModel], Awaitable[str]]:
            async def handler(_operation: BaseModel) -> str:
                effects.append(operation_name)
                return operation_name

            return handler

        registry.register(
            OperationSpec(
                name=literal,
                model=model,
                handler=handler_for(literal),
                permission=literal,
                summarize=_safe_summary,
                quota_scope="live_world",
                world_key=lambda operation: cast("Any", operation).world_id,
                durable=None,
                trusted=True,
                untrusted=True,
                token_cost=token_costs[model_name],
            )
        )

    dispatcher, access, scheduler = _dispatcher(registry)
    allowed_roles = {
        "AutoResearch": "operator",
        "IngestArtifacts": "operator",
        "QueryArtifacts": "viewer",
        "Evaluate": "operator",
    }
    for model_name in sorted(models):
        actor = ActorCtx(id=uuid7(), roles={allowed_roles[model_name]})
        operation = _actor_operation(models[model_name])
        assert await dispatcher.apply_as(actor, operation) == operation.operation
        evidence = access.rows[-1]
        assert evidence.operation == operation.operation
        assert evidence.actor_id == str(actor.id)
        assert evidence.world_id == "world-1"
        assert evidence.decision == "allowed"
        assert evidence.outcome == "succeeded"
        assert evidence.metadata == {
            "operation": operation.operation,
            "world_id": "world-1",
        }

    assert len(access.rows) == 4
    assert effects == sorted(literal for _module, _model_name, literal in _ACTOR_MODEL_BOUNDARIES)

    effects_before_denial = list(effects)
    evidence_before_denial = list(access.rows)
    for model_name in sorted(models):
        denied_role = "viewer" if model_name != "QueryArtifacts" else "unknown"
        denied = ActorCtx(id=uuid7(), roles={denied_role})
        with pytest.raises(PermissionError, match="cannot execute permission"):
            await dispatcher.apply_as(
                denied,
                _actor_operation(models[model_name]),
            )

    assert effects == effects_before_denial
    # Role preauthorization rejects before world/provider reads and preserves
    # the landed bridge behavior: it does not fabricate post-policy evidence.
    assert access.rows == evidence_before_denial
    assert scheduler.calls == []


@pytest.mark.asyncio
async def test_actor_aware_ingress_adds_only_actor_and_policy_evidence() -> None:
    """Trusted runtime and API share a handler; only API emits access rows."""

    from archetype.api.models import StepRequest
    from archetype.api.routes.simulation import step_world

    registry = OperationRegistry()
    handled: list[Step] = []

    async def handler(operation: BaseModel) -> int:
        assert type(operation) is Step
        handled.append(cast("Step", operation))
        return 3

    registry.register(
        OperationSpec(
            name="step",
            model=Step,
            handler=handler,
            permission="step",
            summarize=lambda operation: {
                "operation": cast("Step", operation).operation,
                "world_id": cast("Step", operation).world_id,
            },
            quota_scope="live_world",
            world_key=lambda operation: cast("Step", operation).world_id,
            trusted=True,
            untrusted=True,
        )
    )
    dispatcher, access, scheduler = _dispatcher(registry)
    runtime_world = _runtime_world(dispatcher)
    config = RunConfig(num_steps=1)
    capability = object()

    await runtime_world.step(config=config, capability=capability)
    assert len(handled) == 1
    assert access.rows == []

    actor = ActorCtx(id=uuid7(), roles={"operator"})
    response = await step_world(
        "world-1",
        StepRequest(run_config=config),
        dispatcher,
        actor,
    )
    assert response.commands_applied == 3
    assert len(handled) == 2
    assert type(handled[0]) is type(handled[1]) is Step
    assert handled[0].input_kwargs["capability"] is capability
    assert handled[1].input_kwargs == {}
    assert len(access.rows) == 1
    assert access.rows[0].actor_id == actor.id
    assert access.rows[0].operation == "step"
    assert access.rows[0].decision == "allowed"
    assert access.rows[0].outcome == "succeeded"
    assert scheduler.calls == []


@pytest.mark.asyncio
async def test_request_dependencies_expose_dispatcher_not_registry_storage_or_sandbox() -> None:
    """Request injection reads one dispatcher from the lifespan-owned value."""

    deps = import_module("archetype.api.deps")
    dispatcher = object()
    resources = _ResourcesSurfaceProbe(dispatcher)
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(resources=resources)),
    )

    assert await deps.get_dispatcher(cast("Any", request)) is dispatcher
    assert resources.forbidden_reads == []
    assert not hasattr(deps, "get_container")
    assert not hasattr(deps, "set_container")
    assert not hasattr(deps, "get_registry")
    assert not hasattr(deps, "get_storage")
    assert not hasattr(deps, "get_sandbox")

    source_path = Path(inspect.getsourcefile(deps) or "")
    assert source_path.is_file()
    tree = ast.parse(source_path.read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)

    forbidden_prefixes = (
        "archetype.app",
        "archetype.storage",
        "archetype.missions.sandboxes",
    )
    assert {
        name
        for name in imported
        if any(name == prefix or name.startswith(f"{prefix}.") for prefix in forbidden_prefixes)
    } == set()


class _LifespanResources:
    def __init__(self) -> None:
        self.dispatcher = object()
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_fastapi_lifespan_owns_and_closes_runtime_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifespan builds one resource owner and closes that exact owner once."""

    api_app = import_module("archetype.api.app")
    deps = import_module("archetype.api.deps")
    wiring = import_module("archetype.wiring")
    resources = _LifespanResources()
    build_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def build(*args: object, **kwargs: object) -> _LifespanResources:
        build_calls.append((args, kwargs))
        return resources

    monkeypatch.setattr(wiring, "build_runtime_resources", build)
    monkeypatch.setattr(api_app, "build_runtime_resources", build, raising=False)

    app = api_app.create_app()
    assert build_calls == []
    assert not hasattr(app.state, "resources")

    async with app.router.lifespan_context(app):
        assert len(build_calls) == 1
        assert app.state.resources is resources
        request = SimpleNamespace(app=app)
        assert await deps.get_dispatcher(cast("Any", request)) is resources.dispatcher
        assert resources.close_calls == 0

    assert resources.close_calls == 1
    assert not hasattr(app.state, "container")


def test_create_app_and_imports_start_no_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Import and factory construction remain inert until lifespan entry."""

    api_app = import_module("archetype.api.app")
    build_calls: list[str] = []

    def unexpected_build(*_args: object, **_kwargs: object) -> object:
        build_calls.append("build")
        raise AssertionError("create_app must not start process resources")

    monkeypatch.setattr(
        api_app,
        "build_runtime_resources",
        unexpected_build,
        raising=False,
    )
    try:
        wiring = import_module("archetype.wiring")
    except ModuleNotFoundError:
        wiring = None
    if wiring is not None:
        monkeypatch.setattr(
            wiring,
            "build_runtime_resources",
            unexpected_build,
        )

    app = api_app.create_app()

    assert build_calls == []
    assert not hasattr(app.state, "resources")
    assert not hasattr(app.state, "container")

    source_path = Path(inspect.getsourcefile(api_app) or "")
    tree = ast.parse(source_path.read_text())
    top_level_calls = [
        node
        for statement in tree.body
        if not isinstance(
            statement,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
            ),
        )
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
    ]
    called_names = {node.func.id for node in top_level_calls if isinstance(node.func, ast.Name)}
    assert "build_runtime_resources" not in called_names
    assert "RuntimeResources" not in called_names
    assert "ServiceContainer" not in called_names


_EXPECTED_DECLARED_ROUTES = {
    ("POST", "/worlds"): 201,
    ("GET", "/worlds"): 200,
    ("GET", "/worlds/{world_id}"): 200,
    ("DELETE", "/worlds/{world_id}"): 204,
    ("POST", "/worlds/{world_id}/fork"): 201,
    ("POST", "/worlds/{world_id}/entities"): 201,
    ("DELETE", "/worlds/{world_id}/entities/{entity_id}"): 204,
    ("PATCH", "/worlds/{world_id}/entities/{entity_id}"): 204,
    ("POST", "/worlds/{world_id}/entities/{entity_id}/components"): 204,
    ("DELETE", "/worlds/{world_id}/entities/{entity_id}/components"): 204,
    ("POST", "/worlds/{world_id}/commands"): 200,
    ("POST", "/worlds/{world_id}/commands/batch"): 200,
    ("GET", "/worlds/{world_id}/commands"): 200,
    ("POST", "/worlds/{world_id}/step"): 200,
    ("POST", "/worlds/{world_id}/run"): 200,
    ("POST", "/worlds/{world_id}/episode"): 200,
    ("POST", "/worlds/{world_id}/rollout"): 200,
    ("GET", "/worlds/{world_id}/state"): 200,
    ("GET", "/worlds/{world_id}/entities/{entity_id}"): 200,
    ("GET", "/worlds/{world_id}/components"): 200,
    ("GET", "/worlds/{world_id}/history"): 200,
    ("GET", "/worlds/{world_id}/processors"): 200,
    ("GET", "/worlds/{world_id}/hooks"): 200,
    ("GET", "/worlds/{world_id}/resources"): 200,
    ("GET", "/signatures"): 200,
    ("GET", "/worlds/{world_id}/missions"): 200,
    ("GET", "/worlds/{world_id}/missions/{mission_id}/tasks"): 200,
    ("GET", "/worlds/{world_id}/tasks/{task_id}"): 200,
    ("GET", "/"): 200,
    ("GET", "/healthz"): 200,
}


def test_supported_paths_statuses_and_response_shapes_are_unchanged() -> None:
    """PR-4 does not manufacture REST routes for the fourteen pull-forwards."""

    from fastapi.routing import APIRoute

    app = import_module("archetype.api.app").create_app()
    generated_paths = {
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
    }
    actual: dict[tuple[str, str], int] = {}
    for route in app.routes:
        if route.path in generated_paths:
            continue
        assert isinstance(route, APIRoute)
        methods = set(route.methods or ()) - {"HEAD", "OPTIONS"}
        assert len(methods) == 1
        method = methods.pop()
        actual[(method, route.path)] = route.status_code or 200

    assert len(app.routes) == 34
    assert len(actual) == 30
    assert actual == _EXPECTED_DECLARED_ROUTES
    assert not any(
        literal.replace("_", "-") in path or literal in path
        for _method, path in actual
        for literal in _PULL_FORWARD_LITERALS
    )

    schema = app.openapi()
    assert (
        schema["paths"]["/worlds"]["post"]["responses"]["201"]["content"]["application/json"][
            "schema"
        ]["$ref"]
        == "#/components/schemas/WorldInfo"
    )
    assert (
        schema["paths"]["/worlds/{world_id}/entities"]["post"]["responses"]["201"]["content"][
            "application/json"
        ]["schema"]["$ref"]
        == "#/components/schemas/EntityResponse"
    )
    assert schema["paths"]["/worlds/{world_id}"]["delete"]["responses"]["204"] == {
        "description": "Successful Response"
    }
    assert (
        schema["paths"]["/worlds/{world_id}/step"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["$ref"]
        == "#/components/schemas/StepResponse"
    )
    assert (
        schema["paths"]["/worlds/{world_id}/run"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["$ref"]
        == "#/components/schemas/RunResultResponse"
    )
