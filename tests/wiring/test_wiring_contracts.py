# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contracts for the single explicit PR-4 composition transaction."""

from __future__ import annotations

import ast
import inspect
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import is_dataclass
from functools import partial
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import BaseModel
from uuid_utils import uuid7

from archetype.commands.models import ActorCtx, DurableOptions, GetAuditHistory
from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.config import ControlCatalogConfig
from archetype.world.handlers import WORLD_OPERATION_HANDLERS
from archetype.world.models import PORTABLE_TICK_OPERATION_TYPES, WORLD_OPERATION_TYPES

_PULL_FORWARD_MODELS = (
    ("archetype.artifacts.models", "IngestArtifacts"),
    ("archetype.artifacts.models", "QueryArtifacts"),
    ("archetype.evaluation.models", "RunGraders"),
    ("archetype.evaluation.models", "Evaluate"),
    ("archetype.research.models", "AutoResearch"),
    ("archetype.physical_ai.models", "EvaluatePhysicalTask"),
    ("archetype.physical_ai.models", "SweepPhysicalInstructions"),
    ("archetype.episodes.models", "IngestClaudeTranscript"),
    ("archetype.episodes.models", "QueryTranscriptRows"),
    ("archetype.episodes.models", "QueryTrajectory"),
    ("archetype.episodes.models", "GradeTrajectory"),
    ("archetype.missions.models", "SubmitMission"),
    ("archetype.missions.models", "RunMission"),
    ("archetype.missions.models", "RestoreMissionSandbox"),
)
_ACTOR_AWARE = frozenset(
    {
        "autoresearch",
        "evaluate",
        "ingest_artifacts",
        "query_artifacts",
    }
)
_TRUSTED_ONLY = frozenset(
    {
        "evaluate_physical_task",
        "grade_trajectory",
        "ingest_claude_transcript",
        "query_trajectory",
        "query_transcript_rows",
        "restore_mission_sandbox",
        "run_graders",
        "run_mission",
        "submit_mission",
        "sweep_physical_instructions",
    }
)
_DELETED_MODULES = (
    "archetype.app.application",
    "archetype.app.container",
    "archetype.app.gateway",
)
_SETTER_NAMES = frozenset(
    {
        "set_container",
        "set_outbox_source",
        "set_quota_reset",
    }
)
_APPLICATION_SCOPED_WORLD_OPERATIONS = frozenset(
    {
        "create_world",
        "discover_worlds",
        "list_signatures",
        "list_worlds",
    }
)
_DURABLE_WORLD_SCOPED_OPERATIONS = frozenset(
    {
        "destroy_world",
        "list_world_signatures",
        "open_world_readonly",
        "query_archetype",
        "query_components",
        "resume_world",
    }
)
_INTERNAL_WORLD_OPERATIONS = frozenset({"reserve_entity_ids", "spawn_reserved"})
_WORLD_PERMISSION_OVERRIDES = {
    "list_world_signatures": "list_signatures",
    "reserve_entity_ids": "spawn",
    "spawn_reserved": "spawn",
}
_WORLD_TOKEN_COSTS = {
    "add_components": 8,
    "add_hook": 10,
    "add_processor": 15,
    "add_resource": 10,
    "create_entities": 10,
    "create_world": 50,
    "despawn": 5,
    "destroy_world": 10,
    "discover_worlds": 2,
    "fork_world": 100,
    "get_world_info": 2,
    "list_hooks": 2,
    "list_processors": 2,
    "list_resources": 2,
    "list_signatures": 2,
    "list_world_signatures": 2,
    "list_worlds": 2,
    "open_world_readonly": 2,
    "query_archetype": 5,
    "query_components": 5,
    "remove_components": 5,
    "remove_hook": 5,
    "remove_processor": 5,
    "reserve_entity_ids": 10,
    "resume_world": 50,
    "run": 50,
    "run_episode": 500,
    "run_rollout": 200,
    "spawn": 10,
    "spawn_reserved": 10,
    "step": 10,
    "update": 8,
}
_PULL_FORWARD_SCOPES = {
    "autoresearch": "live_world",
    "evaluate": "durable_world",
    "evaluate_physical_task": "application",
    "grade_trajectory": "durable_world",
    "ingest_artifacts": "live_world",
    "ingest_claude_transcript": "live_world",
    "query_artifacts": "durable_world",
    "query_trajectory": "durable_world",
    "query_transcript_rows": "durable_world",
    "restore_mission_sandbox": "application",
    "run_graders": "application",
    "run_mission": "application",
    "submit_mission": "application",
    "sweep_physical_instructions": "application",
}


def _wiring() -> Any:
    try:
        return import_module("archetype.wiring")
    except ModuleNotFoundError as error:
        if error.name != "archetype.wiring":
            raise
        pytest.fail(
            f"PR-4 explicit wiring boundary is absent: {error.name}",
            pytrace=False,
        )


def _pull_forward_types() -> tuple[type[BaseModel], ...]:
    return tuple(
        cast("type[BaseModel]", getattr(import_module(module_name), model_name))
        for module_name, model_name in _PULL_FORWARD_MODELS
    )


def _operation_name(model: type[BaseModel]) -> str:
    value = model.model_fields["operation"].default
    assert isinstance(value, str) and value
    return value


def _config(wiring: Any, tmp_path: Path) -> Any:
    return wiring.RuntimeBootstrapConfig(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / "catalogs",
        ),
        audit_storage_config=StorageConfig(
            uri=str(tmp_path / "audit"),
            namespace="pr4-wiring-red",
            backend=StorageBackend.ICEBERG,
        ),
    )


@asynccontextmanager
async def _built_resources(wiring: Any, tmp_path: Path):
    resources = wiring.build_runtime_resources(_config(wiring, tmp_path))
    try:
        yield resources
    finally:
        await resources.aclose()


def _registry(resources: Any) -> Any:
    dispatcher = resources.dispatcher
    assert not hasattr(resources, "registry"), "registry must not escape RuntimeResources"
    registry = getattr(dispatcher, "_registry", None)
    assert registry is not None, "dispatcher has no exact operation registry"
    return registry


def _specs(resources: Any) -> tuple[Any, ...]:
    return tuple(_registry(resources).specs)


def _expected_models() -> tuple[type[BaseModel], ...]:
    return (
        *(cast("type[BaseModel]", model) for model in WORLD_OPERATION_TYPES),
        GetAuditHistory,
        *_pull_forward_types(),
    )


def _inventory(resources: Any) -> tuple[tuple[str, type[BaseModel]], ...]:
    return tuple((spec.name, spec.model) for spec in _specs(resources))


def _handler_target(handler: object) -> tuple[str, str, object]:
    """Return stable implementation identity through partials and bound methods."""

    target = handler
    while isinstance(target, partial):
        target = target.func
    target = getattr(target, "__func__", target)
    return (
        str(getattr(target, "__module__", "")),
        str(getattr(target, "__qualname__", "")),
        getattr(target, "__code__", target),
    )


def _token_cost_identity(value: object) -> object:
    return _handler_target(value) if callable(value) else value


def _expected_world_scope(operation_name: str) -> str:
    if operation_name in _APPLICATION_SCOPED_WORLD_OPERATIONS:
        return "application"
    if operation_name in _DURABLE_WORLD_SCOPED_OPERATIONS:
        return "durable_world"
    return "live_world"


def _environment_reads(source: str) -> set[str]:
    """Find direct environment reads that belong in RuntimeBootstrapConfig."""

    reads: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Name) and target.id == "getenv":
                reads.add("getenv")
            elif (
                isinstance(target, ast.Attribute)
                and target.attr in {"get", "getenv"}
                and isinstance(target.value, ast.Attribute)
                and isinstance(target.value.value, ast.Name)
                and target.value.value.id == "os"
                and target.value.attr == "environ"
            ):
                reads.add("os.environ.get")
            elif (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "os"
                and target.attr == "getenv"
            ):
                reads.add("os.getenv")
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Attribute)
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "os"
            and node.value.attr == "environ"
        ):
            reads.add("os.environ[]")
    return reads


def _inventory_defects(
    actual: tuple[tuple[str, type[BaseModel]], ...],
    expected: tuple[type[BaseModel], ...],
) -> tuple[set[str], set[str], set[type[BaseModel]], set[type[BaseModel]]]:
    actual_names = [name for name, _model in actual]
    actual_models = [model for _name, model in actual]
    expected_names = {_operation_name(model) for model in expected}
    expected_models = set(expected)
    duplicate_names = {name for name, count in Counter(actual_names).items() if count != 1}
    duplicate_models = {model for model, count in Counter(actual_models).items() if count != 1}
    return (
        duplicate_names | (expected_names - set(actual_names)),
        set(actual_names) - expected_names,
        duplicate_models | (expected_models - set(actual_models)),
        set(actual_models) - expected_models,
    )


@pytest.mark.asyncio
async def test_builder_returns_complete_runtime_resources(tmp_path: Path) -> None:
    from archetype.runtime_resources import RuntimeResources

    assert not hasattr(SimpleNamespace(dispatcher=object()), "aclose")
    wiring = _wiring()
    config_type = wiring.RuntimeBootstrapConfig
    assert is_dataclass(config_type)
    assert config_type.__dataclass_params__.frozen is True
    assert "__slots__" in vars(config_type)

    signature = inspect.signature(wiring.build_runtime_resources)
    assert tuple(signature.parameters) == ("config",)
    assert signature.parameters["config"].default is inspect.Parameter.empty

    resources = wiring.build_runtime_resources(_config(wiring, tmp_path))
    try:
        assert type(resources) is RuntimeResources
        assert resources.dispatcher is not None
        assert len(_specs(resources)) == 47
        assert resources.close_state.value == "OPEN"
    finally:
        await resources.aclose()
    assert resources.close_state.value == "CLOSED"


@pytest.mark.asyncio
async def test_registry_contains_world_plus_fourteen_pull_forward_specs_exactly_once(
    tmp_path: Path,
) -> None:
    expected = _expected_models()
    assert len(expected) == 47
    duplicate_counterfactual = (
        (_operation_name(expected[0]), expected[0]),
        (_operation_name(expected[0]), expected[0]),
    )
    defects = _inventory_defects(duplicate_counterfactual, expected)
    assert defects[0] and defects[2]

    wiring = _wiring()
    async with _built_resources(wiring, tmp_path) as resources:
        actual = _inventory(resources)
        assert len(actual) == 47
        assert _inventory_defects(actual, expected) == (set(), set(), set(), set())
        assert set(actual) == {(_operation_name(model), model) for model in expected}

        specs = {spec.name: spec for spec in _specs(resources)}
        assert set(_WORLD_TOKEN_COSTS) == {
            _operation_name(cast("type[BaseModel]", model)) for model in WORLD_OPERATION_TYPES
        }
        for model in WORLD_OPERATION_TYPES:
            model = cast("type[BaseModel]", model)
            name = _operation_name(model)
            spec = specs[name]
            assert isinstance(spec.handler, partial)
            assert spec.handler.func is WORLD_OPERATION_HANDLERS[model]
            assert spec.permission == _WORLD_PERMISSION_OVERRIDES.get(name, name)
            assert spec.quota_scope == _expected_world_scope(name)
            assert spec.trusted is True
            assert spec.untrusted is (name not in _INTERNAL_WORLD_OPERATIONS)
            assert spec.token_cost == _WORLD_TOKEN_COSTS[name]
            assert (spec.durable is not None) is (model in PORTABLE_TICK_OPERATION_TYPES)

            world_field = "source_world_id" if name == "fork_world" else "world_id"
            if spec.quota_scope == "application":
                assert spec.world_key is None
            else:
                sentinel = object()
                operation = model.model_construct(**{world_field: sentinel})
                assert spec.world_key is not None
                assert spec.world_key(operation) is sentinel
            summary = spec.summarize(model.model_construct())
            assert summary == {"operation": name}

        audit = specs["get_audit_history"]
        assert audit.model is GetAuditHistory
        assert audit.permission == "get_audit_history"
        assert audit.quota_scope == "durable_world"
        assert audit.trusted is audit.untrusted is True
        assert audit.token_cost == 5
        assert audit.durable is None
        audit_world = object()
        audit_operation = GetAuditHistory.model_construct(world_id=audit_world)
        assert audit.world_key is not None
        assert audit.world_key(audit_operation) is audit_world
        assert audit.summarize(audit_operation) == {
            "operation": "get_audit_history",
            "world_id": str(audit_world),
        }


@pytest.mark.asyncio
async def test_runtime_and_fastapi_have_identical_registered_handler_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiring = _wiring()
    runtime_module = import_module("archetype.runtime.runtime")
    api_module = import_module("archetype.api.app")
    original_builder = wiring.build_runtime_resources
    explicit_config = _config(wiring, tmp_path)
    built: list[Any] = []
    resolved: list[dict[str, object]] = []

    @classmethod
    def from_env(cls: type[Any], **kwargs: object) -> Any:
        assert cls is wiring.RuntimeBootstrapConfig
        resolved.append(dict(kwargs))
        return explicit_config

    def build(config: object) -> Any:
        assert config is explicit_config
        resources = original_builder(config)
        built.append(resources)
        return resources

    monkeypatch.setattr(wiring.RuntimeBootstrapConfig, "from_env", from_env)
    monkeypatch.setattr(wiring, "build_runtime_resources", build)
    monkeypatch.setattr(
        runtime_module,
        "build_runtime_resources",
        build,
        raising=False,
    )
    monkeypatch.setattr(
        api_module,
        "build_runtime_resources",
        build,
        raising=False,
    )

    runtime = None
    try:
        runtime = runtime_module.ArchetypeRuntime()
        app = api_module.create_app()
        async with app.router.lifespan_context(app):
            assert len(built) == 2
            runtime_resources, api_resources = built
            assert runtime._resources is runtime_resources
            assert app.state.resources is api_resources
            assert set(_inventory(runtime_resources)) == set(_inventory(api_resources))
            runtime_specs = {spec.name: spec for spec in _specs(runtime_resources)}
            api_specs = {spec.name: spec for spec in _specs(api_resources)}
            assert set(runtime_specs) == set(api_specs)
            assert {
                name: (
                    spec.model,
                    _handler_target(spec.handler),
                    spec.permission,
                    spec.quota_scope,
                    spec.trusted,
                    spec.untrusted,
                    _token_cost_identity(spec.token_cost),
                    spec.durable is not None,
                )
                for name, spec in runtime_specs.items()
            } == {
                name: (
                    spec.model,
                    _handler_target(spec.handler),
                    spec.permission,
                    spec.quota_scope,
                    spec.trusted,
                    spec.untrusted,
                    _token_cost_identity(spec.token_cost),
                    spec.durable is not None,
                )
                for name, spec in api_specs.items()
            }
    finally:
        if runtime is not None:
            await runtime.shutdown()
        for resources in built:
            if resources.close_state.value != "CLOSED":
                await resources.aclose()
    assert len(resolved) == 2


def _setter_calls(source: str) -> set[str]:
    tree = ast.parse(source)
    calls: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            name = node.func.attr
        else:
            continue
        if name.startswith("set_"):
            calls.add(name)
    return calls


@pytest.mark.asyncio
async def test_wiring_is_explicit_topological_and_has_no_setter_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.commands.scheduler import CommandScheduler

    assert _setter_calls("owner.set_outbox_source(source)\nset_container(owner)") == {
        "set_container",
        "set_outbox_source",
    }
    outbox_events: list[tuple[str, object]] = []
    outbox_rows: list[object] = []

    async def read_outbox(
        self: object,
        *,
        world_id: object | None = None,
        limit: int = 1000,
    ) -> list[object]:
        outbox_events.append(("read", (self, world_id, limit)))
        return outbox_rows

    async def acknowledge_outbox(self: object, events: list[object]) -> None:
        outbox_events.append(("ack", (self, events)))

    monkeypatch.setattr(CommandScheduler, "read_outbox", read_outbox)
    monkeypatch.setattr(CommandScheduler, "acknowledge_outbox", acknowledge_outbox)
    wiring = _wiring()
    source_path = Path(inspect.getsourcefile(wiring) or "")
    source = source_path.read_text()
    assert _setter_calls(source) == set()
    assert all(name not in source for name in _SETTER_NAMES)

    async with _built_resources(wiring, tmp_path) as resources:
        dispatcher = resources.dispatcher
        registry = _registry(resources)
        scheduler = dispatcher._scheduler
        audit = resources._audit
        assert scheduler._registry is registry
        assert await audit._read_outbox(world_id="world-1", limit=7) is outbox_rows
        await audit._acknowledge_outbox(outbox_rows)
        assert outbox_events == [
            ("read", (scheduler, "world-1", 7)),
            ("ack", (scheduler, outbox_rows)),
        ]
        assert resources._storage is audit._storage_service
        assert len(registry.specs) == 47


@pytest.mark.asyncio
async def test_bootstrap_environment_is_resolved_once_before_family_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.app.artifacts.service import ArtifactService
    from archetype.app.evaluation.service import EvaluationService
    from archetype.app.missions.trajectory_service import TrajectoryService
    from archetype.app.missions.transcript_service import TranscriptIngestionService
    from archetype.app.physical_ai.service import PhysicalAIService
    from archetype.app.research.service import AutoResearchService
    from archetype.commands.audit import AuditLog
    from archetype.commands.policy import Policy
    from archetype.commands.scheduler import CommandScheduler
    from archetype.storage.service import StorageService
    from archetype.world.lifecycle import WorldLifecycle
    from archetype.world.registry import WorldRegistry

    wiring = _wiring()
    counterfactual = "import os\nvalue = os.getenv('A')\nother = os.environ['B']\n"
    assert _environment_reads(counterfactual) == {"os.getenv", "os.environ[]"}
    source_path = Path(inspect.getsourcefile(wiring) or "")
    assert _environment_reads(source_path.read_text()) == set()
    constructor_types = (
        StorageService,
        WorldRegistry,
        CommandScheduler,
        WorldLifecycle,
        AuditLog,
        Policy,
        ArtifactService,
        TranscriptIngestionService,
        EvaluationService,
        TrajectoryService,
        PhysicalAIService,
        AutoResearchService,
    )
    constructor_modules = {
        inspect.getmodule(constructor_type) for constructor_type in constructor_types
    }
    for module in constructor_modules:
        assert module is not None
        module_path = Path(inspect.getsourcefile(module) or "")
        assert _environment_reads(module_path.read_text()) == set()
    resolved = ControlCatalogConfig(catalog_dir=tmp_path / "catalogs")
    calls: list[object] = []

    def from_env(
        cls: type[ControlCatalogConfig],
        environ: object = None,
    ) -> ControlCatalogConfig:
        assert cls is ControlCatalogConfig
        calls.append(environ)
        return resolved

    monkeypatch.setattr(
        ControlCatalogConfig,
        "from_env",
        classmethod(from_env),
    )
    config = wiring.RuntimeBootstrapConfig.from_env(
        audit_storage_config=StorageConfig(
            uri=str(tmp_path / "audit"),
            namespace="pr4-env-once",
            backend=StorageBackend.ICEBERG,
        ),
    )
    assert calls == [None]
    resources = wiring.build_runtime_resources(config)
    try:
        assert calls == [None]
        assert resources._storage._control_catalog_config is resolved
    finally:
        await resources.aclose()
    assert calls == [None]


def _forbidden_imports(source: str) -> set[str]:
    forbidden = set(_DELETED_MODULES) | {
        "archetype.app.gateway._pr3_commands_bridge",
    }
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names = (node.module,)
        else:
            continue
        imported.update(
            name
            for name in names
            if any(name == prefix or name.startswith(f"{prefix}.") for prefix in forbidden)
        )
    return imported


@pytest.mark.asyncio
async def test_no_runtime_or_api_operation_can_fall_back_to_a_legacy_bridge(
    tmp_path: Path,
) -> None:
    counterfactual = (
        "from archetype.app.application import RuntimeApplication\n"
        "from archetype.app.gateway._pr3_commands_bridge import PR3_BRIDGE_MODEL_LITERALS\n"
    )
    assert _forbidden_imports(counterfactual) == {
        "archetype.app.application",
        "archetype.app.gateway._pr3_commands_bridge",
    }

    wiring = _wiring()
    checked = (
        wiring,
        import_module("archetype.runtime.runtime"),
        import_module("archetype.runtime.world"),
        import_module("archetype.api.app"),
        import_module("archetype.api.deps"),
    )
    for module in checked:
        source_path = Path(inspect.getsourcefile(module) or "")
        assert _forbidden_imports(source_path.read_text()) == set()
    for module_name in _DELETED_MODULES:
        assert find_spec(module_name) is None
        with pytest.raises(ModuleNotFoundError) as caught:
            import_module(module_name)
        assert caught.value.name == module_name

    async with _built_resources(wiring, tmp_path) as resources:
        pull_forward = set(_pull_forward_types())
        specs = tuple(spec for spec in _specs(resources) if spec.model in pull_forward)
        assert len(specs) == 14
        assert all(callable(spec.handler) for spec in specs)


def _pull_forward_specs(resources: Any) -> dict[str, Any]:
    pull_forward = set(_pull_forward_types())
    return {spec.name: spec for spec in _specs(resources) if spec.model in pull_forward}


@pytest.mark.asyncio
async def test_pull_forward_registration_has_exact_four_actor_aware_and_ten_trusted_only_specs(
    tmp_path: Path,
) -> None:
    assert len(_ACTOR_AWARE) == 4
    assert len(_TRUSTED_ONLY) == 10
    assert _ACTOR_AWARE.isdisjoint(_TRUSTED_ONLY)
    assert len(_ACTOR_AWARE | _TRUSTED_ONLY) == 14
    bad = {
        "one": SimpleNamespace(trusted=True, untrusted=True, durable=object()),
        "two": SimpleNamespace(trusted=False, untrusted=False, durable=None),
    }
    assert {name for name, spec in bad.items() if spec.untrusted} != _ACTOR_AWARE

    wiring = _wiring()
    async with _built_resources(wiring, tmp_path) as resources:
        specs = _pull_forward_specs(resources)
        assert set(specs) == _ACTOR_AWARE | _TRUSTED_ONLY
        assert {name for name, spec in specs.items() if spec.untrusted} == _ACTOR_AWARE
        assert {name for name, spec in specs.items() if not spec.untrusted} == _TRUSTED_ONLY
        assert all(spec.trusted is True and spec.durable is None for spec in specs.values())
        assert {name: spec.quota_scope for name, spec in specs.items()} == _PULL_FORWARD_SCOPES
        assert all(spec.permission == name for name, spec in specs.items())

        for name, spec in specs.items():
            sentinel = object()
            values: dict[str, object] = {}
            if "world_id" in spec.model.model_fields:
                values["world_id"] = sentinel
            operation = spec.model.model_construct(**values)
            if spec.quota_scope == "application":
                assert spec.world_key is None
            else:
                assert spec.world_key is not None
                assert spec.world_key(operation) is sentinel
            expected_summary = {"operation": name}
            if "world_id" in spec.model.model_fields:
                expected_summary["world_id"] = str(sentinel)
            assert spec.summarize(operation) == expected_summary

        assert specs["autoresearch"].quota_scope == "live_world"
        assert specs["ingest_artifacts"].quota_scope == "live_world"
        assert specs["query_artifacts"].quota_scope == "durable_world"
        assert specs["evaluate"].quota_scope == "durable_world"
        assert (
            specs["autoresearch"].token_cost(
                SimpleNamespace(config=SimpleNamespace(max_iterations=3))
            )
            == 600
        )
        assert (
            specs["autoresearch"].token_cost(
                SimpleNamespace(config=SimpleNamespace(max_iterations=0))
            )
            == 200
        )
        assert specs["ingest_artifacts"].token_cost == 10
        assert specs["query_artifacts"].token_cost == 5
        assert specs["evaluate"].token_cost == 10
        assert all(specs[name].token_cost == 0 for name in _TRUSTED_ONLY)


@pytest.mark.asyncio
async def test_pull_forward_handlers_translate_exact_values_without_recursive_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.app.artifacts.service import ArtifactService
    from archetype.app.evaluation.service import EvaluationService
    from archetype.app.missions.service import MissionService
    from archetype.app.missions.trajectory_service import TrajectoryService
    from archetype.app.missions.transcript_service import TranscriptIngestionService
    from archetype.app.physical_ai.service import PhysicalAIService
    from archetype.app.research.service import AutoResearchService
    from archetype.missions.sandboxes.service import SandboxService
    from archetype.world import query as world_query

    calls: dict[str, list[tuple[tuple[object, ...], dict[str, object]]]] = {}
    results = {name: object() for name in _ACTOR_AWARE | _TRUSTED_ONLY}

    def record(name: str):
        async def method(_self: object, *args: object, **kwargs: object) -> object:
            calls.setdefault(name, []).append((args, kwargs))
            return results[name]

        return method

    method_bindings = (
        (ArtifactService, "ingest", "ingest_artifacts"),
        (ArtifactService, "index", "query_artifacts"),
        (EvaluationService, "run_graders", "run_graders"),
        (EvaluationService, "evaluate", "evaluate"),
        (AutoResearchService, "run", "autoresearch"),
        (PhysicalAIService, "evaluate_task", "evaluate_physical_task"),
        (PhysicalAIService, "sweep_instructions", "sweep_physical_instructions"),
        (TranscriptIngestionService, "ingest", "ingest_claude_transcript"),
        (TranscriptIngestionService, "read", "query_transcript_rows"),
        (TrajectoryService, "query", "query_trajectory"),
        (TrajectoryService, "grade", "grade_trajectory"),
        (MissionService, "submit", "submit_mission"),
        (MissionService, "run", "run_mission"),
        (MissionService, "restore_sandbox", "restore_mission_sandbox"),
    )
    for service_type, method_name, operation_name in method_bindings:
        monkeypatch.setattr(service_type, method_name, record(operation_name))

    mission_init: list[dict[str, object]] = []

    def initialize_mission(_self: object, **kwargs: object) -> None:
        mission_init.append(kwargs)

    async def close_mission(_self: object) -> None:
        calls.setdefault("mission_close", []).append(((), {}))

    monkeypatch.setattr(MissionService, "__init__", initialize_mission)
    monkeypatch.setattr(MissionService, "close", close_mission)

    lineage = [object()]
    lineage_calls: list[tuple[object, ...]] = []

    async def get_lineage(*args: object, **kwargs: object) -> list[object]:
        lineage_calls.append((*args, kwargs))
        return lineage

    monkeypatch.setattr(world_query, "get_lineage", get_lineage)

    wiring = _wiring()
    monkeypatch.setattr(wiring, "get_lineage", get_lineage, raising=False)
    async with _built_resources(wiring, tmp_path) as resources:
        specs = _pull_forward_specs(resources)
        dispatcher = resources.dispatcher

        async def recursive_dispatch(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("a registered handler recursively entered dispatcher admission")

        original_apply = dispatcher.apply
        original_apply_as = dispatcher.apply_as
        dispatcher.apply = recursive_dispatch
        dispatcher.apply_as = recursive_dispatch
        try:
            world_id = "world-1"
            run_id = "run-1"
            storage = object()
            source = object()
            frame = object()
            grader = object()
            contract = object()
            research_config = SimpleNamespace(max_iterations=2)
            evaluator = object()
            prepare_candidate = object()
            on_iteration = object()
            physical_config = object()
            sweep_config = object()
            env_client = object()
            policy_client = object()
            transcript = object()
            component = object()
            selection = object()
            owner_id = "mission-owner-1"
            backend = SimpleNamespace(name="provider")
            mission_config = SimpleNamespace(sandbox_backend=backend)
            mission_storage = object()
            task = object()
            submission = SimpleNamespace(
                repository="repo",
                branch="branch",
                tasks=(task,),
                name="mission",
                base_ref="main",
            )
            mission = object()
            checkpoint = object()

            def operation(operation_name: str, **values: object) -> BaseModel:
                return specs[operation_name].model.model_construct(
                    operation=operation_name,
                    **values,
                )

            operations = {
                "ingest_artifacts": operation(
                    "ingest_artifacts",
                    world_id=world_id,
                    sources=(source,),
                    storage_config=storage,
                ),
                "query_artifacts": operation(
                    "query_artifacts",
                    world_id=world_id,
                    storage_config=storage,
                ),
                "run_graders": operation(
                    "run_graders",
                    df=frame,
                    graders=(grader,),
                ),
                "evaluate": operation(
                    "evaluate",
                    world_id=world_id,
                    components=(component,),
                    contract=contract,
                    grader=grader,
                    evaluation_id="evaluation-1",
                    storage_config=storage,
                    ticks=(2,),
                    entity_ids=(3,),
                ),
                "autoresearch": operation(
                    "autoresearch",
                    world_id=world_id,
                    config=research_config,
                    evaluator=evaluator,
                    prepare_candidate=prepare_candidate,
                    lab_world_id="lab-world",
                    on_iteration=on_iteration,
                ),
                "evaluate_physical_task": operation(
                    "evaluate_physical_task",
                    config=physical_config,
                    env_client=env_client,
                    policy_client=policy_client,
                ),
                "sweep_physical_instructions": operation(
                    "sweep_physical_instructions",
                    config=sweep_config,
                    env_client=env_client,
                    policy_client=policy_client,
                ),
                "ingest_claude_transcript": operation(
                    "ingest_claude_transcript",
                    world_id=world_id,
                    source=transcript,
                    storage_config=storage,
                ),
                "query_transcript_rows": operation(
                    "query_transcript_rows",
                    world_id=world_id,
                    storage_config=storage,
                ),
                "query_trajectory": operation(
                    "query_trajectory",
                    component=component,
                    world_id=world_id,
                    run_id=run_id,
                    storage_config=storage,
                    selection=selection,
                    ticks=(4,),
                    entity_ids=(5,),
                ),
                "grade_trajectory": operation(
                    "grade_trajectory",
                    component=component,
                    world_id=world_id,
                    run_id=run_id,
                    graders=(grader,),
                    storage_config=storage,
                    selection=selection,
                    ticks=(6,),
                    entity_ids=(7,),
                ),
                "submit_mission": operation(
                    "submit_mission",
                    owner_id=owner_id,
                    name="mission-runtime",
                    config=mission_config,
                    storage=mission_storage,
                    submission=submission,
                ),
                "run_mission": operation(
                    "run_mission",
                    owner_id=owner_id,
                    mission=mission,
                    max_ticks=9,
                ),
                "restore_mission_sandbox": operation(
                    "restore_mission_sandbox",
                    owner_id=owner_id,
                    mission=mission,
                    checkpoint=checkpoint,
                ),
            }

            unknown_run = operations["run_mission"].model_copy(update={"owner_id": "unknown-owner"})
            with pytest.raises((KeyError, RuntimeError, ValueError)):
                await specs["run_mission"].handler(unknown_run)
            unknown_submit = operations["submit_mission"].model_copy(
                update={"owner_id": "unknown-owner"}
            )
            with pytest.raises((KeyError, RuntimeError, ValueError)):
                await specs["submit_mission"].handler(unknown_submit)
            assert "run_mission" not in calls
            assert mission_init == []

            reservation = resources.reserve_owner(owner_id, phase="workflow-handles")
            for name in (
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
            ):
                assert await specs[name].handler(operations[name]) is results[name]

            assert set(calls) == _ACTOR_AWARE | _TRUSTED_ONLY
            assert all(len(calls[name]) == 1 for name in _ACTOR_AWARE | _TRUSTED_ONLY)
            assert calls["ingest_artifacts"] == [
                ((world_id, (source,)), {"storage_config": storage})
            ]
            assert calls["query_artifacts"] == [((world_id,), {"storage_config": storage})]
            assert calls["run_graders"] == [((frame, (grader,)), {})]
            assert calls["evaluate"] == [
                (
                    (world_id, (component,)),
                    {
                        "contract": contract,
                        "grader": grader,
                        "evaluation_id": "evaluation-1",
                        "storage_config": storage,
                        "ticks": [2],
                        "entity_ids": [3],
                    },
                )
            ]
            assert calls["autoresearch"] == [
                (
                    (world_id, research_config, evaluator),
                    {
                        "prepare_candidate": prepare_candidate,
                        "lab_world_id": "lab-world",
                        "on_iteration": on_iteration,
                    },
                )
            ]
            assert calls["evaluate_physical_task"] == [
                (
                    (physical_config,),
                    {"env_client": env_client, "policy_client": policy_client},
                )
            ]
            assert calls["sweep_physical_instructions"] == [
                (
                    (sweep_config,),
                    {"env_client": env_client, "policy_client": policy_client},
                )
            ]
            assert calls["ingest_claude_transcript"] == [
                ((world_id, transcript), {"storage_config": storage})
            ]
            assert calls["query_transcript_rows"] == [((world_id,), {"storage_config": storage})]
            assert calls["query_trajectory"] == [
                (
                    (component,),
                    {
                        "world_id": world_id,
                        "run_id": run_id,
                        "storage_config": storage,
                        "lineage": lineage,
                        "selection": selection,
                        "ticks": [4],
                        "entity_ids": [5],
                    },
                )
            ]
            assert calls["grade_trajectory"] == [
                (
                    (component,),
                    {
                        "world_id": world_id,
                        "run_id": run_id,
                        "graders": (grader,),
                        "storage_config": storage,
                        "lineage": lineage,
                        "selection": selection,
                        "ticks": [6],
                        "entity_ids": [7],
                    },
                )
            ]
            assert len(lineage_calls) == 2
            assert [call[1:4] for call in lineage_calls] == [
                (world_id, run_id, storage),
                (world_id, run_id, storage),
            ]
            assert len(mission_init) == 1
            assert isinstance(reservation.require_bound(), MissionService)
            assert mission_init[0]["name"] == "mission-runtime"
            assert mission_init[0]["config"] is mission_config
            assert mission_init[0]["storage"] is mission_storage
            assert isinstance(mission_init[0]["sandbox_service"], SandboxService)
            assert callable(mission_init[0]["world_factory"])
            assert calls["submit_mission"] == [
                (
                    (),
                    {
                        "repository": "repo",
                        "branch": "branch",
                        "tasks": (task,),
                        "name": "mission",
                        "base_ref": "main",
                    },
                )
            ]
            assert calls["run_mission"] == [((mission,), {"max_ticks": 9})]
            assert calls["restore_mission_sandbox"] == [((mission, checkpoint), {})]
        finally:
            dispatcher.apply = original_apply
            dispatcher.apply_as = original_apply_as


@pytest.mark.asyncio
async def test_compound_destroy_and_audit_handlers_preserve_owned_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.commands.audit import AuditLog
    from archetype.commands.scheduler import CommandScheduler
    from archetype.world import simulation
    from archetype.world.lifecycle import WorldLifecycle
    from archetype.world.models import DestroyWorld
    from archetype.world.registry import WorldRegistry

    events: list[tuple[str, object]] = []
    lease = object()
    world = object()
    audit_result = object()

    async def begin_close(_self: object, world_id: str) -> object:
        events.append(("begin", (_self, world_id)))
        return lease

    @asynccontextmanager
    async def cleanup_operation(_self: object, actual_lease: object):
        events.append(("cleanup-enter", (_self, actual_lease)))
        try:
            yield world
        finally:
            events.append(("cleanup-exit", (_self, actual_lease)))

    async def reconcile(
        registry: object,
        world_id: str,
        actual_world: object,
    ) -> None:
        events.append(("reconcile", (registry, world_id, actual_world)))

    async def cancel_world(_self: object, world_id: object) -> int:
        events.append(("cancel", world_id))
        return 1

    async def destroy_world(
        _self: object,
        world_id: object,
        *,
        lease: object,
    ) -> None:
        events.append(("destroy", (world_id, lease)))

    async def query_audit(_self: object, world_id: object, **filters: object) -> object:
        events.append(("audit", (world_id, filters)))
        return audit_result

    monkeypatch.setattr(WorldRegistry, "begin_close", begin_close)
    monkeypatch.setattr(WorldRegistry, "cleanup_operation", cleanup_operation)
    monkeypatch.setattr(simulation, "reconcile_committed_work_locked", reconcile)
    monkeypatch.setattr(CommandScheduler, "cancel_world", cancel_world)
    monkeypatch.setattr(WorldLifecycle, "destroy_world", destroy_world)
    monkeypatch.setattr(AuditLog, "query", query_audit)

    wiring = _wiring()
    monkeypatch.setattr(
        wiring,
        "reconcile_committed_work_locked",
        reconcile,
        raising=False,
    )
    async with _built_resources(wiring, tmp_path) as resources:
        specs = {spec.name: spec for spec in _specs(resources)}
        dispatcher = resources.dispatcher

        async def recursive_dispatch(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("compound handler recursively entered dispatcher admission")

        original_apply = dispatcher.apply
        dispatcher.apply = recursive_dispatch
        try:
            assert await specs["destroy_world"].handler(DestroyWorld(world_id="world-1")) is None
            assert [name for name, _value in events] == [
                "begin",
                "cleanup-enter",
                "reconcile",
                "cancel",
                "cleanup-exit",
                "destroy",
            ]
            world_registry = cast("tuple[object, str]", events[0][1])[0]
            assert isinstance(world_registry, WorldRegistry)
            assert events[1] == ("cleanup-enter", (world_registry, lease))
            assert events[2] == ("reconcile", (world_registry, "world-1", world))
            assert events[3] == ("cancel", "world-1")
            assert events[4] == ("cleanup-exit", (world_registry, lease))
            assert events[5] == ("destroy", ("world-1", lease))

            operation = GetAuditHistory.model_construct(
                world_id="world-1",
                tick_range=(2, 4),
                actor_id="actor-1",
                idempotency_key="command-1",
                status="succeeded",
                limit=17,
            )
            assert await specs["get_audit_history"].handler(operation) is audit_result
            assert events[-1] == (
                "audit",
                (
                    "world-1",
                    {
                        "tick_range": (2, 4),
                        "actor_id": "actor-1",
                        "idempotency_key": "command-1",
                        "status": "succeeded",
                        "limit": 17,
                    },
                ),
            )
        finally:
            dispatcher.apply = original_apply


class _Untouched:
    def __init__(self) -> None:
        self.reads: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.reads.append(name)
        raise AssertionError(f"rejected operation read live capability {name!r}")

    def __str__(self) -> str:
        self.reads.append("__str__")
        raise AssertionError("rejected operation rendered live capability")


class _AllowPolicy:
    def preauthorize(self, _actor: object, *, permission: str) -> None:
        assert permission

    def authorize(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("rejected operation reached world authorization")

    def authorize_application(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("rejected operation reached application authorization")


class _SchedulerTrap:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.calls.append(name)
        raise AssertionError(f"direct-only operation reached scheduler {name!r}")


@pytest.mark.asyncio
async def test_nondurable_pull_forward_rejections_have_no_provider_or_scheduler_side_effect(
    tmp_path: Path,
) -> None:
    from archetype.commands.registry import OperationRegistry, OperationSpec

    class Probe(BaseModel):
        operation: str = "probe"

    probe_effects: list[str] = []

    async def probe_handler(_operation: BaseModel) -> None:
        probe_effects.append("handler")

    probe_registry = OperationRegistry()
    with pytest.raises(ValueError, match="Literal"):
        probe_registry.register(
            OperationSpec(
                name="probe",
                model=Probe,
                handler=probe_handler,
                permission="probe",
                summarize=lambda operation: {"operation": operation.operation},
                quota_scope="application",
                world_key=None,
                durable=None,
            )
        )
    assert probe_effects == []

    wiring = _wiring()
    async with _built_resources(wiring, tmp_path) as resources:
        dispatcher = resources.dispatcher
        specs = _pull_forward_specs(resources)
        trap = _Untouched()
        scheduler = _SchedulerTrap()
        access_rows: list[object] = []

        async def record_access(row: object) -> None:
            access_rows.append(row)

        original_policy = dispatcher._policy
        original_scheduler = dispatcher._scheduler
        original_record_access = dispatcher._record_access
        try:
            dispatcher._policy = _AllowPolicy()
            dispatcher._scheduler = scheduler
            dispatcher._record_access = record_access
            actor = ActorCtx(id=uuid7(), roles={"admin"})
            options = DurableOptions(target_tick=0)

            for name, spec in sorted(specs.items()):
                values = {field: trap for field in spec.model.model_fields if field != "operation"}
                operation = spec.model.model_construct(
                    operation=name,
                    **values,
                )
                with pytest.raises(ValueError, match="direct-only"):
                    await dispatcher.defer(operation, options)
                expected = ValueError if name in _ACTOR_AWARE else PermissionError
                with pytest.raises(expected):
                    await dispatcher.defer_as(actor, operation, options)
                if name in _TRUSTED_ONLY:
                    with pytest.raises(PermissionError, match="not available to untrusted"):
                        await dispatcher.apply_as(actor, operation)

            assert trap.reads == []
            assert scheduler.calls == []
            assert len(access_rows) == 24
            assert all(getattr(row, "outcome", None) == "rejected" for row in access_rows)
        finally:
            dispatcher._policy = original_policy
            dispatcher._scheduler = original_scheduler
            dispatcher._record_access = original_record_access
