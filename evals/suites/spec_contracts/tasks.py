# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Spec-contract eval suite.

These tasks are derived from the normative documents under ``docs/guide/``.
They are intentionally structural and deterministic: each task checks that a
spec clause still has an executable guardrail independent of the older app/core
test modules.
"""

from __future__ import annotations

import ast
import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")

from uuid_utils import uuid7

from archetype.app.application.service import RuntimeApplication
from archetype.app.gateway.auth.guard import reset_daily_tokens
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.auth.permissions import COMMANDS_BY_ROLE
from archetype.app.gateway.service import CommandGateway
from archetype.app.models import CommandType
from archetype.core.config import WorldConfig
from archetype.world.models import (
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    WorldInfo,
)
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "spec"

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src" / "archetype"
DOCS = ROOT / "docs" / "guide"


@dataclass(frozen=True)
class SpecCase:
    """Traceability record from normative text to executable eval."""

    spec_id: str
    source: str
    anchors: tuple[str, ...]
    task_id: str


SPEC_CASES: tuple[SpecCase, ...] = (
    SpecCase(
        spec_id="runtime.R1",
        source="runtime.md",
        anchors=("R1", "`app.application` port", "internal container"),
        task_id="spec.runtime_gate_only_boundary",
    ),
    SpecCase(
        spec_id="runtime.R2",
        source="runtime.md",
        anchors=("R2", "`world_id` after activation", "concrete app service"),
        task_id="spec.runtime_gate_only_boundary",
    ),
    SpecCase(
        spec_id="command-gate.3",
        source="command-gate.md",
        anchors=("The permissions matrix", "`list_worlds`"),
        task_id="spec.role_permission_matrix",
    ),
    SpecCase(
        spec_id="command-gate.1",
        source="command-gate.md",
        anchors=("guardrail_allow", "delegate", "audit.record"),
        task_id="spec.command_gateway_gate_map",
    ),
    SpecCase(
        spec_id="world-lifecycle.6",
        source="world-lifecycle.md",
        anchors=("info-class downgrade", "Live objects"),
        task_id="spec.info_class_downgrades",
    ),
    SpecCase(
        spec_id="dataset-eval-ontology.4",
        source="dataset-eval-ontology.md",
        anchors=("evidence, never authority", "evaluation owns those decisions"),
        task_id="spec.receipt_authority_firewall",
    ),
    SpecCase(
        spec_id="audit-log.2",
        source="audit-log.md",
        anchors=("Append-only invariant", "no `drop_*` or `delete_*` methods"),
        task_id="spec.append_only_protocols",
    ),
    SpecCase(
        spec_id="dataset-eval-ontology.1",
        source="dataset-eval-ontology.md",
        anchors=("Dataset coordinates are natural keys", "Runtime coordinates are provenance"),
        task_id="spec.dataset_eval_ontology",
    ),
    SpecCase(
        spec_id="dataset-eval-ontology.2",
        source="dataset-eval-ontology.md",
        anchors=(
            "A trial produces exactly one dataset episode",
            "A runtime episode MAY batch many trials",
        ),
        task_id="spec.dataset_eval_ontology",
    ),
    SpecCase(
        spec_id="dataset-eval-ontology.3",
        source="dataset-eval-ontology.md",
        anchors=(
            "StorageService envelope is durable ownership",
            "does not prove where an imported episode",
        ),
        task_id="spec.dataset_eval_ontology",
    ),
)

_EXPECTED_TASK_IDS = frozenset(case.task_id for case in SPEC_CASES)

_RUNTIME_TYPE_ONLY_APP_IMPORTS = frozenset(
    {
        "archetype.app.evaluation.interfaces",
        "archetype.app.research.contracts",
    }
)
_RUNTIME_ALLOWED_APP_IMPORTS = _RUNTIME_TYPE_ONLY_APP_IMPORTS | frozenset(
    {
        "archetype.app.application.interfaces",
        "archetype.app.container",
        "archetype.app.models",
    }
)

_EXPECTED_ROLE_MATRIX: dict[str, frozenset[CommandType]] = {
    "viewer": frozenset(
        {
            CommandType.QUERY_WORLD,
            CommandType.GET_WORLD_INFO,
            CommandType.GET_AUDIT_HISTORY,
            CommandType.LIST_SIGNATURES,
            CommandType.LIST_WORLDS,
            CommandType.LIST_PROCESSORS,
            CommandType.LIST_HOOKS,
            CommandType.LIST_RESOURCES,
        }
    ),
    "player": frozenset(
        {
            CommandType.QUERY_WORLD,
            CommandType.GET_WORLD_INFO,
            CommandType.GET_AUDIT_HISTORY,
            CommandType.LIST_SIGNATURES,
            CommandType.LIST_WORLDS,
            CommandType.LIST_PROCESSORS,
            CommandType.LIST_HOOKS,
            CommandType.LIST_RESOURCES,
            CommandType.SPAWN,
            CommandType.DESPAWN,
            CommandType.UPDATE,
            CommandType.MESSAGE,
            CommandType.CUSTOM,
        }
    ),
    "operator": frozenset(
        {
            CommandType.QUERY_WORLD,
            CommandType.GET_WORLD_INFO,
            CommandType.GET_AUDIT_HISTORY,
            CommandType.LIST_SIGNATURES,
            CommandType.LIST_WORLDS,
            CommandType.LIST_PROCESSORS,
            CommandType.LIST_HOOKS,
            CommandType.LIST_RESOURCES,
            CommandType.SPAWN,
            CommandType.DESPAWN,
            CommandType.UPDATE,
            CommandType.MESSAGE,
            CommandType.CUSTOM,
            CommandType.ADD_COMPONENT,
            CommandType.REMOVE_COMPONENT,
            CommandType.ADD_PROCESSOR,
            CommandType.REMOVE_PROCESSOR,
            CommandType.ADD_HOOK,
            CommandType.REMOVE_HOOK,
            CommandType.ADD_RESOURCE,
            CommandType.STEP,
            CommandType.RUN,
            CommandType.RUN_EPISODE,
            CommandType.RUN_ROLLOUT,
            CommandType.AUTORESEARCH,
            CommandType.FORK_WORLD,
            CommandType.DESTROY_WORLD,
            CommandType.INGEST_ARTIFACTS,
            CommandType.EVALUATE,
        }
    ),
    "admin": frozenset(CommandType),
}

_COMMAND_GATE_MAP: dict[str, CommandType] = {
    "create_entity": CommandType.SPAWN,
    "create_entities": CommandType.SPAWN,
    "reserve_entity_ids": CommandType.SPAWN,
    "spawn_with_reserved_id": CommandType.SPAWN,
    "remove_entity": CommandType.DESPAWN,
    "update_entity": CommandType.UPDATE,
    "add_components": CommandType.ADD_COMPONENT,
    "remove_components": CommandType.REMOVE_COMPONENT,
    "add_processor": CommandType.ADD_PROCESSOR,
    "remove_processor": CommandType.REMOVE_PROCESSOR,
    "create_world": CommandType.CREATE_WORLD,
    "fork_world": CommandType.FORK_WORLD,
    "destroy_world": CommandType.DESTROY_WORLD,
    "get_world_info": CommandType.GET_WORLD_INFO,
    "list_worlds": CommandType.LIST_WORLDS,
    "discover_worlds": CommandType.LIST_WORLDS,
    "open_world_readonly": CommandType.GET_WORLD_INFO,
    "resume_world": CommandType.CREATE_WORLD,
    "ingest_artifacts": CommandType.INGEST_ARTIFACTS,
    "query_artifacts": CommandType.QUERY_WORLD,
    "evaluate": CommandType.EVALUATE,
    "step": CommandType.STEP,
    "run": CommandType.RUN,
    "run_episode": CommandType.RUN_EPISODE,
    "run_rollout": CommandType.RUN_ROLLOUT,
    "autoresearch": CommandType.AUTORESEARCH,
    "query_components": CommandType.QUERY_WORLD,
    "query_archetype": CommandType.QUERY_WORLD,
    "list_signatures": CommandType.LIST_SIGNATURES,
    "add_resource": CommandType.ADD_RESOURCE,
    "add_hook": CommandType.ADD_HOOK,
    "remove_hook": CommandType.REMOVE_HOOK,
    "list_processors": CommandType.LIST_PROCESSORS,
    "list_hooks": CommandType.LIST_HOOKS,
    "list_resources": CommandType.LIST_RESOURCES,
    "get_audit_history": CommandType.GET_AUDIT_HISTORY,
    "submit_spawn": CommandType.SPAWN,
}

_DYNAMIC_GATE_METHODS = {
    "submit": "_gate",
    "submit_batch": "_gate_batch",
}

_OUTBOX_AUDITED_METHODS = {"submit", "submit_batch", "submit_spawn"}
_SYNCHRONOUS_GATE_METHODS = {"reserve_entity_ids"}
_GATE_HELPERS = frozenset({"_gate", "_gate_world", "_gate_application"})


def _python_files(path: Path) -> list[Path]:
    return sorted(path.rglob("*.py"))


def _type_checking_ranges(tree: ast.AST) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        is_type_checking = (
            isinstance(test, ast.Name)
            and test.id == "TYPE_CHECKING"
            or isinstance(test, ast.Attribute)
            and test.attr == "TYPE_CHECKING"
        )
        if not is_type_checking or not node.body:
            continue
        start = min(getattr(child, "lineno", node.lineno) for child in node.body)
        end = max(
            getattr(child, "end_lineno", getattr(child, "lineno", node.lineno))
            for child in node.body
        )
        ranges.append((start, end))
    return ranges


def _in_ranges(lineno: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start <= lineno <= end for start, end in ranges)


def _is_app_module(module: str) -> bool:
    return module == "archetype.app" or module.startswith("archetype.app.")


def _runtime_app_import_is_allowed(
    module: str, lineno: int, type_checking_ranges: list[tuple[int, int]]
) -> bool:
    return module in _RUNTIME_ALLOWED_APP_IMPORTS and (
        module not in _RUNTIME_TYPE_ONLY_APP_IMPORTS or _in_ranges(lineno, type_checking_ranges)
    )


def _called_attr_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def _command_type_from_gate_call(call: ast.Call) -> str | None:
    """Extract ``CommandType.NAME`` from a gateway helper call."""
    if _called_attr_name(call) not in _GATE_HELPERS or not call.args:
        return None
    command_call = call.args[0]
    if not isinstance(command_call, ast.Call):
        return None
    return _command_type_from_command_call(command_call)


def _command_type_from_command_call(command_call: ast.Call) -> str | None:
    for keyword in command_call.keywords:
        if keyword.arg != "type":
            continue
        value = keyword.value
        if (
            isinstance(value, ast.Attribute)
            and isinstance(value.value, ast.Name)
            and value.value.id == "CommandType"
        ):
            return value.attr
    return None


def _assigned_command_type_names(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
) -> dict[str, str]:
    names: dict[str, str] = {}
    for child in ast.walk(node):
        if isinstance(child, ast.Assign):
            value = child.value
            if not isinstance(value, ast.Call):
                continue
            command_type = _command_type_from_command_call(value)
            if command_type is None:
                continue
            for target in child.targets:
                if isinstance(target, ast.Name):
                    names[target.id] = command_type
        elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            value = child.value
            if not isinstance(value, ast.Call):
                continue
            command_type = _command_type_from_command_call(value)
            if command_type is not None:
                names[child.target.id] = command_type
    return names


def task_spec_manifest_traceability() -> list[GraderResult]:
    """Every spec eval cites a normative source and anchor text."""
    checks: dict[str, bool] = {}
    for case in SPEC_CASES:
        source = DOCS / case.source
        text = source.read_text() if source.exists() else ""
        checks[f"{case.spec_id}:source_exists"] = source.exists()
        for anchor in case.anchors:
            checks[f"{case.spec_id}:anchor:{anchor}"] = anchor in text

    return [
        state_check(checks, name="spec_sources_and_anchors"),
        exact_match(
            _EXPECTED_TASK_IDS - frozenset(_registered_task_ids()),
            frozenset(),
            name="task_ids_registered",
        ),
    ]


def task_role_permission_matrix() -> list[GraderResult]:
    """The code permission matrix exactly matches command-gate.md."""
    actual = {role: frozenset(commands) for role, commands in COMMANDS_BY_ROLE.items()}
    explicit_non_admin_review = all(
        command in actual["admin"]
        and (
            command in actual["viewer"]
            or command in actual["player"]
            or command in actual["operator"]
            or command == CommandType.CREATE_WORLD
        )
        for command in CommandType
    )
    return [
        exact_match(actual, _EXPECTED_ROLE_MATRIX, name="exact_role_matrix"),
        exact_match(actual["admin"], frozenset(CommandType), name="admin_auto_includes_all"),
        exact_match(explicit_non_admin_review, True, name="non_admin_commands_reviewed"),
    ]


def task_runtime_gate_only_boundary() -> list[GraderResult]:
    """Runtime depends on the application port and stores no live world refs."""
    runtime_dir = SRC / "runtime"
    import_checks: dict[str, bool] = {}
    world_ref_checks: dict[str, bool] = {}

    for py in _python_files(runtime_dir):
        tree = ast.parse(py.read_text(), filename=str(py))
        tc_ranges = _type_checking_ranges(tree)
        rel = py.relative_to(ROOT)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if _is_app_module(node.module):
                    key = f"{rel}:{node.lineno}:allowed_app_import:{node.module}"
                    import_checks[key] = _runtime_app_import_is_allowed(
                        node.module, node.lineno, tc_ranges
                    )
                for alias in node.names:
                    if alias.name in {"iWorld", "AsyncWorld"}:
                        key = f"{rel}:{node.lineno}:world_import:{alias.name}"
                        world_ref_checks[key] = _in_ranges(node.lineno, tc_ranges)

            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_app_module(alias.name):
                        key = f"{rel}:{node.lineno}:allowed_app_import:{alias.name}"
                        import_checks[key] = _runtime_app_import_is_allowed(
                            alias.name, node.lineno, tc_ranges
                        )
                    if alias.name in {"iWorld", "AsyncWorld"}:
                        key = f"{rel}:{node.lineno}:world_import:{alias.name}"
                        world_ref_checks[key] = _in_ranges(node.lineno, tc_ranges)

            if isinstance(node, ast.AnnAssign):
                target = ast.unparse(node.target)
                annotation = ast.unparse(node.annotation)
                if "iWorld" in annotation or "AsyncWorld" in annotation:
                    world_ref_checks[f"{rel}:{node.lineno}:annotation:{target}"] = False

    return [
        state_check(import_checks or {"no_app_imports": True}, name="runtime_app_imports"),
        state_check(world_ref_checks or {"no_world_refs": True}, name="runtime_no_live_world_refs"),
    ]


def task_command_gateway_gate_map() -> list[GraderResult]:
    """Every public gate method has the expected command type and audit emit."""
    path = SRC / "app" / "gateway" / "service.py"
    tree = ast.parse(path.read_text(), filename=str(path))
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }

    checks: dict[str, bool] = {}
    for method, command_type in _COMMAND_GATE_MAP.items():
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue

        calls = [call for call in ast.walk(node) if isinstance(call, ast.Call)]
        gate_calls = [call for call in calls if _called_attr_name(call) in _GATE_HELPERS]
        emit_calls = [call for call in calls if _called_attr_name(call) == "_emit"]
        assigned_types = _assigned_command_type_names(node)
        gate_type_names: list[str] = []
        for call in gate_calls:
            if name := _command_type_from_gate_call(call):
                gate_type_names.append(name)
            elif call.args and isinstance(call.args[0], ast.Name):
                if name := assigned_types.get(call.args[0].id):
                    gate_type_names.append(name)
        checks[f"{method}:gate_type"] = command_type.name in gate_type_names
        if method not in _OUTBOX_AUDITED_METHODS | _SYNCHRONOUS_GATE_METHODS:
            checks[f"{method}:emits_audit"] = bool(emit_calls)
        if gate_calls and emit_calls:
            checks[f"{method}:gate_before_emit"] = min(c.lineno for c in gate_calls) < min(
                c.lineno for c in emit_calls
            )

    for method, gate_method in _DYNAMIC_GATE_METHODS.items():
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue
        calls = [call for call in ast.walk(node) if isinstance(call, ast.Call)]
        checks[f"{method}:has_dynamic_gate"] = any(
            _called_attr_name(call) == gate_method for call in calls
        )
        checks[f"{method}:ledger_is_audit_authority"] = method in _OUTBOX_AUDITED_METHODS

    return [state_check(checks, name="command_gateway_gate_shape")]


def task_append_only_protocols() -> list[GraderResult]:
    """Storage and audit protocols expose no destructive methods."""
    from archetype.app.audit.interfaces import iAuditLog
    from archetype.core.interfaces import iAsyncStore

    destructive = ("delete", "drop", "truncate")

    def _protocol_ok(cls: type) -> bool:
        method_names = {
            name
            for name, value in cls.__dict__.items()
            if not name.startswith("_") and callable(value)
        }
        return not any(any(term in name.lower() for term in destructive) for name in method_names)

    return [
        exact_match(_protocol_ok(iAsyncStore), True, name="iAsyncStore_append_only"),
        exact_match(_protocol_ok(iAuditLog), True, name="iAuditLog_append_only"),
    ]


def task_info_class_downgrades() -> list[GraderResult]:
    """The gate returns immutable info snapshots instead of live objects."""
    return asyncio.run(_task_info_class_downgrades())


async def _task_info_class_downgrades() -> list[GraderResult]:
    reset_daily_tokens()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    graph: Any = _FakeWorldGraph()
    unused: Any = _UnusedService()
    application = RuntimeApplication(
        registry=graph,
        lifecycle=graph,
        storage=unused,
        commands=unused,
        audit=None,
    )
    service = CommandGateway(application, audit=None, target_tick_for_world=graph.target_tick)

    try:
        created = await service.create_world(ctx, WorldConfig(name="spec-info"))
        forked = await service.fork_world(ctx, created.world_id, "spec-fork")
        fetched = await service.get_world_info(ctx, created.world_id)
        worlds = await service.list_worlds(ctx)
        processors = await service.list_processors(ctx, created.world_id)
        hooks = await service.list_hooks(ctx, created.world_id)
        resources = await service.list_resources(ctx, created.world_id)
    finally:
        reset_daily_tokens()

    info_values = [created, forked, fetched, *worlds, *processors, *hooks, *resources]
    type_checks = {
        "create_world_returns_world_info": isinstance(created, WorldInfo),
        "fork_world_returns_world_info": isinstance(forked, WorldInfo),
        "get_world_info_returns_world_info": isinstance(fetched, WorldInfo),
        "list_worlds_downgrades": all(isinstance(item, WorldInfo) for item in worlds),
        "list_processors_downgrades": all(isinstance(item, ProcessorInfo) for item in processors),
        "list_hooks_downgrades": all(isinstance(item, HookInfo) for item in hooks),
        "list_resources_downgrades": all(isinstance(item, ResourceInfo) for item in resources),
        "no_live_world_escape": all(not isinstance(item, _FakeWorld) for item in info_values),
        "models_are_frozen": all(_is_frozen_model(item) for item in info_values),
    }
    return [state_check(type_checks, name="info_downgrade_types")]


def _is_frozen_model(value: Any) -> bool:
    fields = getattr(value.__class__, "model_fields", {})
    if not fields:
        return False
    field_name = next(iter(fields))
    try:
        setattr(value, field_name, getattr(value, field_name))
    except Exception:
        return True
    return False


class _FakeWorld:
    def __init__(self, name: str = "world") -> None:
        self.world_id = uuid7()
        self.name = name
        self.tick = 7
        self.run_id = uuid7()
        self.system = SimpleNamespace(processors=[_FakeProcessor()])
        self.hooks = _FakeHooks()
        self.resources = _FakeResources()


class _FakeProcessor:
    priority = 11
    components = ()


class _FakeResource:
    pass


class _FakeEvent:
    pass


class _FakeHookHandle:
    _event_type = _FakeEvent
    _id = 101

    # Public accessors mirroring core HookHandle's contract.
    @property
    def id(self):
        return self._id

    @property
    def event_type(self):
        return self._event_type


def _fake_handler() -> None:
    pass


class _FakeHooks:
    def items(self):
        return [(_FakeEvent, _FakeHookHandle(), _fake_handler, "blocking")]


class _FakeResources:
    def items(self):
        return [(_FakeResource, _FakeResource())]


class _FakeWorldGraph:
    def __init__(self) -> None:
        self.created = _FakeWorld("spec-info")
        self.forked = _FakeWorld("spec-fork")

    async def create_world(self, config, storage_config=None, cache_config=None):
        self.created.name = config.name
        return self.created

    async def fork_world(self, source_world_id, name=None, storage_config=None, cache_config=None):
        self.forked.name = name or "spec-fork"
        return self.forked

    @asynccontextmanager
    async def operation(self, world_id):
        worlds = {str(world.world_id): world for world in (self.created, self.forked)}
        yield worlds[str(world_id)]

    async def list_worlds(self):
        return [self.created, self.forked]

    def target_tick(self, world_id):
        worlds = {str(world.world_id): world for world in (self.created, self.forked)}
        return worlds[str(world_id)].tick


class _UnusedService:
    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected call to unused fake service: {name}")


def _registered_task_ids() -> list[str]:
    harness = EvalHarness()
    register(harness)
    return [task_id for task_id, _, _, _ in harness.registered_tasks]


def task_receipt_authority_firewall() -> list[GraderResult]:
    """Evaluation results and artifact references carry no authority.

    The non-negotiable boundary (issue #275): no field on a persisted
    result/reference may name an authority decision. A PASS means one
    grader passed under one pinned contract — the layer above owns meaning.
    """
    from archetype.artifacts import ArtifactRef
    from archetype.evaluation.components import EvalReceipt

    forbidden = {
        "accepted",
        "accept",
        "approved",
        "approve",
        "promote",
        "promoted",
        "allowed_next_action",
        "authorized",
        "authorize",
        "permitted",
    }
    checks = {}
    for model in (EvalReceipt, ArtifactRef):
        fields = {name.lower() for name in model.model_fields}
        checks[f"{model.__name__}_carries_no_authority"] = not (fields & forbidden)
    return [state_check(checks, name="receipt_authority_firewall")]


def task_dataset_eval_ontology() -> list[GraderResult]:
    """Typed vocabulary preserves dataset identity and runtime provenance."""
    from dataclasses import fields, is_dataclass
    from typing import get_type_hints

    from archetype.evaluation import contracts as defs

    task = defs.TaskRef(benchmark="libero", suite="libero_spatial", task_key="3")
    episode = defs.EpisodeRef(benchmark="libero", episode_id=17)
    runtime = defs.RuntimeSlice(
        world_id="world-7",
        run_id="run-9",
        entity_id=12,
        start_tick=0,
        final_tick=41,
    )
    trial = defs.Trial(task=task, seed=5, episode=episode, runtime=runtime)
    rubric = defs.Rubric(graders=(defs.Grader(name="success", kind=defs.GraderKind.CHECK),))
    evaluation = defs.Eval(task=task, rubric=rubric)
    vocabulary = (
        defs.TaskRef,
        defs.EpisodeRef,
        defs.RuntimeSlice,
        defs.Grader,
        defs.Rubric,
        defs.Eval,
        defs.Trial,
    )
    episode_hints = get_type_hints(defs.EpisodeRef)

    checks = {
        "task_natural_key_fields": [field.name for field in fields(defs.TaskRef)]
        == ["benchmark", "suite", "task_key"],
        "episode_natural_key_fields": [field.name for field in fields(defs.EpisodeRef)]
        == ["benchmark", "episode_id"],
        "episode_id_is_integer": episode_hints["episode_id"] is int,
        "runtime_slice_fields": [field.name for field in fields(defs.RuntimeSlice)]
        == ["world_id", "run_id", "entity_id", "start_tick", "final_tick"],
        "dataset_coordinates_are_independent": trial.dataset_coordinates
        == ("libero", "libero_spatial", "3", 17),
        "runtime_provenance_is_preserved": trial.runtime == runtime,
        "trial_produces_one_episode": trial.episode is episode,
        "reader_runtime_is_optional": defs.Trial(task=task, seed=5, episode=episode).runtime
        is None,
        "eval_binds_one_task": evaluation.task is task,
        "rubric_is_non_empty": evaluation.rubric.graders == rubric.graders,
        "grader_kinds_are_exact": {kind.value for kind in defs.GraderKind}
        == {"check", "test", "judge"},
        "vocabulary_is_frozen": all(
            is_dataclass(item) and item.__dataclass_params__.frozen for item in vocabulary
        ),
    }
    return [state_check(checks, name="dataset_eval_ontology")]


def register(harness: EvalHarness) -> None:
    harness.add(
        "spec.manifest_traceability",
        suite=SUITE,
        fn=task_spec_manifest_traceability,
        desc="Spec cases cite normative docs and registered eval tasks.",
    )
    harness.add(
        "spec.role_permission_matrix",
        suite=SUITE,
        fn=task_role_permission_matrix,
        desc="Role permissions match command-gate.md exactly.",
    )
    harness.add(
        "spec.runtime_gate_only_boundary",
        suite=SUITE,
        fn=task_runtime_gate_only_boundary,
        desc="Runtime depends on RuntimeApplication-facing ports and stores no live world refs.",
    )
    harness.add(
        "spec.command_gateway_gate_map",
        suite=SUITE,
        fn=task_command_gateway_gate_map,
        desc="CommandGateway public methods use the documented gate and audit shape.",
    )
    harness.add(
        "spec.append_only_protocols",
        suite=SUITE,
        fn=task_append_only_protocols,
        desc="Storage and audit protocols expose no destructive delete/drop methods.",
    )
    harness.add(
        "spec.receipt_authority_firewall",
        suite=SUITE,
        fn=task_receipt_authority_firewall,
        desc="Evaluation evidence and artifact references carry no authority fields.",
    )
    harness.add(
        "spec.dataset_eval_ontology",
        suite=SUITE,
        fn=task_dataset_eval_ontology,
        desc="Dataset identity remains separate from optional runtime provenance.",
    )
    harness.add(
        "spec.info_class_downgrades",
        suite=SUITE,
        fn=task_info_class_downgrades,
        desc="Gate lifecycle and introspection methods return frozen info snapshots.",
    )
