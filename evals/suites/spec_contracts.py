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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")

from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.auth.permissions import COMMANDS_BY_ROLE
from archetype.app.gateway.gate import CommandService
from archetype.app.models import (
    CommandType,
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    WorldInfo,
)
from archetype.core.config import WorldConfig
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "spec"

ROOT = Path(__file__).resolve().parents[2]
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
        anchors=("R1", "Single-gate enforcement", "imports from `archetype.app` ONLY"),
        task_id="spec.runtime_gate_only_boundary",
    ),
    SpecCase(
        spec_id="runtime.R2",
        source="runtime.md",
        anchors=("R2", "Handles hold `world_id`, never `iWorld`"),
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
        task_id="spec.command_service_gate_map",
    ),
    SpecCase(
        spec_id="world-lifecycle.6",
        source="world-lifecycle.md",
        anchors=("info-class downgrade", "Live objects"),
        task_id="spec.info_class_downgrades",
    ),
    SpecCase(
        spec_id="audit-log.2",
        source="audit-log.md",
        anchors=("Append-only invariant", "no `drop_*` or `delete_*` methods"),
        task_id="spec.append_only_protocols",
    ),
)

_EXPECTED_TASK_IDS = frozenset(case.task_id for case in SPEC_CASES)

_RUNTIME_ALLOWED_APP_IMPORTS = frozenset(
    {
        "archetype.app.gateway.gate",
        "archetype.app.container",
        "archetype.app.models",
        "archetype.app.auth.models",
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
            CommandType.FORK_WORLD,
            CommandType.DESTROY_WORLD,
        }
    ),
    "admin": frozenset(CommandType),
}

_COMMAND_GATE_MAP: dict[str, CommandType] = {
    "create_entity": CommandType.SPAWN,
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
    "step": CommandType.STEP,
    "run": CommandType.RUN,
    "run_episode": CommandType.RUN_EPISODE,
    "run_rollout": CommandType.RUN_ROLLOUT,
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

_DYNAMIC_GATE_METHODS = frozenset({"submit", "submit_batch"})


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


def _called_attr_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def _command_type_from_gate_call(call: ast.Call) -> str | None:
    """Extract ``CommandType.NAME`` from ``self._gate(Command(type=...))``."""
    if _called_attr_name(call) != "_gate" or not call.args:
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


def _assigned_command_type_names(node: ast.AsyncFunctionDef) -> dict[str, str]:
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
    """Runtime imports only allowed app modules and stores no live world refs."""
    runtime_dir = SRC / "runtime"
    import_checks: dict[str, bool] = {}
    world_ref_checks: dict[str, bool] = {}

    for py in _python_files(runtime_dir):
        tree = ast.parse(py.read_text(), filename=str(py))
        tc_ranges = _type_checking_ranges(tree)
        rel = py.relative_to(ROOT)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("archetype.app"):
                    key = f"{rel}:{node.lineno}:allowed_app_import:{node.module}"
                    import_checks[key] = node.module in _RUNTIME_ALLOWED_APP_IMPORTS
                for alias in node.names:
                    if alias.name in {"iWorld", "AsyncWorld"}:
                        key = f"{rel}:{node.lineno}:world_import:{alias.name}"
                        world_ref_checks[key] = _in_ranges(node.lineno, tc_ranges)

            if isinstance(node, ast.Import):
                for alias in node.names:
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


def task_command_service_gate_map() -> list[GraderResult]:
    """Every public gate method has the expected command type and audit emit."""
    path = SRC / "app" / "gateway" / "gate.py"
    tree = ast.parse(path.read_text(), filename=str(path))
    functions = {
        node.name: node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
    }

    checks: dict[str, bool] = {}
    for method, command_type in _COMMAND_GATE_MAP.items():
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue

        calls = [call for call in ast.walk(node) if isinstance(call, ast.Call)]
        gate_calls = [call for call in calls if _called_attr_name(call) == "_gate"]
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
        checks[f"{method}:emits_audit"] = bool(emit_calls)
        if gate_calls and emit_calls:
            checks[f"{method}:gate_before_emit"] = min(c.lineno for c in gate_calls) < min(
                c.lineno for c in emit_calls
            )

    for method in _DYNAMIC_GATE_METHODS:
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue
        calls = [call for call in ast.walk(node) if isinstance(call, ast.Call)]
        checks[f"{method}:has_dynamic_gate"] = any(
            _called_attr_name(call) == "_gate" for call in calls
        )
        checks[f"{method}:emits_audit"] = any(_called_attr_name(call) == "_emit" for call in calls)

    return [state_check(checks, name="command_service_gate_shape")]


def task_append_only_protocols() -> list[GraderResult]:
    """Storage and audit protocols expose no destructive methods."""
    from archetype.app.interfaces import iAuditLog
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
    reset_tick_counters()
    reset_daily_tokens()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    service = CommandService(
        mutations=_UnusedService(),
        worlds=_FakeWorldService(),
        simulation=_UnusedService(),
        queries=_UnusedService(),
        broker=_UnusedService(),
        audit=None,
    )

    try:
        created = await service.create_world(ctx, WorldConfig(name="spec-info"))
        forked = await service.fork_world(ctx, created.world_id, "spec-fork")
        fetched = await service.get_world_info(ctx, created.world_id)
        worlds = await service.list_worlds(ctx)
        processors = await service.list_processors(ctx, created.world_id)
        hooks = await service.list_hooks(ctx, created.world_id)
        resources = await service.list_resources(ctx, created.world_id)
    finally:
        reset_tick_counters()
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


class _FakeWorldService:
    def __init__(self) -> None:
        self.created = _FakeWorld("spec-info")
        self.forked = _FakeWorld("spec-fork")

    async def create_world(self, config, storage_config=None, cache_config=None):
        self.created.name = config.name
        return self.created

    async def fork_world(self, source_world_id, name=None, storage_config=None, cache_config=None):
        self.forked.name = name
        return self.forked

    def get_world(self, world_id):
        return self.created

    def list_worlds(self):
        return [self.created, self.forked]

    def list_processors(self, world_id):
        return [_FakeProcessor()]

    def list_hooks(self, world_id):
        # Hook-bus items() rows: (event_type, handle, fn, mode)
        return [(_FakeEvent, _FakeHookHandle(), _fake_handler, "blocking")]

    def list_resources(self, world_id):
        return [(_FakeResource, _FakeResource())]


class _UnusedService:
    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected call to unused fake service: {name}")


def _registered_task_ids() -> list[str]:
    harness = EvalHarness()
    register(harness)
    return [task_id for task_id, _, _, _ in harness._tasks]


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
        desc="Runtime imports only the gate-facing app modules and stores no live world refs.",
    )
    harness.add(
        "spec.command_service_gate_map",
        suite=SUITE,
        fn=task_command_service_gate_map,
        desc="CommandService public methods use the documented gate and audit shape.",
    )
    harness.add(
        "spec.append_only_protocols",
        suite=SUITE,
        fn=task_append_only_protocols,
        desc="Storage and audit protocols expose no destructive delete/drop methods.",
    )
    harness.add(
        "spec.info_class_downgrades",
        suite=SUITE,
        fn=task_info_class_downgrades,
        desc="Gate lifecycle and introspection methods return frozen info snapshots.",
    )
