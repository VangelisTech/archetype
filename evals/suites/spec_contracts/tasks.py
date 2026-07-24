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
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")

from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.commands.policy import PERMISSIONS_BY_ROLE
from archetype.core.config import StorageConfig, WorldConfig
from archetype.core.hooks import PreTick
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
        anchors=("Four roles and permissions", "`list_worlds`"),
        task_id="spec.role_permission_matrix",
    ),
    SpecCase(
        spec_id="command-gate.1",
        source="command-gate.md",
        anchors=(
            "policy and admission",
            "Pure role denial happens before",
            "access evidence",
        ),
        task_id="spec.command_gateway_gate_map",
    ),
    SpecCase(
        spec_id="world-lifecycle.7",
        source="world-lifecycle.md",
        anchors=("Boundary-safe information", "downgrade it to frozen values"),
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
        anchors=("Append-only invariant", "`AuditLog` has no delete or drop operation"),
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

_EXPECTED_ROLE_MATRIX: dict[str, frozenset[str]] = {
    "viewer": frozenset(
        {
            "discover_worlds",
            "get_audit_history",
            "get_world_info",
            "list_hooks",
            "list_processors",
            "list_resources",
            "list_signatures",
            "list_worlds",
            "open_world_readonly",
            "query_archetype",
            "query_artifacts",
            "query_components",
        }
    ),
    "player": frozenset(
        {
            "create_entities",
            "despawn",
            "discover_worlds",
            "get_audit_history",
            "get_world_info",
            "list_hooks",
            "list_processors",
            "list_resources",
            "list_signatures",
            "list_worlds",
            "open_world_readonly",
            "query_archetype",
            "query_artifacts",
            "query_components",
            "spawn",
            "update",
        }
    ),
    "operator": frozenset(
        {
            "add_components",
            "add_hook",
            "add_processor",
            "add_resource",
            "autoresearch",
            "create_entities",
            "despawn",
            "destroy_world",
            "discover_worlds",
            "evaluate",
            "fork_world",
            "get_audit_history",
            "get_world_info",
            "ingest_artifacts",
            "list_hooks",
            "list_processors",
            "list_resources",
            "list_signatures",
            "list_worlds",
            "open_world_readonly",
            "query_archetype",
            "query_artifacts",
            "query_components",
            "remove_components",
            "remove_hook",
            "remove_processor",
            "run",
            "run_episode",
            "run_rollout",
            "spawn",
            "step",
            "update",
        }
    ),
    "admin": frozenset(
        {
            "add_components",
            "add_hook",
            "add_processor",
            "add_resource",
            "autoresearch",
            "create_entities",
            "create_world",
            "despawn",
            "destroy_world",
            "discover_worlds",
            "evaluate",
            "fork_world",
            "get_audit_history",
            "get_world_info",
            "ingest_artifacts",
            "list_hooks",
            "list_processors",
            "list_resources",
            "list_signatures",
            "list_worlds",
            "open_world_readonly",
            "query_archetype",
            "query_artifacts",
            "query_components",
            "remove_components",
            "remove_hook",
            "remove_processor",
            "resume_world",
            "run",
            "run_episode",
            "run_rollout",
            "spawn",
            "step",
            "update",
        }
    ),
}

_COMMAND_GATE_MAP: dict[str, tuple[tuple[str, str], ...]] = {
    "create_entity": (("Spawn", "spawn"),),
    "create_entities": (("CreateEntities", "create_entities"),),
    "reserve_entity_ids": (("ReserveEntityIds", "spawn"),),
    "spawn_with_reserved_id": (("SpawnReserved", "spawn"),),
    "remove_entity": (("Despawn", "despawn"),),
    "update_entity": (("Update", "update"),),
    "add_components": (("AddComponents", "add_components"),),
    "remove_components": (("RemoveComponents", "remove_components"),),
    "add_processor": (("AddProcessor", "add_processor"),),
    "remove_processor": (("RemoveProcessor", "remove_processor"),),
    "create_world": (("CreateWorld", "create_world"),),
    "fork_world": (("ForkWorld", "fork_world"),),
    "destroy_world": (("DestroyWorld", "destroy_world"),),
    "get_world_info": (("GetWorldInfo", "get_world_info"),),
    "list_worlds": (("ListWorlds", "list_worlds"),),
    "discover_worlds": (("DiscoverWorlds", "discover_worlds"),),
    "open_world_readonly": (("OpenWorldReadonly", "open_world_readonly"),),
    "resume_world": (("ResumeWorld", "resume_world"),),
    "step": (("Step", "step"),),
    "run": (("Run", "run"),),
    "run_episode": (("RunEpisode", "run_episode"),),
    "run_rollout": (("RunRollout", "run_rollout"),),
    "query_components": (("QueryComponents", "query_components"),),
    "query_archetype": (("QueryArchetype", "query_archetype"),),
    "list_signatures": (
        ("ListSignatures", "list_signatures"),
        ("ListWorldSignatures", "list_signatures"),
    ),
    "get_audit_history": (("GetAuditHistory", "get_audit_history"),),
    "add_resource": (("AddResource", "add_resource"),),
    "add_hook": (("AddHook", "add_hook"),),
    "remove_hook": (("RemoveHook", "remove_hook"),),
    "list_processors": (("ListProcessors", "list_processors"),),
    "list_hooks": (("ListHooks", "list_hooks"),),
    "list_resources": (("ListResources", "list_resources"),),
}

_BRIDGE_GATE_MAP = {
    "autoresearch": "autoresearch",
    "ingest_artifacts": "ingest_artifacts",
    "query_artifacts": "query_artifacts",
    "evaluate": "evaluate",
}

_DEFERRED_GATE_MAP = {
    "submit": "defer_as",
    "submit_batch": "defer_batch_as",
    "submit_spawn": "defer_spawn_as",
}


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
    actual = {role: frozenset(permissions) for role, permissions in PERMISSIONS_BY_ROLE.items()}
    all_permissions = frozenset().union(*actual.values())
    explicit_review = all(
        permission in _EXPECTED_ROLE_MATRIX["admin"] for permission in all_permissions
    )
    return [
        exact_match(actual, _EXPECTED_ROLE_MATRIX, name="exact_role_matrix"),
        exact_match(
            actual["admin"],
            _EXPECTED_ROLE_MATRIX["admin"],
            name="admin_exact_finite_permissions",
        ),
        exact_match(explicit_review, True, name="all_permissions_explicitly_reviewed"),
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
    """Gateway methods construct exact models and enter the shared dispatcher."""
    path = SRC / "app" / "gateway" / "service.py"
    tree = ast.parse(path.read_text(), filename=str(path))
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }
    container = ServiceContainer()
    try:
        registered_permissions = {
            spec.model.__name__: spec.permission for spec in container.operation_registry.specs
        }
    finally:
        asyncio.run(container.shutdown())

    checks: dict[str, bool] = {}
    for method, expected_models in _COMMAND_GATE_MAP.items():
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue

        calls = [call for call in ast.walk(node) if isinstance(call, ast.Call)]
        constructed_models: set[str] = set()
        for call in calls:
            if isinstance(call.func, ast.Name):
                constructed_models.add(call.func.id)
            elif (
                isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.attr.startswith("from_")
            ):
                constructed_models.add(call.func.value.id)
        called_methods = {name for call in calls if (name := _called_attr_name(call)) is not None}
        checks[f"{method}:dispatcher_entry"] = "apply_as" in called_methods
        for model_name, permission in expected_models:
            checks[f"{method}:constructs:{model_name}"] = model_name in constructed_models
            checks[f"{method}:registered_permission:{model_name}"] = (
                registered_permissions.get(model_name) == permission
            )
        checks[f"{method}:no_legacy_gate_or_emit"] = not (
            {"_gate", "_gate_batch", "_emit", "guardrail_allow"} & called_methods
        )

    for method, operation in _BRIDGE_GATE_MAP.items():
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue
        calls = [call for call in ast.walk(node) if isinstance(call, ast.Call)]
        bridge_calls = [call for call in calls if _called_attr_name(call) == "_run_bridge_world"]
        checks[f"{method}:bridge_entry"] = len(bridge_calls) == 1
        checks[f"{method}:exact_bridge_permission"] = any(
            any(
                keyword.arg == "operation"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == operation
                for keyword in call.keywords
            )
            for call in bridge_calls
        )

    for method, dispatcher_entry in _DEFERRED_GATE_MAP.items():
        node = functions.get(method)
        checks[f"{method}:exists"] = node is not None
        if node is None:
            continue
        called_methods = {
            name
            for call in ast.walk(node)
            if isinstance(call, ast.Call) and (name := _called_attr_name(call)) is not None
        }
        checks[f"{method}:dispatcher_entry"] = dispatcher_entry in called_methods
        checks[f"{method}:no_legacy_gate_or_emit"] = not (
            {"_gate", "_gate_batch", "_emit", "guardrail_allow"} & called_methods
        )

    return [state_check(checks, name="command_gateway_dispatch_shape")]


def task_append_only_protocols() -> list[GraderResult]:
    """Storage protocol and commands-owned audit expose no destructive methods."""
    from archetype.commands.audit import AuditLog
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
        exact_match(_protocol_ok(AuditLog), True, name="AuditLog_append_only"),
    ]


def task_info_class_downgrades() -> list[GraderResult]:
    """The gate returns immutable info snapshots instead of live objects."""
    return asyncio.run(_task_info_class_downgrades())


async def _task_info_class_downgrades() -> list[GraderResult]:
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            created = await container.command_gateway.create_world(
                ctx,
                WorldConfig(name="spec-info"),
                StorageConfig(uri=f"{tmp}/store", namespace="spec_info"),
            )
            await container.command_gateway.add_processor(
                ctx,
                created.world_id,
                _FakeProcessor(),
            )
            await container.command_gateway.add_resource(
                ctx,
                created.world_id,
                _FakeResource(),
            )
            await container.command_gateway.add_hook(
                ctx,
                created.world_id,
                PreTick,
                _fake_handler,
            )
            forked = await container.command_gateway.fork_world(
                ctx,
                created.world_id,
                "spec-fork",
            )
            fetched = await container.command_gateway.get_world_info(
                ctx,
                created.world_id,
            )
            worlds = await container.command_gateway.list_worlds(ctx)
            processors = await container.command_gateway.list_processors(
                ctx,
                created.world_id,
            )
            hooks = await container.command_gateway.list_hooks(ctx, created.world_id)
            resources = await container.command_gateway.list_resources(
                ctx,
                created.world_id,
            )
        finally:
            await container.shutdown()

    info_values = [created, forked, fetched, *worlds, *processors, *hooks, *resources]
    type_checks = {
        "create_world_returns_world_info": isinstance(created, WorldInfo),
        "fork_world_returns_world_info": isinstance(forked, WorldInfo),
        "get_world_info_returns_world_info": isinstance(fetched, WorldInfo),
        "list_worlds_downgrades": all(isinstance(item, WorldInfo) for item in worlds),
        "list_processors_downgrades": all(isinstance(item, ProcessorInfo) for item in processors),
        "list_hooks_downgrades": all(isinstance(item, HookInfo) for item in hooks),
        "list_resources_downgrades": all(isinstance(item, ResourceInfo) for item in resources),
        "nonempty_introspection": bool(processors and hooks and resources),
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


class _FakeProcessor:
    priority = 11
    components = ()


class _FakeResource:
    pass


async def _fake_handler(_event: PreTick) -> None:
    pass


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
