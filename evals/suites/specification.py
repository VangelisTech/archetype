# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Specification regression evals.

These tasks pin contract-level invariants from ``docs/guide/specification.md``
and the focused normative docs. They are deliberately code-based so they can
run in CI without model credentials.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from daft import DataFrame
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.auth.permissions import COMMANDS_BY_ROLE, ROLES_BY_COMMAND
from archetype.app.container import ServiceContainer
from archetype.app.models import CommandType, HookInfo, ProcessorInfo, ResourceInfo, WorldInfo
from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_lancedb_store import AsyncLancedbStore
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_store import AsyncStore
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig
from archetype.core.hooks import PreTick
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

if TYPE_CHECKING:
    from collections.abc import Iterable


SUITE = "regression"
_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _ROOT / "src" / "archetype"
_RUNTIME_DIR = _SRC_ROOT / "runtime"

_RUNTIME_ALLOWED_APP_IMPORTS = frozenset(
    {
        "archetype.app.command_service",
        "archetype.app.container",
        "archetype.app.models",
        "archetype.app.auth.models",
    }
)
_RUNTIME_FORBIDDEN_WORLD_NAMES = frozenset({"iWorld", "AsyncWorld"})


class _SpecPosition(Component):
    x: int = 0


class _SpecProcessor(AsyncProcessor):
    components = (_SpecPosition,)
    priority = 7

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df


class _SpecResource:
    pass


def _python_files(directory: Path) -> list[Path]:
    return sorted(directory.rglob("*.py"))


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
        start = node.body[0].lineno
        end = max(getattr(child, "end_lineno", child.lineno) for child in node.body)
        ranges.append((start, end))
    return ranges


def _in_ranges(lineno: int, ranges: Iterable[tuple[int, int]]) -> bool:
    return any(start <= lineno <= end for start, end in ranges)


def _runtime_import_violations() -> list[str]:
    violations: list[str] = []
    for path in _python_files(_RUNTIME_DIR):
        tree = ast.parse(path.read_text(), filename=str(path))
        type_checking_ranges = _type_checking_ranges(tree)

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if (
                    module.startswith("archetype.app")
                    and module not in _RUNTIME_ALLOWED_APP_IMPORTS
                ):
                    violations.append(f"{path.relative_to(_ROOT)}:{node.lineno} imports {module}")
                if not _in_ranges(node.lineno, type_checking_ranges):
                    for alias in node.names:
                        if alias.name in _RUNTIME_FORBIDDEN_WORLD_NAMES:
                            violations.append(
                                f"{path.relative_to(_ROOT)}:{node.lineno} imports {alias.name}"
                            )

            if isinstance(node, ast.Import) and not _in_ranges(node.lineno, type_checking_ranges):
                for alias in node.names:
                    if alias.name in _RUNTIME_FORBIDDEN_WORLD_NAMES:
                        violations.append(
                            f"{path.relative_to(_ROOT)}:{node.lineno} imports {alias.name}"
                        )
    return violations


def task_command_role_contract() -> list[GraderResult]:
    """The implemented role matrix must stay aligned with command-gate.md."""

    reads = frozenset(
        {
            CommandType.QUERY_WORLD,
            CommandType.GET_WORLD_INFO,
            CommandType.GET_AUDIT_HISTORY,
            CommandType.LIST_SIGNATURES,
            CommandType.LIST_PROCESSORS,
            CommandType.LIST_HOOKS,
            CommandType.LIST_RESOURCES,
        }
    )
    player_adds = frozenset(
        {
            CommandType.SPAWN,
            CommandType.DESPAWN,
            CommandType.UPDATE,
            CommandType.MESSAGE,
            CommandType.CUSTOM,
        }
    )
    operator_adds = frozenset(
        {
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
    )
    expected = {
        "viewer": reads,
        "player": reads | player_adds,
        "operator": reads | player_adds | operator_adds,
        "admin": frozenset(CommandType),
    }
    inverse = {
        cmd: frozenset(role for role, commands in COMMANDS_BY_ROLE.items() if cmd in commands)
        for cmd in CommandType
    }
    classified_non_admin = expected["viewer"] | player_adds | operator_adds
    admin_only = frozenset(CommandType) - classified_non_admin

    return [
        exact_match(frozenset(COMMANDS_BY_ROLE), frozenset(expected), name="roles_exact"),
        exact_match(COMMANDS_BY_ROLE, expected, name="matrix_exact"),
        exact_match(COMMANDS_BY_ROLE["admin"], frozenset(CommandType), name="admin_all"),
        exact_match(ROLES_BY_COMMAND, inverse, name="inverse_index_consistent"),
        exact_match(
            admin_only,
            frozenset({CommandType.CREATE_WORLD, CommandType.LIST_WORLDS}),
            name="admin_only_commands",
        ),
    ]


def task_runtime_gate_boundary_static() -> list[GraderResult]:
    """Runtime must route through the command gate and avoid live world leaks."""

    violations = _runtime_import_violations()

    from archetype.runtime.world import RuntimeWorld, _RuntimeWorldState

    annotation_violations: list[str] = []
    for cls in (RuntimeWorld, _RuntimeWorldState):
        for field, hint in getattr(cls, "__annotations__", {}).items():
            hint_text = str(hint)
            if any(forbidden in hint_text for forbidden in _RUNTIME_FORBIDDEN_WORLD_NAMES):
                annotation_violations.append(f"{cls.__name__}.{field}: {hint_text}")

    return [
        exact_match(violations, [], name="runtime_import_boundary"),
        exact_match(annotation_violations, [], name="runtime_annotations_no_live_world"),
    ]


def task_lifecycle_append_only_static() -> list[GraderResult]:
    """Storage and audit protocols must not expose destructive lifecycle verbs."""

    from archetype.app.interfaces import iAuditLog
    from archetype.core.interfaces import iAsyncStore

    forbidden_prefixes = ("delete", "drop", "truncate")
    protocol_violations: list[str] = []
    for cls in (iAsyncStore, iAuditLog):
        for name, value in cls.__dict__.items():
            if name.startswith("_") or not callable(value):
                continue
            if name.lower().startswith(forbidden_prefixes):
                protocol_violations.append(f"{cls.__name__}.{name}")

    concrete_violations: list[str] = []
    for cls in (AsyncStore, AsyncLancedbStore, AsyncCachedStore):
        for name, _value in inspect.getmembers(cls, predicate=callable):
            if name.startswith("_"):
                continue
            if name.lower().startswith(forbidden_prefixes):
                concrete_violations.append(f"{cls.__name__}.{name}")

    source_violations: list[str] = []
    for path in [
        _SRC_ROOT / "app" / "audit_log.py",
        _SRC_ROOT / "core" / "aio" / "async_lancedb_store.py",
        _SRC_ROOT / "core" / "aio" / "async_store.py",
        _SRC_ROOT / "core" / "aio" / "async_cached_store.py",
    ]:
        source = path.read_text().lower()
        if re.search(r"\bdelete\s+from\b|\bdrop\s+table\b|\btruncate\s+table\b", source):
            source_violations.append(str(path.relative_to(_ROOT)))

    return [
        exact_match(protocol_violations, [], name="protocols_append_only"),
        exact_match(concrete_violations, [], name="stores_no_destructive_methods"),
        exact_match(source_violations, [], name="no_destructive_sql"),
    ]


def task_gate_info_downgrade_smoke() -> list[GraderResult]:
    """The command gate returns info snapshots rather than live objects."""

    return asyncio.run(_task_gate_info_downgrade_smoke())


async def _task_gate_info_downgrade_smoke() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            storage = StorageConfig(uri=f"{tmp}/store", namespace="spec_eval")
            info = await container.command_service.create_world(
                ctx,
                WorldConfig(name="spec-info-downgrade"),
                storage,
            )
            await container.command_service.add_processor(ctx, info.world_id, _SpecProcessor())
            await container.command_service.add_resource(ctx, info.world_id, _SpecResource())

            async def on_pre_tick(event) -> None:
                return None

            hook = await container.command_service.add_hook(
                ctx,
                info.world_id,
                PreTick,
                on_pre_tick,
            )

            fetched = await container.command_service.get_world_info(ctx, info.world_id)
            processors = await container.command_service.list_processors(ctx, info.world_id)
            resources = await container.command_service.list_resources(ctx, info.world_id)
            hooks = await container.command_service.list_hooks(ctx, info.world_id)

            return [
                exact_match(isinstance(info, WorldInfo), True, name="create_returns_world_info"),
                exact_match(isinstance(fetched, WorldInfo), True, name="get_returns_world_info"),
                exact_match(hasattr(info, "create_entity"), False, name="world_info_not_live"),
                exact_match(
                    all(isinstance(item, ProcessorInfo) for item in processors),
                    True,
                    name="processor_info_only",
                ),
                exact_match(
                    all(isinstance(item, ResourceInfo) for item in resources),
                    True,
                    name="resource_info_only",
                ),
                exact_match(
                    all(isinstance(item, HookInfo) for item in hooks),
                    True,
                    name="hook_info_only",
                ),
                state_check(
                    {
                        "processor_present": any(
                            item.qualname.endswith("._SpecProcessor") for item in processors
                        ),
                        "resource_present": any(
                            item.qualname.endswith("._SpecResource") for item in resources
                        ),
                        "hook_present": any(item.handle_id == hook._id for item in hooks),
                    },
                    name="info_payloads_preserve_identity",
                ),
            ]
        finally:
            await container.shutdown()


def task_specification_doc_references_resolve() -> list[GraderResult]:
    """Normative docs must not point readers at missing executable contracts."""

    docs = [
        _ROOT / "docs" / "guide" / "specification.md",
        _ROOT / "docs" / "guide" / "contributing.md",
    ]
    referenced_tests: set[str] = set()
    for path in docs:
        for match in re.finditer(r"tests/[A-Za-z0-9_./-]+\.py", path.read_text()):
            referenced_tests.add(match.group(0))

    missing = sorted(ref for ref in referenced_tests if not (_ROOT / ref).exists())
    expected_runtime_contract = "tests/app/test_runtime_contracts.py" in referenced_tests
    stale_sugar_reference = "tests/app/test_sugar_runtime.py" in referenced_tests

    return [
        exact_match(missing, [], name="referenced_test_paths_exist"),
        exact_match(expected_runtime_contract, True, name="runtime_contract_doc_linked"),
        exact_match(stale_sugar_reference, False, name="no_stale_sugar_runtime_link"),
    ]


def register(harness: EvalHarness) -> None:
    """Register specification regression tasks."""

    harness.add(
        "spec_command_role_contract",
        suite=SUITE,
        fn=task_command_role_contract,
        desc="Command role matrix matches command-gate normative contract",
    )
    harness.add(
        "spec_runtime_gate_boundary",
        suite=SUITE,
        fn=task_runtime_gate_boundary_static,
        desc="Runtime imports only the command-gate surface and does not hold live worlds",
    )
    harness.add(
        "spec_lifecycle_append_only",
        suite=SUITE,
        fn=task_lifecycle_append_only_static,
        desc="Lifecycle protocols and stores expose no destructive storage verbs",
    )
    harness.add(
        "spec_gate_info_downgrade",
        suite=SUITE,
        fn=task_gate_info_downgrade_smoke,
        desc="Command gate returns immutable info snapshots, not live objects",
    )
    harness.add(
        "spec_doc_references",
        suite=SUITE,
        fn=task_specification_doc_references_resolve,
        desc="Normative docs reference executable contracts that exist",
    )
