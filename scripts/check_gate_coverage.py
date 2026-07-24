#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Gate-surface coverage audit (deterministic).

Three checks keep the command gate's claim-vs-effect surface honest:

1. **Exact registry coverage** — the composition root must register every
   ``WORLD_OPERATION_TYPES`` model and ``GetAuditHistory`` exactly once, with
   no unreviewed extra model. Durable eligibility must equal
   ``PORTABLE_TICK_OPERATION_TYPES`` exactly. This is the static guard for the
   accepted-then-dropped class (issues #178/#368): reachability and durability
   are properties of one exact model registration, never an enum side table.

2. **Registry-driven scheduler** — the canonical scheduler must contain
   neither a ``CommandType`` reference nor a ``match`` statement. Durable
   materialization resolves the registered exact model and invokes its
   registered materializer.

3. **API error taxonomy** — every exception class defined in the application
   or canonical storage authority must be mapped by
   ``api.errors.raise_api_error`` to a non-500 branch (issue #180:
   ``WorldNotFoundError`` extended ``LookupError``, missed the ``KeyError``
   branch, and fell through to the 500 fallback while tests stayed green).

Run via ``make lint`` (PYTHONPATH=src). Exits non-zero with a specific
message on any drift.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
COMMAND_SCHEDULER = ROOT / "src/archetype/commands/scheduler.py"
API_ERRORS = ROOT / "src/archetype/api/errors.py"

# ── Check 1: exact operation registry ────────────────────────────────────────


async def _unreachable_handler(*_args: object, **_kwargs: object) -> None:
    """Satisfy composition binding without executing any application effect."""
    raise AssertionError("gate coverage only inspects operation registrations")


def _composed_registry() -> Any:
    """Build the real container registry without constructing runtime services."""
    from archetype.app.container import _register_operations
    from archetype.commands.registry import OperationRegistry

    lifecycle = SimpleNamespace(
        create_world=_unreachable_handler,
        discover_worlds=_unreachable_handler,
        open_world_readonly=_unreachable_handler,
        open_world_mutable=_unreachable_handler,
    )
    registry = OperationRegistry()
    register_operations = cast("Any", _register_operations)
    register_operations(
        registry,
        worlds=object(),
        lifecycle=lifecycle,
        storage=object(),
        audit=object(),
        fork_world=_unreachable_handler,
        destroy_world=_unreachable_handler,
    )
    return registry


def _model_label(model: type[Any]) -> str:
    return f"{model.__module__}.{model.__qualname__}"


def check_registry_coverage() -> list[str]:
    """Require one exact registration and one exact durable disposition."""
    from archetype.commands.models import GetAuditHistory
    from archetype.world.models import (
        PORTABLE_TICK_OPERATION_TYPES,
        WORLD_OPERATION_TYPES,
    )

    problems: list[str] = []
    registry = _composed_registry()
    specs = registry.specs
    expected_models = (*WORLD_OPERATION_TYPES, GetAuditHistory)
    actual_models = tuple(spec.model for spec in specs)
    expected_set = set(expected_models)
    actual_set = set(actual_models)

    if len(actual_models) != len(actual_set):
        problems.append("operation registry contains a duplicate exact model registration")

    missing = expected_set - actual_set
    if missing:
        problems.append(
            "operation registry is missing exact models: "
            + ", ".join(sorted(_model_label(model) for model in missing))
        )
    unexpected = actual_set - expected_set
    if unexpected:
        problems.append(
            "operation registry contains unreviewed models: "
            + ", ".join(sorted(_model_label(model) for model in unexpected))
        )
    if actual_set == expected_set and actual_models != expected_models:
        problems.append("operation registry order differs from WORLD_OPERATION_TYPES + audit")

    for spec in specs:
        operation_field = spec.model.model_fields.get("operation")
        expected_name = operation_field.default if operation_field is not None else None
        if spec.name != expected_name:
            problems.append(
                f"registration {spec.name!r} does not match exact model "
                f"{_model_label(spec.model)} discriminator {expected_name!r}"
            )

    expected_durable = set(PORTABLE_TICK_OPERATION_TYPES)
    actual_durable = {spec.model for spec in specs if spec.durable is not None}
    missing_durable = expected_durable - actual_durable
    if missing_durable:
        problems.append(
            "portable models missing durable registration: "
            + ", ".join(sorted(_model_label(model) for model in missing_durable))
        )
    unexpected_durable = actual_durable - expected_durable
    if unexpected_durable:
        problems.append(
            "direct-only models have durable registration: "
            + ", ".join(sorted(_model_label(model) for model in unexpected_durable))
        )
    return problems


# ── Check 2: registry-driven scheduler ───────────────────────────────────────


def check_scheduler_dispatch_shape() -> list[str]:
    """Reject reintroduction of enum or structural-match dispatch."""
    tree = ast.parse(
        COMMAND_SCHEDULER.read_text(encoding="utf-8"),
        filename=str(COMMAND_SCHEDULER),
    )
    problems: list[str] = []
    command_type_lines = sorted(
        {
            node.lineno
            for node in ast.walk(tree)
            if (
                isinstance(node, ast.Name | ast.Attribute)
                and (node.id if isinstance(node, ast.Name) else node.attr) == "CommandType"
            )
            or (isinstance(node, ast.alias) and node.name.rsplit(".", 1)[-1] == "CommandType")
        }
    )
    if command_type_lines:
        problems.append(
            "canonical scheduler references CommandType at lines "
            + ", ".join(str(line) for line in command_type_lines)
        )

    match_lines = sorted(node.lineno for node in ast.walk(tree) if isinstance(node, ast.Match))
    if match_lines:
        problems.append(
            "canonical scheduler contains match dispatch at lines "
            + ", ".join(str(line) for line in match_lines)
        )
    return problems


# ── Check 3: error taxonomy ──────────────────────────────────────────────────
# The whole archetype.app package and the canonical storage authority are
# walked for Exception subclasses. A hardcoded module list would fail open the
# moment errors are defined elsewhere within either governed surface (footgun
# review on PR #407: app/_catalog.py's four exceptions).

ERROR_SURFACE_PACKAGES = ("archetype.app", "archetype.storage")

# Exceptions that deliberately surface as HTTP 500 for now. Every entry needs
# a rationale and an issue; a stale entry (class gone or now mapped) fails
# the audit so the manifest cannot rot.
INTENTIONAL_UNMAPPED = {
    "archetype.storage.catalog.records.CatalogSchemaMismatchError": (
        "integrity violation intentionally surfaces as 500; decision recorded in #413"
    ),
}


def _mapped_exception_bases() -> tuple[type[BaseException], ...]:
    """Exception bases raise_api_error maps to non-500 statuses (AST-derived)."""
    tree = ast.parse(API_ERRORS.read_text(), filename=str(API_ERRORS))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "raise_api_error":
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Name)
                    and inner.func.id == "isinstance"
                    and len(inner.args) == 2
                ):
                    for leaf in ast.walk(inner.args[1]):
                        if isinstance(leaf, ast.Name):
                            names.add(leaf.id)
    api_errors = importlib.import_module("archetype.api.errors")
    bases: list[type[BaseException]] = []
    for name in names:
        obj = getattr(api_errors, name, None) or getattr(builtins, name, None)
        if isinstance(obj, type) and issubclass(obj, BaseException):
            bases.append(obj)
    return tuple(bases)


def _owned_exception_classes() -> dict[str, type[Exception]]:
    """Every Exception subclass defined in a governed API-facing package."""
    import pkgutil

    classes: dict[str, type[Exception]] = {}
    for package_name in ERROR_SURFACE_PACKAGES:
        package = importlib.import_module(package_name)
        # walk_packages yields children only, never the anchor package itself.
        module_names = [package_name]
        module_names.extend(
            module_info.name
            for module_info in pkgutil.walk_packages(
                package.__path__,
                package_name + ".",
            )
        )
        for name in module_names:
            module = importlib.import_module(name)
            for obj in vars(module).values():
                if isinstance(obj, type) and issubclass(obj, Exception) and obj.__module__ == name:
                    classes[f"{obj.__module__}.{obj.__name__}"] = obj
    return classes


def check_error_taxonomy() -> list[str]:
    problems: list[str] = []
    bases = _mapped_exception_bases()
    if not bases:
        return ["could not derive any mapped exception bases from raise_api_error"]

    owned_exceptions = _owned_exception_classes()

    for qualname, cls in sorted(owned_exceptions.items()):
        mapped = issubclass(cls, bases)
        declared = qualname in INTENTIONAL_UNMAPPED
        if not mapped and not declared:
            problems.append(
                f"{qualname} is not a subclass of any base raise_api_error maps — "
                "it will surface as HTTP 500. Map it in src/archetype/api/errors.py, "
                "subclass a mapped base, or declare it in INTENTIONAL_UNMAPPED "
                "with a rationale and issue."
            )
        elif mapped and declared:
            problems.append(
                f"{qualname} is declared INTENTIONAL_UNMAPPED but is now mapped — "
                "remove the stale manifest entry."
            )

    for qualname in INTENTIONAL_UNMAPPED:
        if qualname not in owned_exceptions:
            problems.append(f"INTENTIONAL_UNMAPPED names a class that no longer exists: {qualname}")
    return problems


def main() -> int:
    sys.path.insert(0, str(ROOT / "src"))
    problems = check_registry_coverage() + check_scheduler_dispatch_shape() + check_error_taxonomy()
    if problems:
        print("Gate coverage audit FAILED:")
        for problem in problems:
            print(f"  - {problem}")
        print(
            "\nEvery reachable operation needs one exact registration, durable "
            "eligibility must equal the portable world-model set, scheduler dispatch "
            "must stay registry-driven, and every app-layer error needs a non-500 "
            "HTTP mapping."
        )
        return 1
    print("Gate coverage audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
