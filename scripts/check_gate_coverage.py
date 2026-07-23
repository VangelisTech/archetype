#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Gate-surface coverage audit (deterministic).

Two checks that keep the command gate's claim-vs-effect surface honest:

1. **Command disposition manifest** — every ``CommandType`` member is
   classified below, and ``CommandScheduler._apply``'s match arms equal the
   tick-deferred set exactly. A new enum member without a classification, or
   a dispatcher arm drifting from the manifest, fails this audit. This is the
   static guard for the accepted-then-dropped class (issues #178/#368): a
   command admitted to durable scheduling must have an explicit disposition;
   every other command is a direct application operation.

2. **API error taxonomy** — every exception class defined in the application
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

ROOT = Path(__file__).resolve().parents[1]
COMMAND_SERVICE = ROOT / "src/archetype/app/commands/service.py"
API_ERRORS = ROOT / "src/archetype/api/errors.py"

# ── Check 1 manifest ─────────────────────────────────────────────────────────
# Every CommandType member must appear in exactly one bucket. Reclassifying a
# type is a contract decision: update the manifest in the same PR as the code.

# Tick-deferred commands: durable admission accepts them and _apply MUST have
# an explicit arm. MESSAGE, CUSTOM, and QUERY_WORLD are data/no-op envelopes,
# but naming them here makes that behavior deliberate rather than fallthrough.
DEFERRED_DISPATCHED = {
    "SPAWN",
    "DESPAWN",
    "UPDATE",
    "ADD_COMPONENT",
    "REMOVE_COMPONENT",
    "MESSAGE",
    "CUSTOM",
    "QUERY_WORLD",
}

# Direct-gated operations (CommandGateway exposes an explicit method; the
# scheduler is not their application path).
DIRECT_ONLY = {
    "INGEST_ARTIFACTS",
    "EVALUATE",
    "CREATE_WORLD",
    "DESTROY_WORLD",
    "FORK_WORLD",
    "STEP",
    "RUN",
    "RUN_ROLLOUT",
    "RUN_EPISODE",
    "AUTORESEARCH",
    "GET_WORLD_INFO",
    "GET_AUDIT_HISTORY",
    "LIST_SIGNATURES",
    "LIST_WORLDS",
    "LIST_PROCESSORS",
    "LIST_HOOKS",
    "LIST_RESOURCES",
    "ADD_PROCESSOR",
    "REMOVE_PROCESSOR",
    "ADD_RESOURCE",
    "ADD_HOOK",
    "REMOVE_HOOK",
}


def _drain_case_arms(path: Path) -> set[str]:
    """CommandType names matched by ``case`` arms inside ``_apply``."""
    tree = ast.parse(path.read_text(), filename=str(path))
    arms: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_apply":
            for inner in ast.walk(node):
                if not isinstance(inner, ast.Match):
                    continue
                for case in inner.cases:
                    for pattern in ast.walk(case.pattern):
                        if not isinstance(pattern, ast.MatchValue) or not isinstance(
                            pattern.value, ast.Attribute
                        ):
                            continue
                        value = pattern.value
                        if isinstance(value.value, ast.Name) and value.value.id == "CommandType":
                            arms.add(value.attr)
    return arms


def check_command_dispositions() -> list[str]:
    from archetype.app.models import CommandType

    problems: list[str] = []
    members = set(CommandType.__members__)
    classified = DEFERRED_DISPATCHED | DIRECT_ONLY

    overlap = DEFERRED_DISPATCHED & DIRECT_ONLY
    if overlap:
        problems.append(f"manifest buckets overlap: {sorted(overlap)}")

    unclassified = members - classified
    if unclassified:
        problems.append(
            "CommandType members with no disposition (classify them in "
            f"scripts/check_gate_coverage.py): {sorted(unclassified)}"
        )
    phantom = classified - members
    if phantom:
        problems.append(f"manifest names non-existent CommandType members: {sorted(phantom)}")

    arms = _drain_case_arms(COMMAND_SERVICE)
    missing_arms = DEFERRED_DISPATCHED - arms
    if missing_arms:
        problems.append(
            "applied-in-drain commands with NO _apply arm (accepted-then-"
            f"dropped at drain): {sorted(missing_arms)}"
        )
    surprise_arms = arms - DEFERRED_DISPATCHED
    if surprise_arms:
        problems.append(
            "_apply handles commands the manifest does not classify as "
            f"tick-deferred (update the manifest): {sorted(surprise_arms)}"
        )
    return problems


# ── Check 2: error taxonomy ──────────────────────────────────────────────────
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
    problems = check_command_dispositions() + check_error_taxonomy()
    if problems:
        print("Gate coverage audit FAILED:")
        for problem in problems:
            print(f"  - {problem}")
        print(
            "\nEvery CommandType needs a disposition (durable dispatcher arm or "
            "direct application operation), and every app-layer error needs a non-500 HTTP "
            "mapping. See the manifest in this script."
        )
        return 1
    print("Gate coverage audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
