#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Gate-surface coverage audit (deterministic).

Two checks that keep the command gate's claim-vs-effect surface honest:

1. **Command disposition manifest** — every ``CommandType`` member is
   classified below, and ``CommandService._apply``'s match arms equal the
   applied-in-drain set exactly. A new enum member without a classification,
   or a drain arm drifting from the manifest, fails this audit. This is the
   static guard for the accepted-then-dropped class (issues #178/#368): a
   command the gate accepts must either have a drain arm, be a direct-gated
   operation, or be documented broker-queued data.

2. **API error taxonomy** — every exception class defined in the app layer's
   error modules must be mapped by ``api.errors.raise_api_error`` to a
   non-500 branch (issue #180: ``WorldNotFoundError`` extended
   ``LookupError``, missed the ``KeyError`` branch, and fell through to the
   500 fallback while tests stayed green).

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
COMMAND_SERVICE = ROOT / "src/archetype/app/command_service.py"
API_ERRORS = ROOT / "src/archetype/api/errors.py"

# ── Check 1 manifest ─────────────────────────────────────────────────────────
# Every CommandType member must appear in exactly one bucket. Reclassifying a
# type is a contract decision: update the manifest in the same PR as the code.

# Tick-deferred mutations: submit() enqueues them and _apply MUST have an arm.
APPLIED_IN_DRAIN = {
    "SPAWN",
    "DESPAWN",
    "UPDATE",
    "ADD_COMPONENT",
    "REMOVE_COMPONENT",
    "ADD_PROCESSOR",
    "REMOVE_PROCESSOR",
}

# Direct-gated operations (CommandService exposes an explicit method; the
# broker is not their application path). KNOWN GAP (issue #368): submit()
# currently accepts these and drain drops them with a warning — when #368
# lands, this audit should additionally assert submit-time rejection.
DIRECT_ONLY = {
    "INGEST_FACT",
    "EVALUATE",
    "CREATE_WORLD",
    "DESTROY_WORLD",
    "FORK_WORLD",
    "STEP",
    "RUN",
    "RUN_ROLLOUT",
    "RUN_EPISODE",
    "AUTORESEARCH",
    "QUERY_WORLD",
    "GET_WORLD_INFO",
    "GET_AUDIT_HISTORY",
    "LIST_SIGNATURES",
    "LIST_WORLDS",
    "LIST_PROCESSORS",
    "LIST_HOOKS",
    "LIST_RESOURCES",
    "ADD_RESOURCE",
    "ADD_HOOK",
    "REMOVE_HOOK",
}

# Application-defined envelopes: the broker supplies queueing only; consumers
# (message-delivery processors, custom handlers) dequeue and interpret them.
BROKER_DATA = {"MESSAGE", "CUSTOM"}


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
                    pattern = case.pattern
                    if isinstance(pattern, ast.MatchValue) and isinstance(
                        pattern.value, ast.Attribute
                    ):
                        value = pattern.value
                        if isinstance(value.value, ast.Name) and value.value.id == "CommandType":
                            arms.add(value.attr)
    return arms


def check_command_dispositions() -> list[str]:
    from archetype.app.models import CommandType

    problems: list[str] = []
    members = set(CommandType.__members__)
    classified = APPLIED_IN_DRAIN | DIRECT_ONLY | BROKER_DATA

    overlap = (
        (APPLIED_IN_DRAIN & DIRECT_ONLY)
        | (APPLIED_IN_DRAIN & BROKER_DATA)
        | (DIRECT_ONLY & BROKER_DATA)
    )
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
    missing_arms = APPLIED_IN_DRAIN - arms
    if missing_arms:
        problems.append(
            "applied-in-drain commands with NO _apply arm (accepted-then-"
            f"dropped at drain): {sorted(missing_arms)}"
        )
    surprise_arms = arms - APPLIED_IN_DRAIN
    if surprise_arms:
        problems.append(
            "_apply handles commands the manifest does not classify as "
            f"applied-in-drain (update the manifest): {sorted(surprise_arms)}"
        )
    return problems


# ── Check 2: error taxonomy ──────────────────────────────────────────────────
# The whole archetype.app package is walked for Exception subclasses — a
# hardcoded module list would fail open the moment errors are defined
# elsewhere (footgun review on PR #407: app/_catalog.py's four exceptions).

# Exceptions that deliberately surface as HTTP 500 for now. Every entry needs
# a rationale and an issue; a stale entry (class gone or now mapped) fails
# the audit so the manifest cannot rot.
INTENTIONAL_UNMAPPED = {
    "archetype.app._catalog.CatalogSchemaMismatchError": (
        "integrity violation intentionally surfaces as 500; decision recorded in #413"
    ),
    "archetype.app.audit_log.AuditBackpressureError": (
        "observable backpressure per the durability posture; 503-shaped, "
        "public mapping tracked in #419"
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


def _app_exception_classes() -> dict[str, type[Exception]]:
    """Every Exception subclass defined anywhere in the archetype.app package."""
    import pkgutil

    import archetype.app as app_pkg

    # walk_packages yields children only, never the anchor package itself —
    # include archetype/app/__init__.py explicitly so an exception defined
    # there cannot escape the scan.
    module_names = ["archetype.app"]
    module_names += [
        module_info.name
        for module_info in pkgutil.walk_packages(app_pkg.__path__, "archetype.app.")
    ]
    classes: dict[str, type[Exception]] = {}
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

    app_exceptions = _app_exception_classes()
    for qualname, cls in sorted(app_exceptions.items()):
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
        if qualname not in app_exceptions:
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
            "\nEvery CommandType needs a disposition (drain arm, direct-gated, "
            "or broker data), and every app-layer error needs a non-500 HTTP "
            "mapping. See the manifest in this script."
        )
        return 1
    print("Gate coverage audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
