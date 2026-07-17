# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enforce public-surface boundaries: imports and @public_api signatures.

Two rules, one principle — **the runtime is the sole public consumer of app**;
public capability must be expressible through ``ArchetypeRuntime`` and its
gated handles, so every mutation carries command-audit provenance.

1. Import scopes (per consumer):
   - ``api`` route handlers go through CommandService — they may not reach
     into lower-level app services directly.
   - ``experiments`` is public surface: it may import app *models* only.
     Driving services directly from here is how the command-gateway bypass
     entered ``src/`` (eval_rollouts, 2026-07-17).
   - ``runtime`` is unrestricted: it hosts the gate.

2. ``@public_api`` signatures: a public callable may not accept raw services
   (typed or named like one). Deprecated migration bridges are allowlisted
   HERE, next to the import rules, with a removal deadline — auditable in one
   place, like the lazy-audit ledger. Quietly adding entries to silence this
   check is itself a signal the change is evading the gateway; PRs that do so
   will be reverted at review.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

API_TARGETS = [
    ROOT / "src/archetype/api/deps.py",
    *sorted((ROOT / "src/archetype/api/routes").glob("*.py")),
]
EXPERIMENTS_TARGETS = sorted((ROOT / "src/archetype/experiments").rglob("*.py"))
PUBLIC_API_SCAN_TARGETS = sorted((ROOT / "src/archetype").rglob("*.py"))

ALLOWED_APP_IMPORTS_API = {
    "archetype.app.auth.models",
    "archetype.app.command_service",
    "archetype.app.container",
    "archetype.app.models",
}

FORBIDDEN_APP_IMPORTS_API = {
    "archetype.app.broker",
    "archetype.app.mutation_service",
    "archetype.app.query_service",
    "archetype.app.simulation_service",
    "archetype.app.world_service",
}

# Public surface: models only. Anything else must arrive through the runtime.
ALLOWED_APP_IMPORTS_EXPERIMENTS = {
    "archetype.app.models",
}

# Raw-service shapes a @public_api callable may not accept.
SERVICE_TYPE_NAMES = {
    "WorldService",
    "SimulationService",
    "EvalService",
    "QueryService",
    "MutationService",
    "CommandService",
    "StorageService",
    "FactService",
    "IngestionService",
    "ServiceContainer",
    "CommandBroker",
}
SERVICE_PARAM_NAMES = {
    "world_service",
    "simulation_service",
    "eval_service",
    "query_service",
    "mutation_service",
    "command_service",
    "storage_service",
    "fact_service",
    "container",
    "broker",
}

# Deprecated service-shaped bridge parameters, keyed "relpath::qualname".
# Every entry carries its removal deadline; delete the entry with the bridge.
PUBLIC_API_BRIDGE_PARAMS: dict[str, set[str]] = {
    # remove in v0.6 — pre-runtime callers migrate to runtime=
    "src/archetype/experiments/eval_rollouts.py::run_task_eval": {
        "world_service",
        "simulation_service",
        "eval_service",
    },
    # remove in v0.6 — pre-runtime callers migrate to runtime=
    "src/archetype/experiments/instruction_sweep.py::run_instruction_sweep": {
        "world_service",
        "simulation_service",
        "eval_service",
    },
}


def _imported_modules(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom) and node.module:
        if node.module == "archetype.app":
            return [f"{node.module}.{alias.name}" for alias in node.names]
        return [node.module]
    return []


def _import_violations(path: Path, allowed: set[str], forbidden: set[str], scope: str) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    violations: list[str] = []
    for node in ast.walk(tree):
        for module in _imported_modules(node):
            if not module.startswith("archetype.app"):
                continue
            if module in forbidden:
                violations.append(
                    f"{path.relative_to(ROOT)} imports forbidden {module} "
                    f"({scope}: the runtime is the sole public consumer of app)"
                )
                continue
            if module not in allowed:
                violations.append(
                    f"{path.relative_to(ROOT)} imports {module}; allowed app imports "
                    f"for {scope} are {', '.join(sorted(allowed))}"
                )
    return violations


def _is_public_api_decorated(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
        if name == "public_api":
            return True
    return False


def _public_api_violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    rel = str(path.relative_to(ROOT))
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not _is_public_api_decorated(node):
            continue
        bridge = PUBLIC_API_BRIDGE_PARAMS.get(f"{rel}::{node.name}", set())
        params = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        for param in params:
            if param.arg in bridge:
                continue
            annotation = ast.unparse(param.annotation) if param.annotation else ""
            typed_as_service = any(name in annotation for name in SERVICE_TYPE_NAMES)
            named_as_service = param.arg in SERVICE_PARAM_NAMES
            if typed_as_service or named_as_service:
                violations.append(
                    f"{rel}::{node.name} — @public_api callable accepts raw service "
                    f"parameter `{param.arg}`; public capability must be expressible "
                    f"through ArchetypeRuntime (gated, audited). Deprecated bridges "
                    f"belong in PUBLIC_API_BRIDGE_PARAMS with a removal deadline."
                )
    return violations


def main() -> int:
    violations = [
        v
        for path in API_TARGETS
        for v in _import_violations(path, ALLOWED_APP_IMPORTS_API, FORBIDDEN_APP_IMPORTS_API, "api")
    ]
    violations += [
        v
        for path in EXPERIMENTS_TARGETS
        for v in _import_violations(path, ALLOWED_APP_IMPORTS_EXPERIMENTS, set(), "experiments")
    ]
    violations += [v for path in PUBLIC_API_SCAN_TARGETS for v in _public_api_violations(path)]
    if violations:
        print("Public-surface boundary violations:")
        for violation in violations:
            print(f"  - {violation}")
        return 1
    print("API import boundary audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
