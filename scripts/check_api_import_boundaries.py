# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enforce public-surface boundaries: imports and @public_api signatures.

Two rules, one principle: concrete application capabilities stay behind the
actor-free application facade. Runtime uses ``iRuntimeApplication`` directly;
untrusted API ingress reaches it through ``iCommandGateway``. Top-level domain
families never reach into concrete application services.

1. Import scopes (per consumer):
   - ``api`` route handlers depend on ``iCommandGateway`` — only the adapter
     composition module may construct the container.
   - the remaining provisional ``experiments`` transcript adapter may not
     import application modules. Physical workflow orchestration has an owning
     application service and no longer lives there.
   - ``runtime`` hosts trusted process composition over the application port.

2. ``@public_api`` signatures: a public callable may not accept raw services
   (typed or named like one). Deprecated migration bridges are allowlisted
   HERE, next to the import rules, with a removal deadline — auditable in one
   place, like the lazy-audit ledger. Quietly adding entries to silence this
   check is itself a signal the change is evading the facade; PRs that do so
   will be reverted at review.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

API_TARGETS = [
    ROOT / "src/archetype/api/deps.py",
    *sorted((ROOT / "src/archetype/api/routes").glob("*.py")),
]
EXPERIMENTS_TARGETS = sorted((ROOT / "src/archetype/experiments").rglob("*.py"))
PUBLIC_API_SCAN_TARGETS = sorted((ROOT / "src/archetype").rglob("*.py"))

ALLOWED_APP_IMPORTS_API = {
    "archetype.app.gateway.auth.models",
    "archetype.app.gateway.interfaces",
    "archetype.app.container",
    "archetype.app.models",
}

FORBIDDEN_APP_IMPORTS_API = {
    "archetype.app.world.mutation",
    "archetype.app.query.service",
    "archetype.app.world.simulation",
    "archetype.app.world.service",
}

# The remaining provisional transcript adapter has no application dependency.
ALLOWED_APP_IMPORTS_EXPERIMENTS: set[str] = set()

# Raw-service shapes a @public_api callable may not accept.
SERVICE_TYPE_NAMES = {
    "WorldService",
    "SimulationService",
    "EvaluationService",
    "QueryService",
    "MutationService",
    "CommandGateway",
    "StorageService",
    "ArtifactTableService",
    "ArtifactService",
    "ServiceContainer",
}
SERVICE_PARAM_NAMES = {
    "world_service",
    "simulation_service",
    "evaluation_service",
    "query_service",
    "mutation_service",
    "command_gateway",
    "storage_service",
    "artifact_table_service",
    "container",
    "broker",
}

# Deprecated service-shaped bridge parameters, keyed "relpath::qualname".
# Every entry carries its removal deadline; delete the entry with the bridge.
PUBLIC_API_BRIDGE_PARAMS: dict[str, set[str]] = {}


def _imported_modules(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom) and node.module:
        if node.module == "archetype.app":
            return [f"{node.module}.{alias.name}" for alias in node.names]
        return [node.module]
    return []


def _is_app_module(module: str) -> bool:
    return module == "archetype.app" or module.startswith("archetype.app.")


def _import_violations(path: Path, allowed: set[str], forbidden: set[str], scope: str) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    violations: list[str] = []
    for node in ast.walk(tree):
        for module in _imported_modules(node):
            if not _is_app_module(module):
                continue
            if module in forbidden:
                violations.append(
                    f"{path.relative_to(ROOT)} imports forbidden {module} "
                    f"({scope}: concrete app capabilities stay behind the application facade)"
                )
                continue
            if module not in allowed:
                violations.append(
                    f"{path.relative_to(ROOT)} imports {module}; allowed app imports "
                    f"for {scope} are {', '.join(sorted(allowed))}"
                )
    return violations


_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _is_public_api_decorated(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> bool:
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
        if name == "public_api":
            return True
    return False


def _service_param_violation(param: ast.arg) -> bool:
    """Whole-token annotation match plus param-name match.

    Tokenizing avoids substring hits (``SimulationServiceConfig`` is not
    ``SimulationService``). Known limit, held by the param-NAME check as the
    backstop: an import alias (``import CommandGateway as Gate``) hides the
    type token — full resolution would need import walking.
    """
    annotation = ast.unparse(param.annotation) if param.annotation else ""
    tokens = set(_IDENTIFIER.findall(annotation))
    return bool(tokens & SERVICE_TYPE_NAMES) or param.arg in SERVICE_PARAM_NAMES


def _signature_violations(
    rel: str, qualname: str, fn: ast.FunctionDef | ast.AsyncFunctionDef
) -> list[str]:
    bridge = PUBLIC_API_BRIDGE_PARAMS.get(f"{rel}::{qualname}", set())
    params = [*fn.args.posonlyargs, *fn.args.args, *fn.args.kwonlyargs]
    violations: list[str] = []
    for param in params:
        if param.arg in bridge or param.arg in ("self", "cls"):
            continue
        if _service_param_violation(param):
            violations.append(
                f"{rel}::{qualname} — @public_api callable accepts raw service "
                f"parameter `{param.arg}`; public capability must be expressible "
                f"through ArchetypeRuntime (gated, audited). Deprecated bridges "
                f"belong in PUBLIC_API_BRIDGE_PARAMS with a removal deadline."
            )
    return violations


def _public_api_violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    rel = str(path.relative_to(ROOT))
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if _is_public_api_decorated(node):
                violations += _signature_violations(rel, node.name, node)
        elif isinstance(node, ast.ClassDef) and _is_public_api_decorated(node):
            # The marker's contract covers classes too: constructors are the
            # signature surface, so a @public_api class taking a raw service
            # in __init__/__new__ is the same bypass in a different costume.
            for member in node.body:
                if isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef) and member.name in (
                    "__init__",
                    "__new__",
                ):
                    violations += _signature_violations(rel, f"{node.name}.{member.name}", member)
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
        print("Outward-boundary violations:")
        for violation in violations:
            print(f"  - {violation}")
        return 1
    print("Outward import boundary audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
