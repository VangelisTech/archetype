# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enforce API route import boundaries.

Route handlers are external entry points and must go through CommandService
instead of reaching into lower-level app services directly.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = [
    ROOT / "src/archetype/api/deps.py",
    *sorted((ROOT / "src/archetype/api/routes").glob("*.py")),
]

ALLOWED_APP_IMPORTS = {
    "archetype.app.auth.models",
    "archetype.app.command_service",
    "archetype.app.container",
    "archetype.app.models",
}

FORBIDDEN_APP_IMPORTS = {
    "archetype.app.broker",
    "archetype.app.mutation_service",
    "archetype.app.query_service",
    "archetype.app.simulation_service",
    "archetype.app.world_service",
}


def _imported_modules(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom) and node.module:
        if node.module == "archetype.app":
            return [f"{node.module}.{alias.name}" for alias in node.names]
        return [node.module]
    return []


def _violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    violations: list[str] = []
    for node in ast.walk(tree):
        for module in _imported_modules(node):
            if not module.startswith("archetype.app"):
                continue
            if module in FORBIDDEN_APP_IMPORTS:
                violations.append(f"{path.relative_to(ROOT)} imports forbidden {module}")
                continue
            if module not in ALLOWED_APP_IMPORTS:
                violations.append(
                    f"{path.relative_to(ROOT)} imports {module}; allowed app imports are "
                    f"{', '.join(sorted(ALLOWED_APP_IMPORTS))}"
                )
    return violations


def main() -> int:
    violations = [violation for path in TARGETS for violation in _violations(path)]
    if violations:
        print("API import boundary violations:")
        for violation in violations:
            print(f"  - {violation}")
        return 1
    print("API import boundary audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
