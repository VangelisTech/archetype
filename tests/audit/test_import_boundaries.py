# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit Section B.1 + E.4 + H.2 — single-gate import boundaries.

Architectural invariant: every external surface (runtime, API, CLI) must
go through `iCommandService`. Reaching past the gate to MutationService /
SimulationService / QueryService / WorldService / broker would silently
bypass guardrails and audit emission.

This file is an AST-based linter. It runs as part of `make test`, so a
PR that introduces a forbidden import goes red in CI rather than at
review time.

Sister checks already in the tree:
  - `scripts/check_api_import_boundaries.py` (Make: api-boundary-audit)
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = [pytest.mark.audit, pytest.mark.audit_critical]

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT / "src" / "archetype" / "runtime"
CLI_DIR = ROOT / "src" / "archetype" / "cli"
RUNTIME_INIT = RUNTIME_DIR / "__init__.py"

# Per docs/guide/runtime.md and audit B.1.1, the runtime/ package may
# import from app/ ONLY through these four modules.
RUNTIME_ALLOWED_APP_IMPORTS = frozenset(
    {
        "archetype.app.auth.models",
        "archetype.app.command_service",
        "archetype.app.container",
        "archetype.app.models",
    }
)

RUNTIME_FORBIDDEN_APP_IMPORTS = frozenset(
    {
        "archetype.app.broker",
        "archetype.app.mutation_service",
        "archetype.app.query_service",
        "archetype.app.simulation_service",
        "archetype.app.world_service",
    }
)

# Per audit E.4.1, cli/ is HTTP-only. The only acceptable app imports are
# the request/response shapes — explicitly NOT the command service.
CLI_ALLOWED_APP_IMPORTS = frozenset(
    {
        "archetype.app.auth.models",
        "archetype.app.models",
    }
)


def _imported_app_modules(node: ast.AST) -> list[str]:
    """Yield archetype.app.* module names referenced by an import node.

    Handles both `import archetype.app.x` and `from archetype.app[.x] import y`.
    For `from archetype.app import foo`, the result is `archetype.app.foo`
    so the caller can match against the per-surface allowlist.
    """
    if isinstance(node, ast.Import):
        return [
            alias.name
            for alias in node.names
            if alias.name.startswith("archetype.app")
        ]
    if isinstance(node, ast.ImportFrom) and node.module:
        if node.module == "archetype.app":
            return [f"archetype.app.{alias.name}" for alias in node.names]
        if node.module.startswith("archetype.app"):
            return [node.module]
    return []


def _walk_python_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _check_imports(
    paths: list[Path],
    *,
    allowed: frozenset[str],
    forbidden: frozenset[str],
) -> list[str]:
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            for module in _imported_app_modules(node):
                if module in forbidden:
                    violations.append(
                        f"{path.relative_to(ROOT)} imports forbidden {module}"
                    )
                    continue
                if module not in allowed:
                    violations.append(
                        f"{path.relative_to(ROOT)} imports {module}; "
                        f"allowed: {', '.join(sorted(allowed))}"
                    )
    return violations


# ── B.1.1 / H.2.1-5 — runtime/ ──────────────────────────────────────────


def test_b_1_1_runtime_no_forbidden_app_imports() -> None:
    """runtime/ imports from app/ only via the gate-facing modules."""
    if not RUNTIME_DIR.exists():
        pytest.skip("src/archetype/runtime/ is not present")
    paths = _walk_python_files(RUNTIME_DIR)
    violations = _check_imports(
        paths,
        allowed=RUNTIME_ALLOWED_APP_IMPORTS,
        forbidden=RUNTIME_FORBIDDEN_APP_IMPORTS,
    )
    assert not violations, "runtime/ import-boundary violations:\n  - " + "\n  - ".join(
        violations
    )


# ── E.4.1-3 / H.2.7 — cli/ ──────────────────────────────────────────────


def test_e_4_cli_no_forbidden_app_imports() -> None:
    """cli/ is HTTP-only; no service imports, not even iCommandService."""
    if not CLI_DIR.exists():
        pytest.skip("src/archetype/cli/ is not present")
    paths = _walk_python_files(CLI_DIR)
    # Anything outside the explicit CLI allowlist is forbidden — including
    # archetype.app.command_service, which is *allowed* in runtime/ but
    # NOT in cli/.
    violations = _check_imports(
        paths,
        allowed=CLI_ALLOWED_APP_IMPORTS,
        forbidden=frozenset(),
    )
    assert not violations, "cli/ import-boundary violations:\n  - " + "\n  - ".join(
        violations
    )


# ── B.1.6 — runtime/__init__.py exports ─────────────────────────────────


def test_b_1_6_runtime_init_exports() -> None:
    """runtime/__init__.py exports the documented script-boundary surface."""
    if not RUNTIME_INIT.exists():
        pytest.skip("src/archetype/runtime/__init__.py is not present")

    # Static parse of __all__ avoids importing the runtime (which spins
    # up asyncio / Iceberg config at module-load time).
    tree = ast.parse(RUNTIME_INIT.read_text(), filename=str(RUNTIME_INIT))
    found_all: list[str] | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            value = node.value
            if isinstance(value, (ast.List, ast.Tuple)):
                found_all = [
                    elt.value
                    for elt in value.elts
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                ]
                break

    assert found_all is not None, "runtime/__init__.py must declare __all__"
    expected = {
        "ArchetypeRuntime",
        "SyncArchetypeRuntime",
        "RuntimeWorld",
        "SyncRuntimeWorld",
        "configure_session",
        "run_sync",
    }
    missing = expected - set(found_all)
    assert not missing, f"runtime/__init__.py __all__ missing: {sorted(missing)}"
