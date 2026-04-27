# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Import-boundary enforcement tests.

Verifies that runtime/ and api/ only reach into the allowed app/ sub-modules,
and that runtime/ never holds a concrete iWorld or AsyncWorld reference outside
TYPE_CHECKING.
"""

from __future__ import annotations

import ast
import typing
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2] / "src" / "archetype"
_RUNTIME_DIR = _ROOT / "runtime"
_API_DIR = _ROOT / "api"

# ─── Allowed app imports ─────────────────────────────────────────────────────

# Modules inside archetype.app that runtime/ may import from.
_RUNTIME_ALLOWED_APP = frozenset(
    {
        "archetype.app.command_service",
        "archetype.app.container",
        "archetype.app.models",
        "archetype.app.auth.models",
    }
)

# Modules inside archetype.app that api/ may import from.
_API_ALLOWED_APP = _RUNTIME_ALLOWED_APP | frozenset(
    {
        "archetype.app.auth.errors",
    }
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _python_files(directory: Path) -> list[Path]:
    """Return all .py files under *directory*, recursively."""
    return sorted(directory.rglob("*.py"))


def _extract_app_imports(filepath: Path) -> list[tuple[str, int, bool]]:
    """Parse *filepath* and return (module, lineno, in_type_checking) for every
    ``from archetype.app...`` import.

    *in_type_checking* is True when the import lives inside an
    ``if TYPE_CHECKING:`` block.
    """
    source = filepath.read_text()
    tree = ast.parse(source, filename=str(filepath))

    # Detect TYPE_CHECKING block ranges
    tc_ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            test = node.test
            # Match: TYPE_CHECKING  or  typing.TYPE_CHECKING
            is_tc = False
            if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
                is_tc = True
            elif isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
                is_tc = True
            if is_tc:
                start = node.body[0].lineno if node.body else node.lineno
                end = max(
                    getattr(n, "end_lineno", n.lineno) for n in node.body if hasattr(n, "lineno")
                )
                tc_ranges.append((start, end))

    def _in_tc(lineno: int) -> bool:
        return any(s <= lineno <= e for s, e in tc_ranges)

    results: list[tuple[str, int, bool]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("archetype.app")
        ):
            results.append((node.module, node.lineno, _in_tc(node.lineno)))
    return results


def _extract_name_imports(filepath: Path, names: frozenset[str]) -> list[tuple[str, int, bool]]:
    """Return (name, lineno, in_type_checking) for imports of any *names*."""
    source = filepath.read_text()
    tree = ast.parse(source, filename=str(filepath))

    # Detect TYPE_CHECKING block ranges (same logic)
    tc_ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            test = node.test
            is_tc = False
            if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
                is_tc = True
            elif isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
                is_tc = True
            if is_tc:
                start = node.body[0].lineno if node.body else node.lineno
                end = max(
                    getattr(n, "end_lineno", n.lineno) for n in node.body if hasattr(n, "lineno")
                )
                tc_ranges.append((start, end))

    def _in_tc(lineno: int) -> bool:
        return any(s <= lineno <= e for s, e in tc_ranges)

    results: list[tuple[str, int, bool]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.ImportFrom, ast.Import)):
            imported: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.names:
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            for name in imported:
                if name in names:
                    results.append((name, node.lineno, _in_tc(node.lineno)))
    return results


# ─── Tests ────────────────────────────────────────────────────────────────────


class TestRuntimeAppBoundary:
    """runtime/ must only import from the allowed app/ modules."""

    def test_runtime_imports_only_allowed_app_modules(self):
        violations: list[str] = []
        for py in _python_files(_RUNTIME_DIR):
            for module, lineno, in_tc in _extract_app_imports(py):
                if module not in _RUNTIME_ALLOWED_APP:
                    rel = py.relative_to(_ROOT)
                    violations.append(f"{rel}:{lineno}  imports {module}")

        assert not violations, "runtime/ imports disallowed app modules:\n  " + "\n  ".join(
            violations
        )


class TestApiAppBoundary:
    """api/ must only import from the allowed app/ modules."""

    def test_api_imports_only_allowed_app_modules(self):
        violations: list[str] = []
        for py in _python_files(_API_DIR):
            for module, lineno, in_tc in _extract_app_imports(py):
                if module not in _API_ALLOWED_APP:
                    rel = py.relative_to(_ROOT)
                    violations.append(f"{rel}:{lineno}  imports {module}")

        assert not violations, "api/ imports disallowed app modules:\n  " + "\n  ".join(violations)


class TestNoWorldLeakInRuntime:
    """runtime/ must never import iWorld or AsyncWorld outside TYPE_CHECKING."""

    _FORBIDDEN = frozenset({"iWorld", "AsyncWorld"})

    def test_no_iworld_or_asyncworld_import(self):
        violations: list[str] = []
        for py in _python_files(_RUNTIME_DIR):
            for name, lineno, in_tc in _extract_name_imports(py, self._FORBIDDEN):
                if not in_tc:
                    rel = py.relative_to(_ROOT)
                    violations.append(f"{rel}:{lineno}  imports {name} outside TYPE_CHECKING")

        assert not violations, (
            "runtime/ imports iWorld/AsyncWorld outside TYPE_CHECKING:\n  "
            + "\n  ".join(violations)
        )

    def test_runtime_world_annotations_clean(self):
        """RuntimeWorld and _RuntimeWorldState must not have iWorld/AsyncWorld in
        field annotations (checked via reflection on the actual classes)."""
        from archetype.runtime.world import RuntimeWorld, _RuntimeWorldState

        violations: list[str] = []
        for cls in (RuntimeWorld, _RuntimeWorldState):
            hints = typing.get_type_hints(cls) if hasattr(cls, "__annotations__") else {}
            for field, hint in hints.items():
                hint_str = str(hint)
                for bad in self._FORBIDDEN:
                    if bad in hint_str:
                        violations.append(f"{cls.__name__}.{field} is typed as {hint_str}")

        assert not violations, (
            "World classes reference iWorld/AsyncWorld in annotations:\n  "
            + "\n  ".join(violations)
        )
