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

import pytest

_ROOT = Path(__file__).resolve().parents[2] / "src" / "archetype"
_RUNTIME_DIR = _ROOT / "runtime"
_API_DIR = _ROOT / "api"

# ─── Allowed app imports ─────────────────────────────────────────────────────

_RUNTIME_TYPE_ONLY_APP = frozenset(
    {
        "archetype.app.autoresearch_service",
        "archetype.app.eval_service",
    }
)

# Modules inside archetype.app that runtime/ may import from.
_RUNTIME_ALLOWED_APP = _RUNTIME_TYPE_ONLY_APP | frozenset(
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
        # The gate's typed error contract; mapping it to HTTP status codes
        # is the adapter's job (issue #180: WorldNotFoundError -> 404).
        "archetype.app.errors",
    }
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _python_files(directory: Path) -> list[Path]:
    """Return all .py files under *directory*, recursively."""
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


def _runtime_app_import_is_allowed(module: str, in_type_checking: bool) -> bool:
    return module in _RUNTIME_ALLOWED_APP and (
        module not in _RUNTIME_TYPE_ONLY_APP or in_type_checking
    )


def _extract_app_imports(filepath: Path) -> list[tuple[str, int, bool]]:
    """Parse *filepath* and return (module, lineno, in_type_checking) for every
    ``archetype.app`` import.

    *in_type_checking* is True when the import lives inside an
    ``if TYPE_CHECKING:`` block.
    """
    source = filepath.read_text()
    tree = ast.parse(source, filename=str(filepath))
    type_checking_ranges = _type_checking_ranges(tree)

    results: list[tuple[str, int, bool]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and _is_app_module(node.module):
            results.append(
                (node.module, node.lineno, _in_ranges(node.lineno, type_checking_ranges))
            )
        elif isinstance(node, ast.Import):
            results.extend(
                (alias.name, node.lineno, _in_ranges(node.lineno, type_checking_ranges))
                for alias in node.names
                if _is_app_module(alias.name)
            )
    return results


def _extract_name_imports(filepath: Path, names: frozenset[str]) -> list[tuple[str, int, bool]]:
    """Return (name, lineno, in_type_checking) for imports of any *names*."""
    source = filepath.read_text()
    tree = ast.parse(source, filename=str(filepath))
    type_checking_ranges = _type_checking_ranges(tree)

    results: list[tuple[str, int, bool]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom | ast.Import):
            imported: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.names:
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            for name in imported:
                if name in names:
                    results.append(
                        (name, node.lineno, _in_ranges(node.lineno, type_checking_ranges))
                    )
    return results


# ─── Tests ────────────────────────────────────────────────────────────────────


class TestRuntimeAppBoundary:
    """runtime/ must only import from the allowed app/ modules."""

    def test_runtime_imports_only_allowed_app_modules(self):
        violations: list[str] = []
        for py in _python_files(_RUNTIME_DIR):
            for module, lineno, in_type_checking in _extract_app_imports(py):
                if not _runtime_app_import_is_allowed(module, in_type_checking):
                    rel = py.relative_to(_ROOT)
                    violations.append(f"{rel}:{lineno}  imports {module}")

        assert not violations, "runtime/ imports disallowed app modules:\n  " + "\n  ".join(
            violations
        )


@pytest.mark.parametrize(
    ("source_text", "expected"),
    [
        (
            "import archetype.app.world_service\n",
            [("archetype.app.world_service", False)],
        ),
        (
            "from archetype.app.eval_service import EvaluationResult\n",
            [("archetype.app.eval_service", False)],
        ),
        ("from archetype.application import Service\n", []),
        (
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    import archetype.app.eval_service\n",
            [("archetype.app.eval_service", True)],
        ),
        (
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    from archetype.app.eval_service import EvaluationResult\n",
            [("archetype.app.eval_service", True)],
        ),
    ],
)
def test_runtime_app_import_oracle_contract(tmp_path, source_text, expected) -> None:
    probe = tmp_path / "probe.py"
    probe.write_text(source_text, encoding="utf-8")

    actual = [
        (module, _runtime_app_import_is_allowed(module, in_type_checking))
        for module, _, in_type_checking in _extract_app_imports(probe)
    ]

    assert actual == expected


class TestApiAppBoundary:
    """api/ must only import from the allowed app/ modules."""

    def test_api_imports_only_allowed_app_modules(self):
        violations: list[str] = []
        for py in _python_files(_API_DIR):
            for module, lineno, _in_tc in _extract_app_imports(py):
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
