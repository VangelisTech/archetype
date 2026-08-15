# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Import-boundary enforcement tests.

Verifies that runtime/ and api/ do not depend on retired app-layer mirrors,
and that runtime/ never holds a concrete AsyncWorld reference outside
TYPE_CHECKING.
"""

from __future__ import annotations

import ast
import typing
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2] / "packages" / "archetype-ecs" / "src" / "archetype"
_RUNTIME_DIR = _ROOT / "runtime"
_API_DIR = _ROOT / "api"

# ─── Retired app-mirror imports ──────────────────────────────────────────────

_RUNTIME_TYPE_ONLY_APP: frozenset[str] = frozenset()
_RUNTIME_ALLOWED_APP: frozenset[str] = frozenset()
_API_ALLOWED_APP: frozenset[str] = frozenset()


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


def _api_app_import_is_allowed(module: str) -> bool:
    return module in _API_ALLOWED_APP


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
    """runtime/ consumes canonical families and process resources directly."""

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
            "from archetype.research.models import AutoResearchConfig\n",
            [],
        ),
        (
            "from archetype.app.missions.service import MissionService\n",
            [("archetype.app.missions.service", False)],
        ),
        ("from archetype.application import Service\n", []),
        (
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    import archetype.app.missions.service\n",
            [("archetype.app.missions.service", False)],
        ),
        (
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    from archetype.app.missions.service import MissionService\n",
            [("archetype.app.missions.service", False)],
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
    """api/ translates into exact command and family operations."""

    def test_api_imports_only_allowed_app_modules(self):
        violations: list[str] = []
        for py in _python_files(_API_DIR):
            for module, lineno, _in_tc in _extract_app_imports(py):
                if not _api_app_import_is_allowed(module):
                    rel = py.relative_to(_ROOT)
                    violations.append(f"{rel}:{lineno}  imports {module}")

        assert not violations, "api/ imports disallowed app modules:\n  " + "\n  ".join(violations)


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("archetype.app.errors", False),
        ("archetype.app.missions.service", False),
    ],
)
def test_api_app_import_oracle_contract(module: str, expected: bool) -> None:
    assert _api_app_import_is_allowed(module) is expected


class TestNoWorldLeakInRuntime:
    """runtime/ must never import AsyncWorld outside TYPE_CHECKING."""

    _FORBIDDEN = frozenset({"AsyncWorld"})

    def test_no_asyncworld_import(self):
        violations: list[str] = []
        for py in _python_files(_RUNTIME_DIR):
            for name, lineno, in_tc in _extract_name_imports(py, self._FORBIDDEN):
                if not in_tc:
                    rel = py.relative_to(_ROOT)
                    violations.append(f"{rel}:{lineno}  imports {name} outside TYPE_CHECKING")

        assert not violations, (
            "runtime/ imports AsyncWorld outside TYPE_CHECKING:\n  " + "\n  ".join(violations)
        )

    def test_runtime_world_annotations_clean(self):
        """RuntimeWorld and _RuntimeWorldState must not have AsyncWorld in
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
            "World classes reference AsyncWorld in annotations:\n  " + "\n  ".join(violations)
        )
