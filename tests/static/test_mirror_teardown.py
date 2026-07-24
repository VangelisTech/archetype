# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contract for PR-4 composition-mirror teardown."""

from __future__ import annotations

import ast
from pathlib import Path

_SOURCE_ROOT = Path(__file__).parents[2] / "src" / "archetype"
_DELETED_PATHS = (
    "app/application",
    "app/container.py",
    "app/gateway",
    "app/models.py",
    "app/redaction",
)
_DELETED_SYMBOLS = frozenset(
    {
        "CommandGateway",
        "RuntimeApplication",
        "ServiceContainer",
        "iCommandGateway",
        "iRuntimeApplication",
    }
)
_DELETED_STATE = frozenset(
    {
        "_ACTIVE_APPLICATION",
        "_ADMITTED_STATE",
        "_HELD_WORLD_LANES",
        "_RUNTIME_CLEANUP_STATE",
    }
)
_DELETED_SETTERS = frozenset(
    {
        "set_container",
        "set_outbox_source",
        "set_quota_reset",
    }
)
_CONTEXTVAR_FORBIDDEN_ROOTS = (
    "api",
    "runtime",
    "runtime_resources.py",
    "wiring.py",
)


def _python_files(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in root.rglob("*.py") if path.is_file()))


def _defined_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (
                ast.AsyncFunctionDef,
                ast.ClassDef,
                ast.FunctionDef,
            ),
        )
    }


def _loaded_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def _mirror_findings(root: Path) -> list[str]:
    findings: list[str] = []
    for relative in _DELETED_PATHS:
        if (root / relative).exists():
            findings.append(f"path:{relative}")

    for path in _python_files(root):
        relative = path.relative_to(root).as_posix()
        names = _defined_names(path)
        for symbol in sorted((_DELETED_SYMBOLS | _DELETED_SETTERS) & names):
            findings.append(f"definition:{relative}:{symbol}")

        loaded = _loaded_names(path)
        for symbol in sorted(_DELETED_STATE & loaded):
            findings.append(f"state:{relative}:{symbol}")

    for relative in _CONTEXTVAR_FORBIDDEN_ROOTS:
        candidate = root / relative
        paths = _python_files(candidate) if candidate.is_dir() else (candidate,)
        for path in paths:
            if path.is_file() and "ContextVar" in _loaded_names(path):
                findings.append(
                    f"contextvar:{path.relative_to(root).as_posix()}",
                )
    return findings


def test_runtime_application_gateway_container_and_context_mirrors_are_absent(
    tmp_path: Path,
) -> None:
    counterfactual = tmp_path / "archetype"
    mirror = counterfactual / "runtime" / "mirror.py"
    mirror.parent.mkdir(parents=True)
    mirror.write_text(
        "from contextvars import ContextVar\n"
        "_ADMITTED_STATE = ContextVar('admitted')\n"
        "class RuntimeApplication: pass\n"
        "def set_container(value): return value\n",
    )
    assert _mirror_findings(counterfactual) == [
        "definition:runtime/mirror.py:RuntimeApplication",
        "definition:runtime/mirror.py:set_container",
        "state:runtime/mirror.py:_ADMITTED_STATE",
        "contextvar:runtime/mirror.py",
    ]

    assert _mirror_findings(_SOURCE_ROOT) == []
