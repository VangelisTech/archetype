#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Statically enforce the provider-neutral sandbox attempt phase order."""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "src" / "archetype" / "app" / "sandboxes" / "common.py"
CLIENT_CLASS = "CodingAgentSandboxClient"
ENTRYPOINT = "run_attempt"
EXPECTED_PHASE_CALLS = (
    "_execution_phase",
    "_validation_phase",
    "_repository_finalization_phase",
    "_evidence_phase",
    "_checkpoint_phase",
    "_artifact_handoff_phase",
)


def _self_method_call(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    function = node.func
    if not isinstance(function, ast.Attribute) or not isinstance(function.value, ast.Name):
        return None
    if function.value.id != "self":
        return None
    return function.attr


def _direct_awaited_method(statement: ast.stmt) -> str | None:
    value: ast.AST | None = None
    if isinstance(statement, ast.Assign | ast.AnnAssign):
        value = statement.value
    elif isinstance(statement, ast.Expr):
        value = statement.value
    if not isinstance(value, ast.Await):
        return None
    return _self_method_call(value.value)


def audit_source(path: Path = DEFAULT_SOURCE) -> list[str]:
    """Return exact contract violations for one common-kernel source file."""
    if not path.is_file():
        return [f"sandbox kernel source does not exist: {path}"]
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        return [f"sandbox kernel source is not valid Python: {exc}"]

    classes = [
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == CLIENT_CLASS
    ]
    if len(classes) != 1:
        return [f"expected exactly one {CLIENT_CLASS} class, found {len(classes)}"]
    client = classes[0]
    methods = {
        node.name: node
        for node in client.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    entrypoint = methods.get(ENTRYPOINT)
    if not isinstance(entrypoint, ast.AsyncFunctionDef):
        return [f"{CLIENT_CLASS}.{ENTRYPOINT} must be an async method"]

    errors: list[str] = []
    missing_methods = [name for name in EXPECTED_PHASE_CALLS if name not in methods]
    if missing_methods:
        errors.append("missing phase method(s): " + ", ".join(missing_methods))

    all_calls = sorted(
        (
            (node.lineno, method)
            for node in ast.walk(entrypoint)
            if (method := _self_method_call(node)) in EXPECTED_PHASE_CALLS
        ),
        key=lambda value: value[0],
    )
    observed = tuple(method for _line, method in all_calls)
    if observed != EXPECTED_PHASE_CALLS:
        errors.append(
            "run_attempt phase calls must occur exactly once in order: "
            + " -> ".join(EXPECTED_PHASE_CALLS)
            + "; observed: "
            + (" -> ".join(observed) if observed else "none")
        )

    direct = tuple(
        method
        for statement in entrypoint.body
        if (method := _direct_awaited_method(statement)) in EXPECTED_PHASE_CALLS
    )
    if direct != EXPECTED_PHASE_CALLS:
        errors.append(
            "phase calls must be unconditional top-level awaited statements in run_attempt"
        )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=DEFAULT_SOURCE)
    args = parser.parse_args(argv)
    errors = audit_source(args.path)
    if errors:
        print("Sandbox phase-order audit failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print("Sandbox phase-order audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
