# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The example dogfoods only Archetype's public root facade."""

from __future__ import annotations

import ast
from pathlib import Path


def test_problem_definition_example_imports_no_archetype_submodules() -> None:
    examples = Path(__file__).parents[3] / "examples"
    package = examples / "problem_definition_mission"
    paths = (
        *sorted(package.glob("*.py")),
        examples / "problem_definition_autoresearch.py",
        examples / "problem_definition_continue.py",
    )
    violations: list[str] = []

    for path in paths:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("archetype."):
                    violations.append(f"{path.name}:{node.lineno}: from {module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("archetype."):
                        violations.append(f"{path.name}:{node.lineno}: import {alias.name}")

    assert violations == []
