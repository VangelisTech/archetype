# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical durable world-query boundary contracts."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from archetype.world import query


def test_world_query_has_no_application_or_audit_dependency() -> None:
    module_path = Path(query.__file__)
    tree = ast.parse(module_path.read_text())
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert not any(name == "archetype.app" or name.startswith("archetype.app.") for name in imports)
    assert not any("audit" in name for name in imports)
    assert "get_command_history" not in {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


@pytest.mark.asyncio
async def test_explicit_visibility_tokens_cannot_be_combined_with_lineage() -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        await query.query_components(
            object(),
            components=[],
            world_id="world",
            run_id="run",
            lineage=[("ancestor", "ancestor-run", 3)],
            visibility_tokens=["token"],
        )


@pytest.mark.asyncio
async def test_lineage_segments_are_clipped_to_their_owned_ticks() -> None:
    class _Frame:
        def __init__(self, label: str) -> None:
            self.labels = [label]

        def concat(self, other: _Frame) -> _Frame:
            result = _Frame("")
            result.labels = [*self.labels, *other.labels]
            return result

    calls: list[tuple[str, str, list[int] | None]] = []

    async def segment(world_id: str, run_id: str, ticks: list[int] | None) -> _Frame:
        calls.append((world_id, run_id, ticks))
        return _Frame(world_id)

    result = await query._union_lineage(
        _Frame("child"),
        [("root", "root-run", 2), ("parent", "parent-run", 5)],
        [0, 2, 3, 5, 8],
        segment,
    )

    assert result.labels == ["child", "root", "parent"]
    assert calls == [
        ("root", "root-run", [0, 2]),
        ("parent", "parent-run", [3, 5]),
    ]
