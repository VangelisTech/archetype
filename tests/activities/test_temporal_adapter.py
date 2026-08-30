# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Architecture and identity evidence for the optional Temporal adapter."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from archetype.activities.temporal import durable_workflow_id
from archetype.missions.temporal import mission_workflow_id

_ROOT = Path(__file__).resolve().parents[2]
_ADAPTER_ROOT = (
    _ROOT / "packages" / "archetype-ecs" / "src" / "archetype" / "activities" / "temporal"
)
_RUNTIME_ROOT = _ROOT / "packages" / "archetype-ecs" / "src" / "archetype" / "runtime"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def test_shared_temporal_adapter_does_not_import_domain_families() -> None:
    domain_prefixes = (
        "archetype.missions",
        "archetype.physical_ai",
        "archetype.research",
    )
    imported = {
        module
        for path in _ADAPTER_ROOT.glob("*.py")
        for module in _imports(path)
        if module.startswith(domain_prefixes)
    }

    assert imported == set()


def test_runtime_facade_does_not_import_temporal_adapter_or_sdk() -> None:
    imported = {
        module
        for path in _RUNTIME_ROOT.rglob("*.py")
        for module in _imports(path)
        if module == "temporalio"
        or module.startswith("temporalio.")
        or module == "archetype.activities.temporal"
        or module.startswith("archetype.activities.temporal.")
    }

    assert imported == set()


def test_mission_workflow_identity_delegates_to_shared_canonicalization() -> None:
    expected = durable_workflow_id(
        "archetype.missions.workflow",
        "operator@example.test",
        "same-request",
        prefix="mission",
    )

    assert mission_workflow_id(" operator@example.test ", " same-request ") == expected


@pytest.mark.parametrize(
    ("namespace", "principal", "idempotency_key", "prefix"),
    [
        ("", "operator", "key", "activity"),
        ("activities", "", "key", "activity"),
        ("activities", "operator", "", "activity"),
        ("activities", "operator", "key", ""),
    ],
)
def test_workflow_identity_rejects_incomplete_authority(
    namespace: str,
    principal: str,
    idempotency_key: str,
    prefix: str,
) -> None:
    with pytest.raises(ValueError, match="requires namespace"):
        durable_workflow_id(
            namespace,
            principal,
            idempotency_key,
            prefix=prefix,
        )
