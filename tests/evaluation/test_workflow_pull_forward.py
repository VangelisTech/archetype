# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract-first proofs for the PR-5 evaluation workflow pull-forward."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from uuid_utils import uuid7

from archetype.core.component import Component
from archetype.evaluation.contracts import GraderContract, Outcome
from archetype.evaluation.models import Evaluate

_SRC = Path(__file__).resolve().parents[2] / "src" / "archetype"
_FAMILY = _SRC / "evaluation"


class _Telemetry(Component):
    reading: float = 0.0


def _grader(_frame: Any) -> Outcome:
    return Outcome(status="pass", score=1.0)


def _operation_fields() -> dict[str, Any]:
    return {
        "world_id": "world",
        "components": (_Telemetry,),
        "contract": GraderContract(
            grader_id="contract-first",
            implementation_version="v1",
        ),
        "grader": _grader,
        "evaluation_id": "evaluation",
    }


def test_canonical_models_are_import_light_and_contract_aliases_are_identical() -> None:
    probe = """
import sys
from archetype.evaluation.models import (
    FrameGrader,
    GraderContract,
    GraderOutput,
    GraderReturn,
    Outcome,
    TrajectoryGrader,
)
heavy = [
    name
    for name in (
        "archetype.core.component",
        "archetype.core.config",
        "archetype.evaluation.components",
        "daft",
        "lancedb",
        "pyarrow",
    )
    if name in sys.modules
]
assert not heavy, f"evaluation.models loaded heavy modules: {heavy}"
from archetype.evaluation import contracts
assert contracts.GraderContract is GraderContract
assert contracts.Outcome is Outcome
assert contracts.GraderOutput is GraderOutput
assert contracts.GraderReturn is GraderReturn
assert contracts.FrameGrader is FrameGrader
assert contracts.TrajectoryGrader is FrameGrader
assert TrajectoryGrader is FrameGrader
"""
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def test_evaluate_requires_explicit_storage_coordinates(tmp_path: Path) -> None:
    from archetype.core.config import StorageConfig

    with pytest.raises(ValidationError, match="storage_config"):
        Evaluate(**_operation_fields())

    world_id = uuid7()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="eval")
    operation = Evaluate(
        **{
            **_operation_fields(),
            "world_id": world_id,
            "storage_config": storage,
            "ticks": (2,),
            "entity_ids": (3,),
        }
    )
    assert operation.storage_config is storage

    dumped = operation.model_dump(mode="python")
    assert dumped == {
        "operation": "evaluate",
        "world_id": world_id,
        "components": (_Telemetry,),
        "contract": {
            "grader_id": "contract-first",
            "implementation_version": "v1",
            "config": {},
            "thresholds": {},
            "seed": None,
        },
        "grader": _grader,
        "evaluation_id": "evaluation",
        "storage_config": {
            "uri": str(tmp_path / "store"),
            "namespace": "eval",
            "backend": storage.backend,
            "io_config": None,
        },
        "ticks": (2,),
        "entity_ids": (3,),
    }
    reconstructed = Evaluate.model_validate(dumped)
    assert isinstance(reconstructed.storage_config, StorageConfig)
    assert reconstructed.storage_config == storage
    assert reconstructed.storage_config is not storage

    from_mapping = Evaluate(
        **{
            **_operation_fields(),
            "storage_config": {
                "uri": str(tmp_path / "store"),
                "namespace": "eval",
            },
        }
    )
    assert isinstance(from_mapping.storage_config, StorageConfig)
    assert from_mapping.storage_config == storage

    assert operation.model_dump(
        mode="json",
        exclude={"components", "grader"},
    ) == {
        "operation": "evaluate",
        "world_id": str(world_id),
        "contract": {
            "grader_id": "contract-first",
            "implementation_version": "v1",
            "config": {},
            "thresholds": {},
            "seed": None,
        },
        "evaluation_id": "evaluation",
        "storage_config": {
            "uri": str(tmp_path / "store"),
            "namespace": "eval",
            "backend": storage.backend.value,
            "io_config": None,
        },
        "ticks": [2],
        "entity_ids": [3],
    }


@pytest.mark.asyncio
async def test_missing_storage_rejects_before_storage_or_grader_effects() -> None:
    from archetype.evaluation.handlers import evaluate

    effects: list[str] = []

    class StorageTrap:
        def __getattr__(self, name: str) -> Any:
            effects.append(f"storage:{name}")
            raise AssertionError(f"missing coordinates reached storage effect {name}")

    def grader_trap(_frame: Any) -> Outcome:
        effects.append("grader")
        raise AssertionError("missing coordinates reached grader")

    operation = Evaluate.model_construct(
        **{
            **_operation_fields(),
            "grader": grader_trap,
            "storage_config": None,
        }
    )
    with pytest.raises(ValueError, match="explicit storage_config"):
        await evaluate(StorageTrap(), operation)

    assert effects == []


@pytest.mark.asyncio
async def test_unsupported_receipt_storage_rejects_before_effects(tmp_path: Path) -> None:
    from archetype.core.config import StorageConfig
    from archetype.evaluation.handlers import evaluate

    effects: list[str] = []

    class StorageTrap:
        def __getattr__(self, name: str) -> Any:
            effects.append(f"storage:{name}")
            raise AssertionError(f"unsupported backend reached storage effect {name}")

    def grader_trap(_frame: Any) -> Outcome:
        effects.append("grader")
        raise AssertionError("unsupported backend reached grader")

    operation = Evaluate(
        **{
            **_operation_fields(),
            "grader": grader_trap,
            "storage_config": StorageConfig(uri=str(tmp_path / "lancedb")),
        }
    )
    with pytest.raises(ValueError, match=r"StorageBackend\.ICEBERG"):
        await evaluate(StorageTrap(), operation)

    assert effects == []


def test_family_topology_has_no_external_ontology_or_application_imports() -> None:
    forbidden = (
        "archetype.app",
        "archetype.api",
        "archetype.cli",
        "archetype.missions",
        "archetype.runtime",
        "archetype.runtime_resources",
        "archetype.wiring",
        "evals",
    )
    expected = {
        "components.py",
        "contracts.py",
        "grading.py",
        "handlers.py",
        "models.py",
        "views.py",
    }
    assert expected.issubset({path.name for path in _FAMILY.glob("*.py")})

    for path in sorted(_FAMILY.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                imports.add(node.module)
        outward = sorted(
            module
            for module in imports
            if any(module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden)
        )
        assert not outward, f"{path.name} imports forbidden ontology: {outward}"
