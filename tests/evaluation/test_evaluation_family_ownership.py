# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation family ownership regressions (issues #557 and #650).

The family owns its durable workflow without changing any serialized field,
default, vocabulary, or digest. These tests pin byte-identical digest vectors,
an unchanged Arrow/Pydantic schema, single class identity behind supported root
exports, and the absence of the former application facade.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

import archetype
from archetype.core.component import Component
from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import (
    OUTCOME_STATUSES,
    FrameGrader,
    GraderContract,
    Outcome,
    evaluation_identity_digest,
    subject_digest,
)

_SRC = Path(__file__).resolve().parents[2] / "packages" / "archetype-ecs" / "src" / "archetype"
_FAMILY_DIR = _SRC / "evaluation"
_APP_EVALUATION_DIR = _SRC / "app" / "evaluation"


def test_eval_receipt_arrow_schema_is_unchanged() -> None:
    schema = EvalReceipt.get_prefixed_schema()
    assert [(field.name, field.type) for field in schema] == [
        ("evalreceipt__evaluation_id", pa.string()),
        ("evalreceipt__subject_digest", pa.string()),
        ("evalreceipt__contract_digest", pa.string()),
        ("evalreceipt__grader_id", pa.string()),
        ("evalreceipt__outcome", pa.string()),
        ("evalreceipt__score", pa.float64()),
        ("evalreceipt__graded_at_ms", pa.int64()),
        ("evalreceipt__evidence_json", pa.string()),
    ]


def test_eval_receipt_pydantic_defaults_are_unchanged() -> None:
    defaults = {name: info.default for name, info in EvalReceipt.model_fields.items()}
    assert defaults == {
        "evaluation_id": "",
        "subject_digest": "",
        "contract_digest": "",
        "grader_id": "",
        "outcome": "",
        "score": None,
        "graded_at_ms": 0,
        "evidence_json": "{}",
    }


def test_outcome_vocabulary_is_unchanged() -> None:
    assert OUTCOME_STATUSES == frozenset({"pass", "fail", "invalid", "inconclusive"})


def test_digest_vectors_are_byte_for_byte_unchanged() -> None:
    """Vectors recorded before evaluation became a self-contained family."""
    contract = GraderContract(
        grader_id="mean-reading-v1",
        implementation_version="2026.07.15",
        config={"prompt": "grade the mean", "temperature": 0.0},
        thresholds={"min": 0.5},
        seed=7,
    )
    assert contract.digest() == ("1a564400f48bb599ae183c9a06edcfcbd6336cc60b6801a8acef8cb875619b6f")
    assert GraderContract(grader_id="g", implementation_version="v").digest() == (
        "65a4850dccb6a2221d571a654b82bafd203767f3fecaa8adb619b345e59e7129"
    )

    subject = subject_digest(
        "world-1",
        "run-1",
        snapshot_tick=3,
        snapshot_tokens=["tok-b", "tok-a"],
        component_names=["Telemetry", "Agent"],
        ticks=[2, 0, 1],
        entity_ids=[5, 3],
    )
    assert subject == "fa5fd5e8f4b490fd2f053cc0abd024dace6e5f1b91887699b6a77d3c96450148"
    assert subject_digest("w", "r", snapshot_tick=0, snapshot_tokens=[], component_names=["C"]) == (
        "e963f7fb8436f4d4c77e53855c7adceb85d0db7614fe2c0444676ad1f4abc8a3"
    )
    inherited = subject_digest(
        "fork",
        "fork-run",
        snapshot_tick=3,
        snapshot_tokens=["fork-head"],
        snapshot_segments=[("source", "source-run", 2, ["source-head"])],
        component_names=["C"],
    )
    assert inherited != subject_digest(
        "fork",
        "fork-run",
        snapshot_tick=3,
        snapshot_tokens=["fork-head"],
        snapshot_segments=[("source", "source-run", 2, ["changed-source-head"])],
        component_names=["C"],
    )

    assert evaluation_identity_digest(subject, contract.digest()) == (
        "2c1ab4dcee38d540ecfdc9d485376d46eb6f2ad06823264b7982b18bda5d7247"
    )


def test_supported_root_exports_resolve_to_the_single_moved_definitions() -> None:
    assert archetype.EvalReceipt is EvalReceipt
    assert archetype.FrameGrader is FrameGrader
    assert archetype.GraderContract is GraderContract
    assert archetype.Outcome is Outcome
    assert EvalReceipt.__module__ == "archetype.evaluation.components"
    assert GraderContract.__module__ == "archetype.evaluation.models"
    assert Outcome.__module__ == "archetype.evaluation.models"


def test_contracts_import_without_the_component_stack() -> None:
    """Digest/contract-only consumers never pay for pyarrow/lancedb.

    The family initializer lazy-loads ``EvalReceipt``; importing the pure
    contracts module must not drag in ``archetype.evaluation.components``
    or ``archetype.core.component``.
    """
    probe = (
        "import sys\n"
        "from archetype.evaluation.contracts import GraderContract, Outcome\n"
        "heavy = [m for m in ('archetype.evaluation.components', 'archetype.core.component',\n"
        "                     'pyarrow', 'lancedb') if m in sys.modules]\n"
        "assert not heavy, f'contracts import loaded {heavy}'\n"
        "from archetype.evaluation import EvalReceipt\n"
        "assert EvalReceipt.__module__ == 'archetype.evaluation.components'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def test_no_duplicate_eval_receipt_component_exists() -> None:
    """`get_type_by_name` raises when two Component subclasses share a name."""
    assert Component.get_type_by_name("EvalReceipt") is EvalReceipt
    assert not any(_APP_EVALUATION_DIR.glob("*.py"))


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return modules


def test_evaluation_family_has_no_outward_dependency_or_app_facade() -> None:
    forbidden_prefixes = ("archetype.app", "archetype.runtime", "archetype.api", "archetype.cli")
    for path in sorted(_FAMILY_DIR.rglob("*.py")):
        outward = {
            module for module in _imported_modules(path) if module.startswith(forbidden_prefixes)
        }
        assert not outward, f"{path} imports outward packages: {sorted(outward)}"

    assert not any(_APP_EVALUATION_DIR.glob("*.py"))
