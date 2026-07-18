# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the normative contract registry."""

from __future__ import annotations

from pathlib import Path

from scripts.generate_contract_traceability import OUTPUT, render
from scripts.generate_contract_traceability import main as generate_traceability
from scripts.validate_contracts import ROOT, validate_contracts


def _registry(contract_rows: str) -> str:
    return f"version = 1\n\n{contract_rows}"


def _row(contract_id: str, *, section: str = "What counts as public") -> str:
    return f'''[[contract]]
id = "{contract_id}"
source = "docs/guide/api-stability.md"
section = "{section}"
owner = "test"
risk = "low"
pytest = ["tests/scripts/test_validate_contracts.py"]
static = []
evals = []
benchmarks = []
profiles = ["pr"]
'''


def test_repository_contract_registry_is_valid() -> None:
    assert validate_contracts() == []


def test_generated_traceability_matches_registry() -> None:
    assert OUTPUT.read_text(encoding="utf-8") == render()


def test_stale_traceability_fails_closed(tmp_path: Path) -> None:
    output = tmp_path / "traceability.md"
    output.write_text("stale\n", encoding="utf-8")

    assert generate_traceability(["--output", str(output), "--check"]) == 1


def test_duplicate_contract_ids_fail_closed(tmp_path: Path) -> None:
    registry = tmp_path / "contracts.toml"
    registry.write_text(_registry(_row("test.duplicate") + _row("test.duplicate")))

    errors = validate_contracts(
        root=ROOT,
        registry_path=registry,
        check_eval_coverage=False,
    )

    assert any("duplicate contract id" in error for error in errors)


def test_stale_normative_heading_fails_closed(tmp_path: Path) -> None:
    registry = tmp_path / "contracts.toml"
    registry.write_text(_registry(_row("test.stale", section="Not a real heading")))

    errors = validate_contracts(
        root=ROOT,
        registry_path=registry,
        check_eval_coverage=False,
    )

    assert any("missing source heading" in error for error in errors)


def test_unknown_contract_marker_fails_closed(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "docs" / "contract.md").write_text("# Contract\n")
    (tmp_path / "tests" / "test_probe.py").write_text(
        "import pytest\npytestmark = pytest.mark.contract('unknown.contract')\n"
    )
    registry = tmp_path / "contracts.toml"
    registry.write_text(
        """version = 1
[[contract]]
id = "known.contract"
source = "docs/contract.md"
section = "Contract"
owner = "test"
risk = "low"
pytest = ["tests/test_probe.py"]
static = []
evals = []
benchmarks = []
profiles = ["pr"]
"""
    )

    errors = validate_contracts(
        root=tmp_path,
        registry_path=registry,
        check_eval_coverage=False,
    )

    assert any("unknown contract unknown.contract" in error for error in errors)
