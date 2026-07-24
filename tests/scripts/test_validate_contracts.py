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


def _oracle_row(nodeid: str) -> str:
    return f'''[[contract]]
id = "test.oracle"
source = "docs/guide/api-stability.md"
section = "What counts as public"
owner = "test"
risk = "low"
pytest = ["{nodeid}"]
static = []
evals = []
benchmarks = []
profiles = ["pr"]
'''


def _oracle_errors(tmp_path: Path, nodeid: str) -> list[str]:
    registry = tmp_path / "contracts.toml"
    registry.write_text(_registry(_oracle_row(nodeid)))
    return validate_contracts(root=ROOT, registry_path=registry, check_eval_coverage=False)


def test_renamed_pytest_oracle_node_fails_closed(tmp_path: Path) -> None:
    """A registry node id whose test no longer exists must not pass.

    The file existing is not evidence the contract is executed. Before this
    check the registry could name any symbol inside a real module and stay
    green, so a test rename silently emptied the oracle.
    """
    errors = _oracle_errors(
        tmp_path,
        "tests/scripts/test_validate_contracts.py::test_this_function_does_not_exist",
    )

    assert any("missing pytest oracle node" in error for error in errors)


def test_renamed_pytest_oracle_class_fails_closed(tmp_path: Path) -> None:
    errors = _oracle_errors(
        tmp_path,
        "tests/scripts/test_validate_contracts.py::NoSuchClass::test_repository_contract_registry_is_valid",
    )

    assert any("missing pytest oracle node" in error for error in errors)


def test_resolvable_pytest_oracle_node_passes(tmp_path: Path) -> None:
    """Negative control: a node id that still resolves raises no oracle error."""
    errors = _oracle_errors(
        tmp_path,
        "tests/scripts/test_validate_contracts.py::test_repository_contract_registry_is_valid",
    )

    assert [error for error in errors if "pytest oracle" in error] == []


def test_parametrized_pytest_oracle_node_resolves_to_its_definition(tmp_path: Path) -> None:
    errors = _oracle_errors(
        tmp_path,
        "tests/scripts/test_validate_contracts.py::test_repository_contract_registry_is_valid[case]",
    )

    assert [error for error in errors if "pytest oracle" in error] == []


def test_unparsable_pytest_oracle_module_fails_closed(tmp_path: Path) -> None:
    """A module the audit cannot parse fails; it never resolves vacuously."""
    (tmp_path / "docs").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "docs" / "contract.md").write_text("# Contract\n")
    (tmp_path / "tests" / "test_broken.py").write_text("def test_x(:\n")
    registry = tmp_path / "contracts.toml"
    registry.write_text(
        """version = 1
[[contract]]
id = "broken.oracle"
source = "docs/contract.md"
section = "Contract"
owner = "test"
risk = "low"
pytest = ["tests/test_broken.py::test_x"]
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

    assert any("unparsable pytest oracle module" in error for error in errors)


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
