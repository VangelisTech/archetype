#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate normative contract traceability and blocking eval coverage."""

from __future__ import annotations

import argparse
import ast
import re
import tomllib
from pathlib import Path
from typing import Any

from scripts.validate_benchmarks import REGISTRY as BENCHMARK_REGISTRY
from scripts.validate_benchmarks import load_benchmarks

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "quality" / "contracts.toml"
EVAL_PROFILES = ROOT / "quality" / "eval_profiles.toml"
_ID = re.compile(r"^[a-z][a-z0-9]*(?:[._][a-z0-9]+)+$")
_RISKS = {"low", "medium", "high"}
_PROFILES = {"quick", "pr", "main", "nightly", "release"}


def load_contracts(path: Path = REGISTRY) -> list[dict[str, Any]]:
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    if payload.get("version") != 1:
        raise ValueError(f"{path}: unsupported contract registry version")
    rows = payload.get("contract")
    if not isinstance(rows, list):
        raise ValueError(f"{path}: [[contract]] rows are required")
    return rows


def contract_eval_map(path: Path = REGISTRY) -> dict[str, tuple[str, ...]]:
    """Return eval task -> stable contract IDs for report traceability."""
    mapped: dict[str, list[str]] = {}
    for contract in load_contracts(path):
        contract_id = contract.get("id")
        if not isinstance(contract_id, str):
            continue
        for task_id in contract.get("evals", []):
            if isinstance(task_id, str):
                mapped.setdefault(task_id, []).append(contract_id)
    return {task_id: tuple(sorted(ids)) for task_id, ids in mapped.items()}


def _headings(path: Path) -> set[str]:
    headings: set[str] = set()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return headings
    for line in text.splitlines():
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
        if match:
            headings.add(match.group(1))
    return headings


def _marker_contract_ids(root: Path) -> list[tuple[Path, int, str]]:
    found: list[tuple[Path, int, str]] = []
    tests = root / "tests"
    if not tests.is_dir():
        return found
    for path in tests.rglob("test_*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            fn = node.func
            if not (
                isinstance(fn, ast.Attribute)
                and fn.attr == "contract"
                and isinstance(fn.value, ast.Attribute)
                and fn.value.attr == "mark"
                and isinstance(fn.value.value, ast.Name)
                and fn.value.value.id == "pytest"
            ):
                continue
            value = node.args[0]
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                found.append((path, node.lineno, value.value))
    return found


def _eval_inventory() -> tuple[set[str], set[str]]:
    from evals.run import build_harness

    harness = build_harness(trials=1)
    tasks = {task_id for task_id, _suite, _fn, _desc in harness._tasks}
    suites_by_task = {task_id: suite for task_id, suite, _fn, _desc in harness._tasks}
    with EVAL_PROFILES.open("rb") as stream:
        profiles = tomllib.load(stream)
    blocking_suites = {
        name
        for name, config in profiles.get("suite", {}).items()
        if isinstance(config, dict) and config.get("blocking") is True
    }
    blocking_tasks = {
        task_id for task_id, suite in suites_by_task.items() if suite in blocking_suites
    }
    return tasks, blocking_tasks


def validate_contracts(
    *,
    root: Path = ROOT,
    registry_path: Path = REGISTRY,
    benchmark_registry: Path = BENCHMARK_REGISTRY,
    check_eval_coverage: bool = True,
) -> list[str]:
    errors: list[str] = []
    try:
        rows = load_contracts(registry_path)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        return [str(exc)]

    try:
        benchmark_ids = {row["id"] for row in load_benchmarks(benchmark_registry)}
    except (OSError, tomllib.TOMLDecodeError, ValueError, KeyError) as exc:
        errors.append(f"cannot load benchmark registry: {exc}")
        benchmark_ids = set()

    eval_tasks: set[str] = set()
    blocking_tasks: set[str] = set()
    if check_eval_coverage:
        try:
            eval_tasks, blocking_tasks = _eval_inventory()
        except Exception as exc:
            errors.append(f"cannot build eval inventory: {exc}")

    seen: set[str] = set()
    mapped_evals: set[str] = set()
    for index, row in enumerate(rows):
        label = f"contract[{index}]"
        contract_id = row.get("id")
        if not isinstance(contract_id, str) or not _ID.fullmatch(contract_id):
            errors.append(f"{label}: invalid contract id {contract_id!r}")
            continue
        label = contract_id
        if contract_id in seen:
            errors.append(f"{label}: duplicate contract id")
        seen.add(contract_id)

        if row.get("risk") not in _RISKS:
            errors.append(f"{label}: risk must be one of {sorted(_RISKS)}")
        if not isinstance(row.get("owner"), str) or not row["owner"]:
            errors.append(f"{label}: owner is required")

        source = row.get("source")
        source_path = root / source if isinstance(source, str) else None
        if source_path is None or not source_path.is_file():
            errors.append(f"{label}: missing normative source {source!r}")
        section = row.get("section")
        if not isinstance(section, str) or not section:
            errors.append(f"{label}: source section is required")
        elif (
            source_path is not None
            and source_path.is_file()
            and section not in _headings(source_path)
        ):
            errors.append(f"{label}: missing source heading {section!r} in {source}")

        oracle_fields = ("pytest", "static", "evals", "benchmarks")
        if not any(row.get(field) for field in oracle_fields) and not row.get("design_only"):
            errors.append(f"{label}: at least one executable oracle is required")
        for field in oracle_fields:
            if not isinstance(row.get(field), list):
                errors.append(f"{label}: {field} must be an array")

        for nodeid in row.get("pytest", []):
            if not isinstance(nodeid, str) or not (root / nodeid.split("::", 1)[0]).is_file():
                errors.append(f"{label}: missing pytest oracle {nodeid!r}")
        for script in row.get("static", []):
            if not isinstance(script, str) or not (root / script).is_file():
                errors.append(f"{label}: missing static oracle {script!r}")
        for task_id in row.get("evals", []):
            if not isinstance(task_id, str):
                errors.append(f"{label}: eval IDs must be strings")
                continue
            mapped_evals.add(task_id)
            if check_eval_coverage and eval_tasks and task_id not in eval_tasks:
                errors.append(f"{label}: unknown eval task {task_id}")
        for benchmark_id in row.get("benchmarks", []):
            if benchmark_id not in benchmark_ids:
                errors.append(f"{label}: unknown benchmark {benchmark_id!r}")

        profiles = row.get("profiles")
        if not isinstance(profiles, list) or not profiles:
            errors.append(f"{label}: at least one execution profile is required")
        else:
            unknown = set(profiles) - _PROFILES
            if unknown:
                errors.append(f"{label}: unknown profiles {sorted(unknown)}")

    if check_eval_coverage:
        unmapped = sorted(blocking_tasks - mapped_evals)
        if unmapped:
            errors.append(f"blocking eval tasks without contract mapping: {unmapped}")

    for path, line, contract_id in _marker_contract_ids(root):
        if contract_id not in seen:
            errors.append(
                f"{path.relative_to(root)}:{line}: pytest marker references unknown contract "
                f"{contract_id}"
            )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    args = parser.parse_args(argv)
    errors = validate_contracts(registry_path=args.registry)
    if errors:
        for error in errors:
            print(f"contract registry error: {error}")
        return 1
    print("Contract registry audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
