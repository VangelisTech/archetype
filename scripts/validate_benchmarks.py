#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate benchmark ownership, correctness, and comparison policy metadata."""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "quality" / "benchmarks.toml"
_PROFILES = {"quick", "pr", "main", "nightly", "release"}
_DIRECTIONS = {"higher", "lower"}
_POLICIES = {"advisory", "blocking"}


def load_benchmarks(path: Path = REGISTRY) -> list[dict[str, Any]]:
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    if payload.get("version") != 1:
        raise ValueError(f"{path}: unsupported benchmark registry version")
    rows = payload.get("benchmark")
    if not isinstance(rows, list):
        raise ValueError(f"{path}: [[benchmark]] rows are required")
    return rows


def validate_benchmarks(*, root: Path = ROOT, registry_path: Path = REGISTRY) -> list[str]:
    errors: list[str] = []
    try:
        rows = load_benchmarks(registry_path)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        return [str(exc)]

    seen: set[str] = set()
    for index, row in enumerate(rows):
        label = f"benchmark[{index}]"
        benchmark_id = row.get("id")
        if not isinstance(benchmark_id, str) or not benchmark_id:
            errors.append(f"{label}: non-empty id is required")
            continue
        label = benchmark_id
        if benchmark_id in seen:
            errors.append(f"{label}: duplicate benchmark id")
        seen.add(benchmark_id)

        for field in ("suite", "owner", "entrypoint", "report_schema"):
            if not isinstance(row.get(field), str) or not row[field]:
                errors.append(f"{label}: {field} must be a non-empty string")

        entrypoint = row.get("entrypoint")
        if isinstance(entrypoint, str) and not (root / entrypoint).is_file():
            errors.append(f"{label}: missing entrypoint {entrypoint}")

        oracles = row.get("correctness_oracles")
        if not isinstance(oracles, list) or not oracles:
            errors.append(f"{label}: at least one correctness oracle is required")
        else:
            for oracle in oracles:
                if not isinstance(oracle, str) or not (root / oracle.split("::", 1)[0]).is_file():
                    errors.append(f"{label}: missing correctness oracle {oracle!r}")

        metrics = row.get("metrics")
        if not isinstance(metrics, list) or not metrics:
            errors.append(f"{label}: at least one metric is required")
        else:
            metric_names: set[str] = set()
            for metric in metrics:
                if not isinstance(metric, dict):
                    errors.append(f"{label}: metric rows must be tables")
                    continue
                name = metric.get("name")
                direction = metric.get("direction")
                if not isinstance(name, str) or not name:
                    errors.append(f"{label}: metric name is required")
                elif name in metric_names:
                    errors.append(f"{label}: duplicate metric {name}")
                else:
                    metric_names.add(name)
                if direction not in _DIRECTIONS:
                    errors.append(f"{label}: metric {name!r} has invalid direction {direction!r}")

        policy = row.get("comparison_policy")
        if policy not in _POLICIES:
            errors.append(f"{label}: invalid comparison_policy {policy!r}")
        stable_runner = row.get("stable_runner")
        if not isinstance(stable_runner, bool):
            errors.append(f"{label}: stable_runner must be boolean")
        if policy == "blocking" and stable_runner is not True:
            errors.append(f"{label}: blocking timing requires a stable runner")

        profiles = row.get("profiles")
        if not isinstance(profiles, list) or not profiles:
            errors.append(f"{label}: at least one execution profile is required")
        else:
            unknown = set(profiles) - _PROFILES
            if unknown:
                errors.append(f"{label}: unknown profiles {sorted(unknown)}")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    args = parser.parse_args(argv)
    errors = validate_benchmarks(registry_path=args.registry)
    if errors:
        for error in errors:
            print(f"benchmark registry error: {error}")
        return 1
    print("Benchmark registry audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
