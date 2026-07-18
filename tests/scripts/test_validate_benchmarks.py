# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for benchmark ownership and comparison policy metadata."""

from pathlib import Path

from scripts.validate_benchmarks import ROOT, validate_benchmarks


def test_repository_benchmark_registry_is_valid() -> None:
    assert validate_benchmarks() == []


def test_blocking_benchmark_requires_stable_runner(tmp_path: Path) -> None:
    registry = tmp_path / "benchmarks.toml"
    registry.write_text(
        """version = 1
[[benchmark]]
id = "test.benchmark"
suite = "test"
owner = "test"
entrypoint = "bench/core/query_latency.py"
correctness_oracles = ["tests/bench/test_query_latency.py"]
report_schema = "test/v1"
comparison_policy = "blocking"
stable_runner = false
profiles = ["nightly"]
metrics = [{ name = "elapsed_s", direction = "lower" }]
"""
    )

    errors = validate_benchmarks(root=ROOT, registry_path=registry)

    assert any("blocking timing requires a stable runner" in error for error in errors)
