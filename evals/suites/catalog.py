# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Current native conformance registration; 0.6 suites are versioned compatibility."""

from evals.harness import EvalHarness


def register_all(harness: EvalHarness) -> None:
    """Register pure exact-cell verification without importing legacy live owners."""
    from evals.types import GraderResult

    def exact_cells():
        from archetype_native.values import float_bits
        from archetype_native.wire import Cell

        values = [("int64", str(2**53 + 117)), ("bool", True), ("float64", float_bits(1.25))]
        passed = all(Cell.decode({tag: value}).kind == tag for tag, value in values)
        return [GraderResult(grader_name="exact_cells", passed=passed, score=float(passed))]

    harness.add(
        "native.exact_cells",
        suite="native",
        fn=exact_cells,
        desc="Exact Int64, Bool and finite Float64 wire values",
    )
