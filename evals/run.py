# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run all evals and report results by tier.

Usage:
    python -m evals.run [--out results.json] [--tier unit|integration|end_to_end]

Exit code 0 if all evals pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from collections.abc import Callable

from evals.types import EvalResult, Status, Tier

# ---------------------------------------------------------------------------
# Registry: (name, tier, run_fn) — each run_fn returns list[EvalResult]
# ---------------------------------------------------------------------------

EvalEntry = tuple[str, Tier, Callable[[], list[EvalResult]]]


def _registry() -> list[EvalEntry]:
    """Lazily import eval modules to avoid import-time side effects."""
    from evals.end_to_end.eval_simulation import run as e2e_simulation
    from evals.integration.eval_command_pipeline import run as integ_command
    from evals.integration.eval_storage_roundtrip import run as integ_storage
    from evals.unit.eval_archetype_signatures import run as unit_archetype
    from evals.unit.eval_command_ordering import run as unit_command
    from evals.unit.eval_component_serde import run as unit_component
    from evals.unit.eval_rbac import run as unit_rbac

    return [
        # Unit evals (fast, deterministic, no I/O)
        ("component_serde", Tier.UNIT, unit_component),
        ("archetype_signatures", Tier.UNIT, unit_archetype),
        ("rbac", Tier.UNIT, unit_rbac),
        ("command_ordering", Tier.UNIT, unit_command),
        # Integration evals (multi-step, storage I/O)
        ("storage_roundtrip", Tier.INTEGRATION, integ_storage),
        ("command_pipeline", Tier.INTEGRATION, integ_command),
        # End-to-end evals (full simulation)
        ("simulation", Tier.END_TO_END, e2e_simulation),
    ]


def run_evals(tier_filter: Tier | None = None) -> list[EvalResult]:
    """Run all registered evals, optionally filtered by tier."""
    all_results: list[EvalResult] = []

    for name, tier, fn in _registry():
        if tier_filter and tier != tier_filter:
            continue

        t0 = time.perf_counter()
        try:
            case_results = fn()
            elapsed = time.perf_counter() - t0
            for r in case_results:
                r.elapsed_s = elapsed / max(len(case_results), 1)
            all_results.extend(case_results)
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            all_results.append(
                EvalResult(
                    case_name=f"{name}__error",
                    tier=tier,
                    status=Status.ERROR,
                    score=0.0,
                    elapsed_s=elapsed,
                    error=str(exc),
                )
            )

    return all_results


def print_report(results: list[EvalResult]) -> None:
    """Print a tier-grouped summary report."""
    by_tier: dict[Tier, list[EvalResult]] = defaultdict(list)
    for r in results:
        by_tier[r.tier].append(r)

    print("\n" + "=" * 70)
    print("EVAL RESULTS")
    print("=" * 70)

    for tier in [Tier.UNIT, Tier.INTEGRATION, Tier.END_TO_END]:
        tier_results = by_tier.get(tier, [])
        if not tier_results:
            continue

        passed = sum(1 for r in tier_results if r.passed)
        total = len(tier_results)
        avg_score = sum(r.score for r in tier_results) / total if total else 0

        print(f"\n  [{tier.value.upper()}] {passed}/{total} passed (avg score: {avg_score:.2f})")
        print(f"  {'-' * 60}")

        for r in tier_results:
            icon = "PASS" if r.passed else "FAIL" if r.status == Status.FAIL else "ERR!"
            line = f"    [{icon}] {r.case_name}"
            if r.score < 1.0 and r.details:
                line += f"  — {r.details[:80]}"
            if r.error:
                line += f"  — error: {r.error[:80]}"
            print(line)

    print("\n" + "=" * 70)
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed
    print(f"TOTAL: {passed}/{total} passed, {failed} failed")
    print("=" * 70)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run archetype evals")
    parser.add_argument("--out", default=None, help="Write JSON results to this file")
    parser.add_argument(
        "--tier",
        choices=["unit", "integration", "end_to_end"],
        default=None,
        help="Run only evals in this tier",
    )
    args = parser.parse_args()

    tier_filter = Tier(args.tier) if args.tier else None
    results = run_evals(tier_filter)

    print_report(results)

    if args.out:
        import os

        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        records = [
            {
                "case_name": r.case_name,
                "tier": r.tier.value,
                "status": r.status.value,
                "score": r.score,
                "elapsed_s": round(r.elapsed_s, 4),
                "details": r.details,
                "error": r.error,
            }
            for r in results
        ]
        with open(args.out, "w") as f:
            json.dump(records, f, indent=2)
        print(f"\nResults written to {args.out}")

    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
