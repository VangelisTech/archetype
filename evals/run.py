# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run all evals and report results.

Usage:
    python -m evals.run [--out results.json] [--suite regression|capability] [--trials 3]

Reports pass@k (at least one trial passes, binary) and pass^k (all k
trials pass, binary) per task, grouped by suite.  Exit code 0 if all
regression tasks pass and no capability tasks error out.
"""

from __future__ import annotations

import argparse
import json
import sys

from evals.harness import EvalHarness
from evals.suites import capability, poison_command, regression
from evals.types import TaskResult


def build_harness(trials: int = 1) -> EvalHarness:
    harness = EvalHarness(trials=trials)
    regression.register(harness)
    poison_command.register(harness)
    capability.register(harness)
    return harness


def print_report(results: list[TaskResult]) -> None:
    """Print a suite-grouped summary report."""
    suites: dict[str, list[TaskResult]] = {}
    for r in results:
        suites.setdefault(r.suite, []).append(r)

    print("\n" + "=" * 72)
    print("EVAL RESULTS")
    print("=" * 72)

    for suite_name in ["regression", "capability"]:
        tasks = suites.get(suite_name, [])
        if not tasks:
            continue

        passed = sum(1 for t in tasks if t.all_passed)
        total = len(tasks)

        print(f"\n  [{suite_name.upper()}] {passed}/{total} tasks fully passing")
        print(f"  {'-' * 64}")

        for t in tasks:
            icon = "PASS" if t.all_passed else "FAIL"
            line = f"    [{icon}] {t.task_id}"
            if t.k > 1:
                line += f"  (pass@{t.k}={t.pass_at_k:.0%}, pass^{t.k}={t.pass_pow_k:.0%})"
            line += f"  score={t.avg_score:.2f}"

            # Show failed graders
            for trial in t.trials:
                if trial.error:
                    line += f"\n           error: {trial.error[:80]}"
                for g in trial.grader_results:
                    if not g.passed:
                        line += f"\n           [{g.grader_name}] {g.details[:80]}"
            print(line)

    print("\n" + "=" * 72)

    # Summary
    reg_tasks = suites.get("regression", [])
    cap_tasks = suites.get("capability", [])
    reg_pass = sum(1 for t in reg_tasks if t.all_passed)
    cap_pass = sum(1 for t in cap_tasks if t.all_passed)
    print(f"  Regression: {reg_pass}/{len(reg_tasks)} passed")
    print(f"  Capability: {cap_pass}/{len(cap_tasks)} passed")
    print("=" * 72)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run archetype evals")
    parser.add_argument("--out", default=None, help="Write JSON results to file")
    parser.add_argument("--suite", choices=["regression", "capability"], default=None)
    parser.add_argument("--trials", type=int, default=1, help="Trials per task (for pass@k)")
    args = parser.parse_args()

    harness = build_harness(trials=args.trials)
    results = harness.run(suite_filter=args.suite)

    print_report(results)

    if args.out:
        import os

        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump([r.to_dict() for r in results], f, indent=2)
        print(f"\nResults written to {args.out}")

    # Exit code: regression tasks must all pass, capability tasks must not error
    reg_ok = all(t.all_passed for t in results if t.suite == "regression")
    cap_no_errors = all(
        not any(trial.error for trial in t.trials) for t in results if t.suite == "capability"
    )
    return 0 if reg_ok and cap_no_errors else 1


if __name__ == "__main__":
    sys.exit(main())
