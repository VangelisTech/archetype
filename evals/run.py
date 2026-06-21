# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run all evals and report results.

Usage:
    python -m evals.run [--out results.json] [--suite regression|capability] [--trials 3]

Reports pass@k (fraction of k trials that passed) and pass^k (1.0 only
when every one of the k trials passed, 0.0 otherwise) per task, grouped
by suite.  Exit code 0 only when at least one task ran, all regression
tasks pass, and no capability tasks error out.
"""

from __future__ import annotations

import argparse
import json

from evals.harness import EvalHarness
from evals.suites import capability, poison_command, regression, spec_contracts
from evals.types import TaskResult

REQUIRED_SUITES = frozenset({"regression", "spec"})
KNOWN_SUITES = ("regression", "spec", "capability")


def _positive_int(value: str) -> int:
    """Argparse type that accepts only strictly positive integers."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {parsed}")
    return parsed


def build_harness(trials: int = 1) -> EvalHarness:
    harness = EvalHarness(trials=trials)
    regression.register(harness)
    poison_command.register(harness)
    spec_contracts.register(harness)
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

    for suite_name in KNOWN_SUITES:
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
    for suite_name in KNOWN_SUITES:
        tasks = suites.get(suite_name, [])
        passed = sum(1 for t in tasks if t.all_passed)
        print(f"  {suite_name.title()}: {passed}/{len(tasks)} passed")
    print("=" * 72)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run archetype evals")
    parser.add_argument("--out", default=None, help="Write JSON results to file")
    parser.add_argument("--suite", choices=KNOWN_SUITES, default=None)
    parser.add_argument(
        "--trials",
        type=_positive_int,
        default=1,
        help="Trials per task (for pass@k); must be >= 1",
    )
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

    if not results:
        print("\nNo eval tasks executed; refusing to report success.")
        return 1

    by_suite: dict[str, list[TaskResult]] = {
        suite_name: [task for task in results if task.suite == suite_name]
        for suite_name in KNOWN_SUITES
    }

    required_to_exist = {args.suite} if args.suite else REQUIRED_SUITES
    required_to_pass = {args.suite} if args.suite else set(KNOWN_SUITES)
    for suite_name in required_to_exist:
        if not by_suite[suite_name]:
            print(f"\nNo {suite_name} tasks executed; refusing to report success.")
            return 1

    suite_status: dict[str, bool] = {}
    for suite_name, tasks in by_suite.items():
        if suite_name in REQUIRED_SUITES:
            suite_status[suite_name] = all(task.all_passed for task in tasks)
        else:
            suite_status[suite_name] = all(
                not any(trial.error for trial in task.trials) for task in tasks
            )

    return 0 if all(suite_status[suite_name] for suite_name in required_to_pass) else 1


if __name__ == "__main__":
    main()
