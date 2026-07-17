# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run repository verification scenarios and report their outcomes.

Usage:
    python -m evals.run [--out results.json] [--suite SUITE] [--trials 3]
    python -m evals.run --list [--suite SUITE]

Reports the trial count, pass rate, average grader score, and whether every
trial passed. Every group fails on a missed grader or scenario error.
"""

from __future__ import annotations

import argparse
import json
import logging

from evals.harness import EvalHarness
from evals.suites import capability, idempotency, poison_command, regression, spec_contracts
from evals.types import TaskResult

REQUIRED_SUITES = frozenset({"regression", "spec", "idempotency", "capability"})
KNOWN_SUITES = ("regression", "spec", "idempotency", "capability")


class _ExpectedEvalNoiseFilter:
    """Drop only records produced by intentional adversarial eval inputs."""

    _NOOP_COMMAND_TYPES = frozenset({"message", "query_world", "custom"})
    _RESERVED_SPAWN_PREFIX = "Entity "
    _RESERVED_SPAWN_SUFFIX = (
        " is already registered. Use update_entity to change component values on a live entity."
    )

    @classmethod
    def _is_expected_apply_failure(cls, record: logging.LogRecord) -> bool:
        if not record.exc_info:
            return False
        error = record.exc_info[1]
        if isinstance(error, KeyError):
            return error.args == ("entity_id",)
        if not isinstance(error, ValueError):
            return False
        detail = str(error)
        if (
            "requires a 'type' key" in detail
            or detail == "Component type 'TotallyFakeComponent' not found."
        ):
            return True
        if not (
            detail.startswith(cls._RESERVED_SPAWN_PREFIX)
            and detail.endswith(cls._RESERVED_SPAWN_SUFFIX)
        ):
            return False
        entity_id = detail[len(cls._RESERVED_SPAWN_PREFIX) : -len(cls._RESERVED_SPAWN_SUFFIX)]
        return entity_id.isdigit()

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if record.name == "archetype.app.command_service":
            if message.startswith("Failed to apply command "):
                return not self._is_expected_apply_failure(record)
            prefix = "Unhandled command type in drain: "
            if message.startswith(prefix):
                return message.removeprefix(prefix) not in self._NOOP_COMMAND_TYPES
        if record.name == "archetype.core.aio.async_world":
            return "Entity Removal Failed: No entity:" not in message
        return True


def _configure_eval_logging() -> None:
    """Filter expected adversarial noise unless a host configured logging.

    Poison-command tasks deliberately exercise failures that the command
    drain converts into graded outcomes.  Without an application-owned
    handler, Python's ``lastResort`` handler prints those expected records as
    tracebacks.  The eval CLI is the application boundary, so it owns a
    filtered package sink while leaving handlers configured on the package or
    an ancestor alone. Unexpected records still reach the host's sink.
    """
    package_logger = logging.getLogger("archetype")
    if package_logger.hasHandlers():
        return
    handler = logging.StreamHandler()
    handler.addFilter(_ExpectedEvalNoiseFilter())
    package_logger.addHandler(handler)
    package_logger.propagate = False


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
    idempotency.register(harness)
    capability.register(harness)
    return harness


def print_report(results: list[TaskResult]) -> None:
    """Print a suite-grouped summary report."""
    suites: dict[str, list[TaskResult]] = {}
    for r in results:
        suites.setdefault(r.suite, []).append(r)

    print("\n" + "=" * 72)
    print("REPOSITORY CHECK RESULTS")
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
            if t.trial_count > 1:
                line += (
                    f"  (trials={t.trial_count}, pass_rate={t.pass_rate:.0%}, "
                    f"all_passed={str(t.all_passed).lower()})"
                )
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


def print_task_list(harness: EvalHarness, *, suite_filter: str | None = None) -> int:
    """Print the live task registry without executing it."""
    tasks = [
        registration
        for registration in harness.registered_tasks
        if suite_filter is None or registration[1] == suite_filter
    ]
    if not tasks:
        print("No registered tasks match the requested suite.")
        return 1

    print("suite\ttask\tdescription")
    for task_id, suite, _fn, desc in tasks:
        print(f"{suite}\t{task_id}\t{desc}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Archetype repository checks")
    parser.add_argument("--out", default=None, help="Write JSON results to file")
    parser.add_argument("--suite", choices=KNOWN_SUITES, default=None)
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_tasks",
        help="List registered tasks without executing them",
    )
    parser.add_argument(
        "--trials",
        type=_positive_int,
        default=1,
        help="Repeated executions per task; must be >= 1",
    )
    args = parser.parse_args()

    harness = build_harness(trials=args.trials)
    if args.list_tasks:
        return print_task_list(harness, suite_filter=args.suite)

    _configure_eval_logging()
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
        suite_status[suite_name] = all(task.all_passed for task in tasks)

    return 0 if all(suite_status[suite_name] for suite_name in required_to_pass) else 1


if __name__ == "__main__":
    raise SystemExit(main())
