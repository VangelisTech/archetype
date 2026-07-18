# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run repository verification scenarios and report their outcomes.

Usage:
    python -m evals.run [--out results.json]
        [--profile conformance|reliability|capability]
        [--suite regression|spec|idempotency|capability] [--trials 3]
    python -m evals.run --list [--suite SUITE]

Reports the trial count, empirical pass rate, unbiased Codex pass@k estimate,
strict pass^n repeatability, and average grader score per task. Named profiles
load membership and failure policy from ``quality/eval_profiles.toml``; every
current profile is blocking.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import tomllib
from pathlib import Path

from evals.harness import EvalHarness
from evals.suites.catalog import register_all
from evals.types import TaskResult, aggregate_pass_at_k
from quality.results import build_result_envelope, utc_now
from scripts.validate_contracts import contract_eval_map

REQUIRED_SUITES = frozenset({"regression", "spec", "idempotency", "capability"})
KNOWN_SUITES = ("regression", "spec", "idempotency", "capability")
ROOT = Path(__file__).resolve().parents[1]
PROFILE_REGISTRY = ROOT / "quality" / "eval_profiles.toml"


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
        if record.name == "archetype.app.gateway.service":
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


def load_profiles(path: Path = PROFILE_REGISTRY) -> dict[str, dict]:
    """Load the named eval profiles that define runner failure semantics."""
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    if payload.get("version") != 1 or not isinstance(payload.get("profile"), dict):
        raise ValueError(f"invalid eval profile registry: {path}")
    return payload["profile"]


def build_harness(trials: int = 1) -> EvalHarness:
    harness = EvalHarness(trials=trials, contract_map=contract_eval_map())
    register_all(harness)
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

        curve = aggregate_pass_at_k(tasks)
        metric_summary = ""
        if curve:
            last_k = max(curve)
            metric_summary = f"; pass@1={curve[1]:.0%}"
            if last_k > 1:
                metric_summary += f", pass@{last_k}={curve[last_k]:.0%}"

        print(f"\n  [{suite_name.upper()}] {passed}/{total} tasks fully passing{metric_summary}")
        print(f"  {'-' * 64}")

        for t in tasks:
            icon = "PASS" if t.all_passed else "FAIL"
            line = f"    [{icon}] {t.task_id}"
            if t.trial_count > 1:
                line += (
                    f"  (trials={t.trial_count}, pass_rate={t.pass_rate:.0%}, "
                    f"pass@{t.trial_count}={t.pass_at_k:.0%}, "
                    f"pass^{t.trial_count}={t.pass_pow_k:.0%})"
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
    _configure_eval_logging()
    profiles = load_profiles()
    parser = argparse.ArgumentParser(description="Run Archetype repository checks")
    parser.add_argument("--out", default=None, help="Write JSON results to file")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--suite", choices=KNOWN_SUITES, default=None)
    selection.add_argument("--profile", choices=tuple(sorted(profiles)), default=None)
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

    started_at = utc_now()
    started = time.perf_counter()
    harness = build_harness(trials=args.trials)
    selected_suites = (
        list(profiles[args.profile]["suites"])
        if args.profile
        else ([args.suite] if args.suite else list(KNOWN_SUITES))
    )
    if args.list_tasks:
        return print_task_list(harness, suite_filter=args.suite)

    results = harness.run(suite_filter=selected_suites)

    print_report(results)

    if not results:
        print("\nNo eval tasks executed; refusing to report success.")
        passed = False
    else:
        passed = _passes_policy(
            results,
            profile=args.profile,
            suite=args.suite,
            selected_suites=selected_suites,
            profiles=profiles,
        )

    if args.out:
        output = Path(args.out)
        output.parent.mkdir(parents=True, exist_ok=True)
        profile_name = args.profile or (f"suite:{args.suite}" if args.suite else "all")
        policy = (
            profiles[args.profile]["failure_policy"]
            if args.profile
            else "every selected suite, task, and trial passes"
        )
        envelope = build_result_envelope(
            kind="eval",
            profile=profile_name,
            suites=selected_suites,
            failure_policy=policy,
            started_at=started_at,
            duration_s=time.perf_counter() - started,
            outcome="passed" if passed else "failed",
            configuration={"trials": args.trials, "seed": None},
            results=[result.to_dict() for result in results],
        )
        envelope["metrics"] = _suite_metrics(results)
        output.write_text(json.dumps(envelope, indent=2) + "\n", encoding="utf-8")
        print(f"\nResults written to {args.out}")

    return 0 if passed else 1


def _suite_metrics(results: list[TaskResult]) -> dict[str, dict]:
    """Build suite-level statistics without conflating their semantics."""
    metrics: dict[str, dict] = {}
    for suite_name in KNOWN_SUITES:
        tasks = [task for task in results if task.suite == suite_name]
        if not tasks:
            continue
        metrics[suite_name] = {
            "tasks": len(tasks),
            "fully_passing_tasks": sum(task.all_passed for task in tasks),
            "trials_per_task": sorted({task.trial_count for task in tasks}),
            "pass_at_k": {
                str(k): round(estimate, 4) for k, estimate in aggregate_pass_at_k(tasks).items()
            },
            "mean_pass_rate": round(sum(task.pass_rate for task in tasks) / len(tasks), 4),
            "mean_score": round(sum(task.avg_score for task in tasks) / len(tasks), 4),
        }
    return metrics


def _passes_policy(
    results: list[TaskResult],
    *,
    profile: str | None,
    suite: str | None,
    selected_suites: list[str],
    profiles: dict[str, dict],
) -> bool:
    """Apply one explicit runner policy to a non-empty result set."""
    by_suite: dict[str, list[TaskResult]] = {
        suite_name: [task for task in results if task.suite == suite_name]
        for suite_name in KNOWN_SUITES
    }

    required_to_exist = set(selected_suites) if profile or suite else set(REQUIRED_SUITES)
    for suite_name in required_to_exist:
        if not by_suite[suite_name]:
            print(f"\nNo {suite_name} tasks executed; refusing to report success.")
            return False

    if profile:
        blocking = bool(profiles[profile]["blocking"])
        if blocking:
            return all(task.all_passed for task in results)
        return all(not any(trial.error for trial in task.trials) for task in results)

    if suite:
        return all(task.all_passed for task in by_suite[suite])

    return all(task.all_passed for task in results)


if __name__ == "__main__":
    raise SystemExit(main())
