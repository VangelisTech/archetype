# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compare one benchmark report with compatible historical runs."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from bench.core.report import (
    ReportFormatError,
    benchmark_identity,
    compatibility_identity,
    load_report,
    report_timestamp,
    validate_report,
)

Direction = Literal["lower", "higher"]


@dataclass(frozen=True)
class MetricRule:
    """Describe which direction represents improvement for one metric."""

    name: str
    direction: Direction


@dataclass(frozen=True)
class BaselineSelection:
    """Compatible prior reports selected for one chronological window."""

    reports: tuple[dict[str, Any], ...]
    compatible_available: int
    incompatible_report_ids: tuple[str, ...]
    non_prior_report_ids: tuple[str, ...]


DEFAULT_RULES = (
    MetricRule("elapsed_s", "lower"),
    MetricRule("steps_per_sec", "higher"),
    MetricRule("entities_per_sec", "higher"),
)


def compare_report(
    current: Mapping[str, Any],
    baselines: Sequence[Mapping[str, Any]],
    *,
    rules: Sequence[MetricRule] = DEFAULT_RULES,
    min_samples: int = 3,
    sigma_multiplier: float = 2.0,
    history_window: int = 20,
) -> dict[str, Any]:
    """Compare current metrics against matching historical distributions.

    Reports must have the same suite, benchmark configuration, runner, OS,
    Python, and dependency versions. Each metric is aligned by benchmark name
    and dimensions. A regression lies strictly beyond the rolling median by
    ``sigma_multiplier`` population standard deviations in the worse direction.
    """
    _validate_options(
        min_samples=min_samples,
        sigma_multiplier=sigma_multiplier,
        history_window=history_window,
    )
    current_report = validate_report(dict(current))
    selection = _select_baselines(current_report, baselines, history_window=history_window)
    baseline_by_benchmark = _index_benchmarks(selection.reports)
    comparisons = _comparison_rows(
        current_report,
        baseline_by_benchmark,
        rules=rules,
        min_samples=min_samples,
        sigma_multiplier=sigma_multiplier,
    )
    regression_count = _status_count(comparisons, "regression")
    insufficient_count = _status_count(comparisons, "insufficient")
    return {
        "current_report_id": current_report["report_id"],
        "compatible_reports_available": selection.compatible_available,
        "compatible_reports": len(selection.reports),
        "baseline_report_ids": [report["report_id"] for report in selection.reports],
        "incompatible_report_ids": list(selection.incompatible_report_ids),
        "non_prior_report_ids": list(selection.non_prior_report_ids),
        "history_window": history_window,
        "min_samples": min_samples,
        "sigma_multiplier": sigma_multiplier,
        "status": _summary_status(comparisons),
        "regression_count": regression_count,
        "insufficient_count": insufficient_count,
        "comparisons": comparisons,
    }


def _validate_options(*, min_samples: int, sigma_multiplier: float, history_window: int) -> None:
    if min_samples < 2:
        raise ValueError("min_samples must be at least 2")
    if not math.isfinite(sigma_multiplier) or sigma_multiplier <= 0:
        raise ValueError("sigma_multiplier must be finite and positive")
    if history_window < min_samples:
        raise ValueError("history_window must be at least min_samples")


def _select_baselines(
    current: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    history_window: int,
) -> BaselineSelection:
    current_id = current["report_id"]
    compatibility = compatibility_identity(current)
    timestamp = report_timestamp(current)
    compatible: dict[str, dict[str, Any]] = {}
    incompatible_ids: set[str] = set()
    non_prior_ids: set[str] = set()

    for candidate in candidates:
        report = validate_report(dict(candidate))
        report_id = report["report_id"]
        if report_id == current_id or report_id in compatible:
            continue
        if compatibility_identity(report) != compatibility:
            incompatible_ids.add(report_id)
        elif report_timestamp(report) >= timestamp:
            non_prior_ids.add(report_id)
        else:
            compatible[report_id] = report

    selected = sorted(
        compatible.values(), key=lambda report: (report_timestamp(report), report["report_id"])
    )[-history_window:]
    return BaselineSelection(
        reports=tuple(selected),
        compatible_available=len(compatible),
        incompatible_report_ids=tuple(sorted(incompatible_ids)),
        non_prior_report_ids=tuple(sorted(non_prior_ids)),
    )


def _index_benchmarks(
    reports: Sequence[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    indexed: dict[str, list[Mapping[str, Any]]] = {}
    for report in reports:
        for benchmark in report["benchmarks"]:
            indexed.setdefault(benchmark_identity(benchmark), []).append(benchmark)
    return indexed


def _comparison_rows(
    current: Mapping[str, Any],
    baselines: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    rules: Sequence[MetricRule],
    min_samples: int,
    sigma_multiplier: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rule_by_name = {rule.name: rule for rule in rules}
    for benchmark in current["benchmarks"]:
        historical = baselines.get(benchmark_identity(benchmark), ())
        for metric_name, current_value in sorted(benchmark["metrics"].items()):
            rule = rule_by_name.get(metric_name)
            if rule is not None:
                rows.append(
                    _compare_metric(
                        benchmark=benchmark,
                        metric=rule,
                        current_value=current_value,
                        samples=_metric_samples(historical, metric_name),
                        min_samples=min_samples,
                        sigma_multiplier=sigma_multiplier,
                    )
                )
    return rows


def _metric_samples(benchmarks: Sequence[Mapping[str, Any]], metric_name: str) -> list[float]:
    return [
        benchmark["metrics"][metric_name]
        for benchmark in benchmarks
        if metric_name in benchmark["metrics"]
    ]


def _status_count(comparisons: Sequence[Mapping[str, Any]], status: str) -> int:
    return sum(row["status"] == status for row in comparisons)


def _summary_status(comparisons: Sequence[Mapping[str, Any]]) -> str:
    statuses = {row["status"] for row in comparisons}
    if "regression" in statuses:
        return "regression"
    if "insufficient" in statuses or not statuses:
        return "insufficient"
    return "ok"


def _compare_metric(
    *,
    benchmark: Mapping[str, Any],
    metric: MetricRule,
    current_value: float,
    samples: Sequence[float],
    min_samples: int,
    sigma_multiplier: float,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "benchmark": benchmark["name"],
        "dimensions": benchmark["dimensions"],
        "metric": metric.name,
        "direction": metric.direction,
        "current": current_value,
        "baseline_samples": len(samples),
        "median": None,
        "population_sigma": None,
        "regression_threshold": None,
        "worse_by_sigma": None,
        "delta_percent": None,
        "status": "insufficient",
    }
    if len(samples) < min_samples:
        return row

    median = statistics.median(samples)
    population_sigma = statistics.pstdev(samples)
    regression_threshold = (
        median + sigma_multiplier * population_sigma
        if metric.direction == "lower"
        else median - sigma_multiplier * population_sigma
    )
    regressed = (
        current_value > regression_threshold
        if metric.direction == "lower"
        else current_value < regression_threshold
    )
    worse_delta = current_value - median if metric.direction == "lower" else median - current_value

    row.update(
        {
            "median": median,
            "population_sigma": population_sigma,
            "regression_threshold": regression_threshold,
            "worse_by_sigma": worse_delta / population_sigma if population_sigma else None,
            "delta_percent": ((current_value - median) / median * 100) if median else None,
            "status": "regression" if regressed else "ok",
        }
    )
    return row


def _load_history(directory: Path, *, current_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    baselines: list[dict[str, Any]] = []
    invalid: list[str] = []
    current_resolved = current_path.resolve()
    if not directory.exists():
        return baselines, invalid

    for path in sorted(directory.glob("*.json")):
        if path.resolve() == current_resolved:
            continue
        try:
            baselines.append(load_report(path))
        except ReportFormatError as exc:
            invalid.append(str(exc))
    return baselines, invalid


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare compatible Archetype benchmark runs")
    parser.add_argument("--current", type=Path, required=True, help="Current schema-v1 report")
    parser.add_argument(
        "--history-dir",
        type=Path,
        required=True,
        help="Directory containing historical schema-v1 reports",
    )
    parser.add_argument("--min-samples", type=int, default=3)
    parser.add_argument("--sigma", type=float, default=2.0, dest="sigma_multiplier")
    parser.add_argument("--window", type=int, default=20, dest="history_window")
    parser.add_argument("--out", type=Path, default=None, help="Optional JSON summary path")
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit 1 when a comparable metric crosses its regression threshold",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        current = load_report(args.current)
        baselines, invalid = _load_history(args.history_dir, current_path=args.current)
        if invalid:
            raise ReportFormatError("; ".join(invalid))
        summary = compare_report(
            current,
            baselines,
            min_samples=args.min_samples,
            sigma_multiplier=args.sigma_multiplier,
            history_window=args.history_window,
        )
    except (OSError, ReportFormatError, ValueError) as exc:
        print(f"benchmark comparison failed: {exc}", file=sys.stderr)
        return 2

    rendered = json.dumps(summary, allow_nan=False, indent=2, sort_keys=True) + "\n"
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered)
    print(rendered, end="")
    if args.fail_on_regression and summary["status"] == "regression":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
