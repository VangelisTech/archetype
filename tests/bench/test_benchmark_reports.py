# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for benchmark history and trend decisions."""

from __future__ import annotations

import json
import statistics
from pathlib import Path

import pytest

from bench.core.compare import compare_report
from bench.core.compare import main as compare_main
from bench.core.report import (
    ReportFormatError,
    benchmark_identity,
    build_report,
    load_report,
    write_report,
)

_ENVIRONMENT = {
    "runner_id": "stable-runner",
    "system": "TestOS",
    "release": "1",
    "machine": "test64",
    "processor": "test-cpu",
    "python_implementation": "CPython",
    "python_version": "3.12.10",
    "packages": {
        "archetype-ecs": "0.4.0",
        "daft": "0.7.19",
        "lancedb": "0.22.0",
        "pyiceberg": "0.10.0",
    },
}
_CONFIG = {
    "steps": 1,
    "storage_backend": "lancedb",
    "cache": False,
}


def _raw(elapsed_s: float, *, world_id: str = "world", run_id: str = "run") -> dict:
    return {
        "name": "packed_iteration",
        "bench_name": "packed_iteration",
        "entities": 100,
        "steps": 1,
        "elapsed_s": elapsed_s,
        "extras": {"width": 5},
        "world_id": world_id,
        "run_id": run_id,
    }


def _report(
    elapsed_s: float,
    index: int,
    *,
    environment: dict | None = None,
    config: dict | None = None,
    world_id: str = "world",
    created_at: str | None = None,
) -> dict:
    return build_report(
        [_raw(elapsed_s, world_id=world_id, run_id=f"run-{index}")],
        suite="ecs",
        config=config or _CONFIG,
        environment=environment or _ENVIRONMENT,
        revision={"commit": f"{index:040x}", "dirty": False},
        created_at=created_at or f"2026-07-17T12:00:{index:02d}Z",
    )


def _comparison(summary: dict, metric: str) -> dict:
    return next(row for row in summary["comparisons"] if row["metric"] == metric)


def test_build_report_normalizes_metrics_and_separates_provenance() -> None:
    report = _report(2.0, 1)
    benchmark = report["benchmarks"][0]

    assert report["schema_version"] == 1
    assert len(report["report_id"]) == 64
    assert benchmark == {
        "name": "packed_iteration",
        "dimensions": {
            "entities": 100,
            "steps": 1,
            "extras": {"width": 5},
        },
        "metrics": {
            "elapsed_s": 2.0,
            "steps_per_sec": 0.5,
            "entities_per_sec": 50.0,
        },
        "provenance": {"world_id": "world", "run_id": "run-1"},
    }


def test_benchmark_identity_ignores_run_provenance() -> None:
    first = _report(2.0, 1, world_id="first")["benchmarks"][0]
    second = _report(3.0, 2, world_id="second")["benchmarks"][0]

    assert benchmark_identity(first) == benchmark_identity(second)


def test_write_report_atomically_keeps_current_and_history(tmp_path: Path) -> None:
    report = _report(2.0, 1)
    current = tmp_path / "current.json"
    history = tmp_path / "history"

    history_path = write_report(report, current_path=current, history_dir=history)

    assert history_path is not None
    assert load_report(current) == report
    assert load_report(history_path) == report
    assert list(history.glob("*.json")) == [history_path]
    assert not list(tmp_path.rglob(".*.json.*")), "atomic temporary files must not leak"


def test_report_content_hash_rejects_hand_edited_history(tmp_path: Path) -> None:
    report = _report(2.0, 1)
    report["benchmarks"][0]["metrics"]["elapsed_s"] = 200.0
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(report))

    with pytest.raises(ReportFormatError, match="report_id does not match"):
        load_report(path)


def test_report_rejects_non_utc_timestamp_and_duplicate_identity() -> None:
    with pytest.raises(ReportFormatError, match="ending in Z"):
        _report(2.0, 1, created_at="2026-07-17T12:00:01+01:00")

    with pytest.raises(ReportFormatError, match="duplicate benchmark identity"):
        build_report(
            [_raw(1.0), _raw(2.0)],
            suite="ecs",
            config=_CONFIG,
            environment=_ENVIRONMENT,
            revision={"commit": "1" * 40, "dirty": False},
            created_at="2026-07-17T12:00:01Z",
        )


def test_elapsed_regression_is_strictly_beyond_two_population_sigma() -> None:
    elapsed = [8.0, 10.0, 12.0]
    baselines = [_report(value, index) for index, value in enumerate(elapsed, start=1)]
    threshold = statistics.median(elapsed) + 2 * statistics.pstdev(elapsed)

    exact = compare_report(_report(threshold, 10), baselines)
    beyond = compare_report(_report(threshold + 1e-9, 11), baselines)

    assert _comparison(exact, "elapsed_s")["status"] == "ok"
    assert _comparison(beyond, "elapsed_s")["status"] == "regression"
    assert exact["status"] == "ok"
    assert beyond["status"] == "regression"


def test_throughput_regression_is_strictly_below_two_population_sigma() -> None:
    throughputs = [8.0, 10.0, 12.0]
    baselines = [
        _report(1.0 / throughput, index) for index, throughput in enumerate(throughputs, start=1)
    ]
    threshold = statistics.median(throughputs) - 2 * statistics.pstdev(throughputs)

    exact = compare_report(_report(1.0 / threshold, 10), baselines)
    beyond = compare_report(_report(1.0 / (threshold - 1e-9), 11), baselines)

    assert _comparison(exact, "steps_per_sec")["status"] == "ok"
    assert _comparison(beyond, "steps_per_sec")["status"] == "regression"


def test_zero_variance_history_flags_any_worse_value() -> None:
    baselines = [_report(10.0, index) for index in range(1, 4)]

    unchanged = compare_report(_report(10.0, 10), baselines)
    slower = compare_report(_report(10.0001, 11), baselines)

    assert _comparison(unchanged, "elapsed_s")["status"] == "ok"
    row = _comparison(slower, "elapsed_s")
    assert row["status"] == "regression"
    assert row["population_sigma"] == 0
    assert row["worse_by_sigma"] is None


def test_comparison_requires_enough_compatible_history() -> None:
    other_environment = {**_ENVIRONMENT, "runner_id": "shared-runner"}
    other_config = {**_CONFIG, "steps": 3}
    baselines = [
        _report(9.0, 1),
        _report(10.0, 2),
        _report(11.0, 3, environment=other_environment),
        _report(12.0, 4, config=other_config),
    ]

    summary = compare_report(_report(20.0, 10), baselines)

    assert summary["compatible_reports"] == 2
    assert len(summary["incompatible_report_ids"]) == 2
    assert summary["regression_count"] == 0
    assert summary["insufficient_count"] == 3
    assert summary["status"] == "insufficient"
    assert {row["status"] for row in summary["comparisons"]} == {"insufficient"}


def test_comparison_rejects_non_finite_sigma() -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        compare_report(_report(10.0, 10), [], sigma_multiplier=float("nan"))


def test_duplicate_history_report_is_not_weighted_twice() -> None:
    baselines = [_report(value, index) for index, value in enumerate((8.0, 10.0, 12.0), 1)]

    summary = compare_report(_report(10.0, 10), [*baselines, baselines[0]])

    assert summary["compatible_reports"] == 3
    assert _comparison(summary, "elapsed_s")["baseline_samples"] == 3


def test_comparison_uses_only_the_most_recent_prior_window() -> None:
    baselines = [_report(value, index) for index, value in enumerate((100.0, 8.0, 10.0, 12.0), 1)]

    summary = compare_report(_report(10.0, 10), baselines, history_window=3)

    assert summary["compatible_reports_available"] == 4
    assert summary["compatible_reports"] == 3
    assert summary["history_window"] == 3
    assert summary["baseline_report_ids"] == [report["report_id"] for report in baselines[1:]]
    assert _comparison(summary, "elapsed_s")["median"] == 10.0


def test_history_window_orders_fractional_utc_timestamps_chronologically() -> None:
    baselines = [
        _report(100.0, 1, created_at="2026-07-17T12:00:00Z"),
        _report(8.0, 2, created_at="2026-07-17T12:00:00.100000Z"),
        _report(10.0, 3, created_at="2026-07-17T12:00:00.200000Z"),
        _report(12.0, 4, created_at="2026-07-17T12:00:00.300000Z"),
    ]
    current = _report(10.0, 10, created_at="2026-07-17T12:00:00.400000Z")

    summary = compare_report(current, baselines, history_window=3)

    assert _comparison(summary, "elapsed_s")["median"] == 10.0


def test_comparison_excludes_same_time_and_future_reports() -> None:
    current = _report(10.0, 10)
    same_time = _report(11.0, 10)
    future = _report(12.0, 11)
    prior = [_report(value, index) for index, value in enumerate((8.0, 9.0, 10.0), 1)]

    summary = compare_report(current, [*prior, same_time, future])

    assert summary["compatible_reports"] == 3
    assert set(summary["non_prior_report_ids"]) == {
        same_time["report_id"],
        future["report_id"],
    }


def test_cli_is_advisory_unless_regression_failure_is_requested(tmp_path: Path, capsys) -> None:
    history = tmp_path / "history"
    for index, elapsed in enumerate((8.0, 10.0, 12.0), 1):
        write_report(_report(elapsed, index), history_dir=history)
    current = tmp_path / "current.json"
    write_report(_report(100.0, 10), current_path=current)
    args = ["--current", str(current), "--history-dir", str(history)]

    assert compare_main(args) == 0
    advisory = json.loads(capsys.readouterr().out)
    assert advisory["status"] == "regression"
    assert compare_main([*args, "--fail-on-regression"]) == 1


def test_cli_fails_closed_on_invalid_history(tmp_path: Path, capsys) -> None:
    history = tmp_path / "history"
    history.mkdir()
    (history / "broken.json").write_text("{not-json")
    current = tmp_path / "current.json"
    write_report(_report(10.0, 10), current_path=current)

    code = compare_main(["--current", str(current), "--history-dir", str(history)])

    assert code == 2
    assert "cannot read" in capsys.readouterr().err
