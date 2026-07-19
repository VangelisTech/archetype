# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the version-pinned Daft execution-attribution workload."""

from __future__ import annotations

import json
from pathlib import Path

import daft
import pytest

from bench.observability.daft_attribution import (
    PINNED_DAFT_VERSION,
    AttributionConfig,
    _ray_disposition,
    build_attribution_report,
    run_attribution_characterization,
)


def _all_mapping_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {key for child in value.values() for key in _all_mapping_keys(child)}
    if isinstance(value, list):
        return {key for child in value for key in _all_mapping_keys(child)}
    return set()


@pytest.mark.asyncio
async def test_delayed_udf_runs_only_at_terminal_collect(tmp_path: Path) -> None:
    config = AttributionConfig(rows=1, delay_seconds=0.01, repetitions=2)

    results, runner = await run_attribution_characterization(
        config,
        marker_root=tmp_path,
    )

    assert runner == "native"
    assert len(results) == 1
    result = results[0]
    assert result["work_deferred_until_collect"] is True
    assert result["output_correct"] is True
    assert result["daft_version"] == PINNED_DAFT_VERSION
    assert result["runner"] == "native"
    assert result["distributed_runner_disposition"] == "not_exercised_ray_extra_not_locked"
    assert result["plan_build_s"] > 0
    assert result["terminal_collect_s"] > 0
    assert result["python_conversion_s"] > 0

    context = result["context_propagation"]
    assert context == {
        "driver_observation_count": 2,
        "driver_active_count": 2,
        "driver_matching_count": 2,
        "udf_observation_count": 2,
        "udf_active_count": 0,
        "udf_matching_count": 0,
    }

    events = result["experimental_subscriber_evidence"]
    assert events["query_count"] == config.repetitions
    assert events["optimization_event_observed"] is True
    assert events["execution_plan_event_observed"] is True
    assert events["udf_operator_observed"] is True
    assert events["processor_class_name_literal_observed"] is False
    assert events["query_local_node_positions_reused"] is True
    assert events["origin_node_id_seen_by_driver_subscriber"] is False
    assert events["duration_stat_observed"] is True
    assert events["row_stats_observed"] is True
    assert events["byte_stats_observed"] is True
    assert events["task_counter_stat_observed"] is True
    assert events["processor_count"] == 3
    assert events["transform_node_count_median"] == 2
    assert events["processor_count_matches_transform_nodes"] is False
    assert result["udf_operator_accumulated_duration_median_s"] > 0


@pytest.mark.asyncio
async def test_report_is_pinned_and_contains_only_reduced_plan_evidence(
    tmp_path: Path,
) -> None:
    config = AttributionConfig(rows=1, delay_seconds=0, repetitions=2)
    results, runner = await run_attribution_characterization(
        config,
        marker_root=tmp_path,
    )

    report = build_attribution_report(
        results,
        config=config,
        runner=runner,
        runner_id="contract-runner",
    )

    assert daft.__version__ == PINNED_DAFT_VERSION
    assert report["suite"] == "daft_attribution"
    assert report["config"]["pinned_daft_version"] == PINNED_DAFT_VERSION
    assert report["config"]["configured_runner"] == "native"
    assert report["config"]["ray_disposition"] == "not_exercised_ray_extra_not_locked"
    result = report["results"][0]
    assert set(result) == {
        "name",
        "daft_version",
        "runner",
        "distributed_runner_disposition",
        "rows",
        "repetitions",
        "plan_build_s",
        "terminal_collect_s",
        "python_conversion_s",
        "udf_operator_accumulated_duration_median_s",
        "work_deferred_until_collect",
        "output_correct",
        "context_propagation",
        "experimental_subscriber_evidence",
    }
    assert set(result["context_propagation"]) == {
        "driver_observation_count",
        "driver_active_count",
        "driver_matching_count",
        "udf_observation_count",
        "udf_active_count",
        "udf_matching_count",
    }
    assert set(result["experimental_subscriber_evidence"]) == {
        "query_count",
        "optimization_event_observed",
        "execution_plan_event_observed",
        "udf_operator_observed",
        "processor_class_name_literal_observed",
        "query_local_node_positions_reused",
        "origin_node_id_seen_by_driver_subscriber",
        "execution_duration_event_reported",
        "duration_stat_observed",
        "row_stats_observed",
        "byte_stats_observed",
        "task_counter_stat_observed",
        "processor_count",
        "transform_node_count_median",
        "processor_count_matches_transform_nodes",
        "udf_operator_accumulated_duration_median_s",
    }
    assert set(report["config"]) == {
        "rows",
        "delay_seconds",
        "repetitions",
        "workload",
        "pinned_daft_version",
        "configured_runner",
        "synthetic_execution_boundary",
        "python_conversion_boundary",
        "production_persistence_boundary",
        "subscriber_api",
        "execution_signal_source",
        "ray_disposition",
    }
    rendered = json.dumps(report, sort_keys=True)
    assert not any(
        class_name in rendered
        for class_name in (
            "IncrementProbeProcessor",
            "DoubleProbeProcessor",
            "DelayedProbeProcessor",
        )
    )
    assert "_delayed_probe" not in rendered
    assert str(tmp_path) not in rendered
    assert not _all_mapping_keys(report).intersection(
        {
            "optimized_plan",
            "physical_plan",
            "operator_name",
            "query_id",
            "node_id",
            "trace_id",
            "span_id",
            "marker_path",
        }
    )


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (AttributionConfig(rows=0), "rows"),
        (AttributionConfig(delay_seconds=-0.1), "delay_seconds"),
        (AttributionConfig(repetitions=1), "repetitions"),
    ],
)
def test_attribution_config_rejects_degenerate_workloads(
    config: AttributionConfig,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        config.validate()


@pytest.mark.parametrize(
    ("runner", "ray_available", "expected"),
    [
        ("native", False, "not_exercised_ray_extra_not_locked"),
        ("native", True, "not_exercised_ray_extra_not_locked"),
        ("ray", True, "exercised_external_ray_environment"),
        ("ray", False, "invalid_ray_runner_without_dependency"),
    ],
)
def test_ray_disposition_is_explicit(
    runner: str,
    ray_available: bool,
    expected: str,
) -> None:
    assert _ray_disposition(runner=runner, ray_available=ray_available) == expected
