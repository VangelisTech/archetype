# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Characterize lazy Daft execution without changing Archetype production code.

The workload proves that an AsyncProcessor.process() call builds a lazy plan
while the delayed Python UDF runs only at the synthetic ``DataFrame.collect``
boundary. Daft's experimental Subscriber API is intentionally confined to this
repository-harness module. Raw plans, operator names, query identifiers, node
identifiers, and trace identifiers never enter the report.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import statistics
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import daft
from daft import DataFrame, DataType, col, lit
from daft.subscribers import Subscriber
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from archetype.core.aio.async_processor import AsyncProcessor
from bench.core.report import build_report, capture_environment, write_report

PINNED_DAFT_VERSION = "0.7.19"
WORKLOAD_ID = "daft-execution-attribution-v1"

_SYNTHETIC_TRACE_ID = 0x1234567890ABCDEF1234567890ABCDEF
_SYNTHETIC_SPAN_ID = 0x1234567890ABCDEF
_PROBE_DTYPE = DataType.struct(
    {
        "value": DataType.int64(),
        "context_valid": DataType.bool(),
        "context_matches_synthetic": DataType.bool(),
    }
)


@daft.func(return_dtype=_PROBE_DTYPE)
def _delayed_probe(
    value: int,
    delay_seconds: float,
    started_marker_path: str,
    finished_marker_path: str,
) -> dict[str, Any]:
    """Perform visible work only when Daft executes the lazy UDF."""
    span_context = trace.get_current_span().get_span_context()
    if started_marker_path:
        Path(started_marker_path).touch(exist_ok=True)
    time.sleep(delay_seconds)
    if finished_marker_path:
        Path(finished_marker_path).touch(exist_ok=True)
    return {
        "value": value + 1,
        "context_valid": span_context.is_valid,
        "context_matches_synthetic": (
            span_context.trace_id == _SYNTHETIC_TRACE_ID
            and span_context.span_id == _SYNTHETIC_SPAN_ID
        ),
    }


@dataclass(frozen=True)
class AttributionConfig:
    """Comparable dimensions for the retained characterization workload."""

    rows: int = 3
    delay_seconds: float = 0.05
    repetitions: int = 2

    def validate(self) -> None:
        if self.rows < 1:
            raise ValueError("rows must be positive")
        if self.delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        if self.repetitions < 2:
            raise ValueError("repetitions must be at least 2")


class IncrementProbeProcessor(AsyncProcessor):
    """First processor in an optimizer-fusion characterization."""

    async def process(self, df: DataFrame, **input_kwargs: Any) -> DataFrame:
        del input_kwargs
        return df.with_column("value", col("value") + lit(1))


class DoubleProbeProcessor(AsyncProcessor):
    """Second processor in an optimizer-fusion characterization."""

    async def process(self, df: DataFrame, **input_kwargs: Any) -> DataFrame:
        del input_kwargs
        return df.with_column("value", col("value") * lit(2))


class DelayedProbeProcessor(AsyncProcessor):
    """Synthetic processor whose expensive operation remains inside the Daft DAG."""

    def __init__(
        self,
        *,
        delay_seconds: float,
        started_marker_path: Path | None,
        finished_marker_path: Path | None,
    ):
        self.delay_seconds = delay_seconds
        self.started_marker_path = started_marker_path
        self.finished_marker_path = finished_marker_path

    async def process(self, df: DataFrame, **input_kwargs: Any) -> DataFrame:
        del input_kwargs
        return df.with_column(
            "probe",
            _delayed_probe(
                col("value"),
                lit(self.delay_seconds),
                lit(str(self.started_marker_path) if self.started_marker_path is not None else ""),
                lit(
                    str(self.finished_marker_path) if self.finished_marker_path is not None else ""
                ),
            ),
        )


_PROCESSOR_CLASSES = (
    IncrementProbeProcessor,
    DoubleProbeProcessor,
    DelayedProbeProcessor,
)


class _ExecutionCapture(Subscriber):
    """Reduce experimental Daft events to bounded characterization evidence."""

    def __init__(self) -> None:
        self._queries: dict[str, dict[str, Any]] = {}

    def _query(self, query_id: str) -> dict[str, Any]:
        return self._queries.setdefault(
            query_id,
            {
                "optimized": False,
                "physical": False,
                "processor_class_name_literal": False,
                "node_ids": set(),
                "udf_node_ids": set(),
                "origin_node": False,
                "metric_names": set(),
                "udf_duration_by_node": {},
                "transform_node_count": 0,
                "execution_duration_reported": False,
            },
        )

    @property
    def query_count(self) -> int:
        return len(self._queries)

    def on_query_started(self, event: Any) -> None:
        self._query(event.query_id)

    def on_optimization_completed(self, event: Any) -> None:
        query = self._query(event.query_id)
        query["optimized"] = True
        query["processor_class_name_literal"] = query["processor_class_name_literal"] or any(
            cls.__name__ in event.optimized_plan for cls in _PROCESSOR_CLASSES
        )

    def on_execution_started(self, event: Any) -> None:
        query = self._query(event.query_id)
        query["physical"] = True
        query["processor_class_name_literal"] = query["processor_class_name_literal"] or any(
            cls.__name__ in event.physical_plan for cls in _PROCESSOR_CLASSES
        )
        try:
            physical_plan = json.loads(event.physical_plan)
        except (TypeError, ValueError):
            return
        query["transform_node_count"] = _count_transform_nodes(physical_plan)

    def on_operator_start(self, event: Any) -> None:
        query = self._query(event.query_id)
        query["node_ids"].add(event.node_id)
        if event.name.startswith("UDF "):
            query["udf_node_ids"].add(event.node_id)
        query["origin_node"] = query["origin_node"] or event.origin_node_id is not None

    def on_stats(self, event: Any) -> None:
        query = self._query(event.query_id)
        for node_id, stats in event.stats.items():
            query["metric_names"].update(stats)
            if node_id not in query["udf_node_ids"] or "duration" not in stats:
                continue
            duration = stats["duration"][1]
            total_seconds = getattr(duration, "total_seconds", None)
            if callable(total_seconds):
                observed = total_seconds()
                previous = query["udf_duration_by_node"].get(node_id, 0.0)
                query["udf_duration_by_node"][node_id] = max(previous, observed)

    def on_execution_finished(self, event: Any) -> None:
        query = self._query(event.query_id)
        query["execution_duration_reported"] = event.duration_ms is not None

    def summary(self) -> dict[str, Any]:
        queries = list(self._queries.values())
        node_sets = [frozenset(query["node_ids"]) for query in queries]
        metric_names = {name for query in queries for name in query["metric_names"]}
        udf_durations = [sum(query["udf_duration_by_node"].values()) for query in queries]
        transform_counts = [query["transform_node_count"] for query in queries]
        return {
            "query_count": len(queries),
            "optimization_event_observed": bool(queries)
            and all(query["optimized"] for query in queries),
            "execution_plan_event_observed": bool(queries)
            and all(query["physical"] for query in queries),
            "udf_operator_observed": bool(queries)
            and all(query["udf_node_ids"] for query in queries),
            "processor_class_name_literal_observed": any(
                query["processor_class_name_literal"] for query in queries
            ),
            "query_local_node_positions_reused": len(node_sets) >= 2
            and len(set(node_sets)) < len(node_sets),
            "origin_node_id_seen_by_driver_subscriber": any(
                query["origin_node"] for query in queries
            ),
            "execution_duration_event_reported": any(
                query["execution_duration_reported"] for query in queries
            ),
            "duration_stat_observed": "duration" in metric_names,
            "row_stats_observed": any(name.startswith("rows.") for name in metric_names),
            "byte_stats_observed": any(name.startswith("bytes.") for name in metric_names),
            "task_counter_stat_observed": any(name.startswith("task.") for name in metric_names),
            "processor_count": len(_PROCESSOR_CLASSES),
            "transform_node_count_median": (
                statistics.median(transform_counts) if transform_counts else 0
            ),
            "processor_count_matches_transform_nodes": bool(transform_counts)
            and all(count == len(_PROCESSOR_CLASSES) for count in transform_counts),
            "udf_operator_accumulated_duration_median_s": (
                statistics.median(udf_durations) if udf_durations else 0.0
            ),
        }


def _count_transform_nodes(node: object) -> int:
    if not isinstance(node, dict):
        return 0
    own = int(node.get("type") in {"Project", "UDFProject"})
    children = node.get("children", [])
    if not isinstance(children, list):
        return own
    return own + sum(_count_transform_nodes(child) for child in children)


def _synthetic_parent() -> NonRecordingSpan:
    context = SpanContext(
        trace_id=_SYNTHETIC_TRACE_ID,
        span_id=_SYNTHETIC_SPAN_ID,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return NonRecordingSpan(context)


def _ray_disposition(*, runner: str, ray_available: bool) -> str:
    if runner == "ray":
        if ray_available:
            return "exercised_external_ray_environment"
        return "invalid_ray_runner_without_dependency"
    return "not_exercised_ray_extra_not_locked"


def _validate_environment() -> str:
    if daft.__version__ != PINNED_DAFT_VERSION:
        raise RuntimeError(
            "Daft execution attribution must be re-characterized when the locked "
            f"version changes: expected {PINNED_DAFT_VERSION}, observed {daft.__version__}"
        )
    runner = daft.get_or_infer_runner_type()
    if runner not in {"native", "ray"}:
        raise RuntimeError(f"unsupported Daft runner for attribution probe: {runner}")
    return runner


def _correct_probe_rows(rows: list[dict[str, Any]], expected_rows: int) -> bool:
    if len(rows) != expected_rows:
        return False
    return all(row["probe"]["value"] == row["value"] + 1 for row in rows)


async def _run_in_directory(
    config: AttributionConfig,
    marker_root: Path,
) -> tuple[dict[str, Any], str]:
    runner = _validate_environment()
    ray_available = importlib.util.find_spec("ray") is not None
    runner_disposition = _ray_disposition(
        runner=runner,
        ray_available=ray_available,
    )
    if runner == "ray" and not ray_available:
        raise RuntimeError("Daft Ray runner was configured but Ray is not installed")
    capture = _ExecutionCapture()
    plan_durations: list[float] = []
    collect_durations: list[float] = []
    conversion_durations: list[float] = []
    deferred: list[bool] = []
    outputs_correct: list[bool] = []
    driver_context_active: list[bool] = []
    driver_context_matches: list[bool] = []
    udf_context_active: list[bool] = []
    udf_context_matches: list[bool] = []

    with daft.with_subscriber("archetype-issue-518", capture):
        for repetition in range(config.repetitions):
            started_marker = (
                marker_root / f"execution-{repetition}.started" if runner == "native" else None
            )
            finished_marker = (
                marker_root / f"execution-{repetition}.finished" if runner == "native" else None
            )
            for marker in (started_marker, finished_marker):
                if marker is not None:
                    marker.unlink(missing_ok=True)
            processors = (
                IncrementProbeProcessor(),
                DoubleProbeProcessor(),
                DelayedProbeProcessor(
                    delay_seconds=config.delay_seconds,
                    started_marker_path=started_marker,
                    finished_marker_path=finished_marker,
                ),
            )
            source_values = list(range(config.rows))
            planned = daft.from_pydict({"value": source_values})

            with trace.use_span(_synthetic_parent(), end_on_exit=False):
                context = trace.get_current_span().get_span_context()
                driver_context_active.append(context.is_valid)
                driver_context_matches.append(
                    context.trace_id == _SYNTHETIC_TRACE_ID
                    and context.span_id == _SYNTHETIC_SPAN_ID
                )

                started = time.perf_counter()
                for processor in processors:
                    planned = await processor.process(planned)
                plan_durations.append(time.perf_counter() - started)
                no_execution_event = capture.query_count == repetition
                no_started_side_effect = started_marker is None or not started_marker.exists()
                deferred.append(no_execution_event and no_started_side_effect)

                started = time.perf_counter()
                collected = planned.collect()
                collect_durations.append(time.perf_counter() - started)

                started = time.perf_counter()
                materialized = collected.to_pylist()
                conversion_durations.append(time.perf_counter() - started)

            if capture.query_count != repetition + 1:
                raise RuntimeError("terminal collect did not emit one Daft query")
            if started_marker is not None and not started_marker.exists():
                raise RuntimeError("delayed UDF did not start at terminal collect")
            if finished_marker is not None and not finished_marker.exists():
                raise RuntimeError("delayed UDF did not finish at terminal collect")
            expected = [((value + 1) * 2) + 1 for value in source_values]
            outputs_correct.append(
                _correct_probe_rows(materialized, config.rows)
                and [row["probe"]["value"] for row in materialized] == expected
            )
            udf_context_active.extend(row["probe"]["context_valid"] for row in materialized)
            udf_context_matches.extend(
                row["probe"]["context_matches_synthetic"] for row in materialized
            )

    if not all(deferred):
        raise RuntimeError("delayed UDF executed while the processor was building its plan")
    if not all(outputs_correct):
        raise RuntimeError("attribution workload produced an incorrect result")

    subscriber_evidence = capture.summary()
    context_evidence = {
        "driver_observation_count": len(driver_context_active),
        "driver_active_count": sum(driver_context_active),
        "driver_matching_count": sum(driver_context_matches),
        "udf_observation_count": len(udf_context_active),
        "udf_active_count": sum(udf_context_active),
        "udf_matching_count": sum(udf_context_matches),
    }
    return (
        {
            "name": "lazy_processor_execution_attribution",
            "daft_version": PINNED_DAFT_VERSION,
            "runner": runner,
            "distributed_runner_disposition": runner_disposition,
            "rows": config.rows,
            "repetitions": config.repetitions,
            "plan_build_s": statistics.median(plan_durations),
            "terminal_collect_s": statistics.median(collect_durations),
            "python_conversion_s": statistics.median(conversion_durations),
            "udf_operator_accumulated_duration_median_s": subscriber_evidence[
                "udf_operator_accumulated_duration_median_s"
            ],
            "work_deferred_until_collect": all(deferred),
            "output_correct": all(outputs_correct),
            "context_propagation": context_evidence,
            "experimental_subscriber_evidence": subscriber_evidence,
        },
        runner,
    )


async def run_attribution_characterization(
    config: AttributionConfig,
    *,
    marker_root: Path | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Run the correctness-first attribution workload on the configured runner."""
    config.validate()
    if marker_root is not None:
        marker_root.mkdir(parents=True, exist_ok=True)
        result, runner = await _run_in_directory(config, marker_root)
        return [result], runner

    with tempfile.TemporaryDirectory(prefix="archetype-daft-attribution-") as temp:
        result, runner = await _run_in_directory(config, Path(temp))
        return [result], runner


def build_attribution_report(
    results: list[dict[str, Any]],
    *,
    config: AttributionConfig,
    runner: str,
    runner_id: str | None = None,
) -> dict[str, Any]:
    """Attach version, runner, and distributed-support disposition."""
    ray_available = importlib.util.find_spec("ray") is not None
    return build_report(
        results,
        suite="daft_attribution",
        config={
            **asdict(config),
            "workload": WORKLOAD_ID,
            "pinned_daft_version": PINNED_DAFT_VERSION,
            "configured_runner": runner,
            "synthetic_execution_boundary": "DataFrame.collect",
            "python_conversion_boundary": "DataFrame.to_pylist",
            "production_persistence_boundary": "not_measured",
            "subscriber_api": "experimental_harness_only",
            "execution_signal_source": "experimental_subscriber_stats",
            "ray_disposition": _ray_disposition(
                runner=runner,
                ray_available=ray_available,
            ),
        },
        environment=capture_environment(runner_id=runner_id),
    )


def _int_at_least(minimum: int) -> Any:
    def parse(value: str) -> int:
        parsed = int(value)
        if parsed < minimum:
            raise argparse.ArgumentTypeError(f"value must be at least {minimum}")
        return parsed

    return parse


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=_int_at_least(1), default=3)
    parser.add_argument("--delay-ms", type=_non_negative_float, default=50.0)
    parser.add_argument("--repetitions", type=_int_at_least(2), default=2)
    parser.add_argument("--out", default=None, help="Write a JSON snapshot here")
    parser.add_argument(
        "--runner-id",
        default=None,
        help="Stable machine identity; defaults to ARCHETYPE_BENCH_RUNNER or hostname",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = AttributionConfig(
        rows=args.rows,
        delay_seconds=args.delay_ms / 1000,
        repetitions=args.repetitions,
    )
    results, runner = asyncio.run(run_attribution_characterization(config))
    report = build_attribution_report(
        results,
        config=config,
        runner=runner,
        runner_id=args.runner_id,
    )
    if args.out is None:
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))
    else:
        write_report(report, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
