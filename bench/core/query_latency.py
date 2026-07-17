# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Materialized QueryService latency benchmarks.

The workload measures four distinct read shapes against one durable world:
latest-tick exact-signature, historical-tick exact-signature, component union
across signatures, and an entity-filtered component query. Setup is outside
the timed region; plan construction and terminal materialization are inside.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from archetype.app.container import ServiceContainer
from archetype.app.query_service import QueryService
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.interfaces import ArchetypeSignature
from bench.core.report import build_report, capture_environment, write_report

_ARCHETYPE_COUNT = 3


class QueryPosition(Component):
    """Component shared by every query benchmark archetype."""

    x: float = 0.0


class QueryVelocity(Component):
    """Distinguishes the second benchmark archetype."""

    dx: float = 1.0


class QueryHealth(Component):
    """Distinguishes the third benchmark archetype."""

    hp: float = 100.0


@dataclass(frozen=True)
class QueryBenchmarkConfig:
    """Comparable query workload dimensions."""

    entities_per_archetype: int = 100
    history_ticks: int = 3
    repetitions: int = 5
    warmups: int = 1

    def validate(self) -> None:
        if self.entities_per_archetype < 1:
            raise ValueError("entities_per_archetype must be positive")
        if self.history_ticks < 2:
            raise ValueError("history_ticks must be at least 2")
        if self.repetitions < 1:
            raise ValueError("repetitions must be positive")
        if self.warmups < 0:
            raise ValueError("warmups must be non-negative")


@dataclass(frozen=True)
class QueryFixture:
    """Durable identifiers and row counts produced by untimed setup."""

    world_id: str
    run_id: str
    latest_tick: int
    total_entities: int
    entities_per_archetype: int
    filtered_entity_id: int


@dataclass(frozen=True)
class QueryCase:
    """One query shape with an exact correctness oracle."""

    name: str
    path: Literal["archetype", "components"]
    signature: ArchetypeSignature | None
    components: tuple[type[Component], ...]
    ticks: tuple[int, ...]
    entity_ids: tuple[int, ...] = ()
    matching_signatures: int = 1
    expected_rows: int = 0


def _query_cases(fixture: QueryFixture) -> tuple[QueryCase, ...]:
    return (
        QueryCase(
            name="current_state_exact_signature",
            path="archetype",
            signature=(QueryPosition,),
            components=(QueryPosition,),
            ticks=(fixture.latest_tick,),
            expected_rows=fixture.entities_per_archetype,
        ),
        QueryCase(
            name="historical_tick_exact_signature",
            path="archetype",
            signature=(QueryPosition,),
            components=(QueryPosition,),
            ticks=(0,),
            expected_rows=fixture.entities_per_archetype,
        ),
        QueryCase(
            name="component_union_across_signatures",
            path="components",
            signature=None,
            components=(QueryPosition,),
            ticks=(fixture.latest_tick,),
            matching_signatures=_ARCHETYPE_COUNT,
            expected_rows=fixture.total_entities,
        ),
        QueryCase(
            name="entity_filtered_component_union",
            path="components",
            signature=None,
            components=(QueryPosition,),
            ticks=(fixture.latest_tick,),
            entity_ids=(fixture.filtered_entity_id,),
            matching_signatures=_ARCHETYPE_COUNT,
            expected_rows=1,
        ),
    )


async def _setup_fixture(
    container: ServiceContainer,
    storage: StorageConfig,
    config: QueryBenchmarkConfig,
) -> QueryFixture:
    info = await container.world_service.create_world(
        WorldConfig(name="query-latency-benchmark"),
        storage,
    )
    count = config.entities_per_archetype
    position_ids = await container.mutation_service.create_entities(
        info.world_id,
        [[QueryPosition(x=float(index))] for index in range(count)],
    )
    velocity_ids = await container.mutation_service.create_entities(
        info.world_id,
        [
            [QueryPosition(x=float(index)), QueryVelocity(dx=float(index + 1))]
            for index in range(count)
        ],
    )
    await container.mutation_service.create_entities(
        info.world_id,
        [
            [QueryPosition(x=float(index)), QueryHealth(hp=float(100 - index))]
            for index in range(count)
        ],
    )
    run = await container.simulation_service.run(
        info.world_id,
        RunConfig.benchmark(steps=config.history_ticks),
    )
    if run.final_tick < 1:
        raise RuntimeError("query benchmark setup did not persist a tick")
    if not position_ids or not velocity_ids:
        raise RuntimeError("query benchmark setup did not create its fixtures")
    return QueryFixture(
        world_id=str(info.world_id),
        run_id=str(run.run_id),
        latest_tick=run.final_tick - 1,
        total_entities=count * _ARCHETYPE_COUNT,
        entities_per_archetype=count,
        filtered_entity_id=velocity_ids[0],
    )


async def _execute_case(
    queries: QueryService,
    storage: StorageConfig,
    fixture: QueryFixture,
    case: QueryCase,
) -> int:
    query_kwargs = {
        "storage_config": storage,
        "ticks": list(case.ticks),
        "entity_ids": list(case.entity_ids) or None,
    }
    if case.path == "archetype":
        if case.signature is None:
            raise ValueError(f"{case.name}: archetype query requires a signature")
        frame = await queries.query_archetype(
            case.signature,
            fixture.world_id,
            fixture.run_id,
            **query_kwargs,
        )
    else:
        frame = await queries.query_components(
            list(case.components),
            fixture.world_id,
            fixture.run_id,
            **query_kwargs,
        )
    return frame.collect().count_rows()


async def _measure_case(
    queries: QueryService,
    storage: StorageConfig,
    fixture: QueryFixture,
    case: QueryCase,
    config: QueryBenchmarkConfig,
) -> dict[str, Any]:
    for _ in range(config.warmups):
        observed = await _execute_case(queries, storage, fixture, case)
        _assert_row_count(case, observed)

    observed_rows: list[int] = []
    started = time.perf_counter()
    for _ in range(config.repetitions):
        observed_rows.append(await _execute_case(queries, storage, fixture, case))
    elapsed = time.perf_counter() - started

    for observed in observed_rows:
        _assert_row_count(case, observed)
    return {
        "name": case.name,
        "bench_name": case.name,
        "entities": case.expected_rows,
        "steps": config.repetitions,
        "elapsed_s": elapsed,
        "extras": {
            "query_path": case.path,
            "components": [component.__name__ for component in case.components],
            "ticks": list(case.ticks),
            "entity_filter_count": len(case.entity_ids),
            "matching_signatures": case.matching_signatures,
            "expected_rows": case.expected_rows,
            "materialization": "collect-count-rows",
        },
        "world_id": fixture.world_id,
        "run_id": fixture.run_id,
    }


def _assert_row_count(case: QueryCase, observed: int) -> None:
    if observed != case.expected_rows:
        raise RuntimeError(
            f"{case.name}: expected {case.expected_rows} rows after materialization, "
            f"observed {observed}"
        )


def _audit_storage_for(storage: StorageConfig) -> StorageConfig:
    """Derive the CommandService audit store without changing the measured backend."""
    return storage.model_copy(
        update={
            "namespace": f"{storage.namespace}__audit",
            "backend": StorageBackend.ICEBERG,
        }
    )


async def run_query_benchmarks(
    config: QueryBenchmarkConfig,
    storage: StorageConfig,
) -> list[dict[str, Any]]:
    """Build one durable fixture and measure every required query shape."""
    config.validate()
    container = ServiceContainer(audit_storage_config=_audit_storage_for(storage))
    try:
        fixture = await _setup_fixture(container, storage, config)
        return [
            await _measure_case(container.query_service, storage, fixture, case, config)
            for case in _query_cases(fixture)
        ]
    finally:
        await container.shutdown()


def build_query_report(
    results: list[dict[str, Any]],
    *,
    config: QueryBenchmarkConfig,
    storage: StorageConfig,
    runner_id: str | None = None,
) -> dict[str, Any]:
    """Wrap query measurements in the shared comparable report contract."""
    return build_report(
        results,
        suite="query_latency",
        config={
            "workload": "query-latency-v1",
            "entities_per_archetype": config.entities_per_archetype,
            "archetype_count": _ARCHETYPE_COUNT,
            "history_ticks": config.history_ticks,
            "repetitions": config.repetitions,
            "warmups": config.warmups,
            "storage_backend": storage.backend.value,
            "query_path": "QueryService",
            "materialization": "collect-count-rows",
        },
        environment=capture_environment(runner_id=runner_id),
    )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _history_ticks(value: str) -> int:
    parsed = int(value)
    if parsed < 2:
        raise argparse.ArgumentTypeError("history ticks must be at least 2")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run materialized QueryService benchmarks")
    parser.add_argument("--entities-per-archetype", type=_positive_int, default=100)
    parser.add_argument("--history-ticks", type=_history_ticks, default=3)
    parser.add_argument("--repetitions", type=_positive_int, default=5)
    parser.add_argument("--warmups", type=_non_negative_int, default=1)
    parser.add_argument(
        "--backend",
        choices=[backend.value for backend in StorageBackend],
        default=StorageBackend.LANCEDB.value,
    )
    parser.add_argument("--uri", default=None, help="Override ARCHETYPE_DATA_URI")
    parser.add_argument("--namespace", default=None, help="Override ARCHETYPE_QUERY_BENCH_NS")
    parser.add_argument("--out", default=None, help="Write the current schema-v1 report here")
    parser.add_argument("--history-dir", default=None, help="Archive the content-addressed report")
    parser.add_argument(
        "--runner-id",
        default=None,
        help="Stable machine identity; defaults to ARCHETYPE_BENCH_RUNNER or hostname",
    )
    return parser.parse_args(argv)


def _storage_from_args(args: argparse.Namespace) -> StorageConfig:
    uri = args.uri
    if uri is None:
        uri = str(Path(os.environ.get("ARCHETYPE_DATA_URI", "./archetype_data")).resolve())
    namespace = args.namespace or os.environ.get("ARCHETYPE_QUERY_BENCH_NS") or "query_benchmarks"
    return StorageConfig(
        uri=uri,
        namespace=namespace,
        backend=StorageBackend(args.backend),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = QueryBenchmarkConfig(
        entities_per_archetype=args.entities_per_archetype,
        history_ticks=args.history_ticks,
        repetitions=args.repetitions,
        warmups=args.warmups,
    )
    storage = _storage_from_args(args)
    results = asyncio.run(run_query_benchmarks(config, storage))
    report = build_query_report(
        results,
        config=config,
        storage=storage,
        runner_id=args.runner_id,
    )
    if args.out is not None or args.history_dir is not None:
        write_report(report, current_path=args.out, history_dir=args.history_dir)
    if args.out is None:
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
