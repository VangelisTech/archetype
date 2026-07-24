# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Materialized command-dispatch query latency benchmarks.

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
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from archetype.commands.dispatch import CommandDispatcher
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.models import (
    ComponentTypeRef,
    CreateEntities,
    CreateWorld,
    QueryArchetype,
    QueryComponents,
    Run,
)
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
    entities_per_archetype: int
    filtered_entity_id: int


@dataclass(frozen=True)
class QueryCase:
    """One query shape with an exact correctness oracle."""

    name: str
    path: Literal["archetype", "components"]
    tick: int
    expected_rows: int
    entity_ids: tuple[int, ...] = ()


def _query_cases(fixture: QueryFixture) -> tuple[QueryCase, ...]:
    count = fixture.entities_per_archetype
    return (
        QueryCase("current_state_exact_signature", "archetype", fixture.latest_tick, count),
        QueryCase("historical_tick_exact_signature", "archetype", 0, count),
        QueryCase(
            "component_union_across_signatures",
            "components",
            fixture.latest_tick,
            count * _ARCHETYPE_COUNT,
        ),
        QueryCase(
            "entity_filtered_component_union",
            "components",
            fixture.latest_tick,
            1,
            (fixture.filtered_entity_id,),
        ),
    )


async def _setup_fixture(
    dispatcher: CommandDispatcher,
    storage: StorageConfig,
    config: QueryBenchmarkConfig,
) -> QueryFixture:
    info = await dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name="query-latency-benchmark"),
            storage_config=storage,
        )
    )
    count = config.entities_per_archetype
    groups: list[list[list[Component]]] = [
        [[QueryPosition(x=float(index))] for index in range(count)],
        [
            [QueryPosition(x=float(index)), QueryVelocity(dx=float(index + 1))]
            for index in range(count)
        ],
        [
            [QueryPosition(x=float(index)), QueryHealth(hp=float(100 - index))]
            for index in range(count)
        ],
    ]
    entity_ids = [
        await dispatcher.apply(
            CreateEntities.from_entities(
                world_id=info.world_id,
                entities=group,
            )
        )
        for group in groups
    ]
    if any(len(ids) != count for ids in entity_ids):
        raise RuntimeError("query benchmark setup did not create its fixtures")

    run = await dispatcher.apply(
        Run(
            world_id=info.world_id,
            run_config=RunConfig.benchmark(steps=config.history_ticks),
        )
    )
    if run.final_tick < 1:
        raise RuntimeError("query benchmark setup did not persist a tick")
    return QueryFixture(
        world_id=str(info.world_id),
        run_id=str(run.run_id),
        latest_tick=run.final_tick - 1,
        entities_per_archetype=count,
        filtered_entity_id=entity_ids[1][0],
    )


async def _execute_case(
    dispatcher: CommandDispatcher,
    storage: StorageConfig,
    fixture: QueryFixture,
    case: QueryCase,
) -> int:
    ticks = (case.tick,)
    entity_ids = case.entity_ids or None
    position_ref = ComponentTypeRef.from_type(QueryPosition)
    if case.path == "archetype":
        frame = await dispatcher.apply(
            QueryArchetype(
                signature=(position_ref,),
                world_id=fixture.world_id,
                run_id=fixture.run_id,
                storage_config=storage,
                ticks=ticks,
                entity_ids=entity_ids,
            )
        )
    else:
        frame = await dispatcher.apply(
            QueryComponents(
                components=(position_ref,),
                world_id=fixture.world_id,
                run_id=fixture.run_id,
                storage_config=storage,
                ticks=ticks,
                entity_ids=entity_ids,
            )
        )
    return frame.collect().count_rows()


async def _measure_case(
    dispatcher: CommandDispatcher,
    storage: StorageConfig,
    fixture: QueryFixture,
    case: QueryCase,
    config: QueryBenchmarkConfig,
) -> dict[str, Any]:
    for _ in range(config.warmups):
        observed = await _execute_case(dispatcher, storage, fixture, case)
        _assert_row_count(case, observed)

    observed_rows: list[int] = []
    started = time.perf_counter()
    for _ in range(config.repetitions):
        observed_rows.append(await _execute_case(dispatcher, storage, fixture, case))
    elapsed = time.perf_counter() - started

    for observed in observed_rows:
        _assert_row_count(case, observed)
    return {
        "name": case.name,
        "elapsed_s": elapsed,
        "repetitions": config.repetitions,
        "rows_per_query": case.expected_rows,
        "query_path": case.path,
        "tick": case.tick,
        "entity_filter_count": len(case.entity_ids),
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
    """Derive the dispatcher audit store without changing the measured backend."""
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
    resources = build_runtime_resources(
        RuntimeBootstrapConfig.from_env(
            audit_storage_config=_audit_storage_for(storage),
        )
    )
    dispatcher = resources.dispatcher
    try:
        fixture = await _setup_fixture(dispatcher, storage, config)
        return [
            await _measure_case(dispatcher, storage, fixture, case, config)
            for case in _query_cases(fixture)
        ]
    finally:
        await resources.aclose()


def build_query_report(
    results: list[dict[str, Any]],
    *,
    config: QueryBenchmarkConfig,
    storage: StorageConfig,
    runner_id: str | None = None,
) -> dict[str, Any]:
    """Attach the query workload context to one local snapshot."""
    return build_report(
        results,
        suite="query_latency",
        config={
            **asdict(config),
            "workload": "query-latency-v1",
            "archetype_count": _ARCHETYPE_COUNT,
            "storage_backend": storage.backend.value,
            "query_path": "CommandDispatcher",
            "materialization": "collect-count-rows",
        },
        environment=capture_environment(runner_id=runner_id),
    )


def _int_at_least(minimum: int) -> Callable[[str], int]:
    def parse(value: str) -> int:
        parsed = int(value)
        if parsed < minimum:
            raise argparse.ArgumentTypeError(f"value must be at least {minimum}")
        return parsed

    return parse


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run materialized command-dispatch query benchmarks"
    )
    parser.add_argument("--entities-per-archetype", type=_int_at_least(1), default=100)
    parser.add_argument("--history-ticks", type=_int_at_least(2), default=3)
    parser.add_argument("--repetitions", type=_int_at_least(1), default=5)
    parser.add_argument("--warmups", type=_int_at_least(0), default=1)
    parser.add_argument(
        "--backend",
        choices=[backend.value for backend in StorageBackend],
        default=StorageBackend.LANCEDB.value,
    )
    parser.add_argument("--uri", default=None, help="Override ARCHETYPE_DATA_URI")
    parser.add_argument("--namespace", default=None, help="Override ARCHETYPE_QUERY_BENCH_NS")
    parser.add_argument("--out", default=None, help="Write a JSON snapshot here")
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
    if args.out is None:
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))
    else:
        write_report(report, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
