# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the materialized world-query benchmark family."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from archetype.core.config import StorageBackend, StorageConfig
from bench.core.query_latency import (
    QueryBenchmarkConfig,
    QueryFixture,
    _audit_storage_for,
    _query_cases,
    _storage_from_args,
    build_query_report,
    run_query_benchmarks,
)


def test_query_cases_cover_each_issue_141_read_shape() -> None:
    fixture = QueryFixture(
        world_id="world",
        run_id="run",
        latest_tick=4,
        entities_per_archetype=10,
        filtered_entity_id=11,
    )

    cases = {case.name: case for case in _query_cases(fixture)}

    assert set(cases) == {
        "current_state_exact_signature",
        "historical_tick_exact_signature",
        "component_union_across_signatures",
        "entity_filtered_component_union",
    }
    assert cases["current_state_exact_signature"].tick == 4
    assert cases["historical_tick_exact_signature"].tick == 0
    assert cases["component_union_across_signatures"].expected_rows == 30
    assert cases["entity_filtered_component_union"].entity_ids == (11,)
    assert cases["entity_filtered_component_union"].expected_rows == 1


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (QueryBenchmarkConfig(entities_per_archetype=0), "entities_per_archetype"),
        (QueryBenchmarkConfig(history_ticks=1), "history_ticks"),
        (QueryBenchmarkConfig(repetitions=0), "repetitions"),
        (QueryBenchmarkConfig(warmups=-1), "warmups"),
    ],
)
def test_query_benchmark_rejects_degenerate_workloads(
    config: QueryBenchmarkConfig, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        config.validate()


def test_query_storage_cli_uses_query_specific_namespace(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ARCHETYPE_DATA_URI", str(tmp_path))
    monkeypatch.delenv("ARCHETYPE_QUERY_BENCH_NS", raising=False)
    args = Namespace(uri=None, namespace=None, backend="iceberg")

    storage = _storage_from_args(args)

    assert storage.uri == str(tmp_path)
    assert storage.namespace == "query_benchmarks"
    assert storage.backend is StorageBackend.ICEBERG


def test_query_benchmark_derives_a_scoped_iceberg_audit_store(tmp_path: Path) -> None:
    storage = StorageConfig(
        uri=str(tmp_path),
        namespace="query-contract",
        backend=StorageBackend.LANCEDB,
    )

    audit_storage = _audit_storage_for(storage)

    assert audit_storage.uri == storage.uri
    assert audit_storage.namespace == "query-contract__audit"
    assert audit_storage.backend is StorageBackend.ICEBERG
    assert storage.backend is StorageBackend.LANCEDB


@pytest.mark.asyncio
async def test_query_benchmark_materializes_all_four_shapes(tmp_path: Path, caplog) -> None:
    config = QueryBenchmarkConfig(
        entities_per_archetype=2,
        history_ticks=2,
        repetitions=1,
        warmups=0,
    )
    storage = StorageConfig(
        uri=str(tmp_path / "query-store"),
        namespace="query-contract",
        backend=StorageBackend.LANCEDB,
    )

    results = await run_query_benchmarks(config, storage)

    assert [result["name"] for result in results] == [
        "current_state_exact_signature",
        "historical_tick_exact_signature",
        "component_union_across_signatures",
        "entity_filtered_component_union",
    ]
    assert all(result["elapsed_s"] > 0 for result in results)
    assert [result["rows_per_query"] for result in results] == [2, 2, 6, 1]
    assert {result["repetitions"] for result in results} == {1}
    assert {result["query_path"] for result in results} == {"archetype", "components"}

    report = build_query_report(
        results,
        config=config,
        storage=storage,
        runner_id="contract-runner",
    )
    assert report["suite"] == "query_latency"
    assert report["config"]["workload"] == "query-latency-v1"
    assert report["results"] == results
    assert "audit emission failed" not in caplog.text
