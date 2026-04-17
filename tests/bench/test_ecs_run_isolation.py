"""Regression tests for bench/core/ecs/run.py dataset isolation (issue #146)."""

from __future__ import annotations

import pytest

from archetype.core.config import StorageBackend, StorageConfig
from bench.core.ecs.run import _storage_for_bench, run_all


def test_storage_for_bench_returns_none_when_storage_is_none():
    assert _storage_for_bench(None, "packed_iteration") is None


def test_storage_for_bench_appends_bench_suffix_to_namespace():
    storage = StorageConfig(uri="/tmp/x", namespace="benchmarks")
    out = _storage_for_bench(storage, "packed_iteration")
    assert out is not None
    assert out.uri == "/tmp/x"
    assert out.namespace == "benchmarks__packed_iteration"


def test_storage_for_bench_normalizes_hyphenated_bench_name():
    storage = StorageConfig(uri="/tmp/x", namespace="bench")
    out = _storage_for_bench(storage, "add-remove")
    assert out.namespace == "bench__add_remove"


def test_storage_for_bench_preserves_non_namespace_fields():
    storage = StorageConfig(
        uri="/tmp/x",
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )
    out = _storage_for_bench(storage, "simple_iteration")
    assert out.backend == StorageBackend.ICEBERG
    assert out.uri == "/tmp/x"


def test_storage_for_bench_gives_each_bench_a_unique_namespace():
    storage = StorageConfig(uri="/tmp/x", namespace="bench")
    names = [
        "packed_iteration",
        "simple_iteration",
        "fragmented_iteration",
        "entity_cycle",
        "add_remove",
    ]
    namespaces = {_storage_for_bench(storage, n).namespace for n in names}
    assert len(namespaces) == len(names)


@pytest.mark.asyncio
async def test_run_all_isolates_each_bench_into_its_own_namespace(tmp_path):
    """Several benches reuse component class names (A, B, C, ...). If they write
    into the same storage namespace their archetype tables collide and later
    benches scan rows from earlier ones. Each bench must land in a distinct
    namespace directory on disk."""
    storage = StorageConfig(
        uri=str(tmp_path),
        namespace="isolation",
        backend=StorageBackend.ICEBERG,
    )
    results = await run_all(steps=1, storage=storage)

    assert len(results) == 5
    assert [r["bench_name"] for r in results] == [
        "packed_iteration",
        "simple_iteration",
        "fragmented_iteration",
        "entity_cycle",
        "add_remove",
    ]

    expected_namespaces = {f"isolation__{r['bench_name']}" for r in results}
    on_disk = {p.name for p in tmp_path.iterdir() if p.is_dir()}
    # Every bench namespace must exist on disk, and the shared "isolation"
    # namespace must NOT — that would mean tables were written without the
    # per-bench suffix and could collide.
    assert expected_namespaces.issubset(on_disk)
    assert "isolation" not in on_disk
