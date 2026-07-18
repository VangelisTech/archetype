"""Regression tests for bench/core/ecs/run.py dataset isolation (issue #338)."""

from __future__ import annotations

from argparse import Namespace

import pytest

from archetype.core.config import StorageBackend, StorageConfig
from bench.core.ecs.run import _storage_for_bench, _storage_from_args, run_all


def test_storage_for_bench_suffixes_default_storage(tmp_path, monkeypatch):
    monkeypatch.setenv("ARCHETYPE_DATA_URI", str(tmp_path))
    monkeypatch.setenv("ARCHETYPE_BENCH_NS", "default")

    packed = _storage_for_bench(None, "packed_iteration")
    simple = _storage_for_bench(None, "simple_iteration")

    assert packed.uri == str(tmp_path)
    assert packed.namespace == "default__packed_iteration"
    assert simple.namespace == "default__simple_iteration"


def test_storage_for_bench_appends_bench_suffix_to_namespace():
    storage = StorageConfig(uri="/tmp/x", namespace="benchmarks")
    out = _storage_for_bench(storage, "packed_iteration")
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


def test_storage_cli_backend_override_does_not_require_a_uri(tmp_path, monkeypatch):
    """The old CLI advertised --backend but ignored it unless --uri was also set."""
    monkeypatch.setenv("ARCHETYPE_DATA_URI", str(tmp_path))
    monkeypatch.setenv("ARCHETYPE_BENCH_NS", "environment-default")

    storage = _storage_from_args(Namespace(uri=None, namespace="explicit", backend="iceberg"))

    assert storage.uri == str(tmp_path)
    assert storage.namespace == "explicit"
    assert storage.backend is StorageBackend.ICEBERG


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
    assert [r["name"] for r in results] == [
        "packed_iteration",
        "simple_iteration",
        "fragmented_iteration",
        "entity_cycle",
        "add_remove",
    ]

    expected_namespaces = {f"isolation__{r['name']}" for r in results}
    on_disk = {p.name for p in tmp_path.iterdir() if p.is_dir()}
    # Every bench namespace must exist on disk, and the shared "isolation"
    # namespace must NOT — that would mean tables were written without the
    # per-bench suffix and could collide.
    assert expected_namespaces.issubset(on_disk)
    assert "isolation" not in on_disk
