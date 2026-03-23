import pytest

from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.storage import AsyncLancedbStore


@pytest.mark.asyncio
async def test_storage_service_multiton_and_caching(tmp_path):
    """Ensure identical (uri, namespace) yield the same cached triplet."""
    svc = StorageService()
    try:
        cfg1 = StorageConfig(uri=str(tmp_path / "store1"), namespace="ns")
        cfg2 = StorageConfig(uri=str(tmp_path / "store1"), namespace="ns")  # identical key

        store1, q1, u1 = await svc.get_backend(cfg1)
        store2, q2, u2 = await svc.get_backend(cfg2)

        # Same triplet for identical (uri, namespace)
        assert store1 is store2
        assert q1 is q2
        assert u1 is u2
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_storage_service_cache_wrapper(tmp_path):
    """When a CacheConfig is provided, the store returned should be wrapped as an AsyncCachedStore."""
    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cache_cfg = CacheConfig(flush_rows=1, flush_mb=1, global_mb=1, idle_sec=1)
        store, _, _ = await svc.get_backend(cfg, cache_config=cache_cfg)
        from archetype.core.aio import AsyncCachedStore

        assert isinstance(store, AsyncCachedStore)
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_backend_selection_between_default_and_lancedb(tmp_path):
    """Verify backend selection flips between default AsyncStore and AsyncLancedbStore based on StorageConfig.backend."""
    svc = StorageService()
    try:
        cfg_default = StorageConfig(
            uri=str(tmp_path / "store_default"),
            namespace="ns_default",
            backend=StorageBackend.ICEBERG,
        )
        store_default, _, _ = await svc.get_backend(cfg_default)
        assert isinstance(store_default, AsyncStore)

        cfg_lance = StorageConfig(
            uri=str(tmp_path / "store_lance"), namespace="ns_lance", backend=StorageBackend.LANCEDB
        )
        store_lance, _, _ = await svc.get_backend(cfg_lance)
        assert isinstance(store_lance, AsyncLancedbStore)
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_multiton_concurrent_calls_return_same_instances(tmp_path):
    """Concurrent get_backend calls for the same key must return identical object instances."""
    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store_cc"), namespace="ns_cc")

        async def get_triplet():
            return await svc.get_backend(cfg)

        results = await __import__("asyncio").gather(*[get_triplet() for _ in range(5)])
        s0, q0, u0 = results[0]
        for s, q, u in results[1:]:
            assert s is s0
            assert q is q0
            assert u is u0
    finally:
        await svc.shutdown()
