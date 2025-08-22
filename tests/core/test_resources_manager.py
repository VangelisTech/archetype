import pytest
import pytest_asyncio

from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.resources import StorageResourceManager
from archetype.core.storage import AsyncLancedbStore
from archetype.core.aio import AsyncStore
from archetype.core.config import StorageBackend


@pytest.mark.asyncio
async def test_storage_resource_manager_multiton_and_caching(tmp_path):
    """Ensure identical (uri, namespace) yield the same cached triplet and instrumented flag does not rewrap existing instances."""
    mgr = StorageResourceManager()
    try:
        cfg1 = StorageConfig(uri=str(tmp_path / "store1"), namespace="ns")
        cfg2 = StorageConfig(uri=str(tmp_path / "store1"), namespace="ns")  # identical key

        store1, q1, u1 = await mgr.get_backend(cfg1)
        store2, q2, u2 = await mgr.get_backend(cfg2)

        # Same triplet for identical (uri, namespace)
        assert store1 is store2
        assert q1 is q2
        assert u1 is u2

        # Instrumented flag yields instrumented querier/store
        store3, q3, u3 = await mgr.get_backend(cfg1, instrumented=True)
        # The manager keys by uri+namespace; first creation wins. So instrumented=True
        # after an existing plain instance should return the same original instances.
        assert store3 is store1
        assert q3 is q1
        assert u3 is u1
    finally:
        await mgr.shutdown()


@pytest.mark.asyncio
async def test_storage_resource_manager_cache_wrapper(tmp_path):
    """When a CacheConfig is provided, the store returned should be wrapped as an AsyncCachedStore."""
    mgr = StorageResourceManager()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cache_cfg = CacheConfig(flush_rows=1, flush_mb=1, global_mb=1, idle_sec=1)
        store, _, _ = await mgr.get_backend(cfg, cache_config=cache_cfg)
        # Ensure the returned store is the cached wrapper type when cache is provided
        from archetype.core.aio import AsyncCachedStore
        assert isinstance(store, AsyncCachedStore)
    finally:
        await mgr.shutdown()


@pytest.mark.asyncio
async def test_backend_selection_between_default_and_lancedb(tmp_path):
    """Verify backend selection flips between default AsyncStore and AsyncLancedbStore based on StorageConfig.backend."""
    mgr = StorageResourceManager()
    try:
        cfg_default = StorageConfig(uri=str(tmp_path / "store_default"), namespace="ns_default")
        store_default, _, _ = await mgr.get_backend(cfg_default)
        assert isinstance(store_default, (AsyncStore,))

        cfg_lance = StorageConfig(uri=str(tmp_path / "store_lance"), namespace="ns_lance", backend=StorageBackend.LANCEDB)
        store_lance, _, _ = await mgr.get_backend(cfg_lance)
        assert isinstance(store_lance, AsyncLancedbStore)
    finally:
        await mgr.shutdown()


@pytest.mark.asyncio
async def test_instrumented_instances_when_first_creation(tmp_path):
    """When instrumented=True on first creation, store and querier are instrumented; updater remains the base AsyncUpdateManager."""
    mgr = StorageResourceManager()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store_instrumented"), namespace="ns_i")
        store, q, u = await mgr.get_backend(cfg, instrumented=True)
        from archetype.core.instrumentation.instrumented_async_store import InstrumentedAsyncStore
        from archetype.core.instrumentation.instrumented_async_querier import InstrumentedAsyncQueryManager
        from archetype.core.instrumentation.instrumented_async_updater import InstrumentedAsyncUpdateManager
        assert isinstance(store, InstrumentedAsyncStore)
        assert isinstance(q, InstrumentedAsyncQueryManager)
        assert isinstance(u, InstrumentedAsyncUpdateManager)
    finally:
        await mgr.shutdown()


@pytest.mark.asyncio
async def test_multiton_concurrent_calls_return_same_instances(tmp_path):
    """Concurrent get_backend calls for the same key must return identical object instances (no duplicate creation)."""
    mgr = StorageResourceManager()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store_cc"), namespace="ns_cc")

        async def get_triplet():
            return await mgr.get_backend(cfg)

        results = await __import__("asyncio").gather(*[get_triplet() for _ in range(5)])
        # All stores/queriers/updaters must be identical objects
        s0, q0, u0 = results[0]
        for s, q, u in results[1:]:
            assert s is s0
            assert q is q0
            assert u is u0
    finally:
        await mgr.shutdown()


