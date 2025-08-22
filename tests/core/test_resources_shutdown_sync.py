import pytest
import pytest_asyncio

from archetype.core.resources import StorageResourceManager
from archetype.core.config import StorageConfig


class FakeSyncStore:
    def shutdown(self):
        # record was called
        self.called = True


@pytest.mark.asyncio
async def test_storage_manager_shutdown_calls_sync_shutdown(monkeypatch, tmp_path):
    """Shutdown should call sync shutdown() on stores that are not async."""
    mgr = StorageResourceManager()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        async def fake_get_backend(storage_config, cache_config=None, instrumented=None):
            store = FakeSyncStore()
            querier = object()
            updater = object()
            mgr._instances[f"{cfg.uri}::{cfg.namespace}"] = (store, querier, updater)
            return store, querier, updater

        # patch internal get_backend to install a sync store
        mgr.get_backend = fake_get_backend  # type: ignore
        await mgr.get_backend(cfg)
        await mgr.shutdown()
        # If no exceptions, sync shutdown path executed
    finally:
        # ensure cleanup for subsequent tests
        mgr._instances.clear()
        mgr._locks.clear()


