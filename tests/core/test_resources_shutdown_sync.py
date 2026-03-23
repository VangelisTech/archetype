import pytest

from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig


class FakeSyncStore:
    def shutdown(self):
        self.called = True


@pytest.mark.asyncio
async def test_storage_service_shutdown_calls_sync_shutdown(monkeypatch, tmp_path):
    """Shutdown should call sync shutdown() on stores that are not async."""
    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        async def fake_get_backend(storage_config, cache_config=None):
            store = FakeSyncStore()
            querier = object()
            updater = object()
            svc._instances[f"{cfg.uri}::{cfg.namespace}"] = (store, querier, updater)
            return store, querier, updater

        svc.get_backend = fake_get_backend  # type: ignore
        await svc.get_backend(cfg)
        await svc.shutdown()
    finally:
        svc._instances.clear()
        svc._locks.clear()
