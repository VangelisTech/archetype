import pytest

from archetype.core.config import StorageConfig
from tests.conftest import make_storage_service


class FakeSyncStore:
    def shutdown(self):
        self.called = True


@pytest.mark.asyncio
async def test_storage_service_shutdown_calls_sync_shutdown(monkeypatch, tmp_path):
    """Shutdown should call sync shutdown() on stores that are not async."""
    svc = make_storage_service()
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


class _FailingAsyncStore:
    """Async store whose shutdown always raises."""

    def __init__(self, message: str = "simulated shutdown failure"):
        self._message = message

    async def shutdown(self):
        raise RuntimeError(self._message)


class _CountingAsyncStore:
    """Async store that records whether shutdown was called."""

    def __init__(self):
        self.shutdown_called = False

    async def shutdown(self):
        self.shutdown_called = True


@pytest.mark.asyncio
async def test_storage_service_shutdown_continues_after_first_store_failure():
    """A failing store does not abort the shutdown loop.

    Before the fix, `StorageService.shutdown` iterated `_instances.values()`
    without exception handling — the first failing store aborted the loop,
    leaving later stores open and `_instances`/`_locks` un-cleared.
    """
    svc = make_storage_service()
    try:
        failing = _FailingAsyncStore()
        counting = _CountingAsyncStore()
        # Insertion order matters: failing must come BEFORE counting
        # so the pre-fix code would abort before reaching counting.
        svc._instances["fail::ns"] = failing
        svc._instances["ok::ns"] = counting

        with pytest.raises(RuntimeError, match="simulated shutdown failure"):
            await svc.shutdown()

        assert counting.shutdown_called, (
            "shutdown aborted on the first failing store; subsequent stores leaked"
        )
        assert svc._instances == {}, "_instances was not cleared after shutdown"
        assert svc._locks == {}, "_locks was not cleared after shutdown"
    finally:
        svc._instances.clear()
        svc._locks.clear()


@pytest.mark.asyncio
async def test_storage_service_shutdown_failing_in_middle_drains_all():
    """A failing store in the middle of iteration
    must not block stores that come after it."""
    svc = make_storage_service()
    try:
        first = _CountingAsyncStore()
        middle = _FailingAsyncStore("middle failure")
        last = _CountingAsyncStore()
        svc._instances["a::ns"] = first
        svc._instances["b::ns"] = middle
        svc._instances["c::ns"] = last

        with pytest.raises(RuntimeError, match="middle failure"):
            await svc.shutdown()

        assert first.shutdown_called
        assert last.shutdown_called, "store after the failing one in iteration order leaked"
        assert svc._instances == {}
        assert svc._locks == {}
    finally:
        svc._instances.clear()
        svc._locks.clear()
