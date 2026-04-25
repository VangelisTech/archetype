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
async def test_get_backend_pool_distinguishes_cache_config(tmp_path):
    """Same (uri, namespace) but different cache_config yields distinct backends."""
    from archetype.core.aio import AsyncCachedStore

    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        store_cached, _, _ = await svc.get_backend(cfg, cache_config=CacheConfig(idle_sec=999))
        store_uncached, _, _ = await svc.get_backend(cfg, cache_config=None)

        assert isinstance(store_cached, AsyncCachedStore), "first call should be cached"
        assert not isinstance(store_uncached, AsyncCachedStore), (
            f"cache_config=None caller got {type(store_uncached).__name__}"
        )
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_get_backend_pool_distinguishes_backend_choice(tmp_path):
    """Same (uri, namespace) but different backend yields distinct instances."""
    svc = StorageService()
    try:
        cfg_iceberg = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.ICEBERG,
        )
        cfg_lance = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.LANCEDB,
        )

        store_iceberg, _, _ = await svc.get_backend(cfg_iceberg)
        store_lance, _, _ = await svc.get_backend(cfg_lance)

        assert isinstance(store_iceberg, AsyncStore), (
            f"iceberg caller got {type(store_iceberg).__name__}"
        )
        assert isinstance(store_lance, AsyncLancedbStore), (
            f"lancedb caller got {type(store_lance).__name__}"
        )
        assert store_iceberg is not store_lance
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_lancedb_backend_does_not_construct_daft_iceberg_session(tmp_path, monkeypatch):
    """LanceDB storage uses its own storage handle instead of the Daft/Iceberg factory."""
    svc = StorageService()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("LanceDB backend should not construct Daft/Iceberg storage")

    monkeypatch.setattr(
        "archetype.app.storage_service.StorageService.build_session",
        fail_if_called,
    )

    try:
        cfg = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.LANCEDB,
        )
        store, _, _ = await svc.get_backend(cfg)

        assert isinstance(store, AsyncLancedbStore)
        assert not hasattr(store, "session")
    finally:
        await svc.shutdown()


def test_iceberg_backend_passes_io_config_to_async_store(tmp_path, monkeypatch):
    from daft.io import IOConfig

    io_config = IOConfig()
    session = object()
    seen = {}

    class FakeStore:
        def __init__(self, session_arg, io_config=None):
            seen["session"] = session_arg
            seen["io_config"] = io_config

    monkeypatch.setattr("archetype.app.storage_service.AsyncStore", FakeStore)
    monkeypatch.setattr(
        StorageService,
        "build_session",
        classmethod(lambda cls, config: session),
    )

    cfg = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
        io_config=io_config,
    )

    store, _, _ = StorageService()._create_backend(cfg, cache_config=None)

    assert isinstance(store, FakeStore)
    assert seen["session"] is session
    assert seen["io_config"] is io_config


@pytest.mark.asyncio
async def test_get_backend_pool_shares_when_cache_config_matches(tmp_path):
    """Two callers with identical (uri, namespace, backend, cache_config) must
    still share the same triplet — the pool's core multiton behaviour."""
    from archetype.core.aio import AsyncCachedStore

    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cache = CacheConfig(idle_sec=123)

        store_a, q_a, u_a = await svc.get_backend(cfg, cache_config=cache)
        store_b, q_b, u_b = await svc.get_backend(cfg, cache_config=CacheConfig(idle_sec=123))

        assert isinstance(store_a, AsyncCachedStore)
        assert store_a is store_b
        assert q_a is q_b
        assert u_a is u_b
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
