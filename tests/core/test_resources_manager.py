import pytest

from archetype.app.storage_service import AsyncLancedbStore, StorageService, create_async_store
from archetype.core.aio import AsyncQueryManager, AsyncStore, AsyncUpdateManager
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from tests.conftest import make_storage_service


@pytest.mark.asyncio
async def test_storage_service_multiton_and_caching(tmp_path):
    """Ensure identical (uri, namespace) yield the same cached store."""
    svc = make_storage_service()
    try:
        cfg1 = StorageConfig(uri=str(tmp_path / "store1"), namespace="ns")
        cfg2 = StorageConfig(uri=str(tmp_path / "store1"), namespace="ns")  # identical key

        store1 = await svc.get_or_create_store(cfg1)
        store2 = await svc.get_or_create_store(cfg2)

        # Same store for identical (uri, namespace)
        assert store1 is store2

        # Querier/updater constructed from the same store are consistent
        q1, q2 = AsyncQueryManager(store=store1), AsyncQueryManager(store=store2)
        u1, u2 = AsyncUpdateManager(store=store1), AsyncUpdateManager(store=store2)
        assert q1._store is q2._store
        assert u1.store is u2.store
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_storage_service_cache_wrapper(tmp_path):
    """When a CacheConfig is provided, the store returned should be wrapped as an AsyncCachedStore."""
    svc = make_storage_service()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cache_cfg = CacheConfig(flush_rows=1, flush_mb=1, global_mb=1, idle_sec=1)
        store = await svc.get_or_create_store(cfg, cache_config=cache_cfg)
        from archetype.core.aio import AsyncCachedStore

        assert isinstance(store, AsyncCachedStore)
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_backend_selection_between_default_and_lancedb(tmp_path):
    """Verify backend selection flips between default AsyncStore and AsyncLancedbStore based on StorageConfig.backend."""
    svc = make_storage_service()
    try:
        cfg_default = StorageConfig(
            uri=str(tmp_path / "store_default"),
            namespace="ns_default",
            backend=StorageBackend.ICEBERG,
        )
        store_default = await svc.get_or_create_store(cfg_default)
        assert isinstance(store_default, AsyncStore)

        cfg_lance = StorageConfig(
            uri=str(tmp_path / "store_lance"), namespace="ns_lance", backend=StorageBackend.LANCEDB
        )
        store_lance = await svc.get_or_create_store(cfg_lance)
        assert isinstance(store_lance, AsyncLancedbStore)
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_get_backend_pool_distinguishes_cache_config(tmp_path):
    """Same (uri, namespace) but different cache_config yields distinct backends."""
    from archetype.core.aio import AsyncCachedStore

    svc = make_storage_service()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        store_cached = await svc.get_or_create_store(cfg, cache_config=CacheConfig(idle_sec=999))
        store_uncached = await svc.get_or_create_store(cfg, cache_config=None)

        assert isinstance(store_cached, AsyncCachedStore), "first call should be cached"
        assert not isinstance(store_uncached, AsyncCachedStore), (
            f"cache_config=None caller got {type(store_uncached).__name__}"
        )
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_get_backend_pool_distinguishes_backend_choice(tmp_path):
    """Same (uri, namespace) but different backend yields distinct instances."""
    svc = make_storage_service()
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

        store_iceberg = await svc.get_or_create_store(cfg_iceberg)
        store_lance = await svc.get_or_create_store(cfg_lance)

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
    svc = make_storage_service()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("LanceDB backend should not construct Daft/Iceberg storage")

    monkeypatch.setattr(
        "archetype.runtime.session.configure_session",
        fail_if_called,
    )

    try:
        cfg = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.LANCEDB,
        )
        store = await svc.get_or_create_store(cfg)

        assert isinstance(store, AsyncLancedbStore)
        assert not hasattr(store, "session")
    finally:
        await svc.shutdown()


def test_iceberg_backend_passes_io_config_to_async_store(tmp_path, monkeypatch):
    from daft.io import IOConfig

    from archetype.runtime.session import configure_session

    io_config = IOConfig()
    cfg = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
        io_config=io_config,
    )
    session = configure_session(cfg)
    seen = {}

    class FakeStore:
        def __init__(self, session_arg, io_config=None):
            seen["session"] = session_arg
            seen["io_config"] = io_config

    monkeypatch.setattr("archetype.app.storage_service.AsyncStore", FakeStore)

    store = create_async_store(cfg, session=session, cache_config=None)

    assert isinstance(store, FakeStore)
    assert seen["session"] is session
    assert seen["io_config"] is io_config


@pytest.mark.asyncio
async def test_iceberg_context_uses_pooled_store_session_and_io_config(tmp_path):
    from daft.io import IOConfig

    from archetype.runtime.session import configure_session

    io_config = IOConfig()
    config = StorageConfig(
        uri=str(tmp_path / "managed-catalog"),
        namespace="managed",
        backend=StorageBackend.ICEBERG,
        io_config=io_config,
    )
    session = configure_session(config)
    service = StorageService(session=session)
    try:
        context = await service.get_iceberg_context(config)

        assert context.session is session
        assert context.io_config is io_config
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_iceberg_context_rejects_lance_backend(tmp_path):
    service = make_storage_service()
    try:
        with pytest.raises(ValueError, match="backend=iceberg"):
            await service.get_iceberg_context(StorageConfig(uri=str(tmp_path / "lance")))
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_iceberg_pool_uses_daft_io_config_fingerprint(tmp_path):
    """Equivalent Daft I/O configs pool while distinct credentials stay isolated."""
    from daft.io import IOConfig

    svc = make_storage_service()
    shared = dict(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )
    first_io = IOConfig()
    equivalent_io = IOConfig()
    distinct_io = IOConfig(disable_suffix_range=True)
    try:
        first = await svc.get_or_create_store(StorageConfig(**shared, io_config=first_io))
        equivalent = await svc.get_or_create_store(StorageConfig(**shared, io_config=equivalent_io))
        distinct = await svc.get_or_create_store(StorageConfig(**shared, io_config=distinct_io))

        assert first is equivalent
        assert first is not distinct
        assert first.io_config is first_io
        assert distinct.io_config is distinct_io
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_injected_session_rejects_second_storage_identity(tmp_path):
    from archetype.runtime.session import configure_session

    first = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="first",
        backend=StorageBackend.ICEBERG,
    )
    service = StorageService(session=configure_session(first))
    try:
        await service.get_or_create_store(first)

        with pytest.raises(ValueError, match="namespace mismatch"):
            await service.get_or_create_store(first.model_copy(update={"namespace": "second"}))
        with pytest.raises(ValueError, match="cannot serve multiple storage identities"):
            await service.get_or_create_store(
                first.model_copy(update={"uri": str(tmp_path / "other")})
            )
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_injected_session_rejects_external_namespace_drift(tmp_path):
    from archetype.runtime.session import configure_session

    config = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="first",
        backend=StorageBackend.ICEBERG,
    )
    session = configure_session(config)
    service = StorageService(session=session)
    try:
        await service.get_or_create_store(config)
        session.create_namespace_if_not_exists("second")
        session.set_namespace("second")

        with pytest.raises(ValueError, match="namespace mismatch"):
            await service.get_or_create_store(config)
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_failed_store_creation_does_not_bind_injected_session(tmp_path, monkeypatch):
    from archetype.app import storage_service
    from archetype.app.storage_service import StorageService
    from archetype.runtime.session import configure_session

    first = StorageConfig(
        uri=str(tmp_path / "first"),
        namespace="managed",
        backend=StorageBackend.ICEBERG,
    )
    corrected = first.model_copy(update={"uri": str(tmp_path / "corrected")})
    session = configure_session(first)
    service = StorageService(session=session)
    real_create = storage_service.create_async_store
    calls = 0

    def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("catalog unavailable")
        return real_create(*args, **kwargs)

    monkeypatch.setattr(storage_service, "create_async_store", fail_once)
    try:
        with pytest.raises(RuntimeError, match="catalog unavailable"):
            await service.get_or_create_store(first)

        store = await service.get_or_create_store(corrected)

        assert isinstance(store, AsyncStore)
        assert service._session_identity == (str(corrected.uri), corrected.namespace)
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_get_backend_pool_shares_when_cache_config_matches(tmp_path):
    """Two callers with identical (uri, namespace, backend, cache_config) must
    still share the same store — the pool's core multiton behaviour."""
    from archetype.core.aio import AsyncCachedStore

    svc = make_storage_service()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        cache = CacheConfig(idle_sec=123)

        store_a = await svc.get_or_create_store(cfg, cache_config=cache)
        store_b = await svc.get_or_create_store(cfg, cache_config=CacheConfig(idle_sec=123))

        assert isinstance(store_a, AsyncCachedStore)
        assert store_a is store_b
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_multiton_concurrent_calls_return_same_instances(tmp_path):
    """Concurrent create_store calls for the same key must return identical object instances."""
    svc = make_storage_service()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store_cc"), namespace="ns_cc")

        async def get_store():
            return await svc.get_or_create_store(cfg)

        results = await __import__("asyncio").gather(*[get_store() for _ in range(5)])
        s0 = results[0]
        for s in results[1:]:
            assert s is s0
    finally:
        await svc.shutdown()
