from pathlib import Path

import pytest
from daft.io import IOConfig, UnityConfig

from archetype.core.aio import AsyncStore
from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.service import create_async_store
from archetype.storage.session import configure_session


def test_builtin_iceberg_factory_rejects_remote_uri(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = StorageConfig(
        uri="s3://bucket/prefix",
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )

    with pytest.raises(ValueError, match="preconfigured Daft Session"):
        configure_session(config)

    assert not Path(".archetype_meta").exists()


def test_preconfigured_session_and_io_config_pass_through_for_remote_iceberg(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    io_config = IOConfig(unity=UnityConfig(endpoint="https://example", token="t"))
    session = configure_session(
        StorageConfig(
            uri=str(tmp_path / "catalog"),
            namespace="managed",
            backend=StorageBackend.ICEBERG,
        )
    )
    config = StorageConfig(
        uri="s3://bucket/prefix",
        namespace="managed",
        backend=StorageBackend.ICEBERG,
        io_config=io_config,
    )

    store = create_async_store(config, session=session)

    assert isinstance(store, AsyncStore)
    assert store.session is session
    assert store.io_config is io_config
    assert not (tmp_path / ".archetype_meta").exists()
