import pathlib
import shutil

from daft.session import Session

from archetype.core.config import StorageConfig
from archetype.runtime.session import configure_session


def test_configure_session_remote_uri_uses_meta_dir(tmp_path):
    """Remote URIs should not be created locally; local sqlite catalog should be under .archetype_meta."""
    remote_uri = "s3://bucket/prefix"
    # Ensure we start clean
    meta_dir = pathlib.Path(".archetype_meta")
    if meta_dir.exists():
        shutil.rmtree(meta_dir)

    cfg = StorageConfig(uri=remote_uri, namespace="ns")
    session = configure_session(cfg)
    # Local meta dir created
    assert pathlib.Path(".archetype_meta").exists()
    # Session returned successfully
    assert isinstance(session, Session)


def test_configure_session_preserves_io_config_on_storage_config():
    """StorageConfig.io_config stays available for explicit Daft Iceberg read/write calls."""
    from daft.io import IOConfig, UnityConfig

    io = IOConfig(unity=UnityConfig(endpoint="https://example", token="t"))

    cfg = StorageConfig(uri="s3://bucket/prefix", namespace="ns", io_config=io)
    session = configure_session(cfg)
    assert isinstance(session, Session)
    # io_config is on the config object, not consumed by configure_session
    assert cfg.io_config is io
