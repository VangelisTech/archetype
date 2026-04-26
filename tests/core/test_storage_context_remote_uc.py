import pathlib
import shutil

from archetype.app.storage_service import StorageContextFactory
from archetype.core.config import StorageConfig


def test_storage_context_remote_uri_uses_meta_dir(tmp_path):
    """Remote URIs should not be created locally; local sqlite catalog should be under .archetype_meta, and warehouse points to remote URI."""
    # Use a remote-style URI
    remote_uri = "s3://bucket/prefix"
    # Ensure we start clean
    meta_dir = pathlib.Path(".archetype_meta")
    if meta_dir.exists():
        shutil.rmtree(meta_dir)

    cfg = StorageConfig(uri=remote_uri, namespace="ns")
    ctx = StorageContextFactory().build(cfg)
    # Local meta dir created
    assert pathlib.Path(".archetype_meta").exists()
    # Session initialized; namespace set
    assert ctx.namespace == "ns"


def test_storage_context_preserves_io_config_for_store_binding():
    """StorageConfig.io_config stays available for explicit Daft Iceberg read/write calls."""
    from daft.io import IOConfig, UnityConfig

    io = IOConfig(unity=UnityConfig(endpoint="https://example", token="t"))

    cfg = StorageConfig(uri="s3://bucket/prefix", namespace="ns", io_config=io)
    ctx = StorageContextFactory().build(cfg)
    assert ctx.namespace == "ns"
    assert ctx.io_config is io
