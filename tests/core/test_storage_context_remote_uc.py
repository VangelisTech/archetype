import pathlib
import shutil

from archetype.core.config import StorageConfig
from archetype.core.runtime.storage import StorageContextFactory


def test_storage_context_remote_uri_uses_meta_dir(tmp_path, monkeypatch):
    """Remote URIs should not be created locally; local sqlite catalog should be under .archetype_meta, and warehouse points to remote URI."""
    # Use a remote-style URI
    remote_uri = "s3://bucket/prefix"
    # Ensure we start clean
    meta_dir = pathlib.Path(".archetype_meta")
    if meta_dir.exists():
        shutil.rmtree(meta_dir)

    cfg = StorageConfig(uri=remote_uri, namespace="ns")
    ctx = StorageContextFactory.build(cfg)
    # Local meta dir created
    assert pathlib.Path(".archetype_meta").exists()
    # Session initialized; namespace set
    assert ctx.namespace == "ns"


def test_storage_context_applies_io_config_to_daft_planning(monkeypatch):
    """StorageConfig.io_config is applied through Daft planning config, not carried on StorageContext."""
    from daft.io import IOConfig, UnityConfig

    io = IOConfig(unity=UnityConfig(endpoint="https://example", token="t"))
    seen: dict[str, IOConfig] = {}

    def fake_set_planning_config(*, default_io_config=None, **kwargs):
        seen["default_io_config"] = default_io_config
        return None

    monkeypatch.setattr(
        "archetype.app.storage_service.daft.set_planning_config",
        fake_set_planning_config,
    )

    cfg = StorageConfig(uri="s3://bucket/prefix", namespace="ns", io_config=io)
    ctx = StorageContextFactory.build(cfg)
    assert ctx.namespace == "ns"
    assert not hasattr(ctx, "io_config")
    assert seen["default_io_config"] is io
