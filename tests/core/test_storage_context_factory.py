import pathlib

from archetype.app.storage_service import StorageContextFactory
from archetype.core.config import StorageConfig


def test_storage_context_requires_absolute_path_for_local(tmp_path):
    """Relative local URIs are resolved to an absolute path."""
    cfg = StorageConfig(uri="relative_dir", namespace="ns")
    ctx = StorageContextFactory().build(cfg)
    assert pathlib.Path(ctx.uri).is_absolute()
    assert pathlib.Path(ctx.uri).name == "relative_dir"


def test_storage_context_builds_local_and_creates_dirs(tmp_path):
    """Building a local storage context creates directories and initializes a session with the namespace set."""
    root = tmp_path / "store"
    cfg = StorageConfig(uri=str(root), namespace="ns")
    ctx = StorageContextFactory().build(cfg)
    # directories should exist
    assert pathlib.Path(str(root)).exists()
    # session has the namespace set
    assert ctx.namespace == "ns"
    # sanity: reading back set namespace via session should succeed
    assert ctx.session is not None
    assert not hasattr(ctx, "catalog")
    assert ctx.io_config is None
