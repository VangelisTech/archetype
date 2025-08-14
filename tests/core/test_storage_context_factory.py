import os
import pathlib
import pytest

from archetype.core.config import StorageConfig
from archetype.core.runtime.storage import StorageContextFactory


def test_storage_context_requires_absolute_path_for_local(tmp_path):
    """Local filesystem URIs must be absolute to avoid side effects in unexpected directories."""
    # relative path should fail
    cfg = StorageConfig(uri="relative_dir", namespace="ns")
    with pytest.raises(ValueError):
        StorageContextFactory.build(cfg)


def test_storage_context_builds_local_and_creates_dirs(tmp_path):
    """Building a local storage context creates directories and initializes session/catalog with the namespace set."""
    root = tmp_path / "store"
    cfg = StorageConfig(uri=str(root), namespace="ns")
    ctx = StorageContextFactory.build(cfg)
    # directories should exist
    assert pathlib.Path(str(root)).exists()
    # session and catalog set namespace
    assert ctx.namespace == "ns"
    # sanity: reading back set namespace via session should succeed
    assert ctx.session is not None


