import os
import pytest
from archetype.core.config import StorageConfig
from archetype.core.runtime.storage import StorageContextFactory


def test_storage_config_is_pure(tmp_path):
    uri = tmp_path / "pure"
    cfg = StorageConfig(uri=str(uri), namespace="ns")

    # No side-effects expected before build
    assert not os.path.exists(uri)

    # Building the context performs I/O
    ctx = StorageContextFactory.build(cfg)
    assert os.path.exists(uri)
    assert ctx.namespace == "ns"


