import os

from archetype.app.storage.session import configure_session
from archetype.core.config import StorageConfig


def test_storage_config_is_pure(tmp_path):
    uri = tmp_path / "pure"
    cfg = StorageConfig(uri=str(uri), namespace="ns")

    # No side-effects expected before build
    assert not os.path.exists(uri)

    # Configuring the session performs I/O
    _session = configure_session(cfg)
    assert os.path.exists(uri)
    assert cfg.namespace == "ns"
