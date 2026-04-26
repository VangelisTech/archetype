import pathlib

from daft.session import Session

from archetype.core.config import StorageConfig
from archetype.runtime.session import configure_session


def test_configure_session_resolves_relative_path_to_absolute(tmp_path):
    """Relative local URIs are resolved to an absolute path."""
    cfg = StorageConfig(uri="relative_dir", namespace="ns")
    session = configure_session(cfg)
    assert isinstance(session, Session)
    resolved = pathlib.Path("relative_dir").resolve()
    assert resolved.is_absolute()


def test_configure_session_builds_local_and_creates_dirs(tmp_path):
    """Building a local session creates directories and sets the namespace."""
    root = tmp_path / "store"
    cfg = StorageConfig(uri=str(root), namespace="ns")
    session = configure_session(cfg)
    # directories should exist
    assert pathlib.Path(str(root)).exists()
    # session is returned and is a Session instance
    assert isinstance(session, Session)
    # io_config stays on the config, not the session
    assert cfg.io_config is None
