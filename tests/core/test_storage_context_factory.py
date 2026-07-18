import pathlib

from daft.session import Session

from archetype.app.storage.session import configure_session
from archetype.core.config import StorageConfig


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


def test_configure_session_normalizes_file_uri(tmp_path):
    root = tmp_path / "file store"

    session = configure_session(StorageConfig(uri=root.as_uri(), namespace="ns"))

    assert isinstance(session, Session)
    assert (root / "catalog.db").is_file()


def test_configure_session_preserves_public_local_session_argument(tmp_path):
    """The exported helper still configures and returns a supplied local session."""
    supplied = Session()
    cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

    configured = configure_session(cfg, supplied)

    assert configured is supplied
