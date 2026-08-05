# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for the storage-family session move."""

import pytest
from daft.session import Session

from archetype.core.config import StorageConfig
from archetype.storage.session import configure_session


def test_configure_session_preserves_supplied_session_and_builds_local_catalog(tmp_path) -> None:
    root = tmp_path / "store"
    supplied = Session()

    configured = configure_session(
        StorageConfig(uri=str(root), namespace="storage_session_move"),
        supplied,
    )

    assert configured is supplied
    assert (root / "catalog.db").is_file()


def test_configure_session_keeps_remote_session_injection_boundary() -> None:
    with pytest.raises(ValueError, match="inject a preconfigured Daft Session"):
        configure_session(StorageConfig(uri="s3://bucket/prefix", namespace="ns"))
