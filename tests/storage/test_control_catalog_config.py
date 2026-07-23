# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bootstrap contracts for the control-catalog configuration snapshot."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.catalog import catalog_path_for
from archetype.storage.config import ControlCatalogConfig


def test_from_env_captures_one_normalized_immutable_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    catalog_dir = tmp_path / "catalogs"
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", " https://catalog.example.test/ ")
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", " secret-token ")
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(catalog_dir))

    config = ControlCatalogConfig.from_env()

    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", "https://changed.invalid")
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", "changed")
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "changed"))

    assert config.remote_url == "https://catalog.example.test"
    assert config.remote_token == "secret-token"
    assert config.catalog_dir == catalog_dir
    assert "secret-token" not in repr(config)
    assert config == ControlCatalogConfig(
        remote_url="https://catalog.example.test",
        remote_token="different-token",
        catalog_dir=catalog_dir,
    )
    with pytest.raises(FrozenInstanceError):
        config.remote_url = "https://mutation.invalid"  # type: ignore[misc]


def test_from_env_requires_token_when_remote_url_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", "https://catalog.example.test")
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", raising=False)

    with pytest.raises(
        RuntimeError,
        match=(
            "ARCHETYPE_CONTROL_CATALOG_TOKEN is required when "
            "ARCHETYPE_CONTROL_CATALOG_URL is configured"
        ),
    ):
        ControlCatalogConfig.from_env()


def test_from_env_preserves_token_without_remote_url() -> None:
    config = ControlCatalogConfig.from_env(
        {
            "ARCHETYPE_CONTROL_CATALOG_TOKEN": "unused-token",
        }
    )

    assert config.remote_url is None
    assert config.remote_token == "unused-token"


def test_catalog_path_uses_explicit_snapshot_not_later_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    initial_dir = tmp_path / "initial"
    config = ControlCatalogConfig(catalog_dir=initial_dir)
    storage = StorageConfig(
        uri="s3://bucket/prefix",
        namespace="tenant",
        backend=StorageBackend.ICEBERG,
    )

    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "ambient"))

    path = catalog_path_for(storage, config)

    assert path.parent == initial_dir
    assert path.name.endswith(".db")


def test_catalog_path_default_never_invokes_environment_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri="s3://bucket/prefix",
        namespace="tenant",
        backend=StorageBackend.ICEBERG,
    )

    def forbidden_bootstrap(*_args: object, **_kwargs: object) -> ControlCatalogConfig:
        raise AssertionError("ordinary catalog path resolution read composition config")

    monkeypatch.setattr(ControlCatalogConfig, "from_env", forbidden_bootstrap)

    assert catalog_path_for(storage).parent == Path("~/.archetype/catalogs").expanduser()


def test_default_config_does_not_consume_ambient_application_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", "https://ambient.invalid")
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", "ambient-token")
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", "/tmp/ambient-catalogs")

    config = ControlCatalogConfig()

    assert config.remote_url is None
    assert config.remote_token is None
    assert config.catalog_dir == Path("~/.archetype/catalogs").expanduser()
