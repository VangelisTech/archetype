# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Path-safety contracts for user-influenced storage locations (issue #327).

Storage URIs and namespaces arrive from the API/CLI and flow into filesystem
paths. Namespaces must be single separator-free segments; local storage paths
must stay inside ``ARCHETYPE_DATA_ROOT`` when a deployment sets it.
"""

from pathlib import Path

import pytest

from archetype.app._catalog import catalog_path_for
from archetype.app.storage_service import _resolve_uri, create_async_store
from archetype.core.config import StorageBackend, StorageConfig
from archetype.core.paths import require_safe_namespace, resolve_local_root
from archetype.runtime.session import _resolve_storage_uri, configure_session

BAD_NAMESPACES = ["../up", "a/b", "a\\b", ".hidden", "..", "", "/abs", "a b", "a\x00b"]
GOOD_NAMESPACES = ["ecs", "ns_1", "a.b", "A-2", "audit"]


class TestRequireSafeNamespace:
    @pytest.mark.parametrize("namespace", BAD_NAMESPACES)
    def test_rejects_traversal_and_separators(self, namespace):
        with pytest.raises(ValueError, match="namespace"):
            require_safe_namespace(namespace)

    @pytest.mark.parametrize("namespace", GOOD_NAMESPACES)
    def test_accepts_identifiers(self, namespace):
        assert require_safe_namespace(namespace) == namespace


class TestResolveLocalRoot:
    def test_rejects_nul_byte(self):
        with pytest.raises(ValueError, match="NUL"):
            resolve_local_root("data\x00dir")

    def test_resolves_relative_to_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert resolve_local_root("store") == (tmp_path / "store").resolve()

    def test_data_root_containment(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(tmp_path))
        inside = resolve_local_root(str(tmp_path / "worlds" / "a"))
        assert inside.is_relative_to(tmp_path.resolve())

        outside = tmp_path.parent / "escape"
        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            resolve_local_root(str(outside))

    def test_data_root_blocks_dotdot_escape(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(tmp_path))
        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            resolve_local_root(str(tmp_path / "worlds" / ".." / ".." / "escape"))


class TestCatalogPathContainment:
    def test_catalog_path_stays_under_store_root(self, tmp_path):
        config = StorageConfig(
            uri=str(tmp_path / "store"), namespace="ns", backend=StorageBackend.ICEBERG
        )
        path = catalog_path_for(config)
        assert path.is_relative_to((tmp_path / "store").resolve())
        assert path.name == ".archetype-catalog-iceberg.db"

    @pytest.mark.parametrize("namespace", ["../evil", "a/b"])
    def test_catalog_path_rejects_traversal_namespace(self, tmp_path, namespace):
        config = StorageConfig(
            uri=str(tmp_path / "store"), namespace=namespace, backend=StorageBackend.ICEBERG
        )
        with pytest.raises(ValueError, match="namespace"):
            catalog_path_for(config)


class TestStoreConstructionGuards:
    def test_create_async_store_rejects_traversal_namespace(self, tmp_path):
        config = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="../../evil",
            backend=StorageBackend.LANCEDB,
        )
        with pytest.raises(ValueError, match="namespace"):
            create_async_store(config)

    def test_configure_session_rejects_traversal_namespace(self, tmp_path):
        config = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="../../evil",
            backend=StorageBackend.ICEBERG,
        )
        with pytest.raises(ValueError, match="namespace"):
            configure_session(config)

    def test_resolve_uri_respects_data_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(tmp_path))
        resolved = _resolve_uri(str(tmp_path / "store"))
        assert Path(resolved).is_relative_to(tmp_path.resolve())

        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            _resolve_uri(str(tmp_path.parent / "escape"))

    def test_session_resolve_respects_data_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(tmp_path))
        resolved, is_remote = _resolve_storage_uri(str(tmp_path / "store"))
        assert not is_remote
        assert Path(resolved).is_relative_to(tmp_path.resolve())

        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            _resolve_storage_uri(str(tmp_path.parent / "escape"))

    def test_remote_uris_pass_through_untouched(self):
        assert _resolve_uri("s3://bucket/prefix") == "s3://bucket/prefix"
        resolved, is_remote = _resolve_storage_uri("s3://bucket/prefix")
        assert is_remote and resolved == "s3://bucket/prefix"


class TestApiSurface:
    def test_create_world_with_traversal_namespace_is_client_error(self, tmp_path):
        """The HTTP body is the taint source CodeQL traced; prove it 4xxs."""
        pytest.importorskip("httpx")
        from fastapi.testclient import TestClient

        from archetype.api.app import create_app
        from archetype.api.deps import set_container
        from archetype.app.container import ServiceContainer

        set_container(ServiceContainer())
        try:
            with TestClient(create_app()) as client:
                resp = client.post(
                    "/worlds",
                    json={
                        "name": "evil",
                        "storage_uri": str(tmp_path / "store"),
                        "namespace": "../../evil",
                    },
                )
                assert 400 <= resp.status_code < 500, resp.text
        finally:
            set_container(None)
