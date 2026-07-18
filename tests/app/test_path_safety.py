# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Path-safety contracts for user-influenced storage locations (issue #327).

Storage URIs and namespaces arrive from the API/CLI and flow into filesystem
paths. Namespaces must be single separator-free segments; local storage paths
must stay inside ``ARCHETYPE_DATA_ROOT`` when a deployment sets it.
"""

from pathlib import Path

import pytest

from archetype.app.storage.catalog import catalog_path_for
from archetype.app.storage.service import _resolve_uri, create_async_store
from archetype.app.storage.session import configure_session
from archetype.core.config import StorageBackend, StorageConfig
from archetype.core.paths import require_safe_namespace, resolve_local_root

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

    def test_configure_session_respects_data_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(tmp_path))
        config = StorageConfig(
            uri=str(tmp_path.parent / "escape"),
            namespace="ns",
            backend=StorageBackend.ICEBERG,
        )
        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            configure_session(config)

        inside = StorageConfig(
            uri=str(tmp_path / "store"), namespace="ns", backend=StorageBackend.ICEBERG
        )
        assert configure_session(inside) is not None

    def test_remote_uris_pass_through_untouched(self):
        assert _resolve_uri("s3://bucket/prefix") == "s3://bucket/prefix"

    def test_injected_session_branch_rejects_traversal_namespace(self, tmp_path):
        """Footgun-review finding: the injected-session Iceberg branch skipped
        every namespace check, deferring the raise past world registration."""
        from unittest.mock import Mock

        config = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="../../evil",
            backend=StorageBackend.ICEBERG,
        )
        session = Mock()
        with pytest.raises(ValueError, match="namespace"):
            create_async_store(config, session=session)
        session.current_namespace.assert_not_called()

    def test_lance_namespace_symlink_escape_rejected(self, tmp_path, monkeypatch):
        """A pre-planted symlink at <uri>/<namespace> must not redirect writes
        outside ARCHETYPE_DATA_ROOT (Codex P2 on PR #379)."""
        root = tmp_path / "root"
        outside = tmp_path / "outside"
        store_dir = root / "store"
        store_dir.mkdir(parents=True)
        outside.mkdir()
        (store_dir / "ns").symlink_to(outside)
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(root))

        config = StorageConfig(uri=str(store_dir), namespace="ns", backend=StorageBackend.LANCEDB)
        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            create_async_store(config)

        # Without the symlink the same config constructs fine.
        (store_dir / "ns").unlink()
        assert create_async_store(config) is not None

    def test_iceberg_namespace_symlink_escape_rejected(self, tmp_path, monkeypatch):
        """The Iceberg warehouse namespace dir (<base>/<ns>.db) gets the same
        symlink-escape probe as LanceDB (footgun-review on PR #379)."""
        root = tmp_path / "root"
        outside = tmp_path / "outside"
        store_dir = root / "store"
        store_dir.mkdir(parents=True)
        outside.mkdir()
        (store_dir / "ns.db").symlink_to(outside)
        monkeypatch.setenv("ARCHETYPE_DATA_ROOT", str(root))

        config = StorageConfig(uri=str(store_dir), namespace="ns", backend=StorageBackend.ICEBERG)
        with pytest.raises(ValueError, match="escapes ARCHETYPE_DATA_ROOT"):
            configure_session(config)

        (store_dir / "ns.db").unlink()
        assert configure_session(config) is not None


class TestCreateWorldUnwind:
    @pytest.mark.asyncio
    async def test_catalog_failure_unwinds_registry(self, tmp_path, monkeypatch):
        """Footgun-review finding on PR #379: a raise from control-catalog
        acquisition (path/namespace resolution) after the orchestrator inserted
        the world must unwind the registry — no live orphan world."""
        from archetype.core.config import WorldConfig
        from tests.conftest import make_world_service

        ws = make_world_service()
        try:

            def boom(storage_config):
                raise ValueError("catalog path rejected")

            monkeypatch.setattr(ws._storage_service, "get_control_catalog", boom)
            config = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

            with pytest.raises(ValueError, match="catalog path rejected"):
                await ws.create_world(WorldConfig(name="orphan"), config)

            assert ws.list_worlds() == [], (
                "failed create left a live, mutable world in the registry"
            )
        finally:
            await ws.shutdown()

    @pytest.mark.asyncio
    async def test_catalog_failure_unwinds_fork_registry(self, tmp_path, monkeypatch):
        """fork_world shares create_world's unwind contract: a raise from
        catalog acquisition must not leave the fork live in the registry."""
        from archetype.core.config import WorldConfig
        from tests.conftest import make_world_service

        ws = make_world_service()
        try:
            config = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            source = await ws.create_world(WorldConfig(name="source"), config)

            def boom(storage_config):
                raise ValueError("catalog path rejected")

            monkeypatch.setattr(ws._storage_service, "get_control_catalog", boom)

            with pytest.raises(ValueError, match="catalog path rejected"):
                await ws.fork_world(source.world_id)

            live = {str(w.world_id) for w in ws.list_worlds()}
            assert live == {str(source.world_id)}, (
                "failed fork left a live, mutable world in the registry"
            )
        finally:
            await ws.shutdown()


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
