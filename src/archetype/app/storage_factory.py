# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Storage factory helpers for app/runtime composition.

The core stores consume backend-specific handles. This module owns the
side-effectful conversion from user-facing StorageConfig into those handles.
"""

from __future__ import annotations

import pathlib
from urllib.parse import urlparse

from daft.catalog import Catalog
from daft.session import Session

from archetype.core.config import StorageConfig
from archetype.core.storage.handles import DaftCatalogStorage, LanceDbStorage


def _resolve_storage_uri(uri: str) -> tuple[str, bool]:
    """Resolve local storage paths while preserving remote object-store URIs."""
    scheme = urlparse(uri).scheme.lower()
    is_remote = scheme not in ("", "file")

    if is_remote:
        return uri, True

    base_path = pathlib.Path(uri)
    if not base_path.is_absolute():
        base_path = pathlib.Path.cwd() / base_path
    base_path.mkdir(parents=True, exist_ok=True)
    return str(base_path), False


class DaftIcebergSessionFactory:
    """Build the default Daft Session backed by a PyIceberg SQL catalog."""

    @staticmethod
    def build(config: StorageConfig) -> DaftCatalogStorage:
        """
        Build Daft catalog/session storage from a storage config.

        Uses Iceberg with a SQLite catalog for local storage, or remote object
        stores (S3, GCS, etc.) with local SQLite metadata.
        """
        from pyiceberg.catalog.sql import SqlCatalog

        resolved_uri, is_remote = _resolve_storage_uri(str(config.uri))

        if is_remote:
            local_meta_dir = pathlib.Path(".archetype_meta")
            local_meta_dir.mkdir(parents=True, exist_ok=True)
            sqlite_db_path = local_meta_dir / "catalog.db"
            warehouse_uri = str(config.uri)
        else:
            base_path = pathlib.Path(resolved_uri)
            sqlite_db_path = base_path / "catalog.db"
            warehouse_uri = f"file://{base_path}"

        catalog = getattr(config, "catalog", None) or Catalog.from_iceberg(
            SqlCatalog(
                "archetype_iceberg_sql_catalog",
                **{
                    "uri": f"sqlite:///{sqlite_db_path}",
                    "warehouse": warehouse_uri,
                },
            )
        )

        session = Session()
        session.attach_catalog(catalog)
        session.create_namespace_if_not_exists(config.namespace)
        session.set_namespace(config.namespace)

        return DaftCatalogStorage(
            uri=resolved_uri,
            namespace=config.namespace,
            session=session,
            catalog=catalog,
            io_config=config.io_config,
        )


class LanceDbStorageFactory:
    """Build LanceDB connection inputs from storage config without Daft setup."""

    @staticmethod
    def build(config: StorageConfig) -> LanceDbStorage:
        resolved_uri, _ = _resolve_storage_uri(str(config.uri))
        return LanceDbStorage(
            uri=resolved_uri,
            namespace=config.namespace,
            io_config=config.io_config,
        )
