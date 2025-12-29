# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Storage Context: Runtime Storage Initialization
===============================================

Handles initialization of Iceberg storage backends.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from daft.catalog import Catalog
from daft.io import IOConfig
from daft.session import Session

if TYPE_CHECKING:
    from archetype.core.config import StorageConfig


@dataclass(frozen=True)
class StorageContext:
    """Runtime storage context created from a declarative StorageConfig.

    Carries initialized, owned runtime resources. Construction may perform I/O.
    """

    uri: str
    namespace: str
    session: Session
    catalog: Catalog
    io_config: IOConfig


class StorageContextFactory:
    """Centralizes side-effectful initialization of storage runtime resources."""

    @staticmethod
    def build(config: StorageConfig) -> StorageContext:
        """
        Builds a storage context from a storage config.

        Uses Iceberg with SQLite catalog for local storage,
        or remote object stores (S3, GCS) with local metadata.
        """
        from pyiceberg.catalog.sql import SqlCatalog

        # Determine if the URI points to a remote object store
        scheme = urlparse(config.uri).scheme.lower()
        is_remote = scheme not in ("", "file")

        # Resolve local paths and ensure directories exist only for local URIs
        if not is_remote:
            base_path = pathlib.Path(config.uri)
            if not base_path.is_absolute():
                base_path = pathlib.Path.cwd() / base_path
            base_path.mkdir(parents=True, exist_ok=True)
            sqlite_db_path = base_path / "catalog.db"
            warehouse_uri = f"file://{base_path}"
        else:
            # For remote warehouses (e.g., gs://, s3://), keep local SQLite catalog
            local_meta_dir = pathlib.Path(".archetype_meta")
            local_meta_dir.mkdir(parents=True, exist_ok=True)
            sqlite_db_path = local_meta_dir / "catalog.db"
            warehouse_uri = config.uri

        # Build Iceberg catalog
        catalog = getattr(config, "catalog", None) or Catalog.from_iceberg(
            SqlCatalog(
                "archetype_iceberg_sql_catalog",
                **{
                    "uri": f"sqlite:///{sqlite_db_path}",
                    "warehouse": warehouse_uri,
                },
            )
        )

        # Session: attach and set namespace
        session = Session()
        session.attach_catalog(catalog)
        session.create_namespace_if_not_exists(config.namespace)
        session.set_namespace(config.namespace)

        return StorageContext(
            uri=str(base_path) if not is_remote else config.uri,
            namespace=config.namespace,
            session=session,
            catalog=catalog,
            io_config=config.io_config,
        )
