# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Daft session configuration for Archetype storage backends."""

from __future__ import annotations

import pathlib
from urllib.parse import urlparse

from daft.catalog import Catalog
from daft.session import Session

from archetype.core.config import StorageConfig


def _resolve_storage_uri(uri: str) -> tuple[str, bool]:
    """Resolve local storage paths while preserving remote object-store URIs.

    Returns (resolved_uri, is_remote).
    """
    scheme = urlparse(uri).scheme.lower()
    is_remote = scheme not in ("", "file")

    if is_remote:
        return uri, True

    base_path = pathlib.Path(uri)
    if not base_path.is_absolute():
        base_path = pathlib.Path.cwd() / base_path
    base_path.mkdir(parents=True, exist_ok=True)
    return str(base_path), False


def configure_session(
    config: StorageConfig,
    session: Session | None = None,
) -> Session:
    """Configure a Daft session for Archetype's Iceberg storage backend.

    Uses the global default session if none is provided.
    Resolves the URI, creates the Iceberg catalog, attaches it, and sets the namespace.

    Args:
        config: Storage configuration with uri and namespace.
        session: Optional explicit session. Defaults to the global Daft session.

    Returns:
        The configured session.
    """
    from pyiceberg.catalog.sql import SqlCatalog

    if session is None:
        session = Session()

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

    catalog = Catalog.from_iceberg(
        SqlCatalog(
            "archetype_iceberg_sql_catalog",
            **{
                "uri": f"sqlite:///{sqlite_db_path}",
                "warehouse": warehouse_uri,
            },
        )
    )

    session.attach_catalog(catalog)
    session.create_namespace_if_not_exists(config.namespace)
    session.set_namespace(config.namespace)

    return session
