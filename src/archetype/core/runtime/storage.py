# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Backward-compatible imports for legacy storage context paths."""

from __future__ import annotations

from dataclasses import dataclass

from daft.catalog import Catalog
from daft.io import IOConfig
from daft.session import Session

from archetype.app.storage_factory import DaftIcebergSessionFactory
from archetype.core.config import StorageConfig


@dataclass(frozen=True)
class StorageContext:
    """Legacy Daft/Iceberg storage context.

    New code should use DaftIcebergSessionFactory to create a native Daft
    Session for AsyncStore.
    """

    uri: str
    namespace: str
    session: Session
    catalog: Catalog
    io_config: IOConfig | None = None


class StorageContextFactory:
    """Compatibility alias for DaftIcebergSessionFactory.

    New code should import `DaftIcebergSessionFactory` from
    `archetype.app.storage_factory`.
    """

    @staticmethod
    def build(config: StorageConfig) -> StorageContext:
        uri, namespace, session, catalog, io_config = DaftIcebergSessionFactory.build_with_metadata(
            config
        )
        return StorageContext(
            uri=uri,
            namespace=namespace,
            session=session,
            catalog=catalog,
            io_config=io_config,
        )


__all__ = ["StorageContext", "StorageContextFactory"]
