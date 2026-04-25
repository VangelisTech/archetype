# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Backward-compatible imports for legacy storage context paths."""

from __future__ import annotations

from archetype.app.storage_factory import DaftIcebergSessionFactory
from archetype.core.storage.handles import DaftCatalogStorage

StorageContext = DaftCatalogStorage


class StorageContextFactory(DaftIcebergSessionFactory):
    """Compatibility alias for DaftIcebergSessionFactory.

    New code should import `DaftIcebergSessionFactory` from
    `archetype.app.storage_factory`.
    """


__all__ = ["StorageContext", "StorageContextFactory"]
