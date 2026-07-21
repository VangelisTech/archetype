# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the storage family."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from archetype.app.storage.catalog import ControlCatalog
from archetype.core.config import CacheConfig, StorageConfig


@runtime_checkable
class iStorageService(Protocol):
    """Pool stores and expose the durable control/catalog boundaries."""

    @property
    def has_injected_session(self) -> bool: ...

    def require_iceberg_identity(self, storage_config: StorageConfig) -> None: ...

    def get_control_catalog(self, storage_config: StorageConfig) -> ControlCatalog: ...

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> Any: ...

    async def get_iceberg_context(self, storage_config: StorageConfig) -> Any: ...

    async def shutdown(self) -> None: ...
