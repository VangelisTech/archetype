# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the storage family."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from daft import DataFrame

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

    async def materialize(self, frame: DataFrame) -> DataFrame: ...

    async def read_table(
        self,
        storage_config: StorageConfig,
        table_name: str,
    ) -> DataFrame: ...

    async def append_table(
        self,
        storage_config: StorageConfig,
        table_name: str,
        rows: DataFrame,
    ) -> int: ...

    async def append_missing(
        self,
        storage_config: StorageConfig,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...],
    ) -> int: ...

    async def shutdown(self) -> None: ...
