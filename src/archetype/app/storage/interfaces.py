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
    """Own app-level Daft execution and both durable storage planes.

    SQLite or the remote Durable Object provides transactional control state;
    Iceberg provides atomic data snapshots and optimistic multi-writer commits.
    App services use this port for terminal materialization and Catalog-backed
    table access instead of coordinating Daft independently.
    """

    @property
    def has_injected_session(self) -> bool:
        """Whether one caller-owned Daft session fixes the storage identity."""
        ...

    def require_iceberg_identity(self, storage_config: StorageConfig) -> None:
        """Bind an injected session to exactly one Iceberg storage identity."""
        ...

    def get_control_catalog(self, storage_config: StorageConfig) -> ControlCatalog:
        """Return the pooled SQLite or remote Durable Object control authority."""
        ...

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> Any:
        """Return the pooled ECS store admitted by this execution authority."""
        ...

    async def materialize(self, frame: DataFrame) -> DataFrame:
        """Execute one app-owned lazy Daft plan through the shared lane."""
        ...

    async def read_table(
        self,
        storage_config: StorageConfig,
        table_name: str,
    ) -> DataFrame:
        """Return a lazy Catalog-backed read of one existing Iceberg table."""
        ...

    async def append_table(
        self,
        storage_config: StorageConfig,
        table_name: str,
        rows: DataFrame,
    ) -> int:
        """Append rows with schema enforcement and optimistic commit retry."""
        ...

    async def append_missing(
        self,
        storage_config: StorageConfig,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...],
    ) -> int:
        """Append absent keys, recomputing the anti-join after conflicts."""
        ...

    async def shutdown(self) -> None:
        """Close every pooled store and control catalog."""
        ...
