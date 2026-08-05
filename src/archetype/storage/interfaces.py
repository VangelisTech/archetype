# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned storage execution protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.config import CacheConfig, StorageConfig
from archetype.storage.catalog import ControlCatalog

if TYPE_CHECKING:
    from archetype.storage.catalog import WorldRecord
    from archetype.storage.commit import CatalogCommitCoordinator
    from archetype.storage.service import PinnedVisibility, VisibleWorldRows


@runtime_checkable
class iStorageService(Protocol):
    """Execute Daft work and coordinate the durable storage planes."""

    @property
    def has_injected_session(self) -> bool: ...

    def require_iceberg_identity(self, storage_config: StorageConfig) -> None: ...

    def get_control_catalog(self, storage_config: StorageConfig) -> ControlCatalog: ...

    def bind_commit_coordinator(
        self,
        storage_config: StorageConfig,
        *,
        world_id: str,
        run_id: str,
        writer_epoch: int,
    ) -> CatalogCommitCoordinator: ...

    async def pin_visibility(
        self,
        storage_config: StorageConfig,
        world_id: str,
        *,
        run_id: str | None = None,
        max_tick: int | None = None,
    ) -> PinnedVisibility: ...

    async def scan_visible_world_rows(
        self,
        storage_config: StorageConfig,
        world_record: WorldRecord,
        visibility: PinnedVisibility,
    ) -> VisibleWorldRows: ...

    async def append_world_rows(
        self,
        storage_config: StorageConfig,
        world_id: str,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...] = (),
    ) -> int: ...

    async def read_world_rows(
        self,
        storage_config: StorageConfig,
        world_id: str,
        table_name: str,
    ) -> DataFrame: ...

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
