# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the storage family."""

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

    def bind_commit_coordinator(
        self,
        storage_config: StorageConfig,
        *,
        world_id: str,
        run_id: str,
        writer_epoch: int,
    ) -> CatalogCommitCoordinator:
        """Construct a coordinator bound to one durable writer identity."""
        ...

    async def pin_visibility(
        self,
        storage_config: StorageConfig,
        world_id: str,
        *,
        run_id: str | None = None,
        max_tick: int | None = None,
    ) -> PinnedVisibility:
        """Capture one immutable physical visibility allowlist."""
        ...

    async def scan_visible_world_rows(
        self,
        storage_config: StorageConfig,
        world_record: WorldRecord,
        visibility: PinnedVisibility,
    ) -> VisibleWorldRows:
        """Return raw physically visible frames without world interpretation."""
        ...

    async def append_world_rows(
        self,
        storage_config: StorageConfig,
        world_id: str,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...] = (),
    ) -> int:
        """Stamp the durable world/run envelope and append typed rows."""
        ...

    async def read_world_rows(
        self,
        storage_config: StorageConfig,
        world_id: str,
        table_name: str,
    ) -> DataFrame:
        """Return a lazy world/run-scoped app-table read."""
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
