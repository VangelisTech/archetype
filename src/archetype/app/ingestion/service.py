# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World-scoped application authority for append-only typed ingestion."""

from __future__ import annotations

from daft import DataFrame

from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.core.config import StorageConfig


class IngestionService:
    """Add application identity and select a storage-owned append operation.

    Producers supply typed rows and a stable table name. This service resolves
    the durable world/run envelope, then delegates registration, execution,
    schema comparison, ordering, and retry to ``StorageService``. It knows
    nothing about files or media.
    """

    def __init__(self, storage_service: iStorageService, world_service: iWorldService) -> None:
        self._storage_service = storage_service
        self._world_service = world_service

    async def append(
        self,
        world_id: str,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...] = (),
        storage_config: StorageConfig | None = None,
    ) -> int:
        """Append typed rows and return the number durably written."""
        storage = self._resolve_storage(str(world_id), storage_config)
        return await self._storage_service.append_world_rows(
            storage,
            str(world_id),
            table_name,
            rows,
            key_columns=key_columns,
        )

    async def read(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        """Return a lazy read scoped to the world's current run."""
        storage = self._resolve_storage(str(world_id), storage_config)
        return await self._storage_service.read_world_rows(
            storage,
            str(world_id),
            table_name,
        )

    def _resolve_storage(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> StorageConfig:
        live = self._world_service.storage_record(world_id)
        return storage_config or (live[0] if live is not None else StorageConfig())
