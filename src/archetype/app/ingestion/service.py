# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World-scoped application authority for append-only typed ingestion."""

from __future__ import annotations

from daft import DataFrame, lit

from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.core.config import StorageBackend, StorageConfig

_ENVELOPE_COLUMNS = ("world_id", "run_id")


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
        if not isinstance(rows, DataFrame):
            raise TypeError("rows must be a daft.DataFrame")
        wid, rid, storage = await self._world_context(world_id, storage_config)
        payload = self._add_envelope(rows, wid, rid, table_name, key_columns)
        if key_columns:
            return await self._storage_service.append_missing(
                storage,
                table_name,
                payload,
                key_columns=(*_ENVELOPE_COLUMNS, *key_columns),
            )
        return await self._storage_service.append_table(storage, table_name, payload)

    async def read(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        """Return a lazy read scoped to the world's current run."""
        wid, rid, storage = await self._world_context(world_id, storage_config)
        try:
            rows = await self._storage_service.read_table(storage, table_name)
        except KeyError as exc:
            raise KeyError(f"ingestion table {table_name!r} does not exist") from exc
        rows = rows.where(rows["world_id"] == wid)  # ty: ignore[invalid-argument-type]
        return rows.where(rows["run_id"] == rid)  # ty: ignore[invalid-argument-type]

    async def _world_context(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> tuple[str, str, StorageConfig]:
        wid = str(world_id)
        live = self._world_service.storage_record(wid)
        storage = storage_config or (live[0] if live is not None else StorageConfig())
        if storage.backend != StorageBackend.ICEBERG:
            raise ValueError("catalog-backed ingestion requires StorageBackend.ICEBERG")

        control = self._storage_service.get_control_catalog(storage)
        record = await control.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; ingestion needs a run key")
        return wid, str(record.run_id), storage

    @staticmethod
    def _add_envelope(
        rows: DataFrame,
        world_id: str,
        run_id: str,
        table_name: str,
        key_columns: tuple[str, ...],
    ) -> DataFrame:
        names = rows.schema().column_names()
        owned = [name for name in _ENVELOPE_COLUMNS if name in names]
        if owned:
            raise ValueError("world_id and run_id are assigned by IngestionService, not callers")
        missing = [name for name in key_columns if name not in names]
        if missing:
            raise ValueError(
                f"ingestion table {table_name!r} is missing key column(s): " + ", ".join(missing)
            )
        return rows.with_columns({"world_id": lit(world_id), "run_id": lit(run_id)}).select(
            *_ENVELOPE_COLUMNS,
            *names,
        )
