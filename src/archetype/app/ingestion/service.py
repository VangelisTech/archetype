# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Catalog-backed application authority for append-only typed ingestion."""

from __future__ import annotations

import asyncio

import pyarrow as pa
from daft import DataFrame, DataType, col, lit
from daft.catalog import Table

from archetype.app.storage.iceberg import IcebergCatalogContext
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.core.config import StorageBackend, StorageConfig
from archetype.ingestion.contracts import IngestionTable, TableVersion

_ENVELOPE_COLUMNS = ("world_id", "run_id")


class IngestionService:
    """Own Daft Catalog registration and schema-checked Iceberg appends.

    Producers supply typed rows and a logical table contract. This service adds
    world/run identity, registers the table in the active Daft catalog, and
    publishes the append. It deliberately knows nothing about files or media.
    """

    def __init__(self, storage_service: iStorageService, world_service: iWorldService) -> None:
        self._storage_service = storage_service
        self._world_service = world_service
        self._locks: dict[str, asyncio.Lock] = {}

    async def append(
        self,
        world_id: str,
        table: IngestionTable,
        rows: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> TableVersion:
        if not isinstance(rows, DataFrame):
            raise TypeError("rows must be a daft.DataFrame")
        wid, rid, storage, iceberg = await self._world_context(world_id, storage_config)
        payload = self._add_envelope(rows, wid, rid, table)

        async with self._lock(storage, table.name):
            registered = self._register(iceberg, table, payload)
            payload = self._align_schema(iceberg, registered, payload, table.name)
            existing = iceberg.read(registered).select(*self._identity_columns(table))
            existing = existing.where(existing["world_id"] == wid)  # ty: ignore[invalid-argument-type]
            existing = existing.where(existing["run_id"] == rid)  # ty: ignore[invalid-argument-type]
            pending = payload.join(
                existing,
                on=list(self._identity_columns(table)),
                how="anti",
            )
            # This is the execution boundary: materialize exactly once so an
            # empty idempotent retry does not create an Iceberg snapshot and a
            # non-empty UDF pipeline cannot be evaluated once for counting and
            # again for writing.
            pending = pending.collect(num_preview_rows=0)
            rows_written = pending.count_rows()
            if rows_written:
                await iceberg.append(registered, pending)
            return TableVersion(
                table_name=table.name,
                rows_written=rows_written,
                snapshot_id=iceberg.current_snapshot_id(registered),
            )

    async def read(
        self,
        world_id: str,
        table: IngestionTable,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        wid, rid, _storage, iceberg = await self._world_context(world_id, storage_config)
        identifier = iceberg.qualify(table.name)
        if not iceberg.catalog.has_table(identifier):
            raise KeyError(f"ingestion table {table.name!r} does not exist")
        rows = iceberg.read(iceberg.catalog.get_table(identifier))
        rows = rows.where(rows["world_id"] == wid)  # ty: ignore[invalid-argument-type]
        return rows.where(rows["run_id"] == rid)  # ty: ignore[invalid-argument-type]

    async def _world_context(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> tuple[str, str, StorageConfig, IcebergCatalogContext]:
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
        iceberg = await self._storage_service.get_iceberg_context(storage)
        return wid, str(record.run_id), storage, iceberg

    def _lock(self, storage: StorageConfig, table_name: str) -> asyncio.Lock:
        key = f"{storage.uri}::{storage.namespace}::{table_name}"
        return self._locks.setdefault(key, asyncio.Lock())

    @staticmethod
    def _identity_columns(table: IngestionTable) -> tuple[str, ...]:
        return (*_ENVELOPE_COLUMNS, *table.key_columns)

    @staticmethod
    def _add_envelope(
        rows: DataFrame,
        world_id: str,
        run_id: str,
        table: IngestionTable,
    ) -> DataFrame:
        names = rows.schema().column_names()
        owned = [name for name in _ENVELOPE_COLUMNS if name in names]
        if owned:
            raise ValueError("world_id and run_id are assigned by IngestionService, not callers")
        missing = [name for name in table.key_columns if name not in names]
        if missing:
            raise ValueError(
                f"ingestion table {table.name!r} is missing key column(s): " + ", ".join(missing)
            )
        return rows.with_columns({"world_id": lit(world_id), "run_id": lit(run_id)}).select(
            *_ENVELOPE_COLUMNS,
            *names,
        )

    @staticmethod
    def _register(
        iceberg: IcebergCatalogContext,
        table: IngestionTable,
        rows: DataFrame,
    ) -> Table:
        """Register through Daft Catalog; the session is only the catalog owner."""

        return iceberg.catalog.create_table_if_not_exists(
            iceberg.qualify(table.name),
            rows.schema(),
        )

    @staticmethod
    def _align_schema(
        iceberg: IcebergCatalogContext,
        registered: Table,
        rows: DataFrame,
        table_name: str,
    ) -> DataFrame:
        existing = iceberg.read(registered).schema().to_pyarrow_schema()
        incoming = rows.schema().to_pyarrow_schema()
        existing_shape = {field.name: field.type for field in existing}
        incoming_shape = {field.name: field.type for field in incoming}
        compatible = existing_shape.keys() == incoming_shape.keys() and all(
            IngestionService._iceberg_compatible(incoming_shape[name], existing_type)
            for name, existing_type in existing_shape.items()
        )
        if not compatible:
            raise ValueError(
                f"ingestion table {table_name!r} already has a different typed schema: "
                f"existing={existing_shape!r}, incoming={incoming_shape!r}"
            )
        return rows.select(
            *(
                col(field.name).cast(DataType.from_arrow_type(field.type)).alias(field.name)
                for field in existing
            )
        )

    @staticmethod
    def _iceberg_compatible(incoming: pa.DataType, existing: pa.DataType) -> bool:
        if incoming == existing:
            return True
        if pa.types.is_timestamp(incoming) and pa.types.is_timestamp(existing):
            return incoming.tz == existing.tz
        if pa.types.is_unsigned_integer(incoming) and pa.types.is_signed_integer(existing):
            return incoming.bit_width <= existing.bit_width
        return False
