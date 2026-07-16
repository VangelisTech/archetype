# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed, Iceberg-native external fact ingestion."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import daft
from daft import DataFrame, DataType, Window, col, lit
from daft.catalog import Table
from daft.functions import file_path
from uuid_utils import uuid7

from archetype.app.facts import (
    FACT_ENVELOPE_COLUMNS,
    FACT_KEY_COLUMNS,
    FactProcessor,
    FactWriteReceipt,
    fact_table_id,
)
from archetype.app.iceberg import IcebergCatalogContext
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.config import StorageBackend, StorageConfig

_EXPECTED_COLUMN = "_archetype_fact_expected"
_PROCESSED_COLUMN = "_archetype_fact_processed"
_RESERVED_PREFIX = "_archetype_fact_"


def _require_identity_columns(names: list[str]) -> None:
    missing = [name for name in ("source_uri", "content_hash") if name not in names]
    if missing:
        raise ValueError(
            "fact processor output is missing required column(s): " + ", ".join(missing)
        )


def _reject_service_columns(names: list[str]) -> None:
    forbidden = [name for name in ("fact_id", "world_id", "run_id") if name in names]
    if forbidden:
        raise ValueError(
            "fact envelope columns are assigned by FactService, not callers: "
            + ", ".join(forbidden)
        )
    reserved = [name for name in names if name.startswith(_RESERVED_PREFIX)]
    if reserved:
        raise ValueError("fact columns use a reserved service prefix: " + ", ".join(reserved))


def _reject_persisted_files(schema: daft.Schema, names: list[str]) -> None:
    file_columns = [name for name in names if schema[name].dtype == DataType.file()]
    unexpected = [name for name in file_columns if name != "file"]
    if unexpected:
        raise ValueError(
            "daft.File columns are processor inputs and cannot be persisted: "
            + ", ".join(unexpected)
        )


@daft.func(return_dtype=DataType.string())
def _file_content_hash(file: daft.File) -> str:
    digest = hashlib.sha256()
    with file.open() as stream:
        while chunk := stream.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


@daft.func(return_dtype=DataType.string())
def _validated_content_hash(source_uri: str, content_hash: str) -> str:
    if not source_uri or ":" not in source_uri:
        raise ValueError("source_uri must be a non-empty canonical URI")
    if len(content_hash) != 64 or content_hash.lower() != content_hash:
        raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
    try:
        bytes.fromhex(content_hash)
    except ValueError as exc:
        raise ValueError("content_hash must be a lowercase SHA-256 hex digest") from exc
    return content_hash


@daft.func(return_dtype=DataType.string())
def _new_fact_id(
    _content_hash: str,
    key_count: int,
    expected: bool,
    processed: bool,
) -> str:
    if not expected or not processed:
        raise ValueError(
            "a fact processor must preserve source_uri and content_hash and emit "
            "exactly one row per input"
        )
    if key_count != 1:
        raise ValueError("a fact pipeline must emit exactly one row per logical fact key")
    return str(uuid7())


class FactService:
    """Write and read typed fact tables in a world's Iceberg catalog."""

    def __init__(self, storage_service: StorageService, world_service: WorldService) -> None:
        self._storage_service = storage_service
        self._world_service = world_service
        self._locks: dict[str, asyncio.Lock] = {}

    async def ingest_files(
        self,
        world_id: str,
        paths: str | Path | list[str | Path],
        processor: FactProcessor,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt:
        table_name = getattr(processor, "table_name", None)
        if not isinstance(table_name, str):
            raise TypeError("a FactProcessor must declare a string table_name")
        table_id = fact_table_id(table_name)
        wid, rid, storage, iceberg = await self._world_context(world_id, storage_config)
        path_input: str | list[str]
        if isinstance(paths, (str, Path)):
            path_input = str(paths)
        else:
            path_input = [str(path) for path in paths]

        async with self._lock(storage, table_id):
            table = self._table_if_exists(iceberg, table_id)
            sources = daft.from_files(path_input, io_config=iceberg.io_config)
            sources = sources.with_column("source_uri", file_path(col("file")))
            sources = sources.with_column("content_hash", _file_content_hash(col("file")))
            sources = sources.select("file", "source_uri", "content_hash")
            sources = sources.distinct("source_uri", "content_hash")
            if table is not None:
                sources = self._anti_join_existing(
                    sources.with_columns({"world_id": lit(wid), "run_id": lit(rid)}),
                    table,
                    iceberg,
                    wid,
                    rid,
                ).select("file", "source_uri", "content_hash")
                if sources.select("source_uri").count_rows() == 0:
                    return self._receipt(wid, rid, table_name, table_id, table, iceberg, 0)

            facts = processor.process(sources)
            if not isinstance(facts, DataFrame):
                raise TypeError("FactProcessor.process() must return a daft.DataFrame")
            payload = self._normalize_file_payload(facts, sources, wid, rid)
            return await self._commit(
                payload,
                table=table,
                table_name=table_name,
                table_id=table_id,
                world_id=wid,
                run_id=rid,
                iceberg=iceberg,
            )

    async def write_facts(
        self,
        world_id: str,
        table_name: str,
        facts: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt:
        if not isinstance(facts, DataFrame):
            raise TypeError("facts must be a daft.DataFrame")
        table_id = fact_table_id(table_name)
        wid, rid, storage, iceberg = await self._world_context(world_id, storage_config)
        payload = self._normalize_payload(facts, wid, rid)

        async with self._lock(storage, table_id):
            table = self._table_if_exists(iceberg, table_id)
            if table is not None:
                payload = self._anti_join_existing(payload, table, iceberg, wid, rid)
            return await self._commit(
                payload,
                table=table,
                table_name=table_name,
                table_id=table_id,
                world_id=wid,
                run_id=rid,
                iceberg=iceberg,
            )

    async def read_facts(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        table_id = fact_table_id(table_name)
        wid, rid, _storage, iceberg = await self._world_context(world_id, storage_config)
        if not iceberg.has_table(table_id):
            raise KeyError(f"fact table {table_name!r} does not exist")
        facts = iceberg.read(iceberg.get_table(table_id))
        facts = facts.where(facts["world_id"] == wid)  # ty: ignore[invalid-argument-type]
        return facts.where(facts["run_id"] == rid)  # ty: ignore[invalid-argument-type]

    async def _world_context(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> tuple[str, str, StorageConfig, IcebergCatalogContext]:
        wid = str(world_id)
        live = self._world_service.storage_record(wid)
        storage = storage_config or (live[0] if live is not None else StorageConfig())
        if storage.backend != StorageBackend.ICEBERG:
            raise ValueError("typed fact tables require StorageBackend.ICEBERG")

        catalog = self._storage_service.get_control_catalog(storage)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; facts need a run key")
        iceberg = await self._storage_service.get_iceberg_context(storage)
        return wid, str(record.run_id), storage, iceberg

    def _lock(self, storage: StorageConfig, table_id: str) -> asyncio.Lock:
        key = f"{storage.uri}::{storage.namespace}::{table_id}"
        return self._locks.setdefault(key, asyncio.Lock())

    @staticmethod
    def _table_if_exists(iceberg: IcebergCatalogContext, table_id: str) -> Table | None:
        return iceberg.get_table(table_id) if iceberg.has_table(table_id) else None

    @staticmethod
    def _anti_join_existing(
        payload: DataFrame,
        table: Table,
        iceberg: IcebergCatalogContext,
        world_id: str,
        run_id: str,
    ) -> DataFrame:
        existing = iceberg.read(table).select(*FACT_KEY_COLUMNS)
        existing = existing.where(
            existing["world_id"] == world_id  # ty: ignore[invalid-argument-type]
        )
        existing = existing.where(
            existing["run_id"] == run_id  # ty: ignore[invalid-argument-type]
        )
        return payload.join(existing, on=list(FACT_KEY_COLUMNS), how="anti")

    @staticmethod
    def _normalize_payload(facts: DataFrame, world_id: str, run_id: str) -> DataFrame:
        user_columns = FactService._payload_columns(facts)
        payload = facts.select("source_uri", "content_hash", *user_columns)
        payload = FactService._add_envelope(payload, world_id, run_id)
        return payload.with_columns(
            {
                _EXPECTED_COLUMN: lit(True),
                _PROCESSED_COLUMN: lit(True),
            }
        ).select(
            *FACT_KEY_COLUMNS,
            *user_columns,
            _EXPECTED_COLUMN,
            _PROCESSED_COLUMN,
        )

    @staticmethod
    def _normalize_file_payload(
        facts: DataFrame,
        sources: DataFrame,
        world_id: str,
        run_id: str,
    ) -> DataFrame:
        user_columns = FactService._payload_columns(facts)
        processed = facts.select("source_uri", "content_hash", *user_columns).with_column(
            _PROCESSED_COLUMN,
            lit(True),
        )
        expected = sources.select("source_uri", "content_hash").with_column(
            _EXPECTED_COLUMN,
            lit(True),
        )
        payload = expected.join(
            processed,
            on=["source_uri", "content_hash"],
            how="outer",
        )
        payload = payload.with_columns(
            {
                _EXPECTED_COLUMN: col(_EXPECTED_COLUMN).fill_null(False),
                _PROCESSED_COLUMN: col(_PROCESSED_COLUMN).fill_null(False),
            }
        )
        payload = FactService._add_envelope(payload, world_id, run_id)
        return payload.select(
            *FACT_KEY_COLUMNS,
            *user_columns,
            _EXPECTED_COLUMN,
            _PROCESSED_COLUMN,
        )

    @staticmethod
    def _payload_columns(facts: DataFrame) -> list[str]:
        schema = facts.schema()
        names = schema.column_names()
        _require_identity_columns(names)
        _reject_service_columns(names)
        _reject_persisted_files(schema, names)
        user_columns = [
            name for name in names if name not in {"file", "source_uri", "content_hash"}
        ]
        if not user_columns:
            raise ValueError("a fact processor must emit at least one typed fact column")
        return user_columns

    @staticmethod
    def _add_envelope(payload: DataFrame, world_id: str, run_id: str) -> DataFrame:
        payload = payload.with_column(
            "content_hash",
            _validated_content_hash(col("source_uri"), col("content_hash")),
        )
        payload = payload.with_column("world_id", lit(world_id))
        payload = payload.with_column("run_id", lit(run_id))
        return payload

    async def _commit(
        self,
        payload: DataFrame,
        *,
        table: Table | None,
        table_name: str,
        table_id: str,
        world_id: str,
        run_id: str,
        iceberg: IcebergCatalogContext,
    ) -> FactWriteReceipt:
        key_count = col("source_uri").count().over(Window().partition_by(*FACT_KEY_COLUMNS))
        facts = payload.with_column(
            "fact_id",
            _new_fact_id(
                col("content_hash"),
                key_count,
                col(_EXPECTED_COLUMN),
                col(_PROCESSED_COLUMN),
            ),
        )
        user_columns = [
            name
            for name in payload.schema().column_names()
            if name not in {*FACT_KEY_COLUMNS, _EXPECTED_COLUMN, _PROCESSED_COLUMN}
        ]
        facts = facts.select(*FACT_ENVELOPE_COLUMNS, *user_columns)
        if table is None:
            table = iceberg.create_table_if_not_exists(table_id, facts.schema())
        self._require_compatible_schema(table, facts, iceberg, table_name)
        rows_written = await iceberg.append_counted(table, facts)
        return self._receipt(
            world_id,
            run_id,
            table_name,
            table_id,
            table,
            iceberg,
            rows_written,
        )

    @staticmethod
    def _require_compatible_schema(
        table: Table,
        facts: DataFrame,
        iceberg: IcebergCatalogContext,
        table_name: str,
    ) -> None:
        existing = iceberg.read(table).schema().to_pyarrow_schema()
        incoming = facts.schema().to_pyarrow_schema()
        existing_shape = [(field.name, field.type) for field in existing]
        incoming_shape = [(field.name, field.type) for field in incoming]
        if existing_shape != incoming_shape:
            raise ValueError(
                f"fact table {table_name!r} already has a different typed schema: "
                f"existing={existing_shape!r}, incoming={incoming_shape!r}"
            )

    @staticmethod
    def _receipt(
        world_id: str,
        run_id: str,
        table_name: str,
        table_id: str,
        table: Table | None,
        iceberg: IcebergCatalogContext,
        rows_written: int,
    ) -> FactWriteReceipt:
        snapshot_id = iceberg.current_snapshot_id(table) if table is not None else None
        return FactWriteReceipt(
            world_id=world_id,
            run_id=run_id,
            table_name=table_name,
            table_id=table_id,
            rows_written=rows_written,
            snapshot_id=snapshot_id,
        )
