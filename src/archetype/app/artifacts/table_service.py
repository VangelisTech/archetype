# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed, Iceberg-native external artifact ingestion."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import daft
from daft import DataFrame, DataType, Window, col, lit
from daft.catalog import Table
from daft.functions import file_path
from uuid_utils import uuid7

from archetype.app.storage.iceberg import IcebergCatalogContext
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.artifacts.contracts import (
    ARTIFACT_ENVELOPE_COLUMNS,
    ARTIFACT_KEY_COLUMNS,
    ArtifactProcessor,
    ArtifactWriteReceipt,
    artifact_table_id,
)
from archetype.core.config import StorageBackend, StorageConfig

_EXPECTED_COLUMN = "_archetype_artifact_expected"
_PROCESSED_COLUMN = "_archetype_artifact_processed"
_RESERVED_PREFIX = "_archetype_artifact_"


def _require_identity_columns(names: list[str]) -> None:
    missing = [name for name in ("source_uri", "content_hash") if name not in names]
    if missing:
        raise ValueError(
            "artifact processor output is missing required column(s): " + ", ".join(missing)
        )


def _reject_service_columns(names: list[str]) -> None:
    forbidden = [name for name in ("artifact_id", "world_id", "run_id") if name in names]
    if forbidden:
        raise ValueError(
            "artifact envelope columns are assigned by ArtifactTableService, not callers: "
            + ", ".join(forbidden)
        )
    reserved = [name for name in names if name.startswith(_RESERVED_PREFIX)]
    if reserved:
        raise ValueError("artifact columns use a reserved service prefix: " + ", ".join(reserved))


def _execution_file_columns(
    schema: daft.Schema,
    names: list[str],
    *,
    processor_output: bool,
) -> set[str]:
    file_columns = [name for name in names if schema[name].dtype == DataType.file()]
    execution_columns = {"file"} if processor_output else set()
    unexpected = [name for name in file_columns if name not in execution_columns]
    if unexpected:
        raise ValueError(
            "daft.File columns are processor inputs and cannot be persisted: "
            + ", ".join(unexpected)
        )
    return set(file_columns) & execution_columns


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
def _new_artifact_id(
    _content_hash: str,
    key_count: int,
    expected: bool,
    processed: bool,
) -> str:
    if not expected or not processed:
        raise ValueError(
            "an artifact processor must preserve source_uri and content_hash and emit "
            "exactly one row per input"
        )
    if key_count != 1:
        raise ValueError("an artifact pipeline must emit exactly one row per logical artifact key")
    return str(uuid7())


class ArtifactTableService:
    """Write and read typed artifact tables in a world's Iceberg catalog."""

    def __init__(self, storage_service: iStorageService, world_service: iWorldService) -> None:
        self._storage_service = storage_service
        self._world_service = world_service
        self._locks: dict[str, asyncio.Lock] = {}

    async def ingest_files(
        self,
        world_id: str,
        paths: str | Path | list[str | Path],
        processor: ArtifactProcessor,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactWriteReceipt:
        table_name = getattr(processor, "table_name", None)
        if not isinstance(table_name, str):
            raise TypeError("an ArtifactProcessor must declare a string table_name")
        table_id = artifact_table_id(table_name)
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
            sources = sources.collect(num_preview_rows=0)
            sources_matched = sources.count_rows()
            if not sources_matched:
                return self._receipt(
                    wid,
                    rid,
                    table_name,
                    table_id,
                    table,
                    iceberg,
                    sources_matched,
                    0,
                )

            sources = sources.distinct("source_uri", "content_hash")
            if table is not None:
                sources = self._anti_join_existing(
                    sources.with_columns({"world_id": lit(wid), "run_id": lit(rid)}),
                    table,
                    iceberg,
                    wid,
                    rid,
                ).select("file", "source_uri", "content_hash")
            sources = sources.collect(num_preview_rows=0)

            if sources.count_rows() == 0:
                return self._receipt(
                    wid,
                    rid,
                    table_name,
                    table_id,
                    table,
                    iceberg,
                    sources_matched,
                    0,
                )

            artifacts = processor.process(sources)
            if not isinstance(artifacts, DataFrame):
                raise TypeError("ArtifactProcessor.process() must return a daft.DataFrame")
            payload = self._normalize_file_payload(artifacts, sources, wid, rid)
            return await self._commit(
                payload,
                table=table,
                table_name=table_name,
                table_id=table_id,
                world_id=wid,
                run_id=rid,
                iceberg=iceberg,
                sources_matched=sources_matched,
            )

    async def write_artifacts(
        self,
        world_id: str,
        table_name: str,
        artifacts: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactWriteReceipt:
        if not isinstance(artifacts, DataFrame):
            raise TypeError("artifacts must be a daft.DataFrame")
        table_id = artifact_table_id(table_name)
        wid, rid, storage, iceberg = await self._world_context(world_id, storage_config)
        payload = self._normalize_payload(artifacts, wid, rid)

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
                sources_matched=None,
            )

    async def read_artifacts(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        table_id = artifact_table_id(table_name)
        wid, rid, _storage, iceberg = await self._world_context(world_id, storage_config)
        if not iceberg.has_table(table_id):
            raise KeyError(f"artifact table {table_name!r} does not exist")
        artifacts = iceberg.read(iceberg.get_table(table_id))
        artifacts = artifacts.where(artifacts["world_id"] == wid)  # ty: ignore[invalid-argument-type]
        return artifacts.where(artifacts["run_id"] == rid)  # ty: ignore[invalid-argument-type]

    async def _world_context(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> tuple[str, str, StorageConfig, IcebergCatalogContext]:
        wid = str(world_id)
        live = self._world_service.storage_record(wid)
        storage = storage_config or (live[0] if live is not None else StorageConfig())
        if storage.backend != StorageBackend.ICEBERG:
            raise ValueError("typed artifact tables require StorageBackend.ICEBERG")

        catalog = self._storage_service.get_control_catalog(storage)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; artifacts need a run key")
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
        existing = iceberg.read(table).select(*ARTIFACT_KEY_COLUMNS)
        existing = existing.where(
            existing["world_id"] == world_id  # ty: ignore[invalid-argument-type]
        )
        existing = existing.where(
            existing["run_id"] == run_id  # ty: ignore[invalid-argument-type]
        )
        return payload.join(existing, on=list(ARTIFACT_KEY_COLUMNS), how="anti")

    @staticmethod
    def _normalize_payload(artifacts: DataFrame, world_id: str, run_id: str) -> DataFrame:
        user_columns = ArtifactTableService._payload_columns(artifacts, processor_output=False)
        payload = artifacts.select("source_uri", "content_hash", *user_columns)
        payload = ArtifactTableService._add_envelope(payload, world_id, run_id)
        return payload.with_columns(
            {
                _EXPECTED_COLUMN: lit(True),
                _PROCESSED_COLUMN: lit(True),
            }
        ).select(
            *ARTIFACT_KEY_COLUMNS,
            *user_columns,
            _EXPECTED_COLUMN,
            _PROCESSED_COLUMN,
        )

    @staticmethod
    def _normalize_file_payload(
        artifacts: DataFrame,
        sources: DataFrame,
        world_id: str,
        run_id: str,
    ) -> DataFrame:
        user_columns = ArtifactTableService._payload_columns(artifacts, processor_output=True)
        processed = artifacts.select("source_uri", "content_hash", *user_columns).with_column(
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
        payload = ArtifactTableService._add_envelope(payload, world_id, run_id)
        return payload.select(
            *ARTIFACT_KEY_COLUMNS,
            *user_columns,
            _EXPECTED_COLUMN,
            _PROCESSED_COLUMN,
        )

    @staticmethod
    def _payload_columns(artifacts: DataFrame, *, processor_output: bool) -> list[str]:
        schema = artifacts.schema()
        names = schema.column_names()
        _require_identity_columns(names)
        _reject_service_columns(names)
        execution_files = _execution_file_columns(
            schema,
            names,
            processor_output=processor_output,
        )
        user_columns = [
            name for name in names if name not in {"source_uri", "content_hash", *execution_files}
        ]
        if not user_columns:
            raise ValueError("a artifact processor must emit at least one typed artifact column")
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
        sources_matched: int | None,
    ) -> ArtifactWriteReceipt:
        key_count = col("source_uri").count().over(Window().partition_by(*ARTIFACT_KEY_COLUMNS))
        artifacts = payload.with_column(
            "artifact_id",
            _new_artifact_id(
                col("content_hash"),
                key_count,
                col(_EXPECTED_COLUMN),
                col(_PROCESSED_COLUMN),
            ),
        )
        user_columns = [
            name
            for name in payload.schema().column_names()
            if name not in {*ARTIFACT_KEY_COLUMNS, _EXPECTED_COLUMN, _PROCESSED_COLUMN}
        ]
        artifacts = artifacts.select(*ARTIFACT_ENVELOPE_COLUMNS, *user_columns)
        created_table = table is None
        if created_table:
            table = iceberg.create_table(table_id, artifacts.schema())
        try:
            artifacts = self._align_to_table_schema(table, artifacts, iceberg, table_name)
            rows_written = await iceberg.append_counted(table, artifacts)
        except BaseException as exc:
            if created_table:
                self._drop_created_table(iceberg, table_id, cause=exc)
            raise
        if created_table and rows_written == 0:
            self._drop_created_table(iceberg, table_id)
            table = None
        return self._receipt(
            world_id,
            run_id,
            table_name,
            table_id,
            table,
            iceberg,
            sources_matched,
            rows_written,
        )

    @staticmethod
    def _drop_created_table(
        iceberg: IcebergCatalogContext,
        table_id: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        try:
            iceberg.drop_table(table_id)
        except Exception as cleanup_error:
            if cause is None:
                raise
            cause.add_note(
                f"failed to remove newly created artifact table {table_id!r}: {cleanup_error}"
            )

    @staticmethod
    def _align_to_table_schema(
        table: Table,
        artifacts: DataFrame,
        iceberg: IcebergCatalogContext,
        table_name: str,
    ) -> DataFrame:
        existing = iceberg.read(table).schema().to_pyarrow_schema()
        incoming = artifacts.schema().to_pyarrow_schema()
        existing_shape = {field.name: field.type for field in existing}
        incoming_shape = {field.name: field.type for field in incoming}
        if existing_shape != incoming_shape:
            raise ValueError(
                f"artifact table {table_name!r} already has a different typed schema: "
                f"existing={existing_shape!r}, incoming={incoming_shape!r}"
            )
        return artifacts.select(*existing_shape)

    @staticmethod
    def _receipt(
        world_id: str,
        run_id: str,
        table_name: str,
        table_id: str,
        table: Table | None,
        iceberg: IcebergCatalogContext,
        sources_matched: int | None,
        rows_written: int,
    ) -> ArtifactWriteReceipt:
        snapshot_id = iceberg.current_snapshot_id(table) if table is not None else None
        return ArtifactWriteReceipt(
            world_id=world_id,
            run_id=run_id,
            table_name=table_name,
            table_id=table_id,
            sources_matched=sources_matched,
            rows_written=rows_written,
            snapshot_id=snapshot_id,
        )
