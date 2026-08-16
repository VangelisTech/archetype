# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Free storage-backed artifact publication and query workflows."""

from __future__ import annotations

from typing import Any

from daft import DataFrame, lit

from archetype.artifacts.models import (
    ArtifactRef,
    ArtifactSource,
    ArtifactStoreConfig,
    IngestArtifacts,
    QueryArtifacts,
    resolve_artifact_object_root,
)
from archetype.artifacts.pipeline import (
    ARTIFACT_FILES,
    FileIngestionPipeline,
    scan_sources,
)
from archetype.artifacts.views import read_artifacts
from archetype.core.config import StorageBackend, StorageConfig
from archetype.core.paths import local_storage_path
from archetype.storage.catalog import WorldRecord
from archetype.storage.interfaces import iStorageService


def _require_storage(value: object) -> StorageConfig:
    if not isinstance(value, StorageConfig):
        raise TypeError("artifact operations require an explicit StorageConfig")
    if value.backend != StorageBackend.ICEBERG:
        raise ValueError("artifact ingestion requires StorageBackend.ICEBERG")
    return value


async def _durable_world(
    storage_service: iStorageService,
    storage_config: StorageConfig,
    world_id: str,
) -> WorldRecord:
    """Resolve the published world/run/tick coordinates before file effects."""

    storage_service.require_iceberg_identity(storage_config)
    wid = str(world_id)
    catalog = storage_service.get_control_catalog(storage_config)
    record = await catalog.get_world(wid)
    if record is None:
        raise KeyError(f"world {wid} is not recorded in catalog for {storage_config.uri}")
    if not record.run_id:
        raise RuntimeError(f"world {wid} has no recorded run; artifacts need a run key")
    published_head = await catalog.max_manifest_tick(wid, str(record.run_id))
    if published_head is None:
        raise RuntimeError(
            f"world {wid} has no published tick head; artifacts require a durable tick"
        )
    if int(published_head) != int(record.tick_head):
        raise RuntimeError(
            f"world {wid} durable tick head {record.tick_head} does not match "
            f"published manifest head {published_head}"
        )
    return record


def _effective_store_config(
    storage_config: StorageConfig,
    store_config: ArtifactStoreConfig,
) -> ArtifactStoreConfig:
    if store_config.io_config is not None or storage_config.io_config is None:
        return store_config
    return store_config.model_copy(update={"io_config": storage_config.io_config})


def _validate_discovery(
    columns: dict[str, list[Any]],
    sources: tuple[ArtifactSource, ...],
) -> None:
    source_indexes = [int(value) for value in columns.get("_source_index", [])]
    logical_paths = [str(value) for value in columns.get("logical_path", [])]
    for index, source in enumerate(sources):
        if source.required and index not in source_indexes:
            raise FileNotFoundError(
                f"required artifact source matched no files: {source.source_uri}"
            )
    if len(logical_paths) != len(set(logical_paths)):
        raise ValueError("artifact sources resolve to duplicate logical paths")


def _references(values: dict[str, list[Any]]) -> tuple[ArtifactRef, ...]:
    return tuple(
        ArtifactRef(
            artifact_id=str(artifact_id),
            logical_path=str(logical_path),
            uri=str(uri),
            sha256=str(sha256),
            xxhash3_64=str(fast_hash),
            media_type=str(media_type),
            size_bytes=int(size_bytes),
        )
        for artifact_id, logical_path, uri, sha256, fast_hash, media_type, size_bytes in zip(
            values["artifact_id"],
            values["logical_path"],
            values["object_uri"],
            values["sha256"],
            values["xxhash3_64"],
            values["mime_type"],
            values["size_bytes"],
            strict=True,
        )
    )


async def _append_index(
    storage_service: iStorageService,
    storage_config: StorageConfig,
    world_id: str,
    table_name: str,
    rows: DataFrame,
) -> int:
    """Publish one occurrence-keyed index through the storage authority."""

    return await storage_service.append_world_rows(
        storage_config,
        world_id,
        table_name,
        rows,
        key_columns=("artifact_id",),
    )


async def ingest_artifacts(
    storage_service: iStorageService,
    operation: IngestArtifacts,
    *,
    store_config: ArtifactStoreConfig | None = None,
) -> tuple[ArtifactRef, ...]:
    """Persist declared files and publish their common visibility rows last."""

    storage = _require_storage(operation.storage_config)
    record = await _durable_world(storage_service, storage, operation.world_id)
    configured_store = _effective_store_config(
        storage,
        store_config or ArtifactStoreConfig(),
    )
    object_uri = resolve_artifact_object_root(storage, configured_store)
    local_object_root = local_storage_path(object_uri)
    pipeline = FileIngestionPipeline(
        io_config=configured_store.io_config,
        object_uri=object_uri,
        local_object_root=(str(local_object_root) if local_object_root is not None else None),
        max_connections=configured_store.max_connections,
    )

    # Materialize occurrence identity and source naming once. Rebuilding this
    # node would mint different UUIDv7 occurrence IDs for downstream branches.
    discovered = await storage_service.materialize(scan_sources(operation.sources, pipeline))
    discovered_values = discovered.to_pydict()
    _validate_discovery(discovered_values, operation.sources)
    if not discovered_values["artifact_id"]:
        return ()

    # The durable record is the sole tick source. Live/uncommitted state cannot
    # move artifact attribution ahead of the catalog-published world head.
    stored = await storage_service.materialize(
        pipeline.persist(discovered.with_column("tick", lit(int(record.tick_head))))
    )

    # Every parser reopens the immutable object URI, never the acquisition URI.
    indexed = pipeline.reopen(stored)
    stored_values = stored.to_pydict()
    present_families = set(stored_values["media_family"])
    logical_paths = stored_values["logical_path"]
    has_diff = any(str(path).lower().endswith((".diff", ".patch")) for path in logical_paths)
    for table_name, index in pipeline.specialized_indexes(
        indexed,
        media_families=present_families,
        include_diff=has_diff,
    ):
        await _append_index(
            storage_service,
            storage,
            operation.world_id,
            table_name,
            index,
        )

    # The common table is the visibility root and therefore the final append.
    await _append_index(
        storage_service,
        storage,
        operation.world_id,
        ARTIFACT_FILES,
        pipeline.common_index(stored),
    )
    return _references(stored_values)


async def query_artifacts(
    storage_service: iStorageService,
    operation: QueryArtifacts,
) -> DataFrame:
    """Read the common index through explicit durable coordinates."""

    storage = _require_storage(operation.storage_config)
    await _durable_world(storage_service, storage, operation.world_id)
    return await read_artifacts(
        storage_service,
        operation.world_id,
        storage_config=storage,
    )


__all__ = ["ingest_artifacts", "query_artifacts"]
