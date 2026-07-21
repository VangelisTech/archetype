# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The single application service for file artifact ingestion and indexing."""

from __future__ import annotations

from collections.abc import Sequence
from glob import has_magic
from typing import Any
from urllib.parse import urlsplit

import daft
from daft import DataFrame, lit
from uuid_utils import UUID

from archetype._storage_uri import local_storage_path
from archetype.app.ingestion.interfaces import iIngestionService
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.artifacts.contracts import ArtifactRef, ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig
from archetype.ingestion.pipeline import (
    ARTIFACT_FILES,
    FileIngestionPipeline,
)


def _is_pattern(source_uri: str) -> bool:
    """Classify only URI path wildcards; signed-query ``?`` stays exact."""

    parsed = urlsplit(source_uri)
    return has_magic(parsed.path if parsed.scheme else source_uri)


def scan_sources(
    sources: tuple[ArtifactSource, ...],
    pipeline: FileIngestionPipeline,
) -> DataFrame:
    """Compose declared sources into one uniformly typed lazy scan."""

    # Daft 0.7's glob scan has no micro-partition when every pattern matches
    # zero files, so materializing that otherwise valid empty graph fails
    # before application-level required-source validation can run. A typed,
    # zero-row exact scan is the concat identity and keeps discovery lazy.
    frames = [pipeline.scan([], pattern=False).with_column("_source_index", lit(-1))]
    for index, source in enumerate(sources):
        frame = pipeline.scan(
            source.source_uri,
            pattern=_is_pattern(source.source_uri),
            logical_path=source.logical_path,
        ).with_column("_source_index", lit(index))
        frames.append(frame)
    return daft.concat(frames)


class ArtifactService:
    """Copy files into object storage and publish their typed catalog indexes.

    Object persistence and specialized metadata tables complete before the
    common ``artifact_files`` row becomes visible. That common index is the
    public commit point; no claim, lease, receipt, or reconciliation state is
    introduced around the operation.
    """

    def __init__(
        self,
        storage_service: iStorageService,
        world_service: iWorldService,
        ingestion_service: iIngestionService,
        store_config: ArtifactStoreConfig | None = None,
    ) -> None:
        self._storage_service = storage_service
        self._world_service = world_service
        self._ingestion = ingestion_service
        self._store_config = store_config or ArtifactStoreConfig()

    async def ingest(
        self,
        world_id: str,
        sources: ArtifactSource | Sequence[ArtifactSource],
        *,
        storage_config: StorageConfig | None = None,
    ) -> tuple[ArtifactRef, ...]:
        """Ingest one set of sources and return portable references."""

        declared = self._normalize_sources(sources)
        wid, storage, tick = await self._world_context(world_id, storage_config)
        config = self._effective_store_config(storage)
        object_uri = self._object_root(storage, config)
        local_object_root = local_storage_path(object_uri)
        pipeline = FileIngestionPipeline(
            io_config=config.io_config,
            object_uri=object_uri,
            local_object_root=(str(local_object_root) if local_object_root is not None else None),
            max_connections=config.max_connections,
        )
        discovered = scan_sources(declared, pipeline)

        # Materialize UUIDv7 occurrence identity and source naming once before
        # persistence. Rebuilding this node would assign different identities.
        discovered = await self._storage_service.materialize(discovered)
        discovered_values = discovered.to_pydict()
        self._validate_discovery(discovered_values, declared)
        if not discovered_values["artifact_id"]:
            return ()

        stored = await self._storage_service.materialize(
            pipeline.persist(discovered.with_column("tick", lit(tick)))
        )

        # Specialized parsers must read the durable object, not reopen the
        # acquisition URI. Remote sources may be slow, mutable, or disappear
        # after the content-addressed copy completes.
        indexed = pipeline.reopen(stored)

        # Typed extensions are not visibility roots. A failure here leaves no
        # common row for public readers to observe.
        stored_values = stored.to_pydict()
        present_families = set(stored_values["media_family"])
        logical_paths = stored_values["logical_path"]
        has_diff = any(str(path).lower().endswith((".diff", ".patch")) for path in logical_paths)
        for table, index in pipeline.specialized_indexes(
            indexed,
            media_families=present_families,
            include_diff=has_diff,
        ):
            await self._ingestion.append(
                wid,
                table,
                index,
                storage_config=storage,
            )

        common = pipeline.common_index(stored)
        await self._ingestion.append(
            wid,
            ARTIFACT_FILES,
            common,
            storage_config=storage,
        )
        return self._references(stored_values)

    async def index(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        """Return this world's current-run common artifact index."""

        return await self._ingestion.read(
            str(world_id),
            ARTIFACT_FILES,
            storage_config=storage_config,
        )

    async def _world_context(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> tuple[str, StorageConfig, int]:
        """Get the world context for artifact ingestion."""
        wid = str(world_id)
        live = self._world_service.storage_record(wid)
        storage = storage_config or (live[0] if live is not None else StorageConfig())
        if storage.backend != StorageBackend.ICEBERG:
            raise ValueError("artifact ingestion requires StorageBackend.ICEBERG")
        control = self._storage_service.get_control_catalog(storage)
        record = await control.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; artifacts need a run key")
        if self._world_service.has_world(UUID(wid)):
            tick = int(self._world_service.get_world(UUID(wid)).tick)
        else:
            tick = int(record.tick_head)
        return wid, storage, tick

    def _effective_store_config(self, storage: StorageConfig) -> ArtifactStoreConfig:
        if self._store_config.io_config is not None or storage.io_config is None:
            return self._store_config
        return self._store_config.model_copy(update={"io_config": storage.io_config})

    @staticmethod
    def _object_root(storage: StorageConfig, config: ArtifactStoreConfig) -> str:
        if config.object_uri is not None:
            return str(config.object_uri)
        local = local_storage_path(str(storage.uri))
        if local is not None:
            return str(local / "artifacts")
        return str(storage.uri).rstrip("/") + "/artifacts"

    @staticmethod
    def _normalize_sources(
        sources: ArtifactSource | Sequence[ArtifactSource],
    ) -> tuple[ArtifactSource, ...]:
        if isinstance(sources, ArtifactSource):
            values = (sources,)
        else:
            values = tuple(sources)
        if not values:
            raise ValueError("artifact ingestion requires at least one source")
        if any(not isinstance(value, ArtifactSource) for value in values):
            raise TypeError("sources must contain ArtifactSource values")
        return values

    @staticmethod
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

    @staticmethod
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
