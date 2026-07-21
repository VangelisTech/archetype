# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The single application service for file artifact ingestion and indexing."""

from __future__ import annotations

from collections.abc import Sequence

from daft import DataFrame, col, lit
from daft.functions import file as daft_file
from uuid_utils import UUID

from archetype._storage_uri import local_storage_path
from archetype.app.artifacts.pipeline import SourcePlan, persist_objects, scan_sources
from archetype.app.ingestion.interfaces import iIngestionService
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.artifacts.contracts import ArtifactRef, ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig
from archetype.ingestion.audio import ARTIFACT_AUDIO, audio_index
from archetype.ingestion.diffs import ARTIFACT_DIFF, diff_index
from archetype.ingestion.documents import ARTIFACT_PDF, pdf_index
from archetype.ingestion.files import ARTIFACT_FILES, common_index
from archetype.ingestion.images import ARTIFACT_IMAGES, image_index
from archetype.ingestion.text import ARTIFACT_TEXT, text_index
from archetype.ingestion.video import ARTIFACT_VIDEO, video_index

_MEDIA_INDEXES = (
    ("audio", ARTIFACT_AUDIO, audio_index),
    ("image", ARTIFACT_IMAGES, image_index),
    ("pdf", ARTIFACT_PDF, pdf_index),
    ("text", ARTIFACT_TEXT, text_index),
    ("video", ARTIFACT_VIDEO, video_index),
)


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
        """Ingest one bounded set of sources and return portable references."""

        declared = self._normalize_sources(sources)
        wid, storage, tick = await self._world_context(world_id, storage_config)
        config = self._effective_store_config(storage)
        discovered, plans = scan_sources(declared, io_config=storage.io_config)

        # Materialize UUIDv7 and both hashes exactly once before multiple media
        # indexes consume the frame. Rebuilding this lazy node would assign a
        # different occurrence identity to each sink.
        discovered = discovered.collect(num_preview_rows=0)
        self._validate_discovery(discovered, plans, config)
        if discovered.count_rows() == 0:
            return ()

        object_uri = self._object_root(storage, config)
        stored = persist_objects(
            discovered.with_column("tick", lit(tick)),
            object_uri=object_uri,
            config=config,
        ).collect(num_preview_rows=0)

        # Specialized parsers must read the durable object, not reopen the
        # acquisition URI. Remote sources may be slow, mutable, or disappear
        # after the content-addressed copy completes.
        indexed = stored.with_column(
            "file",
            daft_file(col("object_uri"), io_config=config.io_config),
        )

        # Typed extensions are not visibility roots. A failure here leaves no
        # common row for public readers to observe.
        present_families = set(indexed.select("media_family").to_pydict()["media_family"])
        for family, table, project in _MEDIA_INDEXES:
            if family not in present_families:
                continue
            await self._ingestion.append(
                wid,
                table,
                project(indexed),
                storage_config=storage,
            )

        logical_paths = indexed.select("logical_path").to_pydict()["logical_path"]
        if any(str(path).lower().endswith((".diff", ".patch")) for path in logical_paths):
            await self._ingestion.append(
                wid,
                ARTIFACT_DIFF,
                diff_index(indexed),
                storage_config=storage,
            )

        common = common_index(stored)
        await self._ingestion.append(
            wid,
            ARTIFACT_FILES,
            common,
            storage_config=storage,
        )
        return self._references(common)

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
        files: DataFrame,
        plans: tuple[SourcePlan, ...],
        config: ArtifactStoreConfig,
    ) -> None:
        columns = files.select("_source_index", "logical_path", "size_bytes").to_pydict()
        source_indexes = [int(value) for value in columns.get("_source_index", [])]
        logical_paths = [str(value) for value in columns.get("logical_path", [])]
        sizes = [int(value) for value in columns.get("size_bytes", [])]
        for plan in plans:
            if plan.source.required and plan.index not in source_indexes:
                raise FileNotFoundError(
                    f"required artifact source matched no files: {plan.source.source_uri}"
                )
        if len(logical_paths) != len(set(logical_paths)):
            raise ValueError("artifact sources resolve to duplicate logical paths")
        oversized = [size for size in sizes if size > config.max_artifact_bytes]
        if oversized:
            raise ValueError(
                f"artifact is {max(oversized)} bytes; per-artifact limit is "
                f"{config.max_artifact_bytes}"
            )
        total = sum(sizes)
        if total > config.max_ingestion_bytes:
            raise ValueError(
                f"artifact ingestion is {total} bytes; batch limit is {config.max_ingestion_bytes}"
            )

    @staticmethod
    def _references(common: DataFrame) -> tuple[ArtifactRef, ...]:
        values = common.select(
            "artifact_id",
            "logical_path",
            "object_uri",
            "sha256",
            "xxhash3_64",
            "mime_type",
            "size_bytes",
        ).to_pydict()
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
