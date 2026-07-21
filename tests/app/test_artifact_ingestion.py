# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Single-service file artifact ingestion contracts."""

import base64
import hashlib
from pathlib import Path

import pytest
import xxhash
from uuid_utils import UUID

from archetype.app.container import ServiceContainer
from archetype.artifacts.contracts import ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.ingestion.files import ARTIFACT_FILES
from archetype.ingestion.images import ARTIFACT_IMAGES

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M/wHwAF/gL+XfBvAAAAAElFTkSuQmCC"
)


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=tmp_path / "world-store",
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


async def _world(container: ServiceContainer, storage: StorageConfig):
    return await container.world_service.create_world(WorldConfig(name="w"), storage)


@pytest.mark.asyncio
async def test_text_file_gets_uuidv7_common_index_and_content_address(tmp_path):
    store = tmp_path / "artifact-store"
    container = ServiceContainer(artifact_store_config=ArtifactStoreConfig.local(store))
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "result.txt"
        source.write_text("factory output")

        (reference,) = await container.artifact_service.ingest(
            str(world.world_id),
            ArtifactSource(source_uri=str(source), logical_path="results/result.txt"),
        )

        digest = hashlib.sha256(b"factory output").hexdigest()
        assert UUID(reference.artifact_id).version == 7
        assert reference.sha256 == digest
        assert reference.xxhash3_64 == xxhash.xxh3_64_hexdigest(b"factory output")
        assert reference.logical_path == "results/result.txt"
        assert (
            Path(reference.uri.removeprefix("file://"))
            == (store / "objects" / "sha256" / digest[:2] / digest).resolve()
        )
        assert reference.ingested_at.tzinfo is not None

        rows = await container.artifact_service.index(str(world.world_id))
        assert rows.select("artifact_id", "logical_path", "object_uri").to_pylist() == [
            {
                "artifact_id": reference.artifact_id,
                "logical_path": reference.logical_path,
                "object_uri": reference.uri,
            }
        ]
        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert iceberg.catalog.has_table("ns.artifact_files")
        assert not iceberg.catalog.has_table("ns.artifact_images")
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_image_metadata_unnests_under_same_artifact_identity(tmp_path):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "pixel.png"
        source.write_bytes(_PNG)

        (reference,) = await container.artifact_service.ingest(
            str(world.world_id), ArtifactSource(source_uri=str(source))
        )

        media = await container.ingestion_service.read(str(world.world_id), ARTIFACT_IMAGES)
        assert media.select("artifact_id", "width", "height", "format", "mode").to_pylist() == [
            {
                "artifact_id": reference.artifact_id,
                "width": 1,
                "height": 1,
                "format": "PNG",
                "mode": "RGBA",
            }
        ]
        common = await container.ingestion_service.read(str(world.world_id), ARTIFACT_FILES)
        assert common.select("artifact_id", "media_family").to_pylist() == [
            {"artifact_id": reference.artifact_id, "media_family": "image"}
        ]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_equal_bytes_share_object_but_remain_distinct_occurrences(tmp_path):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        first = tmp_path / "first.txt"
        second = tmp_path / "second.txt"
        first.write_text("same")
        second.write_text("same")

        refs = await container.artifact_service.ingest(
            str(world.world_id),
            [
                ArtifactSource(source_uri=str(first)),
                ArtifactSource(source_uri=str(second)),
            ],
        )

        assert len({reference.artifact_id for reference in refs}) == 2
        assert len({reference.uri for reference in refs}) == 1
        assert len({reference.sha256 for reference in refs}) == 1
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_required_source_and_logical_path_collisions_fail_closed(tmp_path):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        with pytest.raises(FileNotFoundError, match="matched no files"):
            await container.artifact_service.ingest(
                str(world.world_id), ArtifactSource(source_uri=str(tmp_path / "missing.txt"))
            )

        first = tmp_path / "first.txt"
        second = tmp_path / "second.txt"
        first.write_text("first")
        second.write_text("second")
        with pytest.raises(ValueError, match="duplicate logical paths"):
            await container.artifact_service.ingest(
                str(world.world_id),
                [
                    ArtifactSource(source_uri=str(first), logical_path="same.txt"),
                    ArtifactSource(source_uri=str(second), logical_path="same.txt"),
                ],
            )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_common_index_is_published_last(tmp_path, monkeypatch):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        source = tmp_path / "pixel.png"
        source.write_bytes(_PNG)
        real_append = container.ingestion_service.append

        async def fail_media(world_id, table, rows, **kwargs):
            if table == ARTIFACT_IMAGES:
                raise RuntimeError("metadata index unavailable")
            return await real_append(world_id, table, rows, **kwargs)

        monkeypatch.setattr(container.ingestion_service, "append", fail_media)
        with pytest.raises(RuntimeError, match="metadata index unavailable"):
            await container.artifact_service.ingest(
                str(world.world_id), ArtifactSource(source_uri=str(source))
            )

        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert not iceberg.catalog.has_table("ns.artifact_files")
    finally:
        await container.shutdown()
