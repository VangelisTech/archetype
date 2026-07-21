# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Single-service file artifact ingestion contracts."""

import base64
import hashlib
import wave
from pathlib import Path

import av
import numpy as np
import pytest
import xxhash
from pypdf import PdfWriter
from uuid_utils import UUID

import archetype.app.artifacts.service as artifact_service_module
from archetype.app.artifacts.pipeline import plan_source
from archetype.app.container import ServiceContainer
from archetype.artifacts.contracts import ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.ingestion import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_FILES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
)
from archetype.ingestion.files import logical_path_for
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


def _write_audio(path: Path) -> None:
    with wave.open(str(path), "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(8_000)
        stream.writeframes(b"\x00\x00" * 800)


def _write_video(path: Path) -> None:
    with av.open(str(path), mode="w") as container:
        stream = container.add_stream("mpeg4", rate=10)
        stream.width = 16
        stream.height = 16
        stream.pix_fmt = "yuv420p"
        for index in range(6):
            pixels = np.full((16, 16, 3), index * 20, dtype=np.uint8)
            frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)


def _write_pdf(path: Path) -> None:
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.add_blank_page(width=72, height=72)
    writer.add_metadata({"/Title": "Context paper", "/Author": "Archetype"})
    with path.open("wb") as stream:
        writer.write(stream)


def test_logical_paths_and_source_plans_preserve_declared_roots(tmp_path):
    assert (
        logical_path_for(
            "s3://bucket/source/context%20pack/report.md",
            source_root="s3://bucket/source",
            logical_root="mission",
        )
        == "mission/context pack/report.md"
    )
    assert (
        logical_path_for(
            "s3://other/source/report.md",
            source_root="s3://bucket/source",
        )
        == "report.md"
    )
    assert (
        logical_path_for(
            "file:///tmp/source/report.md",
            logical_path="renamed/report.md",
        )
        == "renamed/report.md"
    )
    with pytest.raises(ValueError, match="portable relative"):
        logical_path_for("result.md", logical_path="../result.md")

    remote = plan_source(
        0,
        ArtifactSource(source_uri="s3://bucket/context", recursive=True),
    )
    assert remote.path == "s3://bucket/context/**/*"
    assert remote.source_root == "s3://bucket/context"

    directory = tmp_path / "context"
    directory.mkdir()
    local = plan_source(
        1,
        ArtifactSource(source_uri=str(directory), recursive=True),
    )
    assert local.path == str(directory / "**/*")
    assert local.source_root == str(directory)
    with pytest.raises(IsADirectoryError):
        plan_source(2, ArtifactSource(source_uri=str(directory)))

    document = directory / "report.md"
    document.write_text("evidence")
    with pytest.raises(NotADirectoryError):
        plan_source(
            3,
            ArtifactSource(source_uri=str(document), recursive=True),
        )

    wildcard = plan_source(
        4,
        ArtifactSource(source_uri=str(directory / "*.md")),
    )
    assert wildcard.path == str(directory / "*.md")
    assert wildcard.source_root == str(directory)


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


@pytest.mark.asyncio
@pytest.mark.integration
async def test_mixed_context_pack_runs_every_concrete_index(tmp_path):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        audio = tmp_path / "context.wav"
        video = tmp_path / "context.mp4"
        pdf = tmp_path / "context.pdf"
        markdown = tmp_path / "brief.md"
        code = tmp_path / "pipeline.py"
        patch = tmp_path / "change.patch"
        _write_audio(audio)
        _write_video(video)
        _write_pdf(pdf)
        markdown.write_text("# Context\nReview the attached implementation.\n")
        code.write_text("def ingest(path: str) -> str:\n    return path\n")
        patch.write_text(
            "diff --git a/pipeline.py b/pipeline.py\n"
            "--- a/pipeline.py\n"
            "+++ b/pipeline.py\n"
            "@@ -1,2 +1,3 @@\n"
            " def ingest(path: str) -> str:\n"
            "+    # retain source identity\n"
            "     return path\n"
        )

        references = await container.artifact_service.ingest(
            str(world.world_id),
            [
                ArtifactSource(source_uri=str(audio), logical_root="context"),
                ArtifactSource(source_uri=str(video), logical_root="context"),
                ArtifactSource(source_uri=str(pdf), logical_root="context"),
                ArtifactSource(source_uri=str(markdown), logical_root="context"),
                ArtifactSource(source_uri=str(code), logical_root="context"),
                ArtifactSource(source_uri=str(patch), logical_root="context"),
            ],
        )

        assert len(references) == 6
        audio_rows = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_AUDIO)
        ).to_pylist()
        assert len(audio_rows) == 1
        assert audio_rows[0]["sample_rate"] == 8_000
        assert audio_rows[0]["channels"] == 1
        assert audio_rows[0]["duration_seconds"] == pytest.approx(0.1)

        video_rows = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_VIDEO)
        ).to_pylist()
        assert len(video_rows) == 1
        assert video_rows[0]["width"] == 16
        assert video_rows[0]["height"] == 16
        assert video_rows[0]["fps"] == pytest.approx(10.0)
        assert video_rows[0]["frame_count"] == 6

        pdf_rows = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_PDF)
        ).to_pylist()
        assert len(pdf_rows) == 1
        assert pdf_rows[0]["page_count"] == 2
        assert pdf_rows[0]["title"] == "Context paper"
        assert pdf_rows[0]["author"] == "Archetype"

        text_rows = sorted(
            (
                await container.ingestion_service.read(str(world.world_id), ARTIFACT_TEXT)
            ).to_pylist(),
            key=lambda row: row["language"],
        )
        assert [row["language"] for row in text_rows] == ["diff", "markdown", "python"]
        assert [row["text_kind"] for row in text_rows] == [
            "diff",
            "markdown",
            "source_code",
        ]
        assert all(row["utf8"] for row in text_rows)

        (diff_row,) = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_DIFF)
        ).to_pylist()
        assert diff_row["format"] == "git"
        assert diff_row["file_count"] == 1
        assert diff_row["hunk_count"] == 1
        assert diff_row["additions"] == 1
        assert diff_row["deletions"] == 0

        common = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_FILES)
        ).to_pylist()
        patch_row = next(row for row in common if row["logical_path"].endswith(".patch"))
        assert patch_row["mime_type"] == "text/x-diff"
        assert patch_row["media_family"] == "text"
    finally:
        await container.shutdown()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_media_metadata_reads_staged_object_not_acquisition_source(tmp_path, monkeypatch):
    source = tmp_path / "ephemeral.wav"
    _write_audio(source)
    real_persist = artifact_service_module.persist_objects

    def persist_then_remove(*args, **kwargs):
        staged = real_persist(*args, **kwargs).collect(num_preview_rows=0)
        source.unlink()
        return staged

    monkeypatch.setattr(artifact_service_module, "persist_objects", persist_then_remove)
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        storage = _storage(tmp_path)
        world = await _world(container, storage)
        await container.artifact_service.ingest(
            str(world.world_id), ArtifactSource(source_uri=str(source))
        )

        assert not source.exists()
        audio = await container.ingestion_service.read(str(world.world_id), ARTIFACT_AUDIO)
        assert audio.select("sample_rate", "channels").to_pylist() == [
            {"sample_rate": 8_000, "channels": 1}
        ]
    finally:
        await container.shutdown()
