# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned file artifact ingestion integration contracts."""

import base64
import hashlib
import wave
from contextlib import asynccontextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import av
import numpy as np
import pytest
import xxhash
from pypdf import PdfWriter
from uuid_utils import UUID

from archetype.artifacts.models import (
    ArtifactSource,
    ArtifactStoreConfig,
    IngestArtifacts,
    QueryArtifacts,
)
from archetype.artifacts.pipeline import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_IMAGES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
    FileIngestionPipeline,
    scan_sources,
)
from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world import simulation
from archetype.world.models import CreateWorld, Step
from archetype.world.registry import WorldRegistry
from tests._runtime import build_test_runtime

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M/wHwAF/gL+XfBvAAAAAElFTkSuQmCC"
)


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=tmp_path / "world-store",
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


@asynccontextmanager
async def _artifact_runtime(tmp_path: Path):
    storage_service = StorageService(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / "control-catalogs",
        )
    )
    resources = build_test_runtime(
        tmp_path,
        storage_service=storage_service,
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store"),
    )
    try:
        yield resources.dispatcher, storage_service
    finally:
        await resources.aclose()
        await storage_service.shutdown()


async def _world(dispatcher, storage: StorageConfig):
    world = await dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name="w"),
            storage_config=storage,
        )
    )
    await dispatcher.apply(
        Step(
            world_id=world.world_id,
            run_config=RunConfig(),
        )
    )
    return world


def _world_registry(dispatcher) -> WorldRegistry:
    handler = dispatcher._registry.resolve_name("get_world_info").handler
    assert isinstance(handler, partial)
    assert handler.args
    registry = handler.args[0]
    assert isinstance(registry, WorldRegistry)
    return registry


async def _live_world(dispatcher, world_id: object) -> AsyncWorld:
    world = await _world_registry(dispatcher).live_world(str(world_id))
    assert isinstance(world, AsyncWorld)
    return world


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


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format, *_args):
        pass


def test_daft_pattern_scan_uses_portable_file_names(tmp_path):
    first = tmp_path / "first.md"
    second = tmp_path / "second.md"
    first.write_text("first")
    second.write_text("second")

    rows = (
        FileIngestionPipeline()
        .scan(str(tmp_path / "*.md"), pattern=True)
        .select("logical_path")
        .to_pylist()
    )

    assert sorted(row["logical_path"] for row in rows) == ["first.md", "second.md"]


def test_signed_query_url_is_read_as_one_exact_daft_file(tmp_path):
    source = tmp_path / "report.md"
    source.write_text("signed evidence")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        partial(_QuietHandler, directory=str(tmp_path)),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        source_uri = f"http://127.0.0.1:{server.server_port}/report.md?token=signed"
        rows = (
            scan_sources(
                (ArtifactSource(source_uri=source_uri),),
                FileIngestionPipeline(),
            )
            .select("source_uri", "logical_path")
            .to_pylist()
        )
    finally:
        server.shutdown()
        thread.join()
        server.server_close()

    assert rows == [{"source_uri": source_uri, "logical_path": "report.md"}]


@pytest.mark.asyncio
async def test_text_file_gets_uuidv7_common_index_and_content_address(tmp_path):
    store = tmp_path / "artifact-store"
    async with _artifact_runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        source = tmp_path / "result.txt"
        source.write_text("factory output")

        (reference,) = await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(
                    ArtifactSource(
                        source_uri=str(source),
                        logical_path="results/result.txt",
                    ),
                ),
                storage_config=storage,
            )
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

        rows = await dispatcher.apply(
            QueryArtifacts(
                world_id=world.world_id,
                storage_config=storage,
            )
        )
        assert rows.select("artifact_id", "logical_path", "object_uri").to_pylist() == [
            {
                "artifact_id": reference.artifact_id,
                "logical_path": reference.logical_path,
                "object_uri": reference.uri,
            }
        ]
        store_handle = await storage_service.get_or_create_store(storage)
        catalog = store_handle.session.current_catalog()
        assert catalog.has_table("ns.artifact_files")
        assert not catalog.has_table("ns.artifact_images")


@pytest.mark.asyncio
async def test_ingest_uses_published_tick_without_live_world_reconciliation(
    tmp_path,
    monkeypatch,
) -> None:
    async with _artifact_runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world_info = await _world(dispatcher, storage)
        world = await _live_world(dispatcher, world_info.world_id)
        source = tmp_path / "prepared.txt"
        source.write_text("prepared evidence")
        world.tick = 9

        async def reconcile(*_args, **_kwargs) -> bool:
            raise AssertionError("artifact ingestion must not reconcile live world state")

        monkeypatch.setattr(simulation, "reconcile_committed_work_locked", reconcile)

        await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(ArtifactSource(source_uri=str(source)),),
                storage_config=storage,
            )
        )
        rows = (
            (
                await dispatcher.apply(
                    QueryArtifacts(
                        world_id=world.world_id,
                        storage_config=storage,
                    )
                )
            )
            .select("tick")
            .to_pylist()
        )

        assert rows == [{"tick": 0}]
        world.tick = 0


@pytest.mark.asyncio
async def test_image_metadata_unnests_under_same_artifact_identity(tmp_path):
    async with _artifact_runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        source = tmp_path / "pixel.png"
        source.write_bytes(_PNG)

        (reference,) = await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(ArtifactSource(source_uri=str(source)),),
                storage_config=storage,
            )
        )

        media = await storage_service.read_world_rows(
            storage,
            str(world.world_id),
            ARTIFACT_IMAGES,
        )
        assert media.select("artifact_id", "width", "height", "format", "mode").to_pylist() == [
            {
                "artifact_id": reference.artifact_id,
                "width": 1,
                "height": 1,
                "format": "PNG",
                "mode": "RGBA",
            }
        ]
        common = await dispatcher.apply(
            QueryArtifacts(
                world_id=world.world_id,
                storage_config=storage,
            )
        )
        assert common.select("artifact_id", "media_family").to_pylist() == [
            {"artifact_id": reference.artifact_id, "media_family": "image"}
        ]


@pytest.mark.asyncio
async def test_equal_bytes_share_object_but_remain_distinct_occurrences(tmp_path):
    async with _artifact_runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        first = tmp_path / "first.txt"
        second = tmp_path / "second.txt"
        first.write_text("same")
        second.write_text("same")

        refs = await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(
                    ArtifactSource(source_uri=str(first)),
                    ArtifactSource(source_uri=str(second)),
                ),
                storage_config=storage,
            )
        )

        assert len({reference.artifact_id for reference in refs}) == 2
        assert len({reference.uri for reference in refs}) == 1
        assert len({reference.sha256 for reference in refs}) == 1


@pytest.mark.asyncio
async def test_required_source_and_logical_path_collisions_fail_closed(tmp_path):
    async with _artifact_runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        with pytest.raises(FileNotFoundError, match="matched no files"):
            await dispatcher.apply(
                IngestArtifacts(
                    world_id=world.world_id,
                    sources=(ArtifactSource(source_uri=str(tmp_path / "missing.txt")),),
                    storage_config=storage,
                )
            )
        assert (
            await dispatcher.apply(
                IngestArtifacts(
                    world_id=world.world_id,
                    sources=(
                        ArtifactSource(
                            source_uri=str(tmp_path / "optional.txt"),
                            required=False,
                        ),
                    ),
                    storage_config=storage,
                )
            )
            == ()
        )

        first = tmp_path / "first.txt"
        second = tmp_path / "second.txt"
        first.write_text("first")
        second.write_text("second")
        with pytest.raises(ValueError, match="duplicate logical paths"):
            await dispatcher.apply(
                IngestArtifacts(
                    world_id=world.world_id,
                    sources=(
                        ArtifactSource(
                            source_uri=str(first),
                            logical_path="same.txt",
                        ),
                        ArtifactSource(
                            source_uri=str(second),
                            logical_path="same.txt",
                        ),
                    ),
                    storage_config=storage,
                )
            )


@pytest.mark.asyncio
async def test_empty_glob_sources_reach_application_validation(tmp_path):
    async with _artifact_runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        missing_pattern = str(tmp_path / "*.missing")

        assert (
            await dispatcher.apply(
                IngestArtifacts(
                    world_id=world.world_id,
                    sources=(
                        ArtifactSource(
                            source_uri=missing_pattern,
                            required=False,
                        ),
                    ),
                    storage_config=storage,
                )
            )
            == ()
        )
        with pytest.raises(FileNotFoundError, match="matched no files"):
            await dispatcher.apply(
                IngestArtifacts(
                    world_id=world.world_id,
                    sources=(ArtifactSource(source_uri=missing_pattern),),
                    storage_config=storage,
                )
            )

        present = tmp_path / "present.txt"
        present.write_text("present")
        references = await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(
                    ArtifactSource(
                        source_uri=missing_pattern,
                        required=False,
                    ),
                    ArtifactSource(source_uri=str(tmp_path / "*.txt")),
                ),
                storage_config=storage,
            )
        )

        assert [reference.logical_path for reference in references] == ["present.txt"]


@pytest.mark.asyncio
async def test_common_index_is_published_last(tmp_path, monkeypatch):
    async with _artifact_runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        source = tmp_path / "pixel.png"
        source.write_bytes(_PNG)
        real_append = storage_service.append_world_rows

        async def fail_media(
            storage_config,
            world_id,
            table_name,
            rows,
            *,
            key_columns=(),
        ):
            if table_name == ARTIFACT_IMAGES:
                raise RuntimeError("metadata index unavailable")
            return await real_append(
                storage_config,
                world_id,
                table_name,
                rows,
                key_columns=key_columns,
            )

        monkeypatch.setattr(storage_service, "append_world_rows", fail_media)
        with pytest.raises(RuntimeError, match="metadata index unavailable"):
            await dispatcher.apply(
                IngestArtifacts(
                    world_id=world.world_id,
                    sources=(ArtifactSource(source_uri=str(source)),),
                    storage_config=storage,
                )
            )

        store_handle = await storage_service.get_or_create_store(storage)
        assert not store_handle.session.current_catalog().has_table("ns.artifact_files")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_mixed_context_pack_runs_every_concrete_index(tmp_path):
    async with _artifact_runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
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

        references = await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(
                    ArtifactSource(
                        source_uri=str(audio),
                        logical_path="context/context.wav",
                    ),
                    ArtifactSource(
                        source_uri=str(video),
                        logical_path="context/context.mp4",
                    ),
                    ArtifactSource(
                        source_uri=str(pdf),
                        logical_path="context/context.pdf",
                    ),
                    ArtifactSource(
                        source_uri=str(markdown),
                        logical_path="context/brief.md",
                    ),
                    ArtifactSource(
                        source_uri=str(code),
                        logical_path="context/pipeline.py",
                    ),
                    ArtifactSource(
                        source_uri=str(patch),
                        logical_path="context/change.patch",
                    ),
                ),
                storage_config=storage,
            )
        )

        assert len(references) == 6
        audio_rows = (
            await storage_service.read_world_rows(
                storage,
                str(world.world_id),
                ARTIFACT_AUDIO,
            )
        ).to_pylist()
        assert len(audio_rows) == 1
        assert audio_rows[0]["sample_rate"] == 8_000
        assert audio_rows[0]["channels"] == 1
        assert audio_rows[0]["duration_seconds"] == pytest.approx(0.1)

        video_rows = (
            await storage_service.read_world_rows(
                storage,
                str(world.world_id),
                ARTIFACT_VIDEO,
            )
        ).to_pylist()
        assert len(video_rows) == 1
        assert video_rows[0]["width"] == 16
        assert video_rows[0]["height"] == 16
        assert video_rows[0]["fps"] == pytest.approx(10.0)
        assert video_rows[0]["frame_count"] == 6

        pdf_rows = (
            await storage_service.read_world_rows(
                storage,
                str(world.world_id),
                ARTIFACT_PDF,
            )
        ).to_pylist()
        assert len(pdf_rows) == 1
        assert pdf_rows[0]["page_count"] == 2
        assert pdf_rows[0]["title"] == "Context paper"
        assert pdf_rows[0]["author"] == "Archetype"

        text_rows = sorted(
            (
                await storage_service.read_world_rows(
                    storage,
                    str(world.world_id),
                    ARTIFACT_TEXT,
                )
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
            await storage_service.read_world_rows(
                storage,
                str(world.world_id),
                ARTIFACT_DIFF,
            )
        ).to_pylist()
        assert diff_row["format"] == "git"
        assert diff_row["file_count"] == 1
        assert diff_row["hunk_count"] == 1
        assert diff_row["additions"] == 1
        assert diff_row["deletions"] == 0

        common = (
            await dispatcher.apply(
                QueryArtifacts(
                    world_id=world.world_id,
                    storage_config=storage,
                )
            )
        ).to_pylist()
        patch_row = next(row for row in common if row["logical_path"].endswith(".patch"))
        assert patch_row["mime_type"] == "application/octet-stream"
        assert patch_row["media_family"] == "text"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_media_metadata_reads_staged_object_not_acquisition_source(tmp_path, monkeypatch):
    source = tmp_path / "ephemeral.wav"
    _write_audio(source)
    real_persist = FileIngestionPipeline.persist

    def persist_then_remove(pipeline, *args, **kwargs):
        staged = real_persist(pipeline, *args, **kwargs).collect(num_preview_rows=0)
        source.unlink()
        return staged

    monkeypatch.setattr(FileIngestionPipeline, "persist", persist_then_remove)
    async with _artifact_runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _world(dispatcher, storage)
        await dispatcher.apply(
            IngestArtifacts(
                world_id=world.world_id,
                sources=(ArtifactSource(source_uri=str(source)),),
                storage_config=storage,
            )
        )

        assert not source.exists()
        audio = await storage_service.read_world_rows(
            storage,
            str(world.world_id),
            ARTIFACT_AUDIO,
        )
        assert audio.select("sample_rate", "channels").to_pylist() == [
            {"sample_rate": 8_000, "channels": 1}
        ]
