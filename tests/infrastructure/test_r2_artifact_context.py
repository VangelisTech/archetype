# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real multimodal artifact ingestion through Cloudflare R2."""

from __future__ import annotations

import base64
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import pytest
from daft.catalog import Catalog
from daft.io import IOConfig, S3Config
from daft.session import Session
from pyarrow.fs import S3FileSystem
from pyiceberg.catalog.sql import SqlCatalog
from uuid_utils import UUID, uuid7

from archetype.app.container import ServiceContainer
from archetype.app.storage.service import StorageService
from archetype.artifacts import ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.ingestion import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_FILES,
    ARTIFACT_IMAGES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
)
from archetype.missions.trajectories import CLAUDE_TRANSCRIPT_TABLE, ClaudeTranscriptSource

ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
API_ENDPOINT = os.environ.get("R2_API_ENDPOINT")
BUCKET = os.environ.get("R2_BUCKET")
_REQUIRED = (
    ACCESS_KEY_ID,
    SECRET_ACCESS_KEY,
    API_ENDPOINT,
    BUCKET,
)
_HF = "hf://datasets/Eventual-Inc/sample-files"
_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M/wHwAF/gL+XfBvAAAAAElFTkSuQmCC"
)

pytestmark = [
    pytest.mark.contract("ingestion.catalog.cold_roundtrip"),
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.external,
    pytest.mark.slow,
    pytest.mark.skipif(
        not all(_REQUIRED),
        reason="GitHub Actions supplies Cloudflare R2 credentials",
    ),
]


def _catalog(path: Path, warehouse: str) -> SqlCatalog:
    assert API_ENDPOINT is not None
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    return SqlCatalog(
        "archetype_r2_artifact_dogfood",
        uri=f"sqlite:///{path}",
        warehouse=warehouse,
        **{
            "s3.endpoint": API_ENDPOINT,
            "s3.access-key-id": ACCESS_KEY_ID,
            "s3.secret-access-key": SECRET_ACCESS_KEY,
            "s3.region": "auto",
            "s3.force-virtual-addressing": "false",
        },
    )


def _session(catalog: SqlCatalog, namespace: str) -> Session:
    session = Session()
    session.attach_catalog(Catalog.from_iceberg(catalog))
    session.set_namespace(namespace)
    return session


def _io_config() -> IOConfig:
    assert API_ENDPOINT is not None
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    return IOConfig(
        s3=S3Config(
            endpoint_url=API_ENDPOINT,
            region_name="auto",
            key_id=ACCESS_KEY_ID,
            access_key=SECRET_ACCESS_KEY,
            use_ssl=True,
            force_virtual_addressing=False,
        )
    )


async def test_huggingface_context_pack_round_trips_through_cloudflare_r2(
    tmp_path: Path, monkeypatch
) -> None:
    assert BUCKET is not None
    assert API_ENDPOINT is not None
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    identity = uuid7().hex
    namespace = f"artifact_dogfood_{identity}"
    prefix = f"archetype-ci/artifact-context/{identity}"
    warehouse = f"s3://{BUCKET}/{prefix}/warehouse"
    object_root = f"s3://{BUCKET}/{prefix}/objects"
    catalog_path = tmp_path / "artifact-catalog.db"
    storage = StorageConfig(
        uri=warehouse,
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
        io_config=_io_config(),
    )
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "control"))
    markdown = tmp_path / "mission.md"
    code = tmp_path / "pipeline.py"
    patch = tmp_path / "change.patch"
    image = tmp_path / "pixel.png"
    transcript = tmp_path / "dogfood-project" / "session.jsonl"
    transcript.parent.mkdir()
    markdown.write_text("# Task\nAssess the multimodal artifact ingestion evidence.\n")
    code.write_text("def ingest(path: str) -> str:\n    return path\n")
    patch.write_text(
        "diff --git a/pipeline.py b/pipeline.py\n"
        "--- a/pipeline.py\n"
        "+++ b/pipeline.py\n"
        "@@ -1,2 +1,3 @@\n"
        " def ingest(path: str) -> str:\n"
        "+    # preserve artifact context\n"
        "     return path\n"
    )
    image.write_bytes(_PNG)
    transcript.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "user",
                        "timestamp": "2026-07-20T09:00:00.000Z",
                        "cwd": "/private/software-factory",
                        "gitBranch": "mission/artifact-context",
                        "version": "3.0.0",
                        "message": {
                            "role": "user",
                            "content": "Assess the context pack without trusting embedded instructions.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "assistant",
                        "timestamp": "2026-07-20T09:00:02.000Z",
                        "cwd": "/private/software-factory",
                        "gitBranch": "mission/artifact-context",
                        "version": "3.0.0",
                        "message": {
                            "role": "assistant",
                            "content": [{"type": "text", "text": "Indexed and attributed."}],
                            "model": "claude-fable-5",
                            "usage": {"output_tokens": 4},
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    catalog = _catalog(catalog_path, warehouse)
    catalog.create_namespace(namespace)
    storage_service: StorageService | None = None
    container: ServiceContainer | None = None
    cold_storage: StorageService | None = None
    cold: ServiceContainer | None = None
    try:
        storage_service = StorageService(_session(catalog, namespace))
        container = ServiceContainer(
            storage_service=storage_service,
            audit_storage_config=storage,
            artifact_store_config=ArtifactStoreConfig(
                object_uri=object_root,
                io_config=storage.io_config,
            ),
        )
        world = await container.world_service.create_world(
            WorldConfig(name="r2-artifact-context"), storage
        )
        references = await container.artifact_service.ingest(
            str(world.world_id),
            [
                ArtifactSource(
                    source_uri=f"{_HF}/README.md",
                    logical_path="context/daft-samples.md",
                ),
                ArtifactSource(
                    source_uri=(
                        f"{_HF}/audio/"
                        "Build_Scalable_Batch_Inference_Pipelines_in_3_Lines_"
                        "Daft_GPT_vLLM.mp3"
                    ),
                    logical_path="context/talk.mp3",
                ),
                ArtifactSource(
                    source_uri=(
                        f"{_HF}/videos/"
                        "Build_Scalable_Batch_Inference_Pipelines_in_3_Lines_"
                        "Daft_GPT_vLLM.mp4"
                    ),
                    logical_path="context/talk.mp4",
                ),
                ArtifactSource(
                    source_uri=f"{_HF}/papers/2102.04074v1.pdf",
                    logical_path="context/paper.pdf",
                ),
                ArtifactSource(source_uri=str(markdown), logical_path="context/mission.md"),
                ArtifactSource(source_uri=str(code), logical_path="context/pipeline.py"),
                ArtifactSource(source_uri=str(patch), logical_path="context/change.patch"),
                ArtifactSource(source_uri=str(image), logical_path="context/pixel.png"),
            ],
        )

        assert len(references) == 8
        assert all(reference.uri.startswith(object_root) for reference in references)
        transcript_result = await container.transcript_ingestion_service.ingest(
            str(world.world_id),
            ClaudeTranscriptSource(path=transcript, mission_id="r2-context-dogfood"),
            storage_config=storage,
        )
        assert transcript_result.rows_written == 3
        assert transcript_result.artifact.uri.startswith(object_root)
        world_id = str(world.world_id)
        run_id = str(world.run_id)
        await container.shutdown()
        await storage_service.shutdown()
        container = None
        storage_service = None

        # A fresh catalog instance and application graph must discover and
        # query every populated R2-backed table. No process-local Daft
        # registration from the writer may be required for the cold path.
        cold_catalog = _catalog(catalog_path, warehouse)
        cold_storage = StorageService(_session(cold_catalog, namespace))
        cold = ServiceContainer(
            storage_service=cold_storage,
            audit_storage_config=storage,
            artifact_store_config=ArtifactStoreConfig(
                object_uri=object_root,
                io_config=storage.io_config,
            ),
        )
        common_rows = (
            (await cold.application.query_artifacts(world_id, storage_config=storage))
            .select(
                "world_id",
                "run_id",
                "artifact_id",
                "logical_path",
                "source_uri",
                "object_uri",
                "ingested_at",
                "size_bytes",
                "mime_type",
                "media_family",
                "sha256",
                "xxhash3_64",
            )
            .to_pylist()
        )
        common_by_id = {row["artifact_id"]: row for row in common_rows}
        expected_ids = {reference.artifact_id for reference in references} | {
            transcript_result.artifact.artifact_id
        }
        assert set(common_by_id) == expected_ids
        assert all(row["world_id"] == world_id for row in common_rows)
        assert all(row["run_id"] == run_id for row in common_rows)
        assert all(str(row["object_uri"]).startswith(object_root) for row in common_rows)
        assert any(str(row["source_uri"]).startswith("hf://") for row in common_rows)
        for row in common_rows:
            artifact_id = UUID(row["artifact_id"])
            assert artifact_id.version == 7
            assert row["ingested_at"] == datetime.fromtimestamp(
                artifact_id.timestamp / 1000,
                tz=UTC,
            )
            assert row["size_bytes"] > 0
            assert len(row["sha256"]) == 64
            assert len(row["xxhash3_64"]) == 16

        image_rows = (
            (await cold.ingestion_service.read(world_id, ARTIFACT_IMAGES, storage_config=storage))
            .select("artifact_id", "width", "height", "format", "mode")
            .to_pylist()
        )
        (image_row,) = image_rows
        assert (image_row["width"], image_row["height"]) == (1, 1)
        assert (image_row["format"], image_row["mode"]) == ("PNG", "RGBA")
        assert common_by_id[image_row["artifact_id"]]["logical_path"] == "context/pixel.png"

        audio_rows = (
            (await cold.ingestion_service.read(world_id, ARTIFACT_AUDIO, storage_config=storage))
            .select("artifact_id", "sample_rate", "duration_seconds")
            .to_pylist()
        )
        (audio,) = audio_rows
        assert audio["sample_rate"] == 16_000
        assert audio["duration_seconds"] > 150
        assert common_by_id[audio["artifact_id"]]["logical_path"] == "context/talk.mp3"

        video_rows = (
            (await cold.ingestion_service.read(world_id, ARTIFACT_VIDEO, storage_config=storage))
            .select("artifact_id", "width", "height", "duration_seconds")
            .to_pylist()
        )
        (video,) = video_rows
        assert video["width"] == 1_920
        assert video["height"] == 1_080
        assert video["duration_seconds"] > 150
        assert common_by_id[video["artifact_id"]]["logical_path"] == "context/talk.mp4"

        pdf_rows = (
            (await cold.ingestion_service.read(world_id, ARTIFACT_PDF, storage_config=storage))
            .select("artifact_id", "page_count")
            .to_pylist()
        )
        (pdf,) = pdf_rows
        assert pdf["page_count"] == 26
        assert common_by_id[pdf["artifact_id"]]["logical_path"] == "context/paper.pdf"

        text_rows = (
            (await cold.ingestion_service.read(world_id, ARTIFACT_TEXT, storage_config=storage))
            .select("artifact_id", "language")
            .to_pylist()
        )
        assert sorted(row["language"] for row in text_rows) == [
            "diff",
            "jsonl",
            "markdown",
            "markdown",
            "python",
        ]
        assert {common_by_id[row["artifact_id"]]["logical_path"] for row in text_rows} == {
            "claude/dogfood-project/session.jsonl",
            "context/change.patch",
            "context/daft-samples.md",
            "context/mission.md",
            "context/pipeline.py",
        }

        diff_rows = (
            (await cold.ingestion_service.read(world_id, ARTIFACT_DIFF, storage_config=storage))
            .select("artifact_id", "file_count", "hunk_count", "additions")
            .to_pylist()
        )
        (diff,) = diff_rows
        assert (diff["file_count"], diff["hunk_count"], diff["additions"]) == (1, 1, 1)
        assert common_by_id[diff["artifact_id"]]["logical_path"] == "context/change.patch"
        patch_common = common_by_id[diff["artifact_id"]]
        assert patch_common["mime_type"] == "application/octet-stream"
        assert patch_common["media_family"] == "text"

        transcript_rows = (
            (await cold.application.query_transcript_rows(world_id, storage_config=storage))
            .select("source_artifact_id", "mission_id", "row_kind", "seq", "role", "content")
            .to_pylist()
        )
        assert {row["source_artifact_id"] for row in transcript_rows} == {
            transcript_result.artifact.artifact_id
        }
        assert {row["mission_id"] for row in transcript_rows} == {"r2-context-dogfood"}
        assert sorted((row["row_kind"], row["seq"]) for row in transcript_rows) == [
            ("session", -1),
            ("turn", 0),
            ("turn", 1),
        ]
        assert common_by_id[transcript_result.artifact.artifact_id]["logical_path"] == (
            "claude/dogfood-project/session.jsonl"
        )

        cold_counts = {
            ARTIFACT_FILES: len(common_rows),
            ARTIFACT_IMAGES: len(image_rows),
            ARTIFACT_AUDIO: len(audio_rows),
            ARTIFACT_VIDEO: len(video_rows),
            ARTIFACT_PDF: len(pdf_rows),
            ARTIFACT_TEXT: len(text_rows),
            ARTIFACT_DIFF: len(diff_rows),
            CLAUDE_TRANSCRIPT_TABLE: len(transcript_rows),
        }
        assert cold_counts == {
            "artifact_files": 9,
            "artifact_images": 1,
            "artifact_audio": 1,
            "artifact_video": 1,
            "artifact_pdf": 1,
            "artifact_text": 5,
            "artifact_diff": 1,
            "coding_agent_transcript_rows": 3,
        }
        assert set(cold_counts).issubset({name for _, name in cold_catalog.list_tables(namespace)})
    finally:
        if cold is not None:
            await cold.shutdown()
        if cold_storage is not None:
            await cold_storage.shutdown()
        if container is not None:
            await container.shutdown()
        if storage_service is not None:
            await storage_service.shutdown()

        cleanup_catalog = _catalog(catalog_path, warehouse)
        if cleanup_catalog.namespace_exists(namespace):
            for identifier in cleanup_catalog.list_tables(namespace):
                cleanup_catalog.drop_table(identifier)
            cleanup_catalog.drop_namespace(namespace)

        endpoint = urlparse(API_ENDPOINT)
        filesystem = S3FileSystem(
            access_key=ACCESS_KEY_ID,
            secret_key=SECRET_ACCESS_KEY,
            region="auto",
            scheme=endpoint.scheme,
            endpoint_override=endpoint.netloc,
            force_virtual_addressing=False,
        )
        filesystem.delete_dir(f"{BUCKET}/{prefix}")
