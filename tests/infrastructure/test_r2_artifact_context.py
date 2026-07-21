# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real multimodal artifact ingestion through R2 Data Catalog and R2 objects."""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import pytest
from daft.catalog import Catalog
from daft.io import IOConfig, S3Config
from daft.session import Session
from pyarrow.fs import S3FileSystem
from pyiceberg.catalog.rest import RestCatalog
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.storage.service import StorageService
from archetype.artifacts import ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.ingestion import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_FILES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
)
from archetype.missions.trajectories import ClaudeTranscriptSource

ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
API_ENDPOINT = os.environ.get("R2_API_ENDPOINT")
BUCKET = os.environ.get("R2_BUCKET")
CATALOG_URI = os.environ.get("R2_CATALOG_URI")
CATALOG_WAREHOUSE = os.environ.get("R2_CATALOG_WAREHOUSE")
CATALOG_TOKEN = os.environ.get("R2_CATALOG_TOKEN")
_REQUIRED = (
    ACCESS_KEY_ID,
    SECRET_ACCESS_KEY,
    API_ENDPOINT,
    BUCKET,
    CATALOG_URI,
    CATALOG_WAREHOUSE,
    CATALOG_TOKEN,
)
_HF = "hf://datasets/Eventual-Inc/sample-files"

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.external,
    pytest.mark.slow,
    pytest.mark.skipif(
        not all(_REQUIRED),
        reason="GitHub Actions supplies R2 object and Data Catalog credentials",
    ),
]


def _catalog() -> RestCatalog:
    assert CATALOG_URI is not None
    assert CATALOG_WAREHOUSE is not None
    assert CATALOG_TOKEN is not None
    return RestCatalog(
        "archetype_r2_artifact_dogfood",
        uri=CATALOG_URI,
        warehouse=CATALOG_WAREHOUSE,
        token=CATALOG_TOKEN,
    )


def _session(catalog: RestCatalog, namespace: str) -> Session:
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


async def test_huggingface_context_pack_round_trips_through_r2_data_catalog(
    tmp_path: Path, monkeypatch
) -> None:
    assert BUCKET is not None
    assert API_ENDPOINT is not None
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    identity = uuid7().hex
    namespace = f"artifact_dogfood_{identity}"
    prefix = f"archetype-ci/artifact-context/{identity}"
    object_root = f"s3://{BUCKET}/{prefix}/objects"
    storage = StorageConfig(
        uri=f"s3://{BUCKET}/{prefix}/worlds",
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
        io_config=_io_config(),
    )
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "control"))
    markdown = tmp_path / "mission.md"
    code = tmp_path / "pipeline.py"
    patch = tmp_path / "change.patch"
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

    catalog = _catalog()
    catalog.create_namespace(namespace)
    table_locations: list[str] = []
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
                ArtifactSource(source_uri=str(markdown), logical_root="context"),
                ArtifactSource(source_uri=str(code), logical_root="context"),
                ArtifactSource(source_uri=str(patch), logical_root="context"),
            ],
        )

        assert len(references) == 7
        assert all(reference.uri.startswith(object_root) for reference in references)
        transcript_result = await container.transcript_ingestion_service.ingest(
            str(world.world_id),
            ClaudeTranscriptSource(path=transcript, mission_id="r2-context-dogfood"),
            storage_config=storage,
        )
        assert transcript_result.rows_written == 3
        assert transcript_result.artifact.uri.startswith(object_root)
        assert (
            await container.transcript_ingestion_service.read(
                str(world.world_id), storage_config=storage
            )
        ).count_rows() == 3
        common = await container.ingestion_service.read(str(world.world_id), ARTIFACT_FILES)
        assert common.count_rows() == 8
        common_rows = common.select("logical_path", "source_uri", "object_uri").to_pylist()
        assert all(row["object_uri"].startswith(object_root) for row in common_rows)
        assert any(str(row["source_uri"]).startswith("hf://") for row in common_rows)

        (audio,) = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_AUDIO)
        ).to_pylist()
        assert audio["sample_rate"] == 16_000
        assert audio["duration_seconds"] > 150

        (video,) = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_VIDEO)
        ).to_pylist()
        assert video["width"] == 1_920
        assert video["height"] == 1_080
        assert video["duration_seconds"] > 150

        (pdf,) = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_PDF)
        ).to_pylist()
        assert pdf["page_count"] == 26

        text = await container.ingestion_service.read(str(world.world_id), ARTIFACT_TEXT)
        assert sorted(text.select("language").to_pydict()["language"]) == [
            "diff",
            "jsonl",
            "markdown",
            "markdown",
            "python",
        ]
        (diff,) = (
            await container.ingestion_service.read(str(world.world_id), ARTIFACT_DIFF)
        ).to_pylist()
        assert (diff["file_count"], diff["hunk_count"], diff["additions"]) == (1, 1, 1)
        world_id = str(world.world_id)
        await container.shutdown()
        await storage_service.shutdown()
        container = None
        storage_service = None

        # A fresh REST catalog and application graph must discover the same
        # tables and read the common index; no process-local Daft registration
        # may be required for the cold path.
        cold_catalog = _catalog()
        cold_storage = StorageService(_session(cold_catalog, namespace))
        cold = ServiceContainer(
            storage_service=cold_storage,
            audit_storage_config=storage,
            artifact_store_config=ArtifactStoreConfig(
                object_uri=object_root,
                io_config=storage.io_config,
            ),
        )
        cold_index = await cold.artifact_service.index(world_id, storage_config=storage)
        assert cold_index.count_rows() == 8
        assert {
            "artifact_files",
            "artifact_audio",
            "artifact_video",
            "artifact_pdf",
            "artifact_text",
            "artifact_diff",
            "coding_agent_transcript_rows",
        }.issubset({name for _, name in cold_catalog.list_tables(namespace)})
    finally:
        if cold is not None:
            await cold.shutdown()
        if cold_storage is not None:
            await cold_storage.shutdown()
        if container is not None:
            await container.shutdown()
        if storage_service is not None:
            await storage_service.shutdown()

        cleanup_catalog = _catalog()
        if cleanup_catalog.namespace_exists(namespace):
            for identifier in cleanup_catalog.list_tables(namespace):
                table_locations.append(cleanup_catalog.load_table(identifier).location())
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
        for location in table_locations:
            parsed = urlparse(location)
            if parsed.scheme == "s3" and parsed.netloc == BUCKET:
                filesystem.delete_dir(f"{parsed.netloc}/{parsed.path.lstrip('/')}")
