# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Synchronous runtime parity for file artifact ingestion."""

from archetype import ArchetypeRuntime, ArtifactSource, ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig


def test_sync_runtime_ingests_and_queries_common_artifact_index(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("sync")
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )

    with ArchetypeRuntime.sync(
        artifact_store=ArtifactStoreConfig.local(tmp_path / "artifacts")
    ) as runtime:
        world = runtime.world("sync-artifacts", storage=storage)
        world.step()
        (reference,) = world.ingest_artifacts(ArtifactSource(source_uri=str(source)))

        assert world.artifacts().select("artifact_id", "logical_path").to_pylist() == [
            {"artifact_id": reference.artifact_id, "logical_path": "source.txt"}
        ]
