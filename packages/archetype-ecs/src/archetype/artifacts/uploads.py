# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded file upload workflow over the declared storage port, outside ticks."""

from __future__ import annotations

import tempfile
from pathlib import Path

from daft import lit

from archetype.artifacts._ingestion import publish_objects
from archetype.artifacts.pipeline import FileIngestionPipeline, ingestion_time_for
from archetype.storage.context_artifacts import ArtifactTarget, ContextArtifactStorage


def publish_upload(
    storage: ContextArtifactStorage,
    target: ArtifactTarget,
    *,
    artifact_id: str,
    logical_path: str,
    content: bytes,
):
    # Verify immutable context/cut authority before writing any submitted bytes.
    root = storage.verify(target)
    pipeline = FileIngestionPipeline(object_uri=root.as_uri(), local_object_root=str(root))
    with tempfile.TemporaryDirectory(prefix="archetype-upload-") as temporary:
        path = Path(temporary) / Path(logical_path).name
        path.write_bytes(content)
        discovered = (
            pipeline.scan(str(path), logical_path=logical_path)
            .with_column("artifact_id", lit(artifact_id))
            .with_column("ingested_at", lit(ingestion_time_for(artifact_id)))
            .with_column("source_uri", lit("upload:" + artifact_id))
        )
        stored = storage.materialize(pipeline.persist(discovered))
        values = stored.to_pydict()
        # Both common and typed indexes reuse the existing cohesive file graph.
        specialized = pipeline.specialized_indexes(
            pipeline.reopen(stored),
            media_families=set(values["media_family"]),
            include_diff=logical_path.lower().endswith((".diff", ".patch")),
        )
        common, _ = publish_objects(storage, target, stored, values)
        occurrences = storage.encode(
            pipeline.intrinsic_common_index(common),
            tuple((name.removeprefix("artifact_"), frame) for name, frame in specialized),
        )
        return storage.publish(target, occurrences)
