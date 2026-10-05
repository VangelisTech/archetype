# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""File ingestion bound to a published context and explicit optional cut.

Cutless means no cut attribution, even if the hosted world later publishes.
Retain prepared metadata for exact retry; a new preparation mints new UUIDs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from archetype.artifacts._ingestion import publish_objects, references, validate_discovery
from archetype.artifacts.models import ArtifactRef, ArtifactSource
from archetype.artifacts.pipeline import FileIngestionPipeline, scan_sources
from archetype.storage.context_artifacts import ArtifactTarget, ContextArtifactStorage
from archetype.storage.cut_artifacts import OccurrenceMetadata


@dataclass(frozen=True)
class PreparedContextAttachments:
    target: ArtifactTarget
    artifacts: tuple[ArtifactRef, ...]
    occurrences: tuple[OccurrenceMetadata, ...]


def prepare_context_attachments(
    storage: ContextArtifactStorage, target: ArtifactTarget, sources: tuple[ArtifactSource, ...]
) -> PreparedContextAttachments:
    root = storage.verify(target)
    pipeline = FileIngestionPipeline(object_uri=root.as_uri(), local_object_root=str(root))
    discovered = storage.materialize(scan_sources(sources, pipeline))
    values = discovered.to_pydict()
    validate_discovery(values, sources)
    if len(values["artifact_id"]) > 32:
        raise ValueError("At most 32 occurrences per attachment call")
    if not values["artifact_id"]:
        return PreparedContextAttachments(target, (), ())
    stored = storage.materialize(pipeline.persist(discovered))
    values = stored.to_pydict()
    typed = pipeline.specialized_indexes(
        pipeline.reopen(stored),
        media_families=set(values["media_family"]),
        include_diff=any(
            path.lower().endswith((".diff", ".patch")) for path in values["logical_path"]
        ),
    )
    common, values = publish_objects(storage, target, stored, values)
    occurrences = storage.encode(
        pipeline.intrinsic_common_index(common),
        tuple((name.removeprefix("artifact_"), frame) for name, frame in typed),
    )
    return PreparedContextAttachments(target, references(values), occurrences)


def publish_context_attachments(
    storage: ContextArtifactStorage, prepared: PreparedContextAttachments
) -> tuple[dict[str, Any], ...]:
    return storage.publish(prepared.target, prepared.occurrences)
