# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Preview file workflow bound to an exact previously committed analytical cut.

Preparation executes file effects once. Retain its immutable metadata payload
for retry; a fresh preparation deliberately creates fresh occurrence UUIDs.
This is synchronous local batch work, separate from simulation execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from daft import lit

from archetype.artifacts._ingestion import references, validate_discovery
from archetype.artifacts.models import ArtifactRef, ArtifactSource
from archetype.artifacts.pipeline import FileIngestionPipeline, scan_sources
from archetype.storage.cut_artifacts import CutArtifactStorage, CutCoordinates, OccurrenceMetadata


@dataclass(frozen=True)
class PreparedAttachments:
    cut: CutCoordinates
    artifacts: tuple[ArtifactRef, ...]
    occurrences: tuple[OccurrenceMetadata, ...]


def prepare_attachments(
    storage: CutArtifactStorage,
    cut: CutCoordinates,
    sources: tuple[ArtifactSource, ...],
) -> PreparedAttachments:
    root = storage.verify(cut)
    pipeline = FileIngestionPipeline(object_uri=root.as_uri(), local_object_root=str(root))
    discovered = storage.materialize(scan_sources(sources, pipeline))
    values = discovered.to_pydict()
    validate_discovery(values, sources)
    if len(values["artifact_id"]) > 32:
        raise ValueError("At most 32 occurrences per attachment call")
    if not values["artifact_id"]:
        return PreparedAttachments(cut, (), ())
    stored = storage.materialize(pipeline.persist(discovered.with_column("tick", lit(cut.tick))))
    values = stored.to_pydict()
    typed = pipeline.specialized_indexes(
        pipeline.reopen(stored),
        media_families=set(values["media_family"]),
        include_diff=any(
            path.lower().endswith((".diff", ".patch")) for path in values["logical_path"]
        ),
    )
    occurrences = storage.encode(
        pipeline.common_index(stored).exclude("tick"),
        tuple((name.removeprefix("artifact_"), frame) for name, frame in typed),
    )
    return PreparedAttachments(cut, references(values), occurrences)


def publish_attachments(
    storage: CutArtifactStorage, prepared: PreparedAttachments
) -> tuple[dict[str, Any], ...]:
    """Return factual per-occurrence receipts after common-root visibility."""
    return storage.publish(prepared.cut, prepared.occurrences)
