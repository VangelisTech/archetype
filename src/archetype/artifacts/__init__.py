# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Artifacts domain family: persistent schemas and deterministic contracts.

This package owns the reusable artifact definitions: the persistent ECS
schemas (``ArtifactMeta``, ``AssetRef`` in ``archetype.artifacts.components``),
the typed-table and content-addressing contracts
(``archetype.artifacts.contracts``), and the bundle value contracts
(``archetype.artifacts.bundles``). Publication, indexing, reconciliation, and
storage authority remain internal application authority under
``archetype.app.artifacts``.

A top-level path does not make a symbol public: the supported surface is
exactly the names re-exported here, which back the artifact root exports.
"""

from __future__ import annotations

from archetype.artifacts.bundles import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactIndexRecord,
    ArtifactPublishReceipt,
    ArtifactReconcileResult,
    ArtifactSourceResolver,
    ArtifactStoreConfig,
    BoundedArtifactSourceResolver,
    MaterializedArtifact,
)
from archetype.artifacts.contracts import (
    ArtifactProcessor,
    ArtifactReceipt,
    ArtifactWriteReceipt,
)

__all__ = [
    "ArtifactBundleRequest",
    "ArtifactCandidate",
    "ArtifactIndexRecord",
    "ArtifactProcessor",
    "ArtifactPublishReceipt",
    "ArtifactReceipt",
    "ArtifactReconcileResult",
    "ArtifactSourceResolver",
    "ArtifactStoreConfig",
    "ArtifactWriteReceipt",
    "BoundedArtifactSourceResolver",
    "MaterializedArtifact",
]
