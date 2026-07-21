# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reusable contracts and transforms for content-addressed file artifacts."""

from archetype.artifacts.context import analyze_artifacts, synthesize_artifact_context
from archetype.artifacts.contracts import (
    ArtifactContext,
    ArtifactRef,
    ArtifactSource,
    ArtifactStoreConfig,
)

__all__ = [
    "ArtifactContext",
    "ArtifactRef",
    "ArtifactSource",
    "ArtifactStoreConfig",
    "analyze_artifacts",
    "synthesize_artifact_context",
]
