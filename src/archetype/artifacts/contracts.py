# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""One-release object-identity shim for canonical artifact models."""

from archetype.artifacts.models import (
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
]
