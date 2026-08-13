# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Content-addressed artifact values and family-owned workflows."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from archetype.artifacts.models import (
    ArtifactContext,
    ArtifactRef,
    ArtifactSource,
    ArtifactStoreConfig,
)

_LAZY_EXPORTS = {
    "analyze_artifacts": ("archetype.artifacts.context", "analyze_artifacts"),
    "synthesize_artifact_context": (
        "archetype.artifacts.context",
        "synthesize_artifact_context",
    ),
}

__all__ = [
    "ArtifactContext",
    "ArtifactRef",
    "ArtifactSource",
    "ArtifactStoreConfig",
    "analyze_artifacts",
    "synthesize_artifact_context",
]


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
