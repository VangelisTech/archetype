# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Archetype 0.7: immutable programs, native live worlds and complete Iceberg cuts.

The root facade imports no analytical engine or native shared library. Install
``archetype-ecs[analysis]`` for explicit analysis outside live execution.
"""

from __future__ import annotations

from importlib import import_module
from pkgutil import extend_path
from typing import Any

__path__ = extend_path(__path__, __name__)
__version__ = "0.7.0"
_EXPORTS: dict[str, tuple[str, str]] = {
    "ArchetypeRuntime": ("archetype.runtime", "ArchetypeRuntime"),
    "SyncArchetypeRuntime": ("archetype.runtime", "SyncArchetypeRuntime"),
    "RuntimeOperationError": ("archetype.runtime", "RuntimeOperationError"),
    "RuntimeWorld": ("archetype.runtime", "RuntimeWorld"),
    "SyncRuntimeWorld": ("archetype.runtime", "SyncRuntimeWorld"),
    "RuntimeProgram": ("archetype.runtime", "RuntimeProgram"),
    "SyncRuntimeProgram": ("archetype.runtime", "SyncRuntimeProgram"),
    "RuntimeCut": ("archetype.runtime", "RuntimeCut"),
    "SyncRuntimeCut": ("archetype.runtime", "SyncRuntimeCut"),
    "RuntimeArtifacts": ("archetype.runtime", "RuntimeArtifacts"),
    "SyncRuntimeArtifacts": ("archetype.runtime", "SyncRuntimeArtifacts"),
    "Change": ("archetype.runtime", "Change"),
    "CutPage": ("archetype.runtime", "CutPage"),
    "ComponentProjection": ("archetype.runtime", "ComponentProjection"),
    "Composition": ("archetype.runtime", "Composition"),
    "Connection": ("archetype.runtime", "Connection"),
    "Endpoint": ("archetype.runtime", "Endpoint"),
    "InputPort": ("archetype.runtime", "InputPort"),
    "LeafProgram": ("archetype.runtime", "LeafProgram"),
    "OutputPort": ("archetype.runtime", "OutputPort"),
    "ProgramNode": ("archetype.runtime", "ProgramNode"),
    "ProgramReference": ("archetype.runtime", "ProgramReference"),
    "Relation": ("archetype.runtime", "Relation"),
    "Boundary": ("archetype.runtime", "Boundary"),
    "WorldStatus": ("archetype.runtime", "WorldStatus"),
    "Admission": ("archetype.runtime", "Admission"),
    "RowPage": ("archetype.runtime", "RowPage"),
    "ArtifactContextInfo": ("archetype.runtime", "ArtifactContextInfo"),
    "ArtifactOccurrence": ("archetype.runtime", "ArtifactOccurrence"),
    "ArtifactPage": ("archetype.runtime", "ArtifactPage"),
    "PreparedArtifacts": ("archetype.runtime", "PreparedArtifacts"),
    "ArtifactSource": ("archetype.artifacts.models", "ArtifactSource"),
    "ArtifactUploadReceipt": ("archetype.runtime", "ArtifactUploadReceipt"),
    "run_sync": ("archetype.runtime", "run_sync"),
    "entrypoint": ("archetype.runtime.entrypoint", "entrypoint"),
    "public_api": ("archetype._api", "public_api"),
    "Component": ("archetype.core.component", "Component"),
    "Outcome": ("archetype.evaluation.contracts", "Outcome"),
    "GraderContract": ("archetype.evaluation.contracts", "GraderContract"),
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
    value = getattr(import_module(target[0]), target[1])
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
