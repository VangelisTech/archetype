# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Immutable values for the version 0.7 native runtime contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from archetype_native.config import RemoteData as RemoteData
from archetype_native.ingress import Component as ComponentProjection
from archetype_native.programs import (
    Composition,
    Connection,
    Endpoint,
    InputPort,
    LeafProgram,
    OutputPort,
    ProgramNode,
    ProgramReference,
    Relation,
)
from archetype_native.wire import Boundary


@dataclass(frozen=True, slots=True)
class WorldStatus:
    state: str
    generation: int
    revision: int | None
    has_error: bool
    lineage_ready: bool | None = None

    @classmethod
    def _decode(cls, value: dict[str, Any]) -> WorldStatus:
        return cls(
            value["state"],
            int(value["generation"]),
            None if value["revision"] is None else int(value["revision"]),
            value["has_error"],
            value.get("lineage_ready"),
        )


@dataclass(frozen=True, slots=True)
class Admission:
    generation: int
    admission_key: str
    state: str
    publication: str
    applied_revision: int | None
    has_error: bool
    boundary: Boundary | None

    @classmethod
    def _decode(cls, value: dict[str, Any]) -> Admission:
        return cls(
            int(value["generation"]),
            value["admission_key"],
            value["state"],
            value["publication"],
            None if value["applied_revision"] is None else int(value["applied_revision"]),
            value["has_error"],
            None if "boundary" not in value else Boundary.decode(value["boundary"]),
        )


@dataclass(frozen=True, slots=True)
class RowPage:
    fields: tuple[str, ...]
    rows: tuple[tuple[int | str | bool | float, ...], ...]
    total_rows: int
    next_offset: int | None


@dataclass(frozen=True, slots=True)
class ArtifactContextInfo:
    world: str
    run: str
    context_id: str
    origin: str


__all__ = [
    "RemoteData",
    "ComponentProjection",
    "Composition",
    "Connection",
    "Endpoint",
    "InputPort",
    "LeafProgram",
    "OutputPort",
    "ProgramNode",
    "ProgramReference",
    "Relation",
    "Boundary",
    "WorldStatus",
    "Admission",
    "RowPage",
    "ArtifactContextInfo",
    "ArtifactOccurrence",
    "ArtifactPage",
    "ArtifactUploadReceipt",
    "PreparedArtifacts",
]


@dataclass(frozen=True, slots=True)
class ArtifactOccurrence:
    artifact_id: str
    context_id: str
    exact_cut: tuple[int, str] | None
    sha256: str
    media_type: str
    size_bytes: int
    common: tuple[tuple[str, int | str | bool | float | datetime | None], ...]
    typed: tuple[
        tuple[str, tuple[tuple[str, int | str | bool | float | datetime | None], ...]], ...
    ]

    def facts(self, index: str = "files"):
        """Immutable verified index facts, including exact occurrence attribution."""
        if index == "files":
            return self.common
        return next((facts for name, facts in self.typed if name == index), ())


@dataclass(frozen=True, slots=True)
class ArtifactPage:
    items: tuple[ArtifactOccurrence, ...]
    total: int
    next_offset: int | None


@dataclass(frozen=True, slots=True)
class ArtifactUploadReceipt:
    artifact_id: str
    context_id: str
    exact_cut: tuple[int, str] | None
    sha256: str
    logical_path: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class PreparedArtifacts:
    """Immutable preparation retained for exact publication retry; no owner inside."""

    context_id: str
    exact_cut: tuple[int, str] | None
    artifact_ids: tuple[str, ...]
    _prepared: Any = field(repr=False, compare=False)
