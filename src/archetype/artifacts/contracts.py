# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reusable file-artifact inputs, references, and object-store bounds."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from daft.io import IOConfig
from pydantic import BaseModel, Field, computed_field, field_validator, model_validator
from uuid_utils import UUID, uuid7

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_XXHASH3_64 = re.compile(r"^[0-9a-f]{16}$")


def _uuidv7_string() -> str:
    return str(uuid7())


def _portable_path(value: str, *, optional: bool) -> str:
    normalized = value.strip().replace("\\", "/").strip("/")
    if not normalized and optional:
        return ""
    path = PurePosixPath(normalized)
    if not normalized or path.is_absolute() or ".." in path.parts:
        raise ValueError("must be a portable relative path")
    return path.as_posix()


class ArtifactSource(BaseModel):
    """One file, glob, or recursive prefix submitted for artifact ingestion."""

    model_config = dict(frozen=True)

    source_uri: str
    logical_root: str = ""
    logical_path: str = ""
    recursive: bool = False
    required: bool = True

    @field_validator("source_uri")
    @classmethod
    def _source_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("source_uri must not be empty")
        return value

    @field_validator("logical_root")
    @classmethod
    def _valid_logical_root(cls, value: str) -> str:
        return _portable_path(value, optional=True)

    @field_validator("logical_path")
    @classmethod
    def _valid_logical_path(cls, value: str) -> str:
        return _portable_path(value, optional=True)

    @model_validator(mode="after")
    def _directory_uses_root(self) -> ArtifactSource:
        if self.recursive and self.logical_path:
            raise ValueError("recursive sources use logical_root, not logical_path")
        return self


class ArtifactRef(BaseModel):
    """Portable reference to one indexed, content-addressed file occurrence."""

    model_config = dict(frozen=True)

    artifact_id: str
    logical_path: str
    uri: str
    sha256: str
    xxhash3_64: str
    media_type: str
    size_bytes: int = Field(ge=0)

    @field_validator("artifact_id")
    @classmethod
    def _uuidv7_identity(cls, value: str) -> str:
        parsed = UUID(value)
        if parsed.version != 7:
            raise ValueError("artifact_id must be UUIDv7")
        return str(parsed)

    @field_validator("logical_path")
    @classmethod
    def _logical_path(cls, value: str) -> str:
        return _portable_path(value, optional=False)

    @field_validator("uri", "media_type")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("sha256")
    @classmethod
    def _sha256_digest(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("sha256 must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("xxhash3_64")
    @classmethod
    def _fast_digest(cls, value: str) -> str:
        if not _XXHASH3_64.fullmatch(value):
            raise ValueError("xxhash3_64 must be a lowercase XXH3-64 hex digest")
        return value

    @computed_field
    @property
    def ingested_at(self) -> datetime:
        """Derive ingestion time from the UUIDv7 identity."""

        return datetime.fromtimestamp(UUID(self.artifact_id).timestamp / 1000, tz=UTC)


class ArtifactContext(BaseModel):
    """One task-scoped interpretation of an immutable artifact set."""

    model_config = dict(frozen=True)

    task: str = Field(description="Authoritative task applied to every artifact")
    context_id: str = Field(
        default_factory=_uuidv7_string,
        description="UUIDv7 identity for this interpretation",
    )

    @field_validator("task")
    @classmethod
    def _task_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("artifact context task must not be empty")
        return value

    @field_validator("context_id")
    @classmethod
    def _uuidv7_context(cls, value: str) -> str:
        parsed = UUID(value)
        if parsed.version != 7:
            raise ValueError("artifact context_id must be UUIDv7")
        return str(parsed)


class ArtifactStoreConfig(BaseModel):
    """Bound object copying while leaving index authority in the world catalog."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)

    object_uri: str | Path | None = None
    io_config: IOConfig | None = None
    max_connections: int = Field(default=32, ge=1)
    max_artifact_bytes: int = Field(default=1 << 30, gt=0)
    max_ingestion_bytes: int = Field(default=4 << 30, gt=0)

    @model_validator(mode="after")
    def _bounded_batch(self) -> ArtifactStoreConfig:
        if self.max_ingestion_bytes < self.max_artifact_bytes:
            raise ValueError("max_ingestion_bytes must be >= max_artifact_bytes")
        return self

    @classmethod
    def local(cls, root: str | Path) -> ArtifactStoreConfig:
        """Place content-addressed objects beneath a local root."""

        return cls(object_uri=Path(root))
