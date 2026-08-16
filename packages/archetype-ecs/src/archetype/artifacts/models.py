# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Import-light values and exact operations owned by the artifact family."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
)
from uuid_utils import UUID, uuid7

if TYPE_CHECKING:
    from daft.io import IOConfig as IOConfigValue

    from archetype.core.config import StorageConfig as StorageConfigValue
else:
    # Artifact operation/value contracts are imported by routing and plugins.
    # Runtime validators restore the exact supported types only when callers
    # actually construct values carrying those heavyweight configurations.
    IOConfigValue = object
    StorageConfigValue = object

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


def _storage_config_value(value: object) -> object:
    """Lazily validate or reconstruct the supported storage value."""

    from archetype.core.config import StorageConfig

    if isinstance(value, StorageConfig):
        return value
    if isinstance(value, Mapping):
        return StorageConfig.model_validate(value)
    raise ValueError("storage_config must be a StorageConfig")


class ArtifactSource(BaseModel):
    """One exact file or Daft-readable pattern submitted for ingestion."""

    model_config = ConfigDict(frozen=True)

    source_uri: str
    logical_path: str = ""
    required: bool = True

    @field_validator("source_uri")
    @classmethod
    def _source_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("source_uri must not be empty")
        return value

    @field_validator("logical_path")
    @classmethod
    def _valid_logical_path(cls, value: str) -> str:
        return _portable_path(value, optional=True)


class ArtifactRef(BaseModel):
    """Portable reference to one indexed, content-addressed file occurrence."""

    model_config = ConfigDict(frozen=True)

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

    model_config = ConfigDict(frozen=True)

    task: str = Field(description="Authoritative task applied to every artifact")
    artifact_ids: tuple[str, ...] = Field(
        description="UUIDv7 occurrence identities selected as evidence",
    )
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

    @field_validator("artifact_ids")
    @classmethod
    def _artifact_occurrences_required(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("artifact context must name at least one artifact occurrence")
        normalized: list[str] = []
        for artifact_id in value:
            parsed = UUID(artifact_id)
            if parsed.version != 7:
                raise ValueError("artifact context IDs must be UUIDv7")
            normalized.append(str(parsed))
        if len(set(normalized)) != len(normalized):
            raise ValueError("artifact context IDs must be unique")
        return tuple(normalized)

    @field_validator("context_id")
    @classmethod
    def _uuidv7_context(cls, value: str) -> str:
        parsed = UUID(value)
        if parsed.version != 7:
            raise ValueError("artifact context_id must be UUIDv7")
        return str(parsed)


class ArtifactStoreConfig(BaseModel):
    """Configure content-addressed object storage and Daft file I/O."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    object_uri: str | Path | None = None
    io_config: IOConfigValue | None = None
    max_connections: int = Field(default=32, ge=1)

    @field_validator("io_config")
    @classmethod
    def _supported_io_config(cls, value: object | None) -> object | None:
        if value is None:
            return None
        from daft.io import IOConfig

        if not isinstance(value, IOConfig):
            raise ValueError("io_config must be a Daft IOConfig")
        return value

    @classmethod
    def local(cls, root: str | Path) -> ArtifactStoreConfig:
        """Place content-addressed objects beneath a local root."""

        return cls(object_uri=Path(root))


def resolve_artifact_object_root(
    storage_config: StorageConfigValue,
    store_config: ArtifactStoreConfig,
) -> str:
    """Resolve the one object authority paired with a storage endpoint.

    Keep this address rule shared by ordinary ingestion and offline migration;
    otherwise the two workflows can silently derive different content paths
    from the same endpoint configuration.
    """

    from archetype.core.config import StorageConfig
    from archetype.core.paths import local_storage_path

    if not isinstance(storage_config, StorageConfig):
        raise TypeError("artifact object-root resolution requires a StorageConfig")
    if not isinstance(store_config, ArtifactStoreConfig):
        raise TypeError("artifact object-root resolution requires an ArtifactStoreConfig")
    if store_config.object_uri is not None:
        return str(store_config.object_uri)
    local = local_storage_path(str(storage_config.uri))
    if local is not None:
        return str(local / "artifacts")
    return str(storage_config.uri).rstrip("/") + "/artifacts"


class _ArtifactOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str

    @field_validator("world_id", check_fields=False, mode="before")
    @classmethod
    def _string_world_id(cls, value: object) -> str:
        if isinstance(value, UUID):
            return str(value)
        if not isinstance(value, str):
            raise ValueError("world_id must be a string or UUID")
        normalized = value.strip()
        if not normalized:
            raise ValueError("world_id must not be empty")
        return normalized


class IngestArtifacts(_ArtifactOperation):
    """Ingest declared artifact sources for one durable world."""

    operation: Literal["ingest_artifacts"] = "ingest_artifacts"
    world_id: str
    sources: tuple[ArtifactSource, ...]
    storage_config: StorageConfigValue

    @field_validator("sources")
    @classmethod
    def _sources_required(
        cls,
        value: tuple[ArtifactSource, ...],
    ) -> tuple[ArtifactSource, ...]:
        if not value:
            raise ValueError("artifact ingestion requires at least one source")
        return value

    @field_validator("storage_config")
    @classmethod
    def _supported_storage_config(cls, value: object) -> object:
        return _storage_config_value(value)


class QueryArtifacts(_ArtifactOperation):
    """Read the common artifact index for one durable world."""

    operation: Literal["query_artifacts"] = "query_artifacts"
    world_id: str
    storage_config: StorageConfigValue

    @field_validator("storage_config")
    @classmethod
    def _supported_storage_config(cls, value: object) -> object:
        return _storage_config_value(value)


def summarize_artifact_operation(
    operation: IngestArtifacts | QueryArtifacts,
) -> Mapping[str, Any]:
    """Return bounded routing identity without source or storage contents."""

    return {
        "operation": operation.operation,
        "world_id": str(operation.world_id),
    }


__all__ = [
    "ArtifactContext",
    "ArtifactRef",
    "ArtifactSource",
    "ArtifactStoreConfig",
    "IngestArtifacts",
    "QueryArtifacts",
    "resolve_artifact_object_root",
    "summarize_artifact_operation",
]
