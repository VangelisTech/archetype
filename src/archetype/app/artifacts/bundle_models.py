# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Public models for durable sandbox-artifact finalization.

The full sandbox checkpoint is deliberately a provider-native recovery
object.  ``ArtifactCandidate`` values name the smaller, portable files that
must be copied out of that checkpoint and indexed independently.
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Literal, Protocol, runtime_checkable

from daft.io import IOConfig
from pydantic import BaseModel, Field, field_validator, model_validator

from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.core.config import StorageBackend, StorageConfig

ArtifactRetention = Literal["attempt", "run", "durable"]
ArtifactStorageKind = Literal["object", "provider_checkpoint"]


class ArtifactPublicationStatus(StrEnum):
    """Typed durable phases for one portable artifact publication."""

    PENDING = "pending"
    UPLOADED = "uploaded"
    INDEXED = "indexed"
    EXPIRED = "expired"


_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class MaterializedArtifact(BaseModel):
    """One local file produced by an ``ArtifactSourceResolver``."""

    model_config = dict(frozen=True)

    path: Path
    source_ref: str
    logical_path: str
    kind: str


class ArtifactSourceResolver(Protocol):
    """Materialize immutable source references into ordinary local files.

    This two-argument method is the stable provider contract. Resolvers that
    can reject oversized provider objects before copying them may additionally
    implement :class:`BoundedArtifactSourceResolver`.
    """

    async def materialize(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
    ) -> list[MaterializedArtifact]: ...


@runtime_checkable
class BoundedArtifactSourceResolver(ArtifactSourceResolver, Protocol):
    """Optional provider capability for resource-bounded materialization."""

    async def materialize_bounded(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
        *,
        max_artifact_bytes: int,
        max_bundle_bytes: int,
    ) -> list[MaterializedArtifact]: ...


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


class ArtifactCandidate(BaseModel):
    """One file or directory requested from a checkpoint.

    ``source_ref`` is immutable and provider-qualified, for example
    ``apple-container-rootfs:///tmp/rootfs.tar#/workspace/repo/result.json``.
    Direct local paths and ``file://`` references are also supported by the
    built-in resolver. Provider integrations can supply additional resolvers.
    """

    model_config = dict(frozen=True)

    source_ref: str = Field(description="Immutable provider or local source reference.")
    logical_path: str = Field(description="Portable relative path exposed in the index.")
    kind: str = Field(default="artifact", description="Free-form artifact classification.")
    recursive: bool = Field(default=False, description="Expand a directory recursively.")
    required: bool = Field(default=True, description="Fail publication when the source is absent.")

    @field_validator("source_ref", "kind")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("logical_path")
    @classmethod
    def _portable_path(cls, value: str) -> str:
        normalized = value.strip().replace("\\", "/").strip("/")
        path = PurePosixPath(normalized)
        if not normalized or path.is_absolute() or ".." in path.parts:
            raise ValueError("logical_path must be a non-empty portable relative path")
        return path.as_posix()


class ArtifactBundleRequest(BaseModel):
    """Immutable request to publish one sandbox attempt's evidence bundle."""

    model_config = dict(frozen=True)

    world_id: str
    run_id: str
    entity_id: int = 0
    tick: int = Field(ge=0)
    attempt_id: str
    idempotency_key: str
    redaction_policy_id: str = ""
    checkpoint_ref: str
    checkpoint_provider: str
    checkpoint_restorable: bool = True
    checkpoint_created_at_ms: int = Field(default=0, ge=0)
    checkpoint_expires_at_ms: int = Field(default=0, ge=0)
    accepted: bool = False
    retention: ArtifactRetention = "attempt"
    artifact_expires_at_ms: int = Field(default=0, ge=0)
    artifacts: tuple[ArtifactCandidate, ...]

    @field_validator(
        "world_id",
        "run_id",
        "attempt_id",
        "idempotency_key",
        "checkpoint_ref",
        "checkpoint_provider",
    )
    @classmethod
    def _required_identity(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("redaction_policy_id")
    @classmethod
    def _optional_policy_identity(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def _unique_logical_paths(self) -> ArtifactBundleRequest:
        if not self.artifacts:
            raise ValueError("an artifact bundle requires at least one candidate")
        paths = [candidate.logical_path for candidate in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("artifact candidate logical paths must be unique")
        if self.checkpoint_restorable and not self.checkpoint_ref:
            raise ValueError("a restorable checkpoint requires checkpoint_ref")
        if (
            self.checkpoint_created_at_ms
            and self.checkpoint_expires_at_ms
            and self.checkpoint_expires_at_ms <= self.checkpoint_created_at_ms
        ):
            raise ValueError("checkpoint expiration must be after checkpoint creation")
        return self

    def canonical_json(self) -> str:
        """Credential-free canonical identity material stored by the reconciler."""
        payload = self.model_dump(mode="json")
        payload["artifacts"] = sorted(
            payload["artifacts"],
            key=lambda value: (
                value["logical_path"],
                value["kind"],
                value["source_ref"],
            ),
        )
        return _canonical_json(payload)

    def digest(self) -> str:
        """Return the policy-independent producer identity.

        This historical method remains the catalog conflict identity.  The
        exact prepared request is authenticated separately by
        :meth:`request_digest` so a scanner upgrade does not create a second
        logical bundle while an in-flight request remains pinned to one policy.
        """

        # ``redaction_policy_id`` is bound by the service, not supplied by the
        # producer. Excluding it preserves producer idempotency across scanner
        # upgrades while the persisted canonical request still pins the exact
        # policy required to resume a PENDING publication.
        payload = json.loads(self.canonical_json())
        payload.pop("redaction_policy_id", None)
        return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()

    def producer_digest(self) -> str:
        """Return the stable producer identity used by the publication catalog."""

        return self.digest()

    def request_digest(self) -> str:
        """Authenticate the exact canonical request, including bound policy."""

        return hashlib.sha256(self.canonical_json().encode()).hexdigest()


class PreparedArtifactBundleRequest(BaseModel):
    """Immutable, scanned identity safe to persist before publication I/O.

    ``request_digest`` authenticates the exact bound canonical JSON while
    ``producer_digest`` preserves the logical publication identity across
    compatible redaction-policy upgrades.
    """

    model_config = dict(frozen=True)

    request_json: str
    request_digest: str
    publication_key: str
    producer_digest: str
    redaction_policy_id: str

    @field_validator("request_json", "redaction_policy_id")
    @classmethod
    def _required_prepared_value(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("request_digest", "publication_key", "producer_digest")
    @classmethod
    def _prepared_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("must be a lowercase SHA-256 digest")
        return value

    @model_validator(mode="after")
    def _authenticates_exact_request(self) -> PreparedArtifactBundleRequest:
        # Keep the deterministic publication identity owned by the control
        # catalog.  The narrow local import avoids duplicating its domain
        # separator here while keeping the public model import graph acyclic.
        from archetype.app.storage.catalog import artifact_publication_key

        try:
            request = ArtifactBundleRequest.model_validate_json(self.request_json)
        except (ValueError, TypeError) as exc:
            raise ValueError("request_json must encode an ArtifactBundleRequest") from exc
        if request.canonical_json() != self.request_json:
            raise ValueError("request_json must use the canonical artifact request encoding")
        if request.request_digest() != self.request_digest:
            raise ValueError("request_digest does not authenticate request_json")
        if request.producer_digest() != self.producer_digest:
            raise ValueError("producer_digest does not authenticate request_json")
        if request.redaction_policy_id != self.redaction_policy_id:
            raise ValueError("redaction_policy_id does not match request_json")
        expected_key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        if self.publication_key != expected_key:
            raise ValueError("publication_key does not match request_json")
        return self


class ArtifactIndexRecord(BaseModel):
    """One row in the queryable artifact index.

    Provider checkpoints use ``storage_kind='provider_checkpoint'`` and keep
    ``content_hash`` empty because the provider may not expose bytes or size.
    Portable objects always carry a lowercase SHA-256 digest and byte count.
    """

    model_config = dict(frozen=True)

    schema_version: Literal[1] = 1
    artifact_id: str
    bundle_id: str
    world_id: str
    run_id: str
    entity_id: int
    tick: int = Field(ge=0)
    attempt_id: str
    idempotency_key: str
    kind: str
    logical_path: str
    source_ref: str
    object_uri: str
    storage_kind: ArtifactStorageKind
    content_hash: str
    size_bytes: int = Field(ge=-1)
    mime_type: str
    checkpoint_provider: str
    checkpoint_ref: str
    restorable: bool
    accepted: bool
    retention: ArtifactRetention
    created_at_ms: int = Field(ge=0)
    expires_at_ms: int = Field(ge=0)

    @field_validator(
        "artifact_id",
        "bundle_id",
        "world_id",
        "run_id",
        "attempt_id",
        "idempotency_key",
        "kind",
        "logical_path",
        "source_ref",
        "object_uri",
        "mime_type",
        "checkpoint_provider",
        "checkpoint_ref",
    )
    @classmethod
    def _nonempty_index_value(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @model_validator(mode="after")
    def _integrity_contract(self) -> ArtifactIndexRecord:
        if not _SHA256_RE.fullmatch(self.artifact_id):
            raise ValueError("artifact_id must be a lowercase SHA-256 digest")
        if not _SHA256_RE.fullmatch(self.bundle_id):
            raise ValueError("bundle_id must be a lowercase SHA-256 digest")
        if self.expires_at_ms and self.expires_at_ms <= self.created_at_ms:
            raise ValueError("artifact expiration must be after creation")
        if self.storage_kind == "object":
            if not _SHA256_RE.fullmatch(self.content_hash):
                raise ValueError("portable objects require a lowercase SHA-256 content hash")
            if self.size_bytes < 0:
                raise ValueError("portable objects require a non-negative size")
            if self.restorable:
                raise ValueError("portable objects are evidence, not provider checkpoints")
        elif self.content_hash or self.size_bytes != -1:
            raise ValueError("provider checkpoints use an empty content hash and size_bytes=-1")
        return self


class ArtifactPublishReceipt(BaseModel):
    """Durable publication result returned by the service and command gate."""

    model_config = dict(frozen=True)

    bundle_id: str
    world_id: str
    run_id: str
    attempt_id: str
    status: ArtifactPublicationStatus
    duplicate: bool = False
    manifest_uri: str = ""
    index_snapshot_id: int = 0
    request_digest: str = ""
    producer_digest: str = ""
    redaction_policy_id: str = ""
    records: tuple[ArtifactIndexRecord, ...] = ()

    @field_validator("index_snapshot_id", mode="before")
    @classmethod
    def _exact_snapshot_integer(cls, value: object) -> int:
        if type(value) is not int:
            raise ValueError("artifact index_snapshot_id must be an exact integer")
        if value < 0 or value > MAX_ICEBERG_SNAPSHOT_ID:
            raise ValueError("artifact index_snapshot_id is outside the signed 64-bit range")
        return value

    @model_validator(mode="after")
    def _snapshot_matches_status(self) -> ArtifactPublishReceipt:
        if self.status is ArtifactPublicationStatus.INDEXED:
            if self.index_snapshot_id < 1:
                raise ValueError("indexed artifact receipt requires a positive index snapshot")
        elif self.index_snapshot_id != 0:
            raise ValueError("only an indexed artifact receipt may carry an index snapshot")
        return self


class ArtifactReconcileResult(BaseModel):
    """One bounded reconciler pass for a world."""

    model_config = dict(frozen=True)

    examined: int = Field(default=0, ge=0)
    indexed: int = Field(default=0, ge=0)
    expired: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    bundle_ids: tuple[str, ...] = ()


class ArtifactStoreConfig(BaseModel):
    """Object destination, Iceberg index, and retry/lifecycle policy.

    Credentials live only in ``io_config`` and are never serialized into a
    publication request or control-catalog row.
    """

    model_config = dict(frozen=True, arbitrary_types_allowed=True)

    object_uri: str | Path
    index_storage: StorageConfig
    io_config: IOConfig | None = None
    max_connections: int = Field(default=32, ge=1)
    lease_seconds: float = Field(default=900.0, gt=0)
    retry_delay_seconds: float = Field(default=30.0, ge=0)
    retry_window_seconds: int = Field(default=7 * 24 * 60 * 60, ge=60)
    attempt_retention_seconds: int = Field(default=30 * 24 * 60 * 60, ge=0)
    run_retention_seconds: int = Field(default=180 * 24 * 60 * 60, ge=0)
    max_artifact_bytes: int = Field(default=1 << 30, gt=0)
    max_bundle_bytes: int = Field(default=4 << 30, gt=0)

    @model_validator(mode="after")
    def _iceberg_index(self) -> ArtifactStoreConfig:
        if self.index_storage.backend != StorageBackend.ICEBERG:
            raise ValueError("artifact index_storage must use StorageBackend.ICEBERG")
        if self.max_bundle_bytes < self.max_artifact_bytes:
            raise ValueError("max_bundle_bytes must be >= max_artifact_bytes")
        return self

    @classmethod
    def local(cls, root: str | Path) -> ArtifactStoreConfig:
        """Build a self-contained local object store and Iceberg index."""
        base = Path(root).expanduser()
        return cls(
            object_uri=base / "objects",
            index_storage=StorageConfig(
                uri=base / "index",
                namespace="artifacts",
                backend=StorageBackend.ICEBERG,
            ),
        )

    def retention_seconds(self, retention: ArtifactRetention) -> int:
        if retention == "attempt":
            return self.attempt_retention_seconds
        if retention == "run":
            return self.run_retention_seconds
        return 0
