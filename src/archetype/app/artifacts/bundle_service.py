# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable, idempotent publication of sandbox evidence bundles."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import mimetypes
import shutil
import tarfile
import tempfile
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, TypedDict, cast
from urllib.parse import quote, unquote, urlparse

import daft
import pyarrow as pa
from daft import DataFrame, DataType, col
from daft.exceptions import DaftCoreException
from daft.functions import download, guess_mime_type, upload
from uuid_utils import uuid7

from archetype import _obs
from archetype.app.artifacts.bundle_models import PreparedArtifactBundleRequest
from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.models import RedactionReceipt
from archetype.app.storage.catalog import (
    ArtifactPublicationExpiredError,
    ArtifactPublicationRecord,
    artifact_publication_key,
)
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.artifacts.bundles import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactIndexRecord,
    ArtifactPublicationStatus,
    ArtifactPublishReceipt,
    ArtifactReconcileCandidate,
    ArtifactReconcileDisposition,
    ArtifactReconcileItemResult,
    ArtifactReconcileResult,
    ArtifactSourceResolver,
    ArtifactStoreConfig,
    BoundedArtifactSourceResolver,
    MaterializedArtifact,
    _canonical_json,
)
from archetype.core.config import StorageConfig

if TYPE_CHECKING:
    from archetype.app.storage.catalog import ControlCatalog

_ARTIFACT_INDEX_TABLE = "artifact_index_v1"
_ROOTFS_SCHEME = "apple-container-rootfs://"
_MAX_FAILURE_DETAIL_CHARS = 4096
_DEFAULT_MAX_ARTIFACT_BYTES = 1 << 30
_DEFAULT_MAX_BUNDLE_BYTES = 4 << 30
logger = logging.getLogger(__name__)


class _FileMetadata(TypedDict):
    local_uri: str
    source_ref: str
    logical_path: str
    kind: str
    content_hash: str
    size_bytes: int
    mime_type: str


class _PreparedArtifact(_FileMetadata):
    artifact_id: str
    destination: str


class _UploadedArtifact(_PreparedArtifact):
    object_uri: str


@daft.func(return_dtype=DataType.string())
def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@daft.func(return_dtype=DataType.int64())
def _bytes_len(value: bytes) -> int:
    return len(value)


class CheckpointArtifactSourceResolver:
    """Resolve direct local files and Apple Container rootfs exports.

    Modal and future providers implement the same protocol and are injected
    into ``ArtifactService``. The app service never imports a provider SDK.
    """

    async def materialize(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
    ) -> list[MaterializedArtifact]:
        return await self.materialize_bounded(
            candidates,
            destination,
            max_artifact_bytes=_DEFAULT_MAX_ARTIFACT_BYTES,
            max_bundle_bytes=_DEFAULT_MAX_BUNDLE_BYTES,
        )

    async def materialize_bounded(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
        *,
        max_artifact_bytes: int,
        max_bundle_bytes: int,
    ) -> list[MaterializedArtifact]:
        if max_artifact_bytes < 1:
            raise ValueError("max_artifact_bytes must be positive")
        if max_bundle_bytes < max_artifact_bytes:
            raise ValueError("max_bundle_bytes must be >= max_artifact_bytes")
        return await asyncio.to_thread(
            self._materialize_sync,
            candidates,
            destination,
            max_artifact_bytes,
            max_bundle_bytes,
        )

    def _materialize_sync(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
        max_artifact_bytes: int,
        max_bundle_bytes: int,
    ) -> list[MaterializedArtifact]:
        destination.mkdir(parents=True, exist_ok=True)
        direct: list[ArtifactCandidate] = []
        archives: dict[Path, list[tuple[ArtifactCandidate, str]]] = defaultdict(list)
        for candidate in candidates:
            if candidate.source_ref.startswith(_ROOTFS_SCHEME):
                archive, member = self._split_rootfs_ref(candidate.source_ref)
                archives[archive].append((candidate, member))
            else:
                parsed = urlparse(candidate.source_ref)
                if parsed.scheme not in ("", "file"):
                    raise ValueError(
                        f"no artifact source resolver is registered for {parsed.scheme!r}"
                    )
                direct.append(candidate)

        resolved = self._materialize_direct(direct)
        materialized_bytes = self._bounded_existing_size(
            resolved,
            max_artifact_bytes=max_artifact_bytes,
            max_bundle_bytes=max_bundle_bytes,
        )
        for archive, requested in archives.items():
            extracted, materialized_bytes = self._materialize_archive(
                archive,
                requested,
                destination,
                materialized_bytes=materialized_bytes,
                max_artifact_bytes=max_artifact_bytes,
                max_bundle_bytes=max_bundle_bytes,
            )
            resolved.extend(extracted)
        self._reject_logical_collisions(resolved)
        return sorted(resolved, key=lambda value: value.logical_path)

    @staticmethod
    def _bounded_existing_size(
        values: list[MaterializedArtifact],
        *,
        max_artifact_bytes: int,
        max_bundle_bytes: int,
    ) -> int:
        total = 0
        for value in values:
            size = value.path.stat().st_size
            if size > max_artifact_bytes:
                raise ValueError(
                    f"artifact {value.logical_path!r} is {size} bytes; "
                    f"limit is {max_artifact_bytes}"
                )
            total += size
            if total > max_bundle_bytes:
                raise ValueError(
                    f"artifact bundle is at least {total} bytes; limit is {max_bundle_bytes}"
                )
        return total

    @staticmethod
    def _split_rootfs_ref(source_ref: str) -> tuple[Path, str]:
        value = source_ref.removeprefix(_ROOTFS_SCHEME)
        archive_value, marker, member_value = value.rpartition("#")
        if not marker or not archive_value or not member_value:
            raise ValueError(
                "Apple Container artifact refs require "
                "apple-container-rootfs://<archive>#<absolute-path>"
            )
        archive = Path(unquote(archive_value)).expanduser().resolve()
        if not archive.is_file():
            raise FileNotFoundError(f"Apple Container checkpoint does not exist: {archive}")
        member = CheckpointArtifactSourceResolver._safe_member_path(member_value)
        return archive, member

    @staticmethod
    def _safe_member_path(value: str) -> str:
        path = PurePosixPath(unquote(value).lstrip("/"))
        if not path.parts or ".." in path.parts:
            raise ValueError(f"unsafe checkpoint member path: {value!r}")
        return path.as_posix()

    @staticmethod
    def _local_path(source_ref: str) -> Path:
        parsed = urlparse(source_ref)
        value = unquote(parsed.path) if parsed.scheme == "file" else source_ref
        return Path(value).expanduser().resolve()

    def _materialize_direct(
        self, candidates: list[ArtifactCandidate]
    ) -> list[MaterializedArtifact]:
        resolved: list[MaterializedArtifact] = []
        for candidate in candidates:
            source = self._local_path(candidate.source_ref)
            if not source.exists():
                if candidate.required:
                    raise FileNotFoundError(f"required artifact source does not exist: {source}")
                continue
            if source.is_dir():
                if not candidate.recursive:
                    raise IsADirectoryError(
                        f"artifact source is a directory but recursive=False: {source}"
                    )
                files = [
                    path for path in source.rglob("*") if path.is_file() and not path.is_symlink()
                ]
                if not files and candidate.required:
                    raise FileNotFoundError(f"required artifact directory is empty: {source}")
                for path in files:
                    relative = path.relative_to(source).as_posix()
                    resolved.append(
                        MaterializedArtifact(
                            path=path,
                            source_ref=f"{candidate.source_ref.rstrip('/')}/{relative}",
                            logical_path=(
                                PurePosixPath(candidate.logical_path) / relative
                            ).as_posix(),
                            kind=candidate.kind,
                        )
                    )
                continue
            if candidate.recursive:
                raise NotADirectoryError(f"artifact source is a file but recursive=True: {source}")
            resolved.append(
                MaterializedArtifact(
                    path=source,
                    source_ref=candidate.source_ref,
                    logical_path=candidate.logical_path,
                    kind=candidate.kind,
                )
            )
        return resolved

    def _materialize_archive(
        self,
        archive: Path,
        requested: list[tuple[ArtifactCandidate, str]],
        destination: Path,
        *,
        materialized_bytes: int,
        max_artifact_bytes: int,
        max_bundle_bytes: int,
    ) -> tuple[list[MaterializedArtifact], int]:
        matches = [0] * len(requested)
        resolved: list[MaterializedArtifact] = []
        archive_destination = destination / hashlib.sha256(str(archive).encode()).hexdigest()[:16]
        archive_destination.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, mode="r:*") as rootfs:
            for member in rootfs:
                if not member.isfile():
                    continue
                member_name = self._safe_member_path(member.name)
                for index, (candidate, root) in enumerate(requested):
                    relative: str | None = None
                    if candidate.recursive and member_name.startswith(root.rstrip("/") + "/"):
                        relative = member_name[len(root.rstrip("/")) + 1 :]
                    elif not candidate.recursive and member_name == root:
                        relative = ""
                    if relative is None:
                        continue
                    stream = rootfs.extractfile(member)
                    if stream is None:
                        continue
                    logical_path = (
                        (PurePosixPath(candidate.logical_path) / relative).as_posix()
                        if relative
                        else candidate.logical_path
                    )
                    size = int(member.size)
                    if size > max_artifact_bytes:
                        raise ValueError(
                            f"artifact {logical_path!r} is {size} bytes; "
                            f"limit is {max_artifact_bytes}"
                        )
                    next_total = materialized_bytes + size
                    if next_total > max_bundle_bytes:
                        raise ValueError(
                            f"artifact bundle would be at least {next_total} bytes; "
                            f"limit is {max_bundle_bytes}"
                        )
                    output = archive_destination / f"{len(resolved):08d}-{Path(member_name).name}"
                    with output.open("wb") as target:
                        shutil.copyfileobj(stream, target, length=1 << 20)
                    materialized_bytes = next_total
                    matches[index] += 1
                    source_ref = f"{_ROOTFS_SCHEME}{archive}#/{member_name}"
                    resolved.append(
                        MaterializedArtifact(
                            path=output,
                            source_ref=source_ref,
                            logical_path=logical_path,
                            kind=candidate.kind,
                        )
                    )

        for index, (candidate, member) in enumerate(requested):
            if matches[index] == 0 and candidate.required:
                raise FileNotFoundError(
                    f"required artifact {member!r} is absent from checkpoint {archive}"
                )
        return resolved, materialized_bytes

    @staticmethod
    def _reject_logical_collisions(values: list[MaterializedArtifact]) -> None:
        seen: set[str] = set()
        for value in values:
            if value.logical_path in seen:
                raise ValueError(f"multiple artifact sources resolve to {value.logical_path!r}")
            seen.add(value.logical_path)


class ArtifactBundleService:
    """Publish portable evidence and index it by world/run/attempt.

    The control catalog is the retry authority. Object upload precedes the
    Iceberg append; uploaded metadata is persisted between those stages.
    Therefore every crash point has a deterministic continuation.
    """

    def __init__(
        self,
        storage_service: iStorageService,
        world_service: iWorldService,
        config: ArtifactStoreConfig | None = None,
        source_resolver: ArtifactSourceResolver | None = None,
        *,
        redaction_service: iRedactionService,
    ) -> None:
        self._storage_service = storage_service
        self._world_service = world_service
        self._config = config
        self._source_resolver = source_resolver or CheckpointArtifactSourceResolver()
        self._redaction_service = redaction_service

    @property
    def enabled(self) -> bool:
        return self._config is not None

    def _bind_redaction_policy(self, request: ArtifactBundleRequest) -> ArtifactBundleRequest:
        policy_id = self._redaction_service.policy_id
        if request.redaction_policy_id and request.redaction_policy_id != policy_id:
            raise ValueError(
                "artifact request redaction_policy_id does not match the active policy"
            )
        bound = (
            request
            if request.redaction_policy_id
            else request.model_copy(update={"redaction_policy_id": policy_id})
        )
        self._assert_request_metadata_safe(bound)
        self._assert_object_root_safe()
        return bound

    def _assert_object_root_safe(self) -> None:
        """Quarantine unsafe destination metadata before the next external write."""

        self._redaction_service.assert_safe_metadata(
            self._normalized_object_uri(),
            field="artifact_store.object_uri",
        )

    def prepare(self, request: ArtifactBundleRequest) -> PreparedArtifactBundleRequest:
        """Bind and scan one request without touching sources, objects, or indexes."""

        self._require_config()
        bound = self._bind_redaction_policy(request)
        return PreparedArtifactBundleRequest(
            request_json=bound.canonical_json(),
            request_digest=bound.request_digest(),
            publication_key=artifact_publication_key(
                bound.world_id,
                bound.run_id,
                bound.idempotency_key,
            ),
            producer_digest=bound.producer_digest(),
            redaction_policy_id=bound.redaction_policy_id,
        )

    def _assert_request_metadata_safe(self, request: ArtifactBundleRequest) -> None:
        values = {
            "world_id": request.world_id,
            "run_id": request.run_id,
            "attempt_id": request.attempt_id,
            "idempotency_key": request.idempotency_key,
            "redaction_policy_id": request.redaction_policy_id,
            "checkpoint_ref": request.checkpoint_ref,
            "checkpoint_provider": request.checkpoint_provider,
        }
        for field, value in values.items():
            self._redaction_service.assert_safe_metadata(
                value,
                field=f"artifact_request.{field}",
            )
        for index, candidate in enumerate(request.artifacts):
            for field in ("source_ref", "logical_path", "kind"):
                self._redaction_service.assert_safe_metadata(
                    str(getattr(candidate, field)),
                    field=f"artifact_request.artifacts[{index}].{field}",
                )

    def _assert_records_safe(self, records: tuple[ArtifactIndexRecord, ...]) -> None:
        for index, record in enumerate(records):
            for field, value in record.model_dump(mode="python").items():
                if isinstance(value, str):
                    self._redaction_service.assert_safe_metadata(
                        value,
                        field=f"artifact_index[{index}].{field}",
                    )

    def _assert_records_bound_to_request(
        self,
        request: ArtifactBundleRequest,
        bundle_id: str,
        records: tuple[ArtifactIndexRecord, ...],
        manifest_uri: str,
        *,
        created_at_ms: int,
    ) -> None:
        """Authenticate durable index rows against their exact publication request."""

        if not records:
            raise ValueError("artifact publication requires durable index records")
        if len({record.artifact_id for record in records}) != len(records):
            raise ValueError("artifact publication contains duplicate artifact IDs")
        if len({record.logical_path for record in records}) != len(records):
            raise ValueError("artifact publication contains duplicate logical paths")

        common = {
            "bundle_id": bundle_id,
            "world_id": request.world_id,
            "run_id": request.run_id,
            "entity_id": request.entity_id,
            "tick": request.tick,
            "attempt_id": request.attempt_id,
            "idempotency_key": request.idempotency_key,
            "checkpoint_provider": request.checkpoint_provider,
            "checkpoint_ref": request.checkpoint_ref,
            "accepted": request.accepted,
            "retention": request.retention,
        }
        for record in records:
            for field, expected in common.items():
                if getattr(record, field) != expected:
                    raise ValueError(
                        f"artifact record {record.artifact_id} {field} does not match "
                        "its durable request"
                    )

        checkpoints = [
            record
            for record in records
            if record.storage_kind == "provider_checkpoint"
            or record.kind == "sandbox_checkpoint"
            or record.logical_path == "sandbox.checkpoint"
        ]
        if len(checkpoints) != 1:
            raise ValueError("artifact publication requires exactly one provider checkpoint record")
        checkpoint = checkpoints[0]
        expected_checkpoint_id = hashlib.sha256(
            f"{bundle_id}\0sandbox.checkpoint\0{request.checkpoint_ref}".encode()
        ).hexdigest()
        expected_checkpoint = {
            "artifact_id": expected_checkpoint_id,
            "kind": "sandbox_checkpoint",
            "logical_path": "sandbox.checkpoint",
            "source_ref": request.checkpoint_ref,
            "object_uri": request.checkpoint_ref,
            "content_hash": "",
            "size_bytes": -1,
            "mime_type": "application/vnd.archetype.sandbox-checkpoint",
            "restorable": request.checkpoint_restorable,
            "created_at_ms": request.checkpoint_created_at_ms or created_at_ms,
            "expires_at_ms": request.checkpoint_expires_at_ms,
        }
        for field, expected in expected_checkpoint.items():
            if getattr(checkpoint, field) != expected:
                raise ValueError(f"artifact checkpoint {field} does not match its durable request")

        manifests = [
            record
            for record in records
            if record.kind == "bundle_manifest"
            or record.logical_path == "artifact-manifest.json"
            or record.source_ref == "generated://artifact-manifest"
        ]
        if len(manifests) != 1:
            raise ValueError("artifact publication requires exactly one bundle manifest record")
        manifest = manifests[0]
        expected_portable_expiry = self._portable_expires_at_ms(request, created_at_ms)
        expected_manifest = {
            "logical_path": "artifact-manifest.json",
            "source_ref": "generated://artifact-manifest",
            "object_uri": manifest_uri,
            "storage_kind": "object",
            "mime_type": "application/json",
            "restorable": False,
            "created_at_ms": created_at_ms,
            "expires_at_ms": expected_portable_expiry,
        }
        for field, expected in expected_manifest.items():
            if getattr(manifest, field) != expected:
                raise ValueError(
                    f"artifact manifest {field} does not match its durable publication"
                )

        candidate_counts = {candidate.logical_path: 0 for candidate in request.artifacts}
        portable = [
            record for record in records if record is not checkpoint and record is not manifest
        ]
        for record in (manifest, *portable):
            if record.storage_kind != "object":
                raise ValueError("portable artifact records must use object storage")
            expected_id = self._artifact_id(bundle_id, record.logical_path, record.content_hash)
            if record.artifact_id != expected_id:
                raise ValueError(
                    f"artifact record {record.logical_path!r} has a non-deterministic artifact ID"
                )
            expected_folder = self._object_folder(request, bundle_id, record.artifact_id)
            folder = expected_folder.rstrip("/")
            if record.object_uri != folder and not record.object_uri.startswith(folder + "/"):
                raise ValueError(
                    f"artifact record {record.logical_path!r} object URI is outside "
                    "its content-addressed folder"
                )
            if record.created_at_ms != created_at_ms:
                raise ValueError(
                    f"artifact record {record.logical_path!r} changed its creation time"
                )
            if record.expires_at_ms != expected_portable_expiry:
                raise ValueError(
                    f"artifact record {record.logical_path!r} changed its retention deadline"
                )

        for record in portable:
            matches: list[ArtifactCandidate] = []
            for candidate in request.artifacts:
                if record.kind != candidate.kind:
                    continue
                if candidate.recursive:
                    prefix = candidate.logical_path.rstrip("/") + "/"
                    if not record.logical_path.startswith(prefix):
                        continue
                    relative = record.logical_path[len(prefix) :]
                    expected_source = candidate.source_ref.rstrip("/") + "/" + relative
                    if record.source_ref != expected_source:
                        continue
                elif (
                    record.logical_path != candidate.logical_path
                    or record.source_ref != candidate.source_ref
                ):
                    continue
                matches.append(candidate)
            if len(matches) != 1:
                raise ValueError(
                    f"artifact record {record.logical_path!r} does not match exactly one "
                    "declared candidate"
                )
            candidate_counts[matches[0].logical_path] += 1

        for candidate in request.artifacts:
            count = candidate_counts[candidate.logical_path]
            if candidate.required and count == 0:
                raise ValueError(
                    f"required artifact candidate {candidate.logical_path!r} has no durable record"
                )
            if not candidate.recursive and count > 1:
                raise ValueError(
                    f"artifact candidate {candidate.logical_path!r} has multiple durable records"
                )

    def _portable_expires_at_ms(
        self,
        request: ArtifactBundleRequest,
        created_at_ms: int,
    ) -> int:
        if request.artifact_expires_at_ms:
            return request.artifact_expires_at_ms
        retention_seconds = self._require_config().retention_seconds(request.retention)
        return created_at_ms + retention_seconds * 1000 if retention_seconds else 0

    def _assert_materialized_metadata_safe(
        self,
        values: list[MaterializedArtifact],
    ) -> None:
        for index, value in enumerate(values):
            for field in ("source_ref", "logical_path", "kind"):
                self._redaction_service.assert_safe_metadata(
                    str(getattr(value, field)),
                    field=f"materialized_artifact[{index}].{field}",
                )

    def _safe_failure_detail(self, exc: BaseException) -> str:
        """Return a bounded diagnostic approved for the durable retry catalog."""
        try:
            result = self._redaction_service.redact_text(
                f"{type(exc).__name__}: {exc}",
                scope="artifact.failure_detail",
            )
        except BaseException:
            return f"{type(exc).__name__}: failure detail unavailable"
        return result.text[:_MAX_FAILURE_DETAIL_CHARS]

    @staticmethod
    def _lease_ms(seconds: float) -> int:
        """Convert validated configuration to a non-shortening wire duration."""

        return max(1, math.ceil(seconds * 1000))

    @staticmethod
    def _delay_ms(seconds: float) -> int:
        """Convert validated retry configuration to an exact wire duration."""

        return max(0, math.ceil(seconds * 1000))

    async def publish(
        self,
        request: ArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt:
        """Upload and index one bundle, or return its original receipt."""

        prepared = self.prepare(request)
        return await self.publish_prepared(prepared, storage_config=storage_config)

    async def publish_prepared(
        self,
        prepared: PreparedArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt:
        """Publish the exact prepared request, recovering only from durable JSON."""

        config = self._require_config()
        request = self._request_from_preparation(prepared)
        catalog = await self._control_catalog(request, storage_config)
        claimant = f"artifact-{uuid7()}"
        attributes = self._span_attributes(request)

        with _obs.span("artifact.publish", attributes=attributes):
            outcome, publication = await catalog.acquire_artifact_publication(
                world_id=request.world_id,
                run_id=request.run_id,
                attempt_id=request.attempt_id,
                idempotency_key=request.idempotency_key,
                request_digest=prepared.producer_digest,
                request_json=prepared.request_json,
                claimant=claimant,
                retry_window_ms=config.retry_window_seconds * 1000,
                retry_not_after_ms=(
                    request.checkpoint_expires_at_ms if request.checkpoint_expires_at_ms else None
                ),
                lease_ms=self._lease_ms(config.lease_seconds),
            )
            if outcome == "duplicate":
                return self._receipt(publication, duplicate=True)
            if outcome == "expired":
                return self._receipt(publication, duplicate=True)
            try:
                # The catalog request is authoritative after acquisition. This
                # deliberately discards the caller's decoded object even for a
                # fresh row, proving that mapping or scanner-code drift cannot
                # alter an in-flight publication.
                request = self._request_from_publication(
                    publication,
                    require_policy=publication.status == "PENDING",
                )
                return await self._resume(request, publication, claimant, catalog)
            except Exception as exc:
                failure_detail = self._safe_failure_detail(exc)
                try:
                    await catalog.fail_artifact_publication(
                        request.world_id,
                        publication.publication_key,
                        claimant,
                        failure_detail,
                        retry_delay_ms=self._delay_ms(config.retry_delay_seconds),
                    )
                except Exception as record_error:
                    exc.add_note(
                        f"failed to record artifact retry state: {type(record_error).__name__}"
                    )
                raise

    async def query(
        self,
        world_id: str,
        run_id: str,
        *,
        attempt_id: str | None = None,
        kinds: list[str] | None = None,
    ) -> DataFrame:
        """Read indexed artifacts without requiring a live world or sandbox."""
        config = self._require_config()
        iceberg = await self._storage_service.get_iceberg_context(config.index_storage)
        if not iceberg.has_table(_ARTIFACT_INDEX_TABLE):
            return self._empty_index()
        frame = iceberg.read(iceberg.get_table(_ARTIFACT_INDEX_TABLE))
        frame = frame.where(frame["world_id"] == str(world_id))
        frame = frame.where(frame["run_id"] == str(run_id))
        if attempt_id is not None:
            frame = frame.where(frame["attempt_id"] == attempt_id)
        if kinds:
            frame = frame.where(col("kind").is_in(kinds))
        # Iceberg appends are at-least-once under a claimant losing its lease
        # after the commit but before catalog completion. Persisted bundle rows
        # are deterministic, so logical reads collapse an identical physical
        # replay without materializing the query in this service.
        return frame.distinct()

    async def reconcile(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
        limit: int = 100,
    ) -> ArtifactReconcileResult:
        """Run one bounded pass over expired leases for a single world."""
        candidates = await self.list_due_publications(
            world_id,
            storage_config=storage_config,
            limit=limit,
        )
        indexed = expired = failed = 0
        for candidate in candidates:
            try:
                result = await self.reconcile_publication(
                    world_id,
                    candidate.publication_key,
                    storage_config=storage_config,
                )
            except Exception as exc:
                failed += 1
                logger.error(
                    "artifact reconciliation failed for %s (%s)",
                    candidate.publication_key,
                    type(exc).__name__,
                )
                continue
            if result.disposition is ArtifactReconcileDisposition.INDEXED:
                indexed += 1
            elif result.disposition is ArtifactReconcileDisposition.EXPIRED:
                expired += 1
        return ArtifactReconcileResult(
            examined=len(candidates),
            indexed=indexed,
            expired=expired,
            failed=failed,
            bundle_ids=tuple(candidate.publication_key for candidate in candidates),
        )

    async def list_due_publications(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
        limit: int = 100,
        after_publication_key: str = "",
    ) -> tuple[ArtifactReconcileCandidate, ...]:
        """Return a bounded page of digest-only publication references."""
        if limit < 1:
            raise ValueError("artifact reconciliation limit must be at least 1")
        if after_publication_key:
            after_publication_key = ArtifactReconcileCandidate(
                publication_key=after_publication_key
            ).publication_key
        self._require_config()
        storage, catalog = await self._catalog_for_world(world_id, storage_config)
        del storage
        due = await catalog.list_due_artifact_publications(
            str(world_id),
            limit=limit,
            after_publication_key=after_publication_key,
        )
        return tuple(ArtifactReconcileCandidate(publication_key=row.publication_key) for row in due)

    async def reconcile_publication(
        self,
        world_id: str,
        publication_key: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactReconcileItemResult:
        """Reconcile one exact durable publication without discovering other work."""
        candidate = ArtifactReconcileCandidate(publication_key=publication_key)
        config = self._require_config()
        storage, catalog = await self._catalog_for_world(world_id, storage_config)
        del storage
        claimant = f"artifact-reconciler-{uuid7()}"
        owned = False
        publication: ArtifactPublicationRecord | None = None
        try:
            outcome, publication = await catalog.recover_artifact_publication(
                str(world_id),
                candidate.publication_key,
                claimant,
                lease_ms=self._lease_ms(config.lease_seconds),
            )
            if outcome == "obsolete":
                return ArtifactReconcileItemResult(
                    publication_key=candidate.publication_key,
                    disposition=ArtifactReconcileDisposition.OBSOLETE,
                )
            if publication is None:
                raise RuntimeError("artifact recovery returned no authoritative publication")
            if outcome == "duplicate":
                return ArtifactReconcileItemResult(
                    publication_key=candidate.publication_key,
                    disposition=ArtifactReconcileDisposition.INDEXED,
                )
            if outcome == "expired":
                return ArtifactReconcileItemResult(
                    publication_key=candidate.publication_key,
                    disposition=ArtifactReconcileDisposition.EXPIRED,
                )
            if outcome not in {"owned", "recovered"}:
                raise RuntimeError(f"artifact recovery returned unexpected outcome {outcome!r}")
            owned = True
            request = self._request_from_publication(
                publication,
                require_policy=publication.status == "PENDING",
            )
            receipt = await self._resume(request, publication, claimant, catalog)
            disposition = (
                ArtifactReconcileDisposition.EXPIRED
                if receipt.status is ArtifactPublicationStatus.EXPIRED
                else ArtifactReconcileDisposition.INDEXED
            )
            return ArtifactReconcileItemResult(
                publication_key=candidate.publication_key,
                disposition=disposition,
            )
        except Exception as exc:
            if not owned:
                logger.error(
                    "artifact reconciliation failed before lease acquisition for %s (%s)",
                    candidate.publication_key,
                    type(exc).__name__,
                )
                raise
            assert publication is not None
            failure_detail = self._safe_failure_detail(exc)
            try:
                await catalog.fail_artifact_publication(
                    publication.world_id,
                    publication.publication_key,
                    claimant,
                    failure_detail,
                    retry_delay_ms=self._delay_ms(config.retry_delay_seconds),
                )
            except Exception as record_error:
                exc.add_note(
                    "failed to record artifact reconciliation retry state: "
                    f"{type(record_error).__name__}"
                )
                logger.error(
                    "artifact reconciler failed to record retry state for %s after %s; "
                    "retry-state recording also failed (%s)",
                    publication.publication_key,
                    type(exc).__name__,
                    type(record_error).__name__,
                )
            raise

    def _request_from_preparation(
        self,
        prepared: PreparedArtifactBundleRequest,
    ) -> ArtifactBundleRequest:
        """Authenticate a caller-supplied prepared identity before catalog I/O."""

        # Revalidation matters for structural protocol implementations that are
        # not instances of the concrete Pydantic model.
        prepared = PreparedArtifactBundleRequest.model_validate(prepared, from_attributes=True)
        request = ArtifactBundleRequest.model_validate_json(prepared.request_json)
        if request.canonical_json() != prepared.request_json:
            raise ValueError("prepared artifact request_json is not canonical")
        if request.request_digest() != prepared.request_digest:
            raise ValueError("prepared artifact request_digest does not match request_json")
        if request.producer_digest() != prepared.producer_digest:
            raise ValueError("prepared artifact producer_digest does not match request_json")
        if request.redaction_policy_id != prepared.redaction_policy_id:
            raise ValueError("prepared artifact policy does not match request_json")
        expected_key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        if prepared.publication_key != expected_key:
            raise ValueError("prepared artifact publication_key does not match request_json")
        self._assert_request_metadata_safe(request)
        self._assert_object_root_safe()
        return request

    def _request_from_publication(
        self,
        publication: ArtifactPublicationRecord,
        *,
        require_policy: bool = True,
    ) -> ArtifactBundleRequest:
        """Decode and authenticate a durable request after owning its lease."""
        request = ArtifactBundleRequest.model_validate_json(publication.request_json)
        persisted_identity = (
            publication.world_id,
            publication.run_id,
            publication.attempt_id,
            publication.idempotency_key,
        )
        request_identity = (
            request.world_id,
            request.run_id,
            request.attempt_id,
            request.idempotency_key,
        )
        if request_identity != persisted_identity:
            raise ValueError("durable artifact request identity does not match its publication row")
        if request.canonical_json() != publication.request_json:
            raise ValueError("durable artifact request is not in canonical form")
        if request.producer_digest() != publication.request_digest:
            raise ValueError("durable artifact request digest does not match its publication row")
        expected_key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        if expected_key != publication.publication_key:
            raise ValueError("durable artifact request does not match its publication key")
        if require_policy and request.redaction_policy_id != self._redaction_service.policy_id:
            raise ValueError("durable artifact request requires an unavailable redaction policy")
        self._assert_request_metadata_safe(request)
        return request

    async def _resume(
        self,
        request: ArtifactBundleRequest,
        publication: ArtifactPublicationRecord,
        claimant: str,
        catalog: ControlCatalog,
    ) -> ArtifactPublishReceipt:
        config = self._require_config()
        outcome, authoritative = await catalog.recover_artifact_publication(
            request.world_id,
            publication.publication_key,
            claimant,
            lease_ms=self._lease_ms(config.lease_seconds),
        )
        if outcome == "obsolete" or authoritative is None:
            raise RuntimeError("artifact publication disappeared before external I/O")
        if outcome in {"duplicate", "expired"}:
            return self._receipt(authoritative, duplicate=outcome == "duplicate")
        publication = authoritative
        request = self._request_from_publication(
            publication,
            require_policy=publication.status == "PENDING",
        )
        # The object destination is deployment configuration rather than part
        # of the durable request. Recheck it on every cold recovery before a
        # PENDING row can upload or an UPLOADED row can reach the index.
        self._assert_object_root_safe()
        created_at_ms = self._publication_created_at_ms(publication)
        records: tuple[ArtifactIndexRecord, ...]
        manifest_uri = publication.manifest_uri
        if publication.status == "PENDING":
            async with self._lease_heartbeat(
                catalog, request.world_id, publication.publication_key, claimant
            ):
                upload_attributes = {
                    **self._span_attributes(request),
                    "archetype.artifact.bundle.digest": publication.publication_key,
                }
                with _obs.span("artifact.upload", attributes=upload_attributes):
                    records, manifest_uri = await self._upload_bundle(
                        request,
                        publication.publication_key,
                        created_at_ms=created_at_ms,
                    )
            self._assert_records_bound_to_request(
                request,
                publication.publication_key,
                records,
                manifest_uri,
                created_at_ms=created_at_ms,
            )
            records_json = _canonical_json([record.model_dump(mode="json") for record in records])
            try:
                await catalog.record_artifact_uploads(
                    request.world_id,
                    publication.publication_key,
                    claimant,
                    records_json,
                    manifest_uri,
                )
            except ArtifactPublicationExpiredError as exc:
                # Expiry can win after object upload but before its metadata
                # commit. Normalize that storage race into the same typed
                # terminal receipt returned when acquisition sees EXPIRED so
                # mission finalization can settle in this invocation.
                expired = await catalog.get_artifact_publication(
                    request.world_id,
                    publication.publication_key,
                )
                if expired is None or expired.status != "EXPIRED":
                    raise RuntimeError(
                        "artifact upload reported expiry without an authoritative "
                        "EXPIRED publication"
                    ) from exc
                return self._receipt(expired, duplicate=False)
        else:
            records = tuple(
                ArtifactIndexRecord.model_validate(value)
                for value in json.loads(publication.records_json)
            )
            self._assert_records_safe(records)

        self._assert_records_bound_to_request(
            request,
            publication.publication_key,
            records,
            manifest_uri,
            created_at_ms=created_at_ms,
        )

        await catalog.renew_artifact_publication(
            request.world_id,
            publication.publication_key,
            claimant,
            lease_seconds=config.lease_seconds,
        )
        async with self._lease_heartbeat(
            catalog, request.world_id, publication.publication_key, claimant
        ):
            index_attributes = {
                **self._span_attributes(request),
                "archetype.artifact.bundle.digest": publication.publication_key,
                "archetype.artifact.count": len(records),
            }
            with _obs.span("artifact.index", attributes=index_attributes):
                snapshot_id = await self._index_records(records)
        if snapshot_id < 1:
            raise RuntimeError("artifact index did not publish a positive snapshot")
        await catalog.complete_artifact_publication(
            request.world_id,
            publication.publication_key,
            claimant,
            snapshot_id,
        )
        settled = await catalog.get_artifact_publication(
            request.world_id, publication.publication_key
        )
        assert settled is not None
        return self._receipt(settled, duplicate=False)

    async def _upload_bundle(
        self,
        request: ArtifactBundleRequest,
        bundle_id: str,
        *,
        created_at_ms: int,
    ) -> tuple[tuple[ArtifactIndexRecord, ...], str]:
        config = self._require_config()
        expires_at_ms = self._portable_expires_at_ms(request, created_at_ms)

        with tempfile.TemporaryDirectory(prefix="archetype-artifacts-") as temp_dir:
            destination = Path(temp_dir)
            if isinstance(self._source_resolver, BoundedArtifactSourceResolver):
                materialized = await self._source_resolver.materialize_bounded(
                    request.artifacts,
                    destination,
                    max_artifact_bytes=config.max_artifact_bytes,
                    max_bundle_bytes=config.max_bundle_bytes,
                )
            else:
                materialized = await self._source_resolver.materialize(
                    request.artifacts,
                    destination,
                )
            self._assert_materialized_metadata_safe(materialized)
            self._validate_materialized(materialized)
            sanitized, redaction_receipts = await asyncio.to_thread(
                self._sanitize_materialized,
                materialized,
                Path(temp_dir) / "redacted",
            )
            self._validate_materialized(sanitized)
            metadata = self._file_metadata(sanitized)
            total = sum(int(row["size_bytes"]) for row in metadata)
            if total > config.max_bundle_bytes:
                raise ValueError(
                    f"artifact bundle is {total} bytes; limit is {config.max_bundle_bytes}"
                )
            for row in metadata:
                if int(row["size_bytes"]) > config.max_artifact_bytes:
                    raise ValueError(
                        f"artifact {row['logical_path']!r} is {row['size_bytes']} bytes; "
                        f"limit is {config.max_artifact_bytes}"
                    )

            portable: list[ArtifactIndexRecord] = []
            pending: list[_PreparedArtifact] = []
            for row in metadata:
                artifact_id = self._artifact_id(bundle_id, row["logical_path"], row["content_hash"])
                destination = self._object_folder(request, bundle_id, artifact_id)
                object_uri = self._existing_object(
                    destination,
                    content_hash=row["content_hash"],
                    size_bytes=row["size_bytes"],
                )
                prepared = _PreparedArtifact(
                    **row,
                    artifact_id=artifact_id,
                    destination=destination,
                )
                if not object_uri:
                    pending.append(prepared)
                else:
                    portable.append(
                        self._portable_record(
                            request,
                            bundle_id,
                            prepared,
                            object_uri,
                            created_at_ms,
                            expires_at_ms,
                        )
                    )

            if pending:
                uploaded = self._upload_files(pending)
                for row in uploaded:
                    portable.append(
                        self._portable_record(
                            request,
                            bundle_id,
                            row,
                            str(row["object_uri"]),
                            created_at_ms,
                            expires_at_ms,
                        )
                    )

            checkpoint = self._checkpoint_record(request, bundle_id, created_at_ms)
            manifest_payload = {
                "schema_version": 1,
                "bundle_id": bundle_id,
                "world_id": request.world_id,
                "run_id": request.run_id,
                "entity_id": request.entity_id,
                "tick": request.tick,
                "attempt_id": request.attempt_id,
                "idempotency_key": request.idempotency_key,
                "redaction": self._redaction_manifest(redaction_receipts),
                "checkpoint": checkpoint.model_dump(mode="json"),
                "artifacts": [
                    record.model_dump(mode="json")
                    for record in sorted(portable, key=lambda value: value.logical_path)
                ],
            }
            manifest_json = _canonical_json(manifest_payload)
            self._redaction_service.assert_safe_metadata(
                manifest_json,
                field="artifact.bundle_manifest",
            )
            manifest_bytes = manifest_json.encode()
            manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
            manifest_id = self._artifact_id(bundle_id, "artifact-manifest.json", manifest_hash)
            manifest_folder = self._object_folder(request, bundle_id, manifest_id)
            manifest_uri = self._existing_object(
                manifest_folder,
                content_hash=manifest_hash,
                size_bytes=len(manifest_bytes),
            ) or self._upload_bytes(manifest_bytes, manifest_folder)
            self._redaction_service.assert_safe_metadata(
                manifest_uri,
                field="artifact.manifest_uri",
            )
            manifest_record = ArtifactIndexRecord(
                schema_version=1,
                artifact_id=manifest_id,
                bundle_id=bundle_id,
                world_id=request.world_id,
                run_id=request.run_id,
                entity_id=request.entity_id,
                tick=request.tick,
                attempt_id=request.attempt_id,
                idempotency_key=request.idempotency_key,
                kind="bundle_manifest",
                logical_path="artifact-manifest.json",
                source_ref="generated://artifact-manifest",
                object_uri=manifest_uri,
                storage_kind="object",
                content_hash=manifest_hash,
                size_bytes=len(manifest_bytes),
                mime_type="application/json",
                checkpoint_provider=request.checkpoint_provider,
                checkpoint_ref=request.checkpoint_ref,
                restorable=False,
                accepted=request.accepted,
                retention=request.retention,
                created_at_ms=created_at_ms,
                expires_at_ms=expires_at_ms,
            )
            records = tuple(
                sorted(
                    [*portable, checkpoint, manifest_record], key=lambda value: value.artifact_id
                )
            )
            self._assert_records_safe(records)
            return records, manifest_uri

    def _sanitize_materialized(
        self,
        values: list[MaterializedArtifact],
        destination: Path,
    ) -> tuple[list[MaterializedArtifact], list[RedactionReceipt]]:
        destination.mkdir(parents=True, exist_ok=True)
        sanitized: list[MaterializedArtifact] = []
        receipts: list[RedactionReceipt] = []
        for index, value in enumerate(values):
            result = self._redaction_service.sanitize_file(
                value.path,
                destination / f"{index:08d}",
                logical_path=value.logical_path,
            )
            sanitized.append(
                MaterializedArtifact(
                    path=result.path,
                    source_ref=value.source_ref,
                    logical_path=value.logical_path,
                    kind=value.kind,
                )
            )
            receipts.append(result.receipt)
        return sanitized, receipts

    def _redaction_manifest(self, receipts: list[RedactionReceipt]) -> dict[str, object]:
        redacted = [receipt for receipt in receipts if receipt.status == "redacted"]
        return {
            "policy_id": self._redaction_service.policy_id,
            "status": "redacted" if redacted else "clean",
            "files_scanned": len(receipts),
            "bytes_scanned": sum(receipt.scanned_bytes for receipt in receipts),
            "redaction_count": sum(receipt.redaction_count for receipt in receipts),
            "rule_ids": sorted({rule for receipt in receipts for rule in receipt.rule_ids}),
            "files": [receipt.model_dump(mode="json") for receipt in receipts],
        }

    def _file_metadata(self, values: list[MaterializedArtifact]) -> list[_FileMetadata]:
        if not values:
            return []
        rows = [
            {
                "local_uri": value.path.resolve().as_uri(),
                "source_ref": value.source_ref,
                "logical_path": value.logical_path,
                "kind": value.kind,
                "mime_fallback": mimetypes.guess_type(value.logical_path)[0]
                or "application/octet-stream",
            }
            for value in values
        ]
        frame = daft.from_pylist(rows).with_column("_bytes", download(col("local_uri")))
        frame = frame.with_columns(
            {
                "content_hash": _sha256_bytes(col("_bytes")),
                "size_bytes": _bytes_len(col("_bytes")),
                "mime_type": guess_mime_type(col("_bytes")).fill_null(col("mime_fallback")),
            }
        )
        return cast(
            list[_FileMetadata],
            frame.select(
                "local_uri",
                "source_ref",
                "logical_path",
                "kind",
                "content_hash",
                "size_bytes",
                "mime_type",
            ).to_pylist(),
        )

    def _validate_materialized(self, values: list[MaterializedArtifact]) -> None:
        """Enforce resolver output and size contracts before loading bytes."""
        config = self._require_config()
        logical_paths: set[str] = set()
        total = 0
        for value in values:
            logical = value.logical_path.replace("\\", "/").strip("/")
            logical_path = PurePosixPath(logical)
            if (
                logical != value.logical_path
                or logical_path.is_absolute()
                or ".." in logical_path.parts
            ):
                raise ValueError(
                    f"artifact resolver returned unsafe logical path {value.logical_path!r}"
                )
            if logical in logical_paths:
                raise ValueError(f"artifact resolver returned duplicate logical path {logical!r}")
            logical_paths.add(logical)

            if not value.path.is_file():
                raise FileNotFoundError(
                    f"artifact resolver did not materialize a regular file: {value.path}"
                )
            size = value.path.stat().st_size
            if size > config.max_artifact_bytes:
                raise ValueError(
                    f"artifact {logical!r} is {size} bytes; limit is {config.max_artifact_bytes}"
                )
            total += size
            if total > config.max_bundle_bytes:
                raise ValueError(
                    f"artifact bundle is at least {total} bytes; limit is {config.max_bundle_bytes}"
                )

    def _upload_files(self, rows: list[_PreparedArtifact]) -> list[_UploadedArtifact]:
        config = self._require_config()
        frame = daft.from_pylist([dict(row) for row in rows]).with_column(
            "_bytes", download(col("local_uri"))
        )
        frame = frame.with_column(
            "object_uri",
            upload(
                col("_bytes"),
                col("destination"),
                max_connections=config.max_connections,
                io_config=config.io_config,
            ),
        )
        return cast(list[_UploadedArtifact], frame.exclude("_bytes").to_pylist())

    def _upload_bytes(self, value: bytes, destination: str) -> str:
        config = self._require_config()
        frame = daft.from_pydict({"value": [value]}).with_column(
            "object_uri",
            upload(
                col("value"),
                destination,
                max_connections=config.max_connections,
                io_config=config.io_config,
            ),
        )
        return str(frame.select("object_uri").to_pylist()[0]["object_uri"])

    def _existing_object(
        self,
        folder: str,
        *,
        content_hash: str,
        size_bytes: int,
    ) -> str:
        """Return only a byte-verified object from one content-addressed folder.

        A process or transport crash may leave a truncated file in the target
        folder before the PENDING publication records upload metadata. Folder
        identity alone is therefore insufficient evidence for reuse.
        """

        config = self._require_config()
        parsed = urlparse(folder)
        if parsed.scheme == "file":
            local = Path(unquote(parsed.path))
            if local.is_symlink():
                candidates: list[Path] = []
            elif local.is_file():
                candidates = [local]
            elif local.is_dir():
                candidates = sorted(local.iterdir())
            else:
                candidates = []
            for path in candidates:
                if path.is_symlink() or not path.is_file():
                    continue
                uri = path.resolve().as_uri()
                self._redaction_service.assert_safe_metadata(
                    uri,
                    field="artifact.existing_object_uri",
                )
                try:
                    if path.stat().st_size != size_bytes:
                        continue
                    digest = hashlib.sha256()
                    with path.open("rb") as stream:
                        for chunk in iter(lambda: stream.read(1 << 20), b""):
                            digest.update(chunk)
                except OSError:
                    continue
                if digest.hexdigest() == content_hash:
                    return uri
            return ""
        base = folder.rstrip("/")
        matches = daft.from_glob_path(
            f"{base}*",
            io_config=config.io_config,
        ).select("path", "size")
        try:
            rows = matches.to_pylist()
        except DaftCoreException as exc:
            # Daft 0.7.19 correctly resolves an empty glob but its empty
            # LocalPartitionSet cannot yet be converted to Python.
            if "Need at least 1 MicroPartition" not in str(exc):
                raise
            return ""
        candidates = sorted(
            str(row["path"])
            for row in rows
            if (
                type(row.get("size")) is int
                and row["size"] == size_bytes
                and (str(row["path"]) == base or str(row["path"]).startswith(base + "/"))
            )
        )
        for uri in candidates:
            self._redaction_service.assert_safe_metadata(
                uri,
                field="artifact.existing_object_uri",
            )
            downloaded = self._download_existing_object(uri, config)
            if (
                isinstance(downloaded, bytes)
                and len(downloaded) == size_bytes
                and hashlib.sha256(downloaded).hexdigest() == content_hash
            ):
                return uri
        return ""

    @staticmethod
    def _download_existing_object(uri: str, config: ArtifactStoreConfig) -> object:
        return (
            daft.from_pydict({"object_uri": [uri]})
            .with_column(
                "_bytes",
                download(
                    col("object_uri"),
                    max_connections=config.max_connections,
                    on_error="null",
                    io_config=config.io_config,
                ),
            )
            .select("_bytes")
            .to_pylist()[0]["_bytes"]
        )

    async def _index_records(self, records: tuple[ArtifactIndexRecord, ...]) -> int:
        config = self._require_config()
        iceberg = await self._storage_service.get_iceberg_context(config.index_storage)
        frame = daft.from_pylist([record.model_dump(mode="python") for record in records])
        table = iceberg.create_table_if_not_exists(_ARTIFACT_INDEX_TABLE, frame.schema())
        existing_frame = iceberg.read(table).where(col("bundle_id") == records[0].bundle_id)
        existing_rows = existing_frame.to_pylist()
        expected = {record.artifact_id: record for record in records}
        existing: dict[str, ArtifactIndexRecord] = {}
        for value in existing_rows:
            record = ArtifactIndexRecord.model_validate(value)
            expected_record = expected.get(record.artifact_id)
            if expected_record is None or expected_record != record:
                raise RuntimeError(f"artifact index conflict for bundle {records[0].bundle_id}")
            existing[record.artifact_id] = record
        missing = [
            record.model_dump(mode="python")
            for record in records
            if record.artifact_id not in existing
        ]
        if missing:
            await iceberg.append_counted(table, daft.from_pylist(missing))
        snapshot_id = iceberg.current_snapshot_id(table)
        if type(snapshot_id) is not int:
            raise ValueError("artifact index returned a non-integer snapshot identity")
        if not 1 <= snapshot_id <= MAX_ICEBERG_SNAPSHOT_ID:
            raise ValueError("artifact index snapshot is outside the positive signed 64-bit range")
        return snapshot_id

    async def _control_catalog(
        self,
        request: ArtifactBundleRequest,
        storage_config: StorageConfig | None,
    ) -> ControlCatalog:
        _storage, catalog = await self._catalog_for_world(request.world_id, storage_config)
        record = await catalog.get_world(request.world_id)
        assert record is not None
        if str(record.run_id or "") != request.run_id:
            raise ValueError(
                f"artifact request run {request.run_id!r} does not match world "
                f"catalog run {record.run_id!r}"
            )
        return catalog

    async def _catalog_for_world(
        self, world_id: str, storage_config: StorageConfig | None
    ) -> tuple[StorageConfig, ControlCatalog]:
        live = self._world_service.storage_record(str(world_id))
        storage = storage_config or (live[0] if live is not None else StorageConfig())
        catalog = self._storage_service.get_control_catalog(storage)
        record = await catalog.get_world(str(world_id))
        if record is None:
            raise KeyError(f"world {world_id} is not recorded in catalog for {storage.uri}")
        return storage, catalog

    def _portable_record(
        self,
        request: ArtifactBundleRequest,
        bundle_id: str,
        row: _PreparedArtifact,
        object_uri: str,
        created_at_ms: int,
        expires_at_ms: int,
    ) -> ArtifactIndexRecord:
        return ArtifactIndexRecord(
            schema_version=1,
            artifact_id=row["artifact_id"],
            bundle_id=bundle_id,
            world_id=request.world_id,
            run_id=request.run_id,
            entity_id=request.entity_id,
            tick=request.tick,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            kind=row["kind"],
            logical_path=row["logical_path"],
            source_ref=row["source_ref"],
            object_uri=object_uri,
            storage_kind="object",
            content_hash=row["content_hash"],
            size_bytes=row["size_bytes"],
            mime_type=row["mime_type"],
            checkpoint_provider=request.checkpoint_provider,
            checkpoint_ref=request.checkpoint_ref,
            restorable=False,
            accepted=request.accepted,
            retention=request.retention,
            created_at_ms=created_at_ms,
            expires_at_ms=expires_at_ms,
        )

    @staticmethod
    def _checkpoint_record(
        request: ArtifactBundleRequest,
        bundle_id: str,
        created_at_ms: int,
    ) -> ArtifactIndexRecord:
        artifact_id = hashlib.sha256(
            f"{bundle_id}\0sandbox.checkpoint\0{request.checkpoint_ref}".encode()
        ).hexdigest()
        return ArtifactIndexRecord(
            schema_version=1,
            artifact_id=artifact_id,
            bundle_id=bundle_id,
            world_id=request.world_id,
            run_id=request.run_id,
            entity_id=request.entity_id,
            tick=request.tick,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            kind="sandbox_checkpoint",
            logical_path="sandbox.checkpoint",
            source_ref=request.checkpoint_ref,
            object_uri=request.checkpoint_ref,
            storage_kind="provider_checkpoint",
            content_hash="",
            size_bytes=-1,
            mime_type="application/vnd.archetype.sandbox-checkpoint",
            checkpoint_provider=request.checkpoint_provider,
            checkpoint_ref=request.checkpoint_ref,
            restorable=request.checkpoint_restorable,
            accepted=request.accepted,
            retention=request.retention,
            created_at_ms=request.checkpoint_created_at_ms or created_at_ms,
            # Provider checkpoints and portable objects have independent
            # lifecycle policies.  A zero provider expiry means "not known to
            # expire"; it must not inherit the portable artifact retention.
            expires_at_ms=request.checkpoint_expires_at_ms,
        )

    def _object_folder(
        self, request: ArtifactBundleRequest, bundle_id: str, artifact_id: str
    ) -> str:
        base = self._normalized_object_uri()
        segments = (
            "worlds",
            request.world_id,
            "runs",
            request.run_id,
            "attempts",
            request.attempt_id,
            "bundles",
            bundle_id,
            "objects",
            artifact_id,
        )
        return base + "/" + "/".join(quote(segment, safe="") for segment in segments)

    def _normalized_object_uri(self) -> str:
        value = str(self._require_config().object_uri)
        parsed = urlparse(value)
        if parsed.scheme in ("", "file"):
            path_value = unquote(parsed.path) if parsed.scheme == "file" else value
            return Path(path_value).expanduser().resolve().as_uri().rstrip("/")
        return value.rstrip("/")

    @staticmethod
    def _artifact_id(bundle_id: str, logical_path: str, content_hash: str) -> str:
        return hashlib.sha256(f"{bundle_id}\0{logical_path}\0{content_hash}".encode()).hexdigest()

    @staticmethod
    def _publication_created_at_ms(publication: ArtifactPublicationRecord) -> int:
        """Derive stable lifecycle timestamps from the durable claim."""
        try:
            return int(datetime.fromisoformat(publication.created_at).timestamp() * 1000)
        except ValueError as exc:
            raise RuntimeError(
                f"artifact publication {publication.publication_key} has an invalid "
                f"created_at timestamp: {publication.created_at!r}"
            ) from exc

    @staticmethod
    def _span_attributes(request: ArtifactBundleRequest) -> dict[str, object]:
        return {
            "archetype.world.id": request.world_id,
            "archetype.run.id": request.run_id,
            "archetype.entity.id": request.entity_id,
            "archetype.tick": request.tick,
        }

    @asynccontextmanager
    async def _lease_heartbeat(
        self,
        catalog: ControlCatalog,
        world_id: str,
        publication_key: str,
        claimant: str,
    ) -> AsyncIterator[None]:
        """Renew ownership while a provider or object-store stage is active."""
        config = self._require_config()
        stop = asyncio.Event()
        interval = max(0.001, min(60.0, config.lease_seconds / 3))

        async def renew() -> None:
            while True:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                    return
                except TimeoutError:
                    await catalog.renew_artifact_publication(
                        world_id,
                        publication_key,
                        claimant,
                        lease_seconds=config.lease_seconds,
                    )

        task = asyncio.create_task(renew())
        body_error: BaseException | None = None
        try:
            yield
        except BaseException as exc:
            body_error = exc
            raise
        finally:
            stop.set()
            try:
                await task
            except BaseException as heartbeat_error:
                if body_error is None:
                    raise
                body_error.add_note(
                    "artifact publication lease renewal also failed: "
                    f"{type(heartbeat_error).__name__}"
                )

    def _receipt(
        self, publication: ArtifactPublicationRecord, *, duplicate: bool
    ) -> ArtifactPublishReceipt:
        request = self._request_from_publication(publication, require_policy=False)
        records = tuple(
            ArtifactIndexRecord.model_validate(value)
            for value in json.loads(publication.records_json)
        )
        self._assert_records_safe(records)
        if publication.status in {"UPLOADED", "INDEXED"}:
            self._assert_records_bound_to_request(
                request,
                publication.publication_key,
                records,
                publication.manifest_uri,
                created_at_ms=self._publication_created_at_ms(publication),
            )
        if publication.status == "INDEXED" and publication.index_snapshot_id < 1:
            raise ValueError("indexed artifact publication lacks a positive snapshot")
        if publication.manifest_uri:
            self._redaction_service.assert_safe_metadata(
                publication.manifest_uri,
                field="artifact.manifest_uri",
            )
        return ArtifactPublishReceipt(
            bundle_id=publication.publication_key,
            world_id=publication.world_id,
            run_id=publication.run_id,
            attempt_id=publication.attempt_id,
            status=ArtifactPublicationStatus(publication.status.lower()),
            duplicate=duplicate,
            manifest_uri=publication.manifest_uri,
            index_snapshot_id=publication.index_snapshot_id,
            request_digest=request.request_digest(),
            producer_digest=request.producer_digest(),
            redaction_policy_id=request.redaction_policy_id,
            records=records,
        )

    @staticmethod
    def _empty_index() -> DataFrame:
        schema = pa.schema(
            [
                ("schema_version", pa.int64()),
                ("artifact_id", pa.string()),
                ("bundle_id", pa.string()),
                ("world_id", pa.string()),
                ("run_id", pa.string()),
                ("entity_id", pa.int64()),
                ("tick", pa.int64()),
                ("attempt_id", pa.string()),
                ("idempotency_key", pa.string()),
                ("kind", pa.string()),
                ("logical_path", pa.string()),
                ("source_ref", pa.string()),
                ("object_uri", pa.string()),
                ("storage_kind", pa.string()),
                ("content_hash", pa.string()),
                ("size_bytes", pa.int64()),
                ("mime_type", pa.string()),
                ("checkpoint_provider", pa.string()),
                ("checkpoint_ref", pa.string()),
                ("restorable", pa.bool_()),
                ("accepted", pa.bool_()),
                ("retention", pa.string()),
                ("created_at_ms", pa.int64()),
                ("expires_at_ms", pa.int64()),
            ]
        )
        return daft.from_arrow(pa.Table.from_batches([], schema=schema))

    def _require_config(self) -> ArtifactStoreConfig:
        if self._config is None:
            raise RuntimeError(
                "artifact publication is not configured; pass ArtifactStoreConfig "
                "to ServiceContainer or ArchetypeRuntime"
            )
        return self._config
