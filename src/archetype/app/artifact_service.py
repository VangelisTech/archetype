# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable, idempotent publication of sandbox evidence bundles."""

from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import shutil
import tarfile
import tempfile
import time
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
from archetype.app._catalog import (
    ArtifactPublicationExpiredError,
    ArtifactPublicationRecord,
)
from archetype.app.artifacts import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactIndexRecord,
    ArtifactPublicationStatus,
    ArtifactPublishReceipt,
    ArtifactReconcileResult,
    ArtifactSourceResolver,
    ArtifactStoreConfig,
    MaterializedArtifact,
    _canonical_json,
)
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.config import StorageConfig

if TYPE_CHECKING:
    from archetype.app._catalog import ControlCatalog

_ARTIFACT_INDEX_TABLE = "artifact_index_v1"
_ROOTFS_SCHEME = "apple-container-rootfs://"


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
        return await asyncio.to_thread(self._materialize_sync, candidates, destination)

    def _materialize_sync(
        self,
        candidates: tuple[ArtifactCandidate, ...],
        destination: Path,
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
        for archive, requested in archives.items():
            resolved.extend(self._materialize_archive(archive, requested, destination))
        self._reject_logical_collisions(resolved)
        return sorted(resolved, key=lambda value: value.logical_path)

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
    ) -> list[MaterializedArtifact]:
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
                    output = archive_destination / f"{len(resolved):08d}-{Path(member_name).name}"
                    with output.open("wb") as target:
                        shutil.copyfileobj(stream, target, length=1 << 20)
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
        return resolved

    @staticmethod
    def _reject_logical_collisions(values: list[MaterializedArtifact]) -> None:
        seen: set[str] = set()
        for value in values:
            if value.logical_path in seen:
                raise ValueError(f"multiple artifact sources resolve to {value.logical_path!r}")
            seen.add(value.logical_path)


class ArtifactService:
    """Publish portable evidence and index it by world/run/attempt.

    The control catalog is the retry authority. Object upload precedes the
    Iceberg append; uploaded metadata is persisted between those stages.
    Therefore every crash point has a deterministic continuation.
    """

    def __init__(
        self,
        storage_service: StorageService,
        world_service: WorldService,
        config: ArtifactStoreConfig | None = None,
        source_resolver: ArtifactSourceResolver | None = None,
    ) -> None:
        self._storage_service = storage_service
        self._world_service = world_service
        self._config = config
        self._source_resolver = source_resolver or CheckpointArtifactSourceResolver()

    @property
    def enabled(self) -> bool:
        return self._config is not None

    async def publish(
        self,
        request: ArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt:
        """Upload and index one bundle, or return its original receipt."""
        config = self._require_config()
        catalog = await self._control_catalog(request, storage_config)
        claimant = f"artifact-{uuid7()}"
        now_ms = int(time.time() * 1000)
        retry_until_ms = now_ms + config.retry_window_seconds * 1000
        if request.checkpoint_expires_at_ms:
            retry_until_ms = min(retry_until_ms, request.checkpoint_expires_at_ms)
        attributes = self._span_attributes(request)

        with _obs.span("artifact.publish", **attributes):
            outcome, publication = await catalog.acquire_artifact_publication(
                world_id=request.world_id,
                run_id=request.run_id,
                attempt_id=request.attempt_id,
                idempotency_key=request.idempotency_key,
                request_digest=request.digest(),
                request_json=request.canonical_json(),
                claimant=claimant,
                retry_until_ms=retry_until_ms,
                lease_seconds=config.lease_seconds,
            )
            if outcome == "duplicate":
                return self._receipt(publication, duplicate=True)
            if outcome == "expired":
                raise ArtifactPublicationExpiredError(
                    publication.last_error
                    or f"artifact publication {publication.publication_key} expired"
                )

            try:
                return await self._resume(request, publication, claimant, catalog)
            except Exception as exc:
                try:
                    await catalog.fail_artifact_publication(
                        request.world_id,
                        publication.publication_key,
                        claimant,
                        f"{type(exc).__name__}: {exc}",
                        retry_at=time.time() + config.retry_delay_seconds,
                    )
                except Exception as record_error:
                    exc.add_note(
                        "failed to record artifact retry state: "
                        f"{type(record_error).__name__}: {record_error}"
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
        frame = frame.where(frame["world_id"] == str(world_id))  # ty: ignore[invalid-argument-type]
        frame = frame.where(frame["run_id"] == str(run_id))  # ty: ignore[invalid-argument-type]
        if attempt_id is not None:
            frame = frame.where(
                frame["attempt_id"] == attempt_id  # ty: ignore[invalid-argument-type]
            )
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
        if limit < 1:
            raise ValueError("artifact reconciliation limit must be at least 1")
        config = self._require_config()
        storage, catalog = await self._catalog_for_world(world_id, storage_config)
        del storage
        due = await catalog.list_due_artifact_publications(
            str(world_id), now=time.time(), limit=limit
        )
        indexed = expired = failed = 0
        bundle_ids: list[str] = []
        for stale in due:
            bundle_ids.append(stale.publication_key)
            request = ArtifactBundleRequest.model_validate_json(stale.request_json)
            claimant = f"artifact-reconciler-{uuid7()}"
            try:
                outcome, publication = await catalog.acquire_artifact_publication(
                    world_id=request.world_id,
                    run_id=request.run_id,
                    attempt_id=request.attempt_id,
                    idempotency_key=request.idempotency_key,
                    request_digest=request.digest(),
                    request_json=request.canonical_json(),
                    claimant=claimant,
                    retry_until_ms=stale.retry_until_ms,
                    lease_seconds=config.lease_seconds,
                )
                if outcome == "duplicate":
                    indexed += 1
                    continue
                if outcome == "expired":
                    expired += 1
                    continue
                if publication.status == "PENDING" and (
                    int(time.time() * 1000) > publication.retry_until_ms
                ):
                    await catalog.expire_artifact_publication(
                        request.world_id,
                        publication.publication_key,
                        claimant,
                        "artifact publication retry window elapsed before upload",
                    )
                    expired += 1
                    continue
                await self._resume(request, publication, claimant, catalog)
                indexed += 1
            except Exception as exc:
                failed += 1
                try:
                    await catalog.fail_artifact_publication(
                        request.world_id,
                        stale.publication_key,
                        claimant,
                        f"{type(exc).__name__}: {exc}",
                        retry_at=time.time() + config.retry_delay_seconds,
                    )
                except Exception:
                    pass
        return ArtifactReconcileResult(
            examined=len(due),
            indexed=indexed,
            expired=expired,
            failed=failed,
            bundle_ids=tuple(bundle_ids),
        )

    async def _resume(
        self,
        request: ArtifactBundleRequest,
        publication: ArtifactPublicationRecord,
        claimant: str,
        catalog: ControlCatalog,
    ) -> ArtifactPublishReceipt:
        config = self._require_config()
        records: tuple[ArtifactIndexRecord, ...]
        manifest_uri = publication.manifest_uri
        if publication.status == "PENDING":
            if int(time.time() * 1000) > publication.retry_until_ms:
                await catalog.expire_artifact_publication(
                    request.world_id,
                    publication.publication_key,
                    claimant,
                    "artifact publication retry window elapsed before upload",
                )
                raise ArtifactPublicationExpiredError(
                    f"artifact publication {publication.publication_key} expired"
                )
            async with self._lease_heartbeat(
                catalog, request.world_id, publication.publication_key, claimant
            ):
                with _obs.span(
                    "artifact.upload",
                    bundle_id=publication.publication_key,
                    **self._span_attributes(request),
                ):
                    records, manifest_uri = await self._upload_bundle(
                        request,
                        publication.publication_key,
                        created_at_ms=self._publication_created_at_ms(publication),
                    )
            records_json = _canonical_json([record.model_dump(mode="json") for record in records])
            await catalog.record_artifact_uploads(
                request.world_id,
                publication.publication_key,
                claimant,
                records_json,
                manifest_uri,
            )
        else:
            records = tuple(
                ArtifactIndexRecord.model_validate(value)
                for value in json.loads(publication.records_json)
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
            with _obs.span(
                "artifact.index",
                bundle_id=publication.publication_key,
                artifact_count=len(records),
                **self._span_attributes(request),
            ):
                snapshot_id = await self._index_records(records)
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
        expires_at_ms = request.artifact_expires_at_ms
        if not expires_at_ms:
            retention_seconds = config.retention_seconds(request.retention)
            expires_at_ms = created_at_ms + retention_seconds * 1000 if retention_seconds else 0

        with tempfile.TemporaryDirectory(prefix="archetype-artifacts-") as temp_dir:
            materialized = await self._source_resolver.materialize(
                request.artifacts, Path(temp_dir)
            )
            self._validate_materialized(materialized)
            metadata = self._file_metadata(materialized)
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
                object_uri = self._existing_object(destination)
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
                "checkpoint": checkpoint.model_dump(mode="json"),
                "artifacts": [
                    record.model_dump(mode="json")
                    for record in sorted(portable, key=lambda value: value.logical_path)
                ],
            }
            manifest_bytes = _canonical_json(manifest_payload).encode()
            manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
            manifest_id = self._artifact_id(bundle_id, "artifact-manifest.json", manifest_hash)
            manifest_folder = self._object_folder(request, bundle_id, manifest_id)
            manifest_uri = self._existing_object(manifest_folder) or self._upload_bytes(
                manifest_bytes, manifest_folder
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
            return records, manifest_uri

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

    def _existing_object(self, folder: str) -> str:
        config = self._require_config()
        parsed = urlparse(folder)
        if parsed.scheme == "file":
            local = Path(unquote(parsed.path))
            if not local.is_dir():
                return ""
            files = sorted(path.resolve().as_uri() for path in local.iterdir() if path.is_file())
            return files[0] if files else ""
        matches = daft.from_glob_path(f"{folder.rstrip('/')}/*", io_config=config.io_config).select(
            "path"
        )
        try:
            rows = matches.to_pylist()
        except DaftCoreException as exc:
            # Daft 0.7.19 correctly resolves an empty glob but its empty
            # LocalPartitionSet cannot yet be converted to Python.
            if "Need at least 1 MicroPartition" not in str(exc):
                raise
            return ""
        paths = sorted(str(row["path"]) for row in rows)
        return paths[0] if paths else ""

    async def _index_records(self, records: tuple[ArtifactIndexRecord, ...]) -> int:
        config = self._require_config()
        iceberg = await self._storage_service.get_iceberg_context(config.index_storage)
        frame = daft.from_pylist([record.model_dump(mode="python") for record in records])
        table = iceberg.create_table_if_not_exists(_ARTIFACT_INDEX_TABLE, frame.schema())
        existing_frame = iceberg.read(table).where(
            col("bundle_id") == records[0].bundle_id  # ty: ignore[invalid-argument-type]
        )
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
        return int(iceberg.current_snapshot_id(table) or 0)

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
            "world_id": request.world_id,
            "run_id": request.run_id,
            "entity_id": request.entity_id,
            "tick": request.tick,
            "attempt_id": request.attempt_id,
            "idempotency_key": request.idempotency_key,
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
                    f"{type(heartbeat_error).__name__}: {heartbeat_error}"
                )

    @staticmethod
    def _receipt(
        publication: ArtifactPublicationRecord, *, duplicate: bool
    ) -> ArtifactPublishReceipt:
        records = tuple(
            ArtifactIndexRecord.model_validate(value)
            for value in json.loads(publication.records_json)
        )
        return ArtifactPublishReceipt(
            bundle_id=publication.publication_key,
            world_id=publication.world_id,
            run_id=publication.run_id,
            attempt_id=publication.attempt_id,
            status=cast(ArtifactPublicationStatus, publication.status.lower()),
            duplicate=duplicate,
            manifest_uri=publication.manifest_uri,
            index_snapshot_id=publication.index_snapshot_id,
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
