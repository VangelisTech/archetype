# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local Artifact participation in whole-storage migration.

``artifact_files`` is the authoritative occurrence inventory.  Migration
never calls ingestion: it verifies the referenced immutable objects, copies
each distinct content identity once, and rewrites only ``object_uri`` in the
destination table.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, cast

import pyarrow as pa
import xxhash
from daft.file.file import BUFFER_COPY

from archetype.core.paths import local_storage_path
from archetype.errors import ConflictError, PayloadRejectedError
from archetype.storage.transfer import TableSnapshotEvidence, table_evidence

ARTIFACT_MIGRATION_FORMAT_VERSION = 1
ARTIFACT_FILES = "artifact_files"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_XXHASH3_64 = re.compile(r"^[0-9a-f]{16}$")
_INVENTORY_DOMAIN = b"archetype.artifact-migration.inventory.v1\x00"
_URI_DOMAIN = b"archetype.artifact-migration.object-uri.v1\x00"
_REQUIRED_COLUMNS = (
    "artifact_id",
    "object_uri",
    "sha256",
    "xxhash3_64",
    "size_bytes",
)


class ArtifactMigrationError(PayloadRejectedError):
    """Base failure for local Artifact migration."""

    public_detail = "Artifact migration input failed integrity validation"


class ArtifactIntegrityError(ArtifactMigrationError):
    """A referenced source object or inventory disagrees with durable rows."""


class ArtifactObjectConflictError(ArtifactMigrationError, ConflictError):
    """A destination content address already contains different bytes."""

    public_detail = "Destination Artifact content conflicts with the migration plan"


@dataclass(frozen=True, slots=True)
class ArtifactContentEvidence:
    """Credential-free evidence for one distinct referenced content object."""

    sha256: str
    xxhash3_64: str
    size_bytes: int
    object_uri_fingerprints: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ArtifactInventory:
    """Verified, occurrence-preserving source Artifact inventory."""

    format_version: int
    occurrence_count: int
    distinct_content_count: int
    total_verified_bytes: int
    inventory_digest: str
    contents: tuple[ArtifactContentEvidence, ...]

    @property
    def is_empty(self) -> bool:
        return self.occurrence_count == 0


@dataclass(frozen=True, slots=True)
class ArtifactRelocationReceipt:
    """Bounded evidence for copied and read-back destination objects."""

    occurrence_count: int
    distinct_content_count: int
    total_verified_bytes: int
    inventory_digest: str
    copied_content_count: int
    reused_content_count: int


@dataclass(frozen=True, slots=True)
class ArtifactRelocationResult:
    """The transformed common table plus verified object-copy evidence."""

    relocated_table: pa.Table = field(repr=False, compare=False)
    destination_evidence: TableSnapshotEvidence
    receipt: ArtifactRelocationReceipt


@dataclass(frozen=True, slots=True)
class _ObjectIdentity:
    sha256: str
    xxhash3_64: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class _ArtifactOccurrence:
    artifact_id: str
    object_uri: str = field(repr=False)
    object_uri_fingerprint: str
    identity: _ObjectIdentity


def _part(payload: bytes) -> bytes:
    return len(payload).to_bytes(8, "big", signed=False) + payload


def _uri_fingerprint(uri: str) -> str:
    digest = hashlib.sha256()
    digest.update(_URI_DOMAIN)
    digest.update(uri.encode("utf-8"))
    return digest.hexdigest()


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ArtifactIntegrityError(f"artifact_files.{field_name} must be non-empty text")
    return value


def _artifact_occurrences(table: pa.Table) -> tuple[_ArtifactOccurrence, ...]:
    if not isinstance(table, pa.Table):
        raise TypeError("Artifact migration requires a pyarrow.Table")
    missing = sorted(set(_REQUIRED_COLUMNS) - set(table.column_names))
    if missing:
        raise ArtifactIntegrityError(
            "artifact_files is missing required columns: " + ", ".join(missing)
        )
    object_field = table.schema.field("object_uri")
    if not (pa.types.is_string(object_field.type) or pa.types.is_large_string(object_field.type)):
        raise ArtifactIntegrityError("artifact_files.object_uri must have a string type")

    columns = {name: table.column(name).combine_chunks() for name in _REQUIRED_COLUMNS}
    occurrences: list[_ArtifactOccurrence] = []
    seen_artifact_ids: set[str] = set()
    for index in range(table.num_rows):
        artifact_id = _required_string(columns["artifact_id"][index].as_py(), "artifact_id")
        if artifact_id in seen_artifact_ids:
            raise ArtifactIntegrityError(
                "artifact_files contains a duplicate artifact occurrence identity"
            )
        seen_artifact_ids.add(artifact_id)
        object_uri = _required_string(columns["object_uri"][index].as_py(), "object_uri")
        sha256 = _required_string(columns["sha256"][index].as_py(), "sha256")
        if not _SHA256.fullmatch(sha256):
            raise ArtifactIntegrityError(
                "artifact_files.sha256 must be a lowercase SHA-256 hex digest"
            )
        fast_hash = _required_string(columns["xxhash3_64"][index].as_py(), "xxhash3_64")
        if not _XXHASH3_64.fullmatch(fast_hash):
            raise ArtifactIntegrityError(
                "artifact_files.xxhash3_64 must be a lowercase XXH3-64 hex digest"
            )
        raw_size = columns["size_bytes"][index].as_py()
        if isinstance(raw_size, bool) or not isinstance(raw_size, int) or raw_size < 0:
            raise ArtifactIntegrityError("artifact_files.size_bytes must be a non-negative integer")
        occurrences.append(
            _ArtifactOccurrence(
                artifact_id=artifact_id,
                object_uri=object_uri,
                object_uri_fingerprint=_uri_fingerprint(object_uri),
                identity=_ObjectIdentity(sha256, fast_hash, raw_size),
            )
        )
    return tuple(occurrences)


def _content_groups(
    occurrences: tuple[_ArtifactOccurrence, ...],
) -> dict[str, tuple[_ObjectIdentity, tuple[_ArtifactOccurrence, ...]]]:
    grouped: dict[str, list[_ArtifactOccurrence]] = {}
    identities: dict[str, _ObjectIdentity] = {}
    for occurrence in occurrences:
        sha256 = occurrence.identity.sha256
        previous = identities.setdefault(sha256, occurrence.identity)
        if previous != occurrence.identity:
            raise ArtifactIntegrityError(
                "artifact_files records inconsistent hashes or sizes for one SHA-256"
            )
        grouped.setdefault(sha256, []).append(occurrence)
    return {sha256: (identities[sha256], tuple(rows)) for sha256, rows in grouped.items()}


def _hash_stream(source: BinaryIO, target: BinaryIO | None = None) -> _ObjectIdentity:
    sha256 = hashlib.sha256()
    fast = xxhash.xxh3_64()
    size = 0
    while chunk := source.read(BUFFER_COPY):
        sha256.update(chunk)
        fast.update(chunk)
        size += len(chunk)
        if target is not None:
            target.write(chunk)
    return _ObjectIdentity(sha256.hexdigest(), fast.hexdigest(), size)


def _local_path(uri: str, fingerprint: str) -> Path:
    path = local_storage_path(uri)
    if path is None:
        raise ArtifactIntegrityError(
            f"artifact object {fingerprint} is not local; remote migration is unsupported"
        )
    return path


def _hash_source(occurrence: _ArtifactOccurrence) -> _ObjectIdentity:
    path = _local_path(occurrence.object_uri, occurrence.object_uri_fingerprint)
    return _hash_source_path(path, occurrence.object_uri_fingerprint)


def _hash_source_path(
    path: Path,
    fingerprint: str,
    target: BinaryIO | None = None,
) -> _ObjectIdentity:
    if path.is_symlink():
        raise ArtifactIntegrityError(f"artifact object {fingerprint} is not a regular file")
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ArtifactIntegrityError(f"artifact object {fingerprint} is not a regular file")
        source = os.fdopen(descriptor, "rb", buffering=BUFFER_COPY)
        descriptor = None
        with source:
            return _hash_stream(source, target)
    except ArtifactIntegrityError:
        raise
    except OSError:
        raise ArtifactIntegrityError(f"artifact object {fingerprint} is unreadable") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _require_identity(
    expected: _ObjectIdentity,
    observed: _ObjectIdentity,
    *,
    label: str,
    error_type: type[ArtifactMigrationError] = ArtifactIntegrityError,
) -> None:
    if observed == expected:
        return
    fields = []
    if observed.sha256 != expected.sha256:
        fields.append("sha256")
    if observed.xxhash3_64 != expected.xxhash3_64:
        fields.append("xxhash3_64")
    if observed.size_bytes != expected.size_bytes:
        fields.append("size_bytes")
    raise error_type(f"{label} disagrees on {', '.join(fields)}")


def _inventory_digest(occurrences: tuple[_ArtifactOccurrence, ...]) -> str:
    digest = hashlib.sha256()
    digest.update(_INVENTORY_DOMAIN)
    digest.update(len(occurrences).to_bytes(16, "big", signed=False))
    for occurrence in sorted(
        occurrences,
        key=lambda item: (
            item.artifact_id,
            item.identity.sha256,
            item.object_uri_fingerprint,
        ),
    ):
        for value in (
            occurrence.artifact_id,
            occurrence.identity.sha256,
            occurrence.identity.xxhash3_64,
            str(occurrence.identity.size_bytes),
            occurrence.object_uri_fingerprint,
        ):
            digest.update(_part(value.encode("utf-8")))
    return digest.hexdigest()


def capture_artifact_inventory(
    table: pa.Table,
    *,
    source_object_root: str | Path | None = None,
) -> ArtifactInventory:
    """Read and verify every distinct object URI referenced by ``artifact_files``.

    When an endpoint root is supplied, every URI must be the exact content
    address under that authority.  This prevents a trusted composition mistake
    from turning an indexed URI into ambient local-filesystem authority.
    """

    occurrences = _artifact_occurrences(table)
    source_root: Path | None = None
    if source_object_root is not None:
        source_root = _destination_root(source_object_root)
        for occurrence in occurrences:
            expected_path = _destination_path(source_root, occurrence.identity.sha256)
            if occurrence.object_uri != expected_path.as_uri():
                raise ArtifactIntegrityError(
                    "artifact object URI is outside its configured source authority"
                )
    groups = _content_groups(occurrences)
    contents: list[ArtifactContentEvidence] = []
    for sha256 in sorted(groups):
        identity, rows = groups[sha256]
        by_uri = {row.object_uri: row for row in rows}
        for occurrence in sorted(by_uri.values(), key=lambda item: item.object_uri_fingerprint):
            observed = (
                _hash_source_path(
                    _destination_path(source_root, occurrence.identity.sha256),
                    occurrence.object_uri_fingerprint,
                )
                if source_root is not None
                else _hash_source(occurrence)
            )
            _require_identity(
                identity,
                observed,
                label=f"artifact object {occurrence.object_uri_fingerprint}",
            )
        contents.append(
            ArtifactContentEvidence(
                sha256=identity.sha256,
                xxhash3_64=identity.xxhash3_64,
                size_bytes=identity.size_bytes,
                object_uri_fingerprints=tuple(sorted({row.object_uri_fingerprint for row in rows})),
            )
        )
    return ArtifactInventory(
        format_version=ARTIFACT_MIGRATION_FORMAT_VERSION,
        occurrence_count=len(occurrences),
        distinct_content_count=len(contents),
        total_verified_bytes=sum(content.size_bytes for content in contents),
        inventory_digest=_inventory_digest(occurrences),
        contents=tuple(contents),
    )


def _destination_root(value: str | Path) -> Path:
    encoded = str(value)
    if not encoded.strip():
        raise ValueError("destination Artifact object root must not be empty")
    root = local_storage_path(encoded)
    if root is None:
        raise ValueError("local Artifact migration requires a local destination object root")
    # Resolve the declared authority once.  Individual content-address paths
    # remain lexical beneath this root so a pre-existing object symlink cannot
    # rewrite the URI stored in ``artifact_files``.
    return root.resolve()


def _destination_path(root: Path, sha256: str) -> Path:
    destination = root / "objects" / "sha256" / sha256[:2] / sha256
    if not destination.is_relative_to(root):  # pragma: no cover - digest is validated hex
        raise AssertionError("Artifact content address escaped its destination root")
    return destination


def relocate_artifact_table(
    table: pa.Table,
    destination_object_root: str | Path,
) -> pa.Table:
    """Purely derive the destination ``artifact_files`` logical rows.

    This function performs no object I/O and creates no directories, so the
    orchestrator can bind the relocated destination evidence into a migration
    plan before reserving or writing anything.
    """

    occurrences = _artifact_occurrences(table)
    root = _destination_root(destination_object_root)
    object_uris = [
        _destination_path(root, occurrence.identity.sha256).as_uri() for occurrence in occurrences
    ]
    column_index = table.schema.get_field_index("object_uri")
    field = table.schema.field(column_index)
    relocated = table.set_column(
        column_index,
        field,
        pa.array(object_uris, type=field.type),
    )
    if relocated.schema != table.schema:
        raise AssertionError("artifact object_uri relocation changed the table schema")
    return relocated


def _hash_destination(path: Path, sha256: str) -> _ObjectIdentity:
    if path.is_symlink():
        raise ArtifactObjectConflictError(
            f"destination artifact object {sha256} is not a regular file"
        )
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ArtifactObjectConflictError(
                f"destination artifact object {sha256} is not a regular file"
            )
        source = os.fdopen(descriptor, "rb", buffering=BUFFER_COPY)
        descriptor = None  # ``source`` owns the descriptor from this point.
        with source:
            return _hash_stream(source)
    except ArtifactObjectConflictError:
        raise
    except OSError:
        raise ArtifactObjectConflictError(
            f"destination artifact object {sha256} is unreadable"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _verify_destination(path: Path, expected: _ObjectIdentity) -> None:
    _require_identity(
        expected,
        _hash_destination(path, expected.sha256),
        label=f"destination artifact object {expected.sha256}",
        error_type=ArtifactObjectConflictError,
    )


def _stage_object(
    occurrence: _ArtifactOccurrence,
    expected: _ObjectIdentity,
    root: Path,
    source_root: Path | None,
) -> Path:
    staging = root / "objects" / ".staging"
    staging.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="migration-",
            dir=staging,
            delete=False,
        ) as target:
            temporary = Path(target.name)
            source_path = (
                _destination_path(source_root, expected.sha256)
                if source_root is not None
                else _local_path(
                    occurrence.object_uri,
                    occurrence.object_uri_fingerprint,
                )
            )
            observed = _hash_source_path(
                source_path,
                occurrence.object_uri_fingerprint,
                cast(BinaryIO, target),
            )
            target.flush()
            os.fsync(target.fileno())
        _require_identity(
            expected,
            observed,
            label=f"artifact object {occurrence.object_uri_fingerprint}",
        )
        return temporary
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


def relocate_artifact_objects(
    table: pa.Table,
    expected_inventory: ArtifactInventory,
    destination_object_root: str | Path,
    *,
    source_evidence: TableSnapshotEvidence,
    source_object_root: str | Path | None = None,
) -> ArtifactRelocationResult:
    """Verify, copy-once, read back, and relocate local Artifact objects."""

    if not isinstance(expected_inventory, ArtifactInventory):
        raise TypeError("expected_inventory must be an ArtifactInventory")
    if not isinstance(source_evidence, TableSnapshotEvidence):
        raise TypeError("source_evidence must be TableSnapshotEvidence")
    if source_evidence.name != ARTIFACT_FILES:
        raise ValueError("Artifact relocation requires artifact_files source evidence")
    observed_source = table_evidence(
        source_evidence.name,
        source_evidence.snapshot_id,
        table,
    )
    if observed_source != source_evidence:
        raise ArtifactIntegrityError("artifact_files changed after migration planning")

    observed_inventory = capture_artifact_inventory(
        table,
        source_object_root=source_object_root,
    )
    if observed_inventory != expected_inventory:
        raise ArtifactIntegrityError("Artifact inventory changed after migration planning")

    occurrences = _artifact_occurrences(table)
    groups = _content_groups(occurrences)
    root = _destination_root(destination_object_root)
    source_root = _destination_root(source_object_root) if source_object_root is not None else None
    missing: list[tuple[_ObjectIdentity, _ArtifactOccurrence, Path]] = []
    reused_count = 0
    for sha256 in sorted(groups):
        identity, rows = groups[sha256]
        destination = _destination_path(root, sha256)
        if destination.exists():
            _verify_destination(destination, identity)
            reused_count += 1
        else:
            missing.append((identity, rows[0], destination))

    staged: list[tuple[_ObjectIdentity, Path, Path]] = []
    try:
        for identity, source, destination in missing:
            staged.append(
                (
                    identity,
                    _stage_object(source, identity, root, source_root),
                    destination,
                )
            )

        copied_count = 0
        for identity, temporary, destination in staged:
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(temporary, destination)
            except FileExistsError:
                _verify_destination(destination, identity)
                reused_count += 1
            else:
                copied_count += 1
            finally:
                temporary.unlink(missing_ok=True)
            _verify_destination(destination, identity)
    finally:
        for _identity, temporary, _destination in staged:
            temporary.unlink(missing_ok=True)

    relocated = relocate_artifact_table(table, root)
    destination_evidence = table_evidence(
        source_evidence.name,
        source_evidence.snapshot_id,
        relocated,
    )
    return ArtifactRelocationResult(
        relocated_table=relocated,
        destination_evidence=destination_evidence,
        receipt=ArtifactRelocationReceipt(
            occurrence_count=expected_inventory.occurrence_count,
            distinct_content_count=expected_inventory.distinct_content_count,
            total_verified_bytes=expected_inventory.total_verified_bytes,
            inventory_digest=expected_inventory.inventory_digest,
            copied_content_count=copied_count,
            reused_content_count=reused_count,
        ),
    )


__all__ = [
    "ARTIFACT_MIGRATION_FORMAT_VERSION",
    "ArtifactContentEvidence",
    "ArtifactIntegrityError",
    "ArtifactInventory",
    "ArtifactMigrationError",
    "ArtifactObjectConflictError",
    "ArtifactRelocationReceipt",
    "ArtifactRelocationResult",
    "capture_artifact_inventory",
    "relocate_artifact_objects",
    "relocate_artifact_table",
]
