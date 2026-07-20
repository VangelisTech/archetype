# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Durable control catalog — private implementation resource of StorageService.

The catalog makes Archetype's *existing* registries durable: which worlds live
in a store, and which archetype tables (signatures) hold their rows. It is the
authority for discovery; the process-local registries in WorldService and the
stores remain what they always were — caches.

Design rules (issue #272, design review 2026-07-14):

- The catalog location is a **pure function of the storage identity**: the
  same StorageConfig always resolves the same catalog, across processes,
  restarts, and crashes.
- Records are compact pointers (a world row, a signature row with its stored
  Arrow schema). Never operational state: no entity directories, no lineage
  copies (``core/lineage`` is already durable), no manifest snapshots.
- Append-only in spirit: worlds transition status; nothing is deleted.
- Same identity + same content → idempotent no-op. Same identity + different
  content → loud ``CatalogConflictError``. Fail closed, never fail quiet.
- SQLite is the control plane (LanceDB ``merge_insert`` is proven non-CAS
  under concurrency). Single-host authority in v0.3; the protocol leaves room
  for a shared backend later.

This module is deliberately not a service: it has no distinct authority or
gate surface (``docs/guide/service-protocols.md`` § new-service bar). A2 will
extend it with manifest heads and claims.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa
from uuid_utils import uuid7

from archetype._storage_uri import local_storage_path, normalized_storage_uri
from archetype.app.errors import AvailabilityError, ConflictError
from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.core.config import StorageConfig
from archetype.core.interfaces import StaleWriterError
from archetype.core.paths import require_safe_namespace, resolve_local_root

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 8
_DIGEST_DOMAIN = "archetype.catalog.v1"
_MAX_PORTABLE_COUNTER = (1 << 53) - 1
_MAX_ARTIFACT_LEASE_MS = 24 * 60 * 60 * 1000
_MAX_ARTIFACT_RETRY_WINDOW_MS = 365 * 24 * 60 * 60 * 1000
_MAX_ARTIFACT_RETRY_DELAY_MS = 365 * 24 * 60 * 60 * 1000
_ARTIFACT_RETRY_EXPIRED_DETAIL = "artifact publication retry window elapsed before upload"

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def _now_ms() -> int:
    """Catalog-authoritative wall time for leases and durable scheduling."""

    return time.time_ns() // 1_000_000


def _require_sha256(value: str, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _require_bounded_text(value: str, *, field: str, max_chars: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if not value.strip():
        raise ValueError(f"{field} must not be empty")
    if len(value) > max_chars:
        raise ValueError(f"{field} exceeds {max_chars} characters")
    return value


def _require_portable_counter(value: int, *, field: str = "fence_epoch") -> int:
    """Require an exact non-boolean fence portable through the Worker wire."""

    if type(value) is not int:
        raise TypeError(f"{field} must be an integer")
    if value < 0 or value > _MAX_PORTABLE_COUNTER:
        raise ValueError(f"{field} must be between 0 and {_MAX_PORTABLE_COUNTER}")
    return value


def _require_artifact_milliseconds(
    value: int,
    *,
    field: str,
    maximum: int,
    allow_zero: bool = True,
) -> int:
    if type(value) is not int:
        raise TypeError(f"{field} must be an integer number of milliseconds")
    minimum = 0 if allow_zero else 1
    if value < minimum or value > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum} milliseconds")
    return value


def _require_artifact_lease_ms(value: int) -> int:
    return _require_artifact_milliseconds(
        value,
        field="artifact lease_ms",
        maximum=_MAX_ARTIFACT_LEASE_MS,
        allow_zero=False,
    )


def _require_artifact_lease_seconds(value: float) -> float:
    if type(value) not in {int, float} or not math.isfinite(value):
        raise TypeError("artifact lease_seconds must be a finite number")
    if value <= 0 or value > _MAX_ARTIFACT_LEASE_MS / 1000:
        raise ValueError(
            "artifact lease_seconds must be greater than zero and no greater than "
            f"{_MAX_ARTIFACT_LEASE_MS / 1000:g}"
        )
    return float(value)


class CatalogConflictError(ConflictError):
    """Same identity registered with different content — never silently resolved."""

    public_detail = "Catalog entry conflicts with existing state"


class CatalogSchemaMismatchError(RuntimeError):
    """Durable catalog schema is incompatible with this build or physical data."""


class CommandConflictError(ConflictError):
    """A command identity was reused with different immutable content."""

    public_detail = "Command conflicts with an existing durable command"


# ─────────────────────────────────────────────────────────────────────────────
# Canonical schema fingerprints (salvaged, trimmed, from the A1 draft branch)
# ─────────────────────────────────────────────────────────────────────────────


def arrow_schema_descriptor(schema: pa.Schema) -> dict[str, object]:
    """A JSON-native, order-preserving description of an Arrow schema.

    Field type identity uses ``str(field.type)``; cross-PyArrow-version
    stability of that string is a documented hypothesis (design review pass 2)
    — the fingerprint guards read-time safety, not archival identity.
    """
    return {
        "fields": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": bool(field.nullable),
            }
            for field in schema
        ],
    }


_TYPE_NORMALIZATION = {
    "large_string": "string",
    "large_binary": "binary",
}


def _normalized_type(type_str: str) -> str:
    for physical, logical in _TYPE_NORMALIZATION.items():
        type_str = type_str.replace(physical, logical)
    return type_str


def schema_fingerprint(schema: pa.Schema) -> str:
    """Domain-separated SHA-256 over the schema's *logical* shape.

    Backends normalize physical encodings — Iceberg round-trips
    ``string`` as ``large_string`` and forces every field nullable — so
    the fingerprint hashes field names and normalized logical types, in
    order, and deliberately excludes nullability and large/small encoding
    variants. Its job is read-safety ("is this table what the descriptor
    claims"), which must hold across backend representations of the same
    declared schema. Renames, reorders, and retypes still mismatch.
    """
    value = [[field.name, _normalized_type(str(field.type))] for field in schema]
    payload = json.dumps(
        {"domain": _DIGEST_DOMAIN, "kind": "arrow-schema", "value": value},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def storage_fingerprint(config: StorageConfig) -> str:
    """Stable identity for a storage location (credential-free).

    Keyed by uri + namespace + backend, matching StorageService's pool
    identity: two configs that resolve to different stores (LanceDB vs
    Iceberg on the same uri/namespace) must never share a catalog, or one
    backend would discover descriptors whose rows live in the other.
    """
    payload = json.dumps(
        {
            "domain": _DIGEST_DOMAIN,
            "kind": "storage",
            "uri": normalized_storage_uri(str(config.uri)),
            "namespace": config.namespace,
            "backend": config.backend.value,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def catalog_path_for(config: StorageConfig) -> Path:
    """The catalog location as a pure function of the storage identity.

    Local stores keep the record beside the data it is about
    (``<uri>/<namespace>/.archetype-catalog-<backend>.db``). Remote stores
    get a deterministic host-local path keyed by the storage fingerprint —
    the same config always resolves the same catalog on this host
    (single-host authority is the documented v0.3 limit). The backend is
    part of the identity in both forms, mirroring storage_fingerprint.
    """
    namespace = require_safe_namespace(config.namespace)
    if local_storage_path(str(config.uri)) is not None:
        base = resolve_local_root(str(config.uri))
        candidate = base / namespace / f".archetype-catalog-{config.backend.value}.db"
        if not candidate.resolve().is_relative_to(base):
            raise ValueError(f"catalog path {candidate} escapes storage root {base} (fail closed)")
        return candidate
    root = Path(os.environ.get("ARCHETYPE_CATALOG_DIR", "~/.archetype/catalogs")).expanduser()
    # The remote-form filename is fingerprint-derived hex, never request data.
    return root / f"{storage_fingerprint(config)[:24]}.db"


# ─────────────────────────────────────────────────────────────────────────────
# Records
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WorldRecord:
    """Compact durable pointer to a world in one store."""

    world_id: str
    name: str | None
    run_id: str | None
    parent_world_id: str | None
    status: str  # "active" | "destroyed"
    tick_head: int  # advisory until A2 manifests land


@dataclass(frozen=True)
class ManifestRecord:
    """One published tick commit: the visibility authority (issue #273).

    A tick is visible iff its manifest row exists. Compact by contract:
    world, run, tick, the commit token that names the winning attempt, the
    fenced writer epoch, and the table ids touched — never entity
    directories or state snapshots.
    """

    world_id: str
    run_id: str
    tick: int
    commit_token: str
    writer_epoch: int
    table_ids: tuple[str, ...]
    created_at: str


class ClaimConflictError(ConflictError):
    """Same external id claimed/completed with a different payload digest."""

    public_detail = "Claim conflicts with existing state"


class ClaimPendingError(ConflictError):
    """A live lease holds this claim; back off — never blind-retry."""

    public_detail = "Claim is currently pending"


@dataclass(frozen=True)
class ClaimRecord:
    """One artifact-publication claim: the exactly-once-visible authority.

    A artifact is visible iff its claim is COMPLETE — completion publishes the
    claim's commit token into the visible set, the same mechanism ticks use.
    """

    scope_key: str
    world_id: str
    run_id: str
    producer: str
    external_id: str
    payload_digest: str
    status: str  # "PENDING" | "COMPLETE"
    commit_token: str
    tick: int
    artifact_entity_id: int
    table_id: str | None
    claimant: str
    lease_expires_at: float
    fence_epoch: int


class ArtifactPublicationConflictError(ConflictError):
    """An artifact publication identity was reused for different content."""

    public_detail = "Artifact publication conflicts with existing state"


class ArtifactPublicationPendingError(AvailabilityError):
    """Another reconciler holds the live lease for this publication."""

    public_detail = "Artifact publication is currently being reconciled"


class ArtifactPublicationExpiredError(ConflictError):
    """A publication exhausted its durable retry window before upload."""

    public_detail = "Artifact publication retry window expired"


@dataclass(frozen=True)
class ArtifactPublicationRecord:
    """Durable control-plane state for one portable evidence bundle."""

    publication_key: str
    world_id: str
    run_id: str
    attempt_id: str
    idempotency_key: str
    request_digest: str
    status: str
    request_json: str
    records_json: str
    claimant: str
    lease_expires_at: float
    retry_until_ms: int
    attempt_count: int
    index_snapshot_id: int
    manifest_uri: str
    last_error: str
    created_at: str
    updated_at: str
    completed_at: str | None


@dataclass(frozen=True)
class ArtifactPublicationCandidate:
    """Digest-only discovery reference; exact recovery returns replay authority."""

    publication_key: str


@dataclass(frozen=True)
class SignatureRecord:
    """Compact durable pointer to one archetype table."""

    table_id: str
    component_names: tuple[str, ...]
    schema_json: str  # canonical arrow_schema_descriptor, JSON-encoded
    fingerprint: str

    def matches(self, schema: pa.Schema) -> bool:
        return self.fingerprint == schema_fingerprint(schema)


@dataclass(frozen=True)
class CommandAdmission:
    """Portable immutable content admitted to the per-world command ledger."""

    command_id: str
    scheduled_tick: int
    priority: int
    command_type: str
    payload_json: str
    payload_digest: str
    version: int
    principal_id: str | None
    origin: str
    reserved_entity_id: int | None = None
    max_attempts: int = 3


@dataclass(frozen=True)
class CommandRecord:
    """One durable command and its scheduler/settlement state."""

    command_id: str
    world_id: str
    sequence: int
    scheduled_tick: int
    priority: int
    command_type: str
    payload_json: str
    payload_digest: str
    version: int
    principal_id: str | None
    origin: str
    reserved_entity_id: int | None
    status: str
    attempts: int
    max_attempts: int
    lease_owner: str | None
    lease_expires_at: float | None
    last_error_code: str | None
    last_error_detail: str | None
    accepted_at: str
    updated_at: str
    applied_tick: int | None
    commit_token: str | None


@dataclass(frozen=True)
class OutboxRecord:
    """Authoritative event awaiting projection into analytical audit storage."""

    sequence: int
    event_id: str
    world_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    command_type: str
    status: str
    actor_id: str | None
    payload_json: str
    occurred_at: str
    projected_at: str | None


class ControlCatalog(Protocol):
    """What StorageService exposes to the app layer. A2 extends this."""

    async def register_world(self, record: WorldRecord) -> None: ...
    async def set_world_status(self, world_id: str, status: str) -> None: ...
    async def set_world_run(self, world_id: str, run_id: str) -> None: ...
    async def get_world(self, world_id: str) -> WorldRecord | None: ...
    async def list_worlds(self) -> list[WorldRecord]: ...
    async def register_signature(self, record: SignatureRecord) -> None: ...
    async def list_signatures(self) -> list[SignatureRecord]: ...
    async def max_manifest_tick(self, world_id: str, run_id: str) -> int | None: ...
    async def acquire_artifact_publication(
        self,
        *,
        world_id: str,
        run_id: str,
        attempt_id: str,
        idempotency_key: str,
        request_digest: str,
        request_json: str,
        claimant: str,
        retry_window_ms: int,
        retry_not_after_ms: int | None = None,
        lease_ms: int = 900_000,
    ) -> tuple[str, ArtifactPublicationRecord]: ...
    async def recover_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_ms: int,
    ) -> tuple[str, ArtifactPublicationRecord | None]: ...
    async def get_artifact_publication(
        self, world_id: str, publication_key: str
    ) -> ArtifactPublicationRecord | None: ...
    async def renew_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_seconds: float,
    ) -> ArtifactPublicationRecord: ...
    async def record_artifact_uploads(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        records_json: str,
        manifest_uri: str,
    ) -> None: ...
    async def complete_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        index_snapshot_id: int,
    ) -> None: ...
    async def fail_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
        *,
        retry_delay_ms: int,
    ) -> None: ...
    async def expire_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
    ) -> None: ...
    async def list_due_artifact_publications(
        self,
        world_id: str,
        *,
        limit: int = 100,
        after_publication_key: str = "",
    ) -> list[ArtifactPublicationCandidate]: ...
    async def admit_commands(
        self, world_id: str, admissions: list[CommandAdmission]
    ) -> list[CommandRecord]: ...
    async def lease_commands(
        self,
        world_id: str,
        tick: int,
        owner: str,
        *,
        lease_seconds: float = 30.0,
        limit: int = 50_000,
    ) -> list[CommandRecord]: ...
    async def fail_command(
        self,
        world_id: str,
        command_id: str,
        owner: str,
        *,
        status: str,
        error_code: str,
        error_detail: str,
    ) -> CommandRecord: ...
    async def release_commands(self, world_id: str, command_ids: list[str], owner: str) -> None: ...
    async def list_commands(
        self, world_id: str, *, status: str | None = None, limit: int = 100
    ) -> list[CommandRecord]: ...
    async def pending_command_count(self, world_id: str) -> int: ...
    async def max_reserved_entity_id(self, world_id: str) -> int | None: ...
    async def read_outbox(self, world_id: str, *, limit: int = 1000) -> list[OutboxRecord]: ...
    async def mark_outbox_projected(self, world_id: str, event_ids: list[str]) -> None: ...
    async def close(self) -> None: ...


# ─────────────────────────────────────────────────────────────────────────────
# SQLite implementation
# ─────────────────────────────────────────────────────────────────────────────

_DDL = f"""
CREATE TABLE IF NOT EXISTS catalog_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS worlds (
    world_id TEXT PRIMARY KEY,
    name TEXT,
    run_id TEXT,
    parent_world_id TEXT,
    status TEXT NOT NULL,
    tick_head INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS signatures (
    table_id TEXT PRIMARY KEY,
    component_names TEXT NOT NULL,
    schema_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    world_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tick INTEGER NOT NULL,
    commit_token TEXT NOT NULL,
    writer_epoch INTEGER NOT NULL,
    tables_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (world_id, run_id, tick)
);
CREATE TABLE IF NOT EXISTS writer_fence (
    world_id TEXT PRIMARY KEY,
    epoch INTEGER NOT NULL,
    holder TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS claims (
    scope_key TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    producer TEXT NOT NULL,
    external_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    commit_token TEXT NOT NULL,
    tick INTEGER NOT NULL,
    artifact_entity_id INTEGER NOT NULL DEFAULT 0,
    table_id TEXT,
    claimant TEXT NOT NULL,
    lease_expires_at REAL NOT NULL,
    fence_epoch INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE TABLE IF NOT EXISTS artifact_publications (
    publication_key TEXT PRIMARY KEY,
    world_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    records_json TEXT NOT NULL DEFAULT '[]',
    claimant TEXT NOT NULL,
    lease_expires_at REAL NOT NULL,
    retry_until_ms INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    index_snapshot_id INTEGER NOT NULL DEFAULT 0,
    manifest_uri TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS artifact_publications_due
ON artifact_publications (world_id, status, lease_expires_at);
CREATE TABLE IF NOT EXISTS commands (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    command_id TEXT NOT NULL UNIQUE,
    world_id TEXT NOT NULL,
    scheduled_tick INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    command_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    version INTEGER NOT NULL,
    principal_id TEXT,
    origin TEXT NOT NULL,
    reserved_entity_id INTEGER,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    last_error_code TEXT,
    last_error_detail TEXT,
    accepted_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    applied_tick INTEGER,
    commit_token TEXT
);
CREATE INDEX IF NOT EXISTS commands_due_idx
    ON commands (world_id, status, scheduled_tick, priority, sequence);
CREATE TABLE IF NOT EXISTS outbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    world_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    command_type TEXT NOT NULL,
    status TEXT NOT NULL,
    actor_id TEXT,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    projected_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (world_id, projected_at, sequence);
INSERT OR IGNORE INTO catalog_meta (key, value) VALUES ('schema_version', '{_SCHEMA_VERSION}');
"""


class SqliteControlCatalog:
    """Hardened per the proven A1-draft settings: WAL, synchronous=FULL,
    busy timeout, BEGIN IMMEDIATE for read-modify-write. All sqlite work runs
    in a worker thread; one connection per catalog instance, serialized by an
    asyncio lock (SQLite write transactions are single-writer anyway)."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    # ── connection ─────────────────────────────────────────────────────────

    def _connect_sync(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        journal = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).upper()
        if journal != "WAL":
            logger.warning("catalog %s: journal_mode=%s (WAL unavailable)", self.path, journal)
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript(_DDL)
        version = int(
            conn.execute("SELECT value FROM catalog_meta WHERE key='schema_version'").fetchone()[0]
        )
        if version > _SCHEMA_VERSION:
            conn.close()
            raise CatalogSchemaMismatchError(
                f"catalog {self.path} has schema_version={version}, "
                f"this build expects {_SCHEMA_VERSION}"
            )
        if version < _SCHEMA_VERSION:
            # Version 5 retires the generic artifact vocabulary. Preserve existing
            # catalogs by renaming the publication-row identifier in place.
            claim_columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(claims)").fetchall()
            }
            if "fact_entity_id" in claim_columns and "artifact_entity_id" not in claim_columns:
                conn.execute(
                    "ALTER TABLE claims RENAME COLUMN fact_entity_id TO artifact_entity_id"
                )
            conn.execute(
                "UPDATE catalog_meta SET value=? WHERE key='schema_version'",
                (str(_SCHEMA_VERSION),),
            )
        conn.commit()
        self._conn = conn
        return conn

    async def _run(self, fn, *args):
        async with self._lock:
            return await asyncio.to_thread(fn, *args)

    async def close(self) -> None:
        def _close() -> None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

        await self._run(_close)

    # ── worlds ───────────────────────────────────────────────────────────────

    async def register_world(self, record: WorldRecord) -> None:
        def _register() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM worlds WHERE world_id=?", (record.world_id,)
                ).fetchone()
                if row is not None:
                    existing = _world_from_row(row)
                    # Identity fields must agree; status/tick may have advanced.
                    if (existing.name, existing.run_id, existing.parent_world_id) != (
                        record.name,
                        record.run_id,
                        record.parent_world_id,
                    ):
                        raise CatalogConflictError(
                            f"world {record.world_id} already registered with "
                            f"different identity in catalog {self.path}"
                        )
                    return
                conn.execute(
                    "INSERT INTO worlds "
                    "(world_id, name, run_id, parent_world_id, status, tick_head) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        record.world_id,
                        record.name,
                        record.run_id,
                        record.parent_world_id,
                        record.status,
                        record.tick_head,
                    ),
                )

        await self._run(_register)

    async def set_world_status(self, world_id: str, status: str) -> None:
        def _set() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("UPDATE worlds SET status=? WHERE world_id=?", (status, world_id))
                if status != "active":
                    _reject_unsettled_commands(
                        conn,
                        world_id=world_id,
                        reason=f"world transitioned to {status}",
                    )

        await self._run(_set)

    async def set_world_run(self, world_id: str, run_id: str) -> None:
        """Track the world's current run (manifests own the tick head)."""

        def _set() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("UPDATE worlds SET run_id=? WHERE world_id=?", (run_id, world_id))

        await self._run(_set)

    async def get_world(self, world_id: str) -> WorldRecord | None:
        def _get() -> WorldRecord | None:
            conn = self._connect_sync()
            row = conn.execute("SELECT * FROM worlds WHERE world_id=?", (world_id,)).fetchone()
            return _world_from_row(row) if row is not None else None

        return await self._run(_get)

    async def list_worlds(self) -> list[WorldRecord]:
        def _list() -> list[WorldRecord]:
            conn = self._connect_sync()
            rows = conn.execute("SELECT * FROM worlds ORDER BY world_id").fetchall()
            return [_world_from_row(row) for row in rows]

        return await self._run(_list)

    # ── artifact claims (issue #274) ────────────────────────────────────────

    async def acquire_claim(
        self,
        *,
        world_id: str,
        run_id: str,
        producer: str,
        external_id: str,
        payload_digest: str,
        claimant: str,
        tick: int,
        lease_seconds: float = 30.0,
    ) -> tuple[str, ClaimRecord]:
        """Put-if-absent claim acquisition with lease takeover.

        Returns (outcome, record) where outcome is one of:
        - "acquired": this claimant owns a fresh PENDING claim (new token).
        - "recovered": this claimant took over an expired PENDING claim —
          the original token is kept only long enough to probe for an
          already-appended orphan. A recovery with no orphan must re-arm
          the claim with a fresh token before appending.
        - "duplicate": an identical artifact is already COMPLETE — the original
          record is the receipt; nothing to do.
        Raises ClaimConflictError on same id + different digest, and
        ClaimPendingError while another claimant's lease is live.
        """
        scope_key = claim_scope_key(world_id, run_id, producer, external_id)

        def _acquire() -> tuple[str, ClaimRecord]:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM claims WHERE scope_key=?", (scope_key,)
                ).fetchone()
                now = time.time()
                if row is not None:
                    existing = _claim_from_row(row)
                    if existing.payload_digest != payload_digest:
                        raise ClaimConflictError(
                            f"external id {external_id!r} from {producer!r} was "
                            f"submitted with a different payload digest "
                            f"(claim {existing.status}); refusing"
                        )
                    if existing.status == "COMPLETE":
                        return ("duplicate", existing)
                    if existing.lease_expires_at > now:
                        raise ClaimPendingError(
                            f"a live lease ({existing.claimant}) holds claim "
                            f"{external_id!r}; retry after it completes or expires"
                        )
                    conn.execute(
                        "UPDATE claims SET claimant=?, lease_expires_at=? WHERE scope_key=?",
                        (claimant, now + lease_seconds, scope_key),
                    )
                    return (
                        "recovered",
                        _claim_from_row(
                            conn.execute(
                                "SELECT * FROM claims WHERE scope_key=?", (scope_key,)
                            ).fetchone()
                        ),
                    )
                fence = conn.execute(
                    "SELECT epoch FROM writer_fence WHERE world_id=?", (world_id,)
                ).fetchone()
                epoch = int(fence["epoch"]) if fence is not None else 0
                token = f"artifact-{scope_key[:32]}"
                cursor = conn.execute(
                    "INSERT INTO claims (scope_key, world_id, run_id, producer, external_id, "
                    "payload_digest, status, commit_token, tick, artifact_entity_id, table_id, "
                    "claimant, lease_expires_at, fence_epoch, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, 0, NULL, ?, ?, ?, ?)",
                    (
                        scope_key,
                        world_id,
                        run_id,
                        producer,
                        external_id,
                        payload_digest,
                        token,
                        tick,
                        claimant,
                        now + lease_seconds,
                        epoch,
                        _utcnow(),
                    ),
                )
                # Catalog-allocated artifact entity id: unique per storage identity,
                # in the negative metadata band, clear of lineage's small ids.
                # (lastrowid is always set after a successful INSERT.)
                artifact_eid = -(100_000 + int(cursor.lastrowid or 0))
                conn.execute(
                    "UPDATE claims SET artifact_entity_id=? WHERE scope_key=?",
                    (artifact_eid, scope_key),
                )
                return (
                    "acquired",
                    _claim_from_row(
                        conn.execute(
                            "SELECT * FROM claims WHERE scope_key=?", (scope_key,)
                        ).fetchone()
                    ),
                )

        return await self._run(_acquire)

    async def rearm_claim(
        self,
        world_id: str,
        scope_key: str,
        claimant: str,
        commit_token: str,
    ) -> ClaimRecord:
        """Rotate a recovered, empty claim to a fresh commit identity.

        This is a claimant-checked CAS. Rows appended late by the expired
        owner retain the old token and can therefore never become visible
        when the recovered claim completes.
        """

        def _rearm() -> ClaimRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM claims WHERE scope_key=?", (scope_key,)
                ).fetchone()
                if row is None:
                    raise ClaimConflictError(f"no claim recorded for scope {scope_key}")
                existing = _claim_from_row(row)
                if existing.world_id != world_id:
                    raise ClaimConflictError(
                        f"claim {scope_key} belongs to world {existing.world_id}, not {world_id}"
                    )
                if existing.status != "PENDING":
                    raise ClaimConflictError(
                        f"claim {scope_key} is already {existing.status}; refusing to re-arm"
                    )
                if existing.claimant != claimant:
                    raise ClaimPendingError(
                        f"claim {scope_key} is held by {existing.claimant}; "
                        "this claimant cannot re-arm it"
                    )
                if existing.commit_token == commit_token:
                    raise ClaimConflictError(
                        f"claim {scope_key} re-arm must use a fresh commit token"
                    )
                conn.execute(
                    "UPDATE claims SET commit_token=?, table_id=NULL WHERE scope_key=?",
                    (commit_token, scope_key),
                )
                return _claim_from_row(
                    conn.execute("SELECT * FROM claims WHERE scope_key=?", (scope_key,)).fetchone()
                )

        return await self._run(_rearm)

    async def record_claim_table(self, world_id: str, scope_key: str, table_id: str) -> None:
        """Record where a claim's rows will land, BEFORE the append.

        Lets lease-takeover recovery probe the exact table for orphan rows
        by commit token — and complete without re-running the payload
        builder (for evaluations: without re-grading)."""

        def _record() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute(
                    "UPDATE claims SET table_id=? WHERE scope_key=? AND status='PENDING'",
                    (table_id, scope_key),
                )

        await self._run(_record)

    async def complete_claim(
        self, world_id: str, scope_key: str, claimant: str, table_id: str
    ) -> None:
        """Publish the artifact's visibility and complete the claim — one CAS.

        Verifies the caller still holds the claim (PENDING + claimant match);
        completion puts the claim's commit token into the visible set. A lost
        lease fails closed: the taker-over owns completion now.
        """

        def _complete() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT status, claimant FROM claims WHERE scope_key=?", (scope_key,)
                ).fetchone()
                if row is None:
                    raise ClaimConflictError(f"no claim recorded for scope {scope_key}")
                if row["status"] == "COMPLETE":
                    return  # idempotent: recovery may race the original claimant
                if row["claimant"] != claimant:
                    raise ClaimPendingError(
                        f"claim {scope_key} was taken over by {row['claimant']}; "
                        "this claimant no longer owns completion"
                    )
                conn.execute(
                    "UPDATE claims SET status='COMPLETE', table_id=?, completed_at=? "
                    "WHERE scope_key=?",
                    (table_id, _utcnow(), scope_key),
                )

        await self._run(_complete)

    async def get_claim(self, world_id: str, scope_key: str) -> ClaimRecord | None:
        def _get() -> ClaimRecord | None:
            conn = self._connect_sync()
            row = conn.execute("SELECT * FROM claims WHERE scope_key=?", (scope_key,)).fetchone()
            return _claim_from_row(row) if row is not None else None

        return await self._run(_get)

    # ── artifact publications ────────────────────────────────────────────────

    async def acquire_artifact_publication(
        self,
        *,
        world_id: str,
        run_id: str,
        attempt_id: str,
        idempotency_key: str,
        request_digest: str,
        request_json: str,
        claimant: str,
        retry_window_ms: int,
        retry_not_after_ms: int | None = None,
        lease_ms: int = 900_000,
    ) -> tuple[str, ArtifactPublicationRecord]:
        """Claim one bundle publication or recover its interrupted phase.

        The request is recorded before external I/O. A recovered ``UPLOADED``
        row contains all object metadata needed to finish indexing without
        reopening the provider checkpoint.
        """
        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        retry_window_ms = _require_artifact_milliseconds(
            retry_window_ms,
            field="artifact retry_window_ms",
            maximum=_MAX_ARTIFACT_RETRY_WINDOW_MS,
        )
        if retry_not_after_ms is not None:
            retry_not_after_ms = _require_portable_counter(
                retry_not_after_ms, field="artifact retry_not_after_ms"
            )
        lease_ms = _require_artifact_lease_ms(lease_ms)
        publication_key = artifact_publication_key(world_id, run_id, idempotency_key)

        def _acquire() -> tuple[str, ArtifactPublicationRecord]:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=?",
                    (publication_key,),
                ).fetchone()
                now_ms = _now_ms()
                now_text = _utcnow()
                if row is not None:
                    existing = _artifact_publication_from_row(row)
                    if existing.request_digest != request_digest:
                        raise ArtifactPublicationConflictError(
                            f"artifact idempotency key {idempotency_key!r} was reused "
                            "with a different publication request"
                        )
                    outcome, recovered = _recover_artifact_publication_row(
                        conn,
                        existing,
                        claimant=claimant,
                        lease_ms=lease_ms,
                        now_ms=now_ms,
                        now_text=now_text,
                    )
                    assert recovered is not None
                    return outcome, recovered

                retry_until_ms = now_ms + retry_window_ms
                if retry_not_after_ms is not None:
                    retry_until_ms = min(retry_until_ms, retry_not_after_ms)
                initially_expired = retry_until_ms <= now_ms
                status = "EXPIRED" if initially_expired else "PENDING"
                lease_expires_at = 0.0 if initially_expired else (now_ms + lease_ms) / 1000
                last_error = _ARTIFACT_RETRY_EXPIRED_DETAIL if initially_expired else ""
                completed_at = now_text if initially_expired else None

                conn.execute(
                    "INSERT INTO artifact_publications (publication_key, world_id, run_id, "
                    "attempt_id, idempotency_key, request_digest, status, request_json, "
                    "records_json, claimant, lease_expires_at, retry_until_ms, attempt_count, "
                    "last_error, created_at, updated_at, completed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?, 1, ?, ?, ?, ?)",
                    (
                        publication_key,
                        world_id,
                        run_id,
                        attempt_id,
                        idempotency_key,
                        request_digest,
                        status,
                        request_json,
                        claimant,
                        lease_expires_at,
                        retry_until_ms,
                        last_error,
                        now_text,
                        now_text,
                        completed_at,
                    ),
                )
                created = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=?",
                    (publication_key,),
                ).fetchone()
                return (
                    "expired" if initially_expired else "acquired",
                    _artifact_publication_from_row(created),
                )

        return await self._run(_acquire)

    async def recover_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_ms: int,
    ) -> tuple[str, ArtifactPublicationRecord | None]:
        """Acquire one exact durable publication without echoing source content."""

        publication_key = _require_sha256(publication_key, field="publication_key")
        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        lease_ms = _require_artifact_lease_ms(lease_ms)

        def _recover() -> tuple[str, ArtifactPublicationRecord | None]:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                ).fetchone()
                if row is None:
                    return "obsolete", None
                return _recover_artifact_publication_row(
                    conn,
                    _artifact_publication_from_row(row),
                    claimant=claimant,
                    lease_ms=lease_ms,
                    now_ms=_now_ms(),
                    now_text=_utcnow(),
                )

        return await self._run(_recover)

    async def renew_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_seconds: float,
    ) -> ArtifactPublicationRecord:
        """Extend a publication lease while a long upload/index stage runs."""

        lease_seconds = _require_artifact_lease_seconds(lease_seconds)

        def _renew() -> ArtifactPublicationRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                ).fetchone()
                if row is None:
                    raise ArtifactPublicationConflictError(
                        f"no artifact publication recorded for {publication_key}"
                    )
                existing = _artifact_publication_from_row(row)
                if existing.status in {"INDEXED", "EXPIRED"}:
                    return existing
                if existing.claimant != claimant:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} was taken over by "
                        f"{existing.claimant}"
                    )
                now_ms = _now_ms()
                if existing.status == "PENDING" and existing.retry_until_ms <= now_ms:
                    expired_at = _utcnow()
                    conn.execute(
                        "UPDATE artifact_publications SET status='EXPIRED', "
                        "lease_expires_at=0, last_error=?, updated_at=?, completed_at=? "
                        "WHERE publication_key=? AND status='PENDING'",
                        (
                            _ARTIFACT_RETRY_EXPIRED_DETAIL,
                            expired_at,
                            expired_at,
                            publication_key,
                        ),
                    )
                    return _artifact_publication_from_row(
                        conn.execute(
                            "SELECT * FROM artifact_publications WHERE publication_key=?",
                            (publication_key,),
                        ).fetchone()
                    )
                if existing.lease_expires_at <= now_ms / 1000:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} lease expired before renewal"
                    )
                conn.execute(
                    "UPDATE artifact_publications SET lease_expires_at=?, updated_at=? "
                    "WHERE publication_key=?",
                    ((now_ms / 1000) + lease_seconds, _utcnow(), publication_key),
                )
                updated = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=?",
                    (publication_key,),
                ).fetchone()
                return _artifact_publication_from_row(updated)

        return await self._run(_renew)

    async def record_artifact_uploads(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        records_json: str,
        manifest_uri: str,
    ) -> None:
        """Persist uploaded object metadata before the Iceberg index commit."""

        def _record() -> bool:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                ).fetchone()
                if row is None:
                    raise ArtifactPublicationConflictError(
                        f"no artifact publication recorded for {publication_key}"
                    )
                existing = _artifact_publication_from_row(row)
                if existing.status == "INDEXED":
                    return False
                if existing.status == "EXPIRED":
                    raise ArtifactPublicationExpiredError(
                        f"artifact publication {publication_key} has expired"
                    )
                if existing.claimant != claimant:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} was taken over by "
                        f"{existing.claimant}"
                    )
                if existing.status == "UPLOADED":
                    if (
                        existing.records_json == records_json
                        and existing.manifest_uri == manifest_uri
                    ):
                        return False
                    raise ArtifactPublicationConflictError(
                        f"artifact publication {publication_key} already recorded "
                        "different uploaded objects"
                    )
                now_ms = _now_ms()
                if existing.retry_until_ms <= now_ms:
                    expired_at = _utcnow()
                    conn.execute(
                        "UPDATE artifact_publications SET status='EXPIRED', "
                        "lease_expires_at=0, last_error=?, updated_at=?, completed_at=? "
                        "WHERE publication_key=? AND status='PENDING'",
                        (
                            _ARTIFACT_RETRY_EXPIRED_DETAIL,
                            expired_at,
                            expired_at,
                            publication_key,
                        ),
                    )
                    return True
                if existing.lease_expires_at <= now_ms / 1000:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} lease expired before uploads"
                    )
                conn.execute(
                    "UPDATE artifact_publications SET status='UPLOADED', records_json=?, "
                    "manifest_uri=?, last_error='', updated_at=? WHERE publication_key=?",
                    (records_json, manifest_uri, _utcnow(), publication_key),
                )
                return False

        expired = await self._run(_record)
        if expired:
            raise ArtifactPublicationExpiredError(
                f"artifact publication {publication_key} expired before uploads"
            )

    async def complete_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        index_snapshot_id: int,
    ) -> None:
        """Mark the bundle indexed after its query rows become visible."""

        if (
            isinstance(index_snapshot_id, bool)
            or not isinstance(index_snapshot_id, int)
            or index_snapshot_id <= 0
            or index_snapshot_id > MAX_ICEBERG_SNAPSHOT_ID
        ):
            raise ValueError("index_snapshot_id must be a positive integer no greater than 2^63-1")

        def _complete() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                ).fetchone()
                if row is None:
                    raise ArtifactPublicationConflictError(
                        f"no artifact publication recorded for {publication_key}"
                    )
                existing = _artifact_publication_from_row(row)
                if existing.status == "INDEXED":
                    if existing.index_snapshot_id == index_snapshot_id:
                        return
                    raise ArtifactPublicationConflictError(
                        f"artifact publication {publication_key} was indexed at snapshot "
                        f"{existing.index_snapshot_id}, not {index_snapshot_id}"
                    )
                if existing.status != "UPLOADED":
                    raise ArtifactPublicationConflictError(
                        f"artifact publication {publication_key} cannot move from "
                        f"{existing.status} to INDEXED"
                    )
                if existing.claimant != claimant:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} was taken over by "
                        f"{existing.claimant}"
                    )
                if existing.lease_expires_at <= _now_ms() / 1000:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} lease expired before completion"
                    )
                completed = _utcnow()
                conn.execute(
                    "UPDATE artifact_publications SET status='INDEXED', "
                    "index_snapshot_id=?, last_error='', updated_at=?, completed_at=? "
                    "WHERE publication_key=?",
                    (
                        index_snapshot_id,
                        completed,
                        completed,
                        publication_key,
                    ),
                )

        await self._run(_complete)

    async def fail_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
        *,
        retry_delay_ms: int,
    ) -> None:
        """Release a failed phase for a later bounded reconciliation pass."""

        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        if not isinstance(error, str):
            raise TypeError("artifact publication error must be a string")
        if len(error) > 8000:
            raise ValueError("artifact publication error exceeds 8000 characters")
        retry_delay_ms = _require_artifact_milliseconds(
            retry_delay_ms,
            field="artifact retry_delay_ms",
            maximum=_MAX_ARTIFACT_RETRY_DELAY_MS,
        )

        def _fail() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                ).fetchone()
                if row is None or row["status"] in {"INDEXED", "EXPIRED"}:
                    return
                if row["claimant"] != claimant:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} is no longer owned by {claimant}"
                    )
                existing = _artifact_publication_from_row(row)
                now_ms = _now_ms()
                if existing.status == "PENDING" and existing.retry_until_ms <= now_ms:
                    expired_at = _utcnow()
                    conn.execute(
                        "UPDATE artifact_publications SET status='EXPIRED', "
                        "lease_expires_at=0, last_error=?, updated_at=?, completed_at=? "
                        "WHERE publication_key=? AND status='PENDING'",
                        (
                            _ARTIFACT_RETRY_EXPIRED_DETAIL,
                            expired_at,
                            expired_at,
                            publication_key,
                        ),
                    )
                    return
                if existing.lease_expires_at <= now_ms / 1000:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} lease expired before failure"
                    )
                conn.execute(
                    "UPDATE artifact_publications SET last_error=?, lease_expires_at=?, "
                    "updated_at=? WHERE publication_key=?",
                    (
                        error,
                        (now_ms + retry_delay_ms) / 1000,
                        _utcnow(),
                        publication_key,
                    ),
                )

        await self._run(_fail)

    async def expire_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
    ) -> None:
        """Terminally expire a PENDING bundle after its replay window closes."""

        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        if not isinstance(error, str):
            raise TypeError("artifact publication error must be a string")
        if len(error) > 8000:
            raise ValueError("artifact publication error exceeds 8000 characters")

        def _expire() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                ).fetchone()
                if row is None:
                    raise ArtifactPublicationConflictError(
                        f"no artifact publication recorded for {publication_key}"
                    )
                if row["status"] in {"INDEXED", "EXPIRED"}:
                    return
                if row["status"] == "UPLOADED":
                    raise ArtifactPublicationConflictError(
                        "uploaded artifact publications must be indexed, not expired"
                    )
                if row["claimant"] != claimant:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} was taken over"
                    )
                existing = _artifact_publication_from_row(row)
                if existing.lease_expires_at <= _now_ms() / 1000:
                    raise ArtifactPublicationPendingError(
                        f"artifact publication {publication_key} lease expired before expiry"
                    )
                completed = _utcnow()
                conn.execute(
                    "UPDATE artifact_publications SET status='EXPIRED', last_error=?, "
                    "updated_at=?, completed_at=? WHERE publication_key=?",
                    (error, completed, completed, publication_key),
                )

        await self._run(_expire)

    async def get_artifact_publication(
        self, world_id: str, publication_key: str
    ) -> ArtifactPublicationRecord | None:
        def _get() -> ArtifactPublicationRecord | None:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM artifact_publications WHERE publication_key=? AND world_id=?",
                    (publication_key, world_id),
                )
                .fetchone()
            )
            return _artifact_publication_from_row(row) if row is not None else None

        return await self._run(_get)

    async def list_due_artifact_publications(
        self,
        world_id: str,
        *,
        limit: int = 100,
        after_publication_key: str = "",
    ) -> list[ArtifactPublicationCandidate]:
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise ValueError("artifact publication page limit must be between 1 and 10000")
        if after_publication_key != "":
            after_publication_key = _require_sha256(
                after_publication_key, field="after_publication_key"
            )

        def _list() -> list[ArtifactPublicationCandidate]:
            now = _now_ms() / 1000
            rows = (
                self._connect_sync()
                .execute(
                    "SELECT publication_key FROM artifact_publications WHERE world_id=? "
                    "AND status IN ('PENDING', 'UPLOADED') AND lease_expires_at<=? "
                    "AND publication_key>? ORDER BY publication_key LIMIT ?",
                    (world_id, now, after_publication_key, limit),
                )
                .fetchall()
            )
            return [
                ArtifactPublicationCandidate(
                    publication_key=_require_sha256(row["publication_key"], field="publication_key")
                )
                for row in rows
            ]

        return await self._run(_list)

    # ── signatures ───────────────────────────────────────────────────────────

    async def register_signature(self, record: SignatureRecord) -> None:
        def _register() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT fingerprint FROM signatures WHERE table_id=?",
                    (record.table_id,),
                ).fetchone()
                if row is not None:
                    if row["fingerprint"] != record.fingerprint:
                        raise CatalogConflictError(
                            f"signature {record.table_id} already registered with a "
                            f"different schema fingerprint in catalog {self.path}"
                        )
                    return
                conn.execute(
                    "INSERT INTO signatures "
                    "(table_id, component_names, schema_json, fingerprint) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        record.table_id,
                        json.dumps(list(record.component_names)),
                        record.schema_json,
                        record.fingerprint,
                    ),
                )

        await self._run(_register)

    async def list_signatures(self) -> list[SignatureRecord]:
        def _list() -> list[SignatureRecord]:
            conn = self._connect_sync()
            rows = conn.execute("SELECT * FROM signatures ORDER BY table_id").fetchall()
            return [
                SignatureRecord(
                    table_id=row["table_id"],
                    component_names=tuple(json.loads(row["component_names"])),
                    schema_json=row["schema_json"],
                    fingerprint=row["fingerprint"],
                )
                for row in rows
            ]

        return await self._run(_list)

    # ── durable commands + transactional outbox ────────────────────────────

    async def admit_commands(
        self,
        world_id: str,
        admissions: list[CommandAdmission],
    ) -> list[CommandRecord]:
        """Atomically admit a batch before acknowledging any command ID.

        Command identity is content-address checked. Replaying the same ID and
        immutable content returns its existing record; reusing the ID with
        changed content fails the whole batch.
        """
        if not admissions:
            return []

        def _admit() -> list[CommandRecord]:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                world = conn.execute(
                    "SELECT status FROM worlds WHERE world_id=?", (world_id,)
                ).fetchone()
                if world is None or world["status"] != "active":
                    raise CommandConflictError(
                        f"world {world_id} is not active in catalog {self.path}"
                    )

                # Validate the entire batch before inserting any member.
                seen: dict[str, str] = {}
                for admission in admissions:
                    prior_digest = seen.get(admission.command_id)
                    if prior_digest is not None and prior_digest != admission.payload_digest:
                        raise CommandConflictError(
                            f"command {admission.command_id} appears twice with different content"
                        )
                    seen[admission.command_id] = admission.payload_digest
                    row = conn.execute(
                        "SELECT payload_digest FROM commands WHERE command_id=?",
                        (admission.command_id,),
                    ).fetchone()
                    if row is not None and row["payload_digest"] != admission.payload_digest:
                        raise CommandConflictError(
                            f"command {admission.command_id} already exists with different content"
                        )

                now = _utcnow()
                for admission in admissions:
                    existing = conn.execute(
                        "SELECT 1 FROM commands WHERE command_id=?", (admission.command_id,)
                    ).fetchone()
                    if existing is not None:
                        continue
                    conn.execute(
                        "INSERT INTO commands "
                        "(command_id, world_id, scheduled_tick, priority, command_type, "
                        "payload_json, payload_digest, version, principal_id, origin, "
                        "reserved_entity_id, status, attempts, max_attempts, accepted_at, "
                        "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', "
                        "0, ?, ?, ?)",
                        (
                            admission.command_id,
                            world_id,
                            admission.scheduled_tick,
                            admission.priority,
                            admission.command_type,
                            admission.payload_json,
                            admission.payload_digest,
                            admission.version,
                            admission.principal_id,
                            admission.origin,
                            admission.reserved_entity_id,
                            admission.max_attempts,
                            now,
                            now,
                        ),
                    )
                    _append_command_event(
                        conn,
                        world_id=world_id,
                        command_id=admission.command_id,
                        command_type=admission.command_type,
                        status="queued",
                        actor_id=admission.principal_id,
                        payload_json=json.dumps(
                            {
                                "origin": admission.origin,
                                "scheduled_tick": admission.scheduled_tick,
                                "priority": admission.priority,
                            },
                            sort_keys=True,
                        ),
                        occurred_at=now,
                    )

                records: list[CommandRecord] = []
                for admission in admissions:
                    row = conn.execute(
                        "SELECT * FROM commands WHERE command_id=?", (admission.command_id,)
                    ).fetchone()
                    assert row is not None
                    records.append(_command_from_row(row))
                return records

        return await self._run(_admit)

    async def lease_commands(
        self,
        world_id: str,
        tick: int,
        owner: str,
        *,
        lease_seconds: float = 30.0,
        limit: int = 50_000,
    ) -> list[CommandRecord]:
        """Lease due commands in durable order without removing them."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if limit < 1:
            raise ValueError("limit must be positive")

        def _lease() -> list[CommandRecord]:
            conn = self._connect_sync()
            now = time.time()
            expires = now + lease_seconds
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                world = conn.execute(
                    "SELECT status FROM worlds WHERE world_id=?", (world_id,)
                ).fetchone()
                if world is None or world["status"] != "active":
                    raise CommandConflictError(
                        f"world {world_id} is not active in catalog {self.path}"
                    )
                rows = conn.execute(
                    "SELECT * FROM commands WHERE world_id=? AND scheduled_tick<=? AND ("
                    "status IN ('PENDING', 'RETRYABLE') OR "
                    "(status='LEASED' AND (lease_owner=? OR lease_expires_at<=?))) "
                    "ORDER BY scheduled_tick, priority, sequence LIMIT ?",
                    (world_id, tick, owner, now, limit),
                ).fetchall()
                leased: list[CommandRecord] = []
                for row in rows:
                    same_live_lease = (
                        row["status"] == "LEASED"
                        and row["lease_owner"] == owner
                        and float(row["lease_expires_at"] or 0) > now
                    )
                    attempts = int(row["attempts"]) + (0 if same_live_lease else 1)
                    conn.execute(
                        "UPDATE commands SET status='LEASED', attempts=?, lease_owner=?, "
                        "lease_expires_at=?, updated_at=? WHERE command_id=?",
                        (attempts, owner, expires, _utcnow(), row["command_id"]),
                    )
                    updated = conn.execute(
                        "SELECT * FROM commands WHERE command_id=?", (row["command_id"],)
                    ).fetchone()
                    assert updated is not None
                    leased.append(_command_from_row(updated))
                return leased

        return await self._run(_lease)

    async def fail_command(
        self,
        world_id: str,
        command_id: str,
        owner: str,
        *,
        status: str,
        error_code: str,
        error_detail: str,
    ) -> CommandRecord:
        """Settle one dispatch failure and append its authoritative event."""
        if status not in {"RETRYABLE", "REJECTED", "DEAD_LETTER"}:
            raise ValueError(f"invalid command failure status: {status}")

        def _fail() -> CommandRecord:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM commands WHERE command_id=? AND world_id=?",
                    (command_id, world_id),
                ).fetchone()
                if row is None:
                    raise CommandConflictError(f"unknown command {command_id} for world {world_id}")
                if row["status"] in {"REJECTED", "DEAD_LETTER"}:
                    return _command_from_row(row)
                if row["status"] != "LEASED" or row["lease_owner"] != owner:
                    raise CommandConflictError(
                        f"command {command_id} is not leased by {owner}; refusing settlement"
                    )
                now = _utcnow()
                conn.execute(
                    "UPDATE commands SET status=?, lease_owner=NULL, lease_expires_at=NULL, "
                    "last_error_code=?, last_error_detail=?, updated_at=? WHERE command_id=?",
                    (status, error_code, error_detail[:2000], now, command_id),
                )
                _append_command_event(
                    conn,
                    world_id=world_id,
                    command_id=command_id,
                    command_type=row["command_type"],
                    status=status.lower(),
                    actor_id=row["principal_id"],
                    payload_json=json.dumps(
                        {"error_code": error_code, "error_detail": error_detail[:500]},
                        sort_keys=True,
                    ),
                    occurred_at=now,
                )
                updated = conn.execute(
                    "SELECT * FROM commands WHERE command_id=?", (command_id,)
                ).fetchone()
                assert updated is not None
                return _command_from_row(updated)

        return await self._run(_fail)

    async def release_commands(
        self,
        world_id: str,
        command_ids: list[str],
        owner: str,
    ) -> None:
        """Release an unprocessed leased tail without charging an attempt."""
        if not command_ids:
            return

        def _release() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                for command_id in command_ids:
                    conn.execute(
                        "UPDATE commands SET status='PENDING', attempts=MAX(attempts - 1, 0), "
                        "lease_owner=NULL, lease_expires_at=NULL, updated_at=? "
                        "WHERE command_id=? AND world_id=? AND status='LEASED' AND lease_owner=?",
                        (_utcnow(), command_id, world_id, owner),
                    )

        await self._run(_release)

    async def list_commands(
        self,
        world_id: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[CommandRecord]:
        if limit < 0:
            raise ValueError("limit must be non-negative")

        def _list() -> list[CommandRecord]:
            conn = self._connect_sync()
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM commands WHERE world_id=? ORDER BY sequence DESC LIMIT ?",
                    (world_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM commands WHERE world_id=? AND status=? "
                    "ORDER BY sequence DESC LIMIT ?",
                    (world_id, status, limit),
                ).fetchall()
            return [_command_from_row(row) for row in reversed(rows)]

        return await self._run(_list)

    async def pending_command_count(self, world_id: str) -> int:
        def _count() -> int:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT COUNT(*) AS count FROM commands WHERE world_id=? "
                    "AND status IN ('PENDING', 'RETRYABLE', 'LEASED')",
                    (world_id,),
                )
                .fetchone()
            )
            return int(row["count"])

        return await self._run(_count)

    async def max_reserved_entity_id(self, world_id: str) -> int | None:
        def _max() -> int | None:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT MAX(reserved_entity_id) AS entity_id FROM commands WHERE world_id=?",
                    (world_id,),
                )
                .fetchone()
            )
            value = row["entity_id"] if row is not None else None
            return int(value) if value is not None else None

        return await self._run(_max)

    async def cancel_commands(self, world_id: str, *, reason: str) -> int:
        """Terminally reject unsettled commands when their world is destroyed."""

        def _cancel() -> int:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                return _reject_unsettled_commands(conn, world_id=world_id, reason=reason)

        return await self._run(_cancel)

    async def read_outbox(self, world_id: str, *, limit: int = 1000) -> list[OutboxRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")

        def _read() -> list[OutboxRecord]:
            rows = (
                self._connect_sync()
                .execute(
                    "SELECT * FROM outbox WHERE world_id=? AND projected_at IS NULL "
                    "ORDER BY sequence LIMIT ?",
                    (world_id, limit),
                )
                .fetchall()
            )
            return [_outbox_from_row(row) for row in rows]

        return await self._run(_read)

    async def mark_outbox_projected(self, world_id: str, event_ids: list[str]) -> None:
        if not event_ids:
            return

        def _mark() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    "UPDATE outbox SET projected_at=COALESCE(projected_at, ?) "
                    "WHERE world_id=? AND event_id=?",
                    [(_utcnow(), world_id, event_id) for event_id in event_ids],
                )

        await self._run(_mark)

    async def outbox_progress(self, world_id: str) -> tuple[int, int]:
        """Return ``(projected_watermark, pending_count)`` for observability."""

        def _progress() -> tuple[int, int]:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT COALESCE(MAX(CASE WHEN projected_at IS NOT NULL THEN sequence END), 0) "
                    "AS watermark, SUM(CASE WHEN projected_at IS NULL THEN 1 ELSE 0 END) AS pending "
                    "FROM outbox WHERE world_id=?",
                    (world_id,),
                )
                .fetchone()
            )
            return int(row["watermark"] or 0), int(row["pending"] or 0)

        return await self._run(_progress)

    # ── commit identity: writer fence + manifests (issue #273) ──────────────

    async def acquire_fence(self, world_id: str, holder: str) -> int:
        """CAS-acquire the world's writer fence; returns the new epoch.

        Every acquisition increments the epoch, so exactly one writer holds
        the live epoch and every earlier writer becomes stale. Publishing
        verifies the epoch inside the same transaction — a stale writer
        fails closed rather than splitting history.
        """

        def _acquire() -> int:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT epoch FROM writer_fence WHERE world_id=?", (world_id,)
                ).fetchone()
                epoch = (int(row["epoch"]) if row is not None else 0) + 1
                conn.execute(
                    "INSERT INTO writer_fence (world_id, epoch, holder, acquired_at) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(world_id) DO UPDATE SET "
                    "epoch=excluded.epoch, holder=excluded.holder, "
                    "acquired_at=excluded.acquired_at",
                    (world_id, epoch, holder, _utcnow()),
                )
                return epoch

        return await self._run(_acquire)

    async def current_fence_epoch(self, world_id: str) -> int | None:
        def _get() -> int | None:
            conn = self._connect_sync()
            row = conn.execute(
                "SELECT epoch FROM writer_fence WHERE world_id=?", (world_id,)
            ).fetchone()
            return int(row["epoch"]) if row is not None else None

        return await self._run(_get)

    async def max_manifest_tick(self, world_id: str, run_id: str) -> int | None:
        def _get() -> int | None:
            row = (
                self._connect_sync()
                .execute(
                    "SELECT MAX(tick) AS tick FROM manifests WHERE world_id=? AND run_id=?",
                    (world_id, run_id),
                )
                .fetchone()
            )
            return int(row["tick"]) if row is not None and row["tick"] is not None else None

        return await self._run(_get)

    async def publish_manifest(
        self,
        world_id: str,
        run_id: str,
        tick: int,
        commit_token: str,
        writer_epoch: int,
        table_ids: list[str],
        *,
        command_ids: list[str] | None = None,
        lease_owner: str | None = None,
    ) -> None:
        """Publish one tick's manifest — the LAST step of a tick commit.

        One transaction: verify the caller still holds the fence, put-if-
        absent the manifest row, settle every staged command, append their
        outbox events, and advance the world's tick head. A stale epoch raises
        StaleWriterError; a different already-published attempt for the same
        tick raises CatalogConflictError. Re-publishing the identical attempt
        is an idempotent retry.
        """
        command_ids = list(command_ids or [])
        if command_ids and not lease_owner:
            raise ValueError("lease_owner is required when settling commands")

        def _publish() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                fence = conn.execute(
                    "SELECT epoch FROM writer_fence WHERE world_id=?", (world_id,)
                ).fetchone()
                if fence is None or int(fence["epoch"]) != writer_epoch:
                    live = None if fence is None else int(fence["epoch"])
                    raise StaleWriterError(
                        f"writer epoch {writer_epoch} for world {world_id} is not the "
                        f"live fence epoch ({live}); refusing to publish tick {tick}"
                    )
                row = conn.execute(
                    "SELECT commit_token FROM manifests WHERE world_id=? AND run_id=? AND tick=?",
                    (world_id, run_id, tick),
                ).fetchone()
                if row is not None:
                    if row["commit_token"] != commit_token:
                        raise CatalogConflictError(
                            f"tick {tick} of world {world_id} already has a published "
                            f"manifest from a different commit attempt"
                        )
                else:
                    conn.execute(
                        "INSERT INTO manifests "
                        "(world_id, run_id, tick, commit_token, writer_epoch, tables_json, "
                        "created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            world_id,
                            run_id,
                            tick,
                            commit_token,
                            writer_epoch,
                            json.dumps(sorted(table_ids)),
                            _utcnow(),
                        ),
                    )

                settled_at = _utcnow()
                for command_id in command_ids:
                    command = conn.execute(
                        "SELECT * FROM commands WHERE command_id=? AND world_id=?",
                        (command_id, world_id),
                    ).fetchone()
                    if command is None:
                        raise CommandConflictError(
                            f"tick {tick} attempted to settle unknown command {command_id}"
                        )
                    if command["status"] == "APPLIED":
                        if (
                            int(command["applied_tick"]) != tick
                            or command["commit_token"] != commit_token
                        ):
                            raise CommandConflictError(
                                f"command {command_id} was applied by a different tick commit"
                            )
                        continue
                    if command["status"] != "LEASED" or command["lease_owner"] != lease_owner:
                        raise CommandConflictError(
                            f"command {command_id} is not leased by {lease_owner}; "
                            "refusing manifest settlement"
                        )
                    conn.execute(
                        "UPDATE commands SET status='APPLIED', lease_owner=NULL, "
                        "lease_expires_at=NULL, updated_at=?, applied_tick=?, commit_token=? "
                        "WHERE command_id=?",
                        (settled_at, tick, commit_token, command_id),
                    )
                    _append_command_event(
                        conn,
                        world_id=world_id,
                        command_id=command_id,
                        command_type=command["command_type"],
                        status="applied",
                        actor_id=command["principal_id"],
                        payload_json=json.dumps(
                            {"tick": tick, "commit_token": commit_token}, sort_keys=True
                        ),
                        occurred_at=settled_at,
                    )
                conn.execute(
                    "UPDATE worlds SET tick_head=MAX(tick_head, ?) WHERE world_id=?",
                    (tick, world_id),
                )

        await self._run(_publish)

    async def visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None = None
    ) -> dict[int, list[str]] | None:
        """The reader-side visibility map for one (world, run).

        Unions tick manifests with COMPLETE artifact claims (issue #274): a tick
        may carry one manifest token plus any number of artifact tokens. None
        only when the pair has neither manifests nor claims AND no fence —
        an uncoordinated or pre-#273 world whose rows are implicitly visible.
        A fence or any claim activates filtering; only published manifests
        and COMPLETE claim tokens are then visible. When the first claim is
        added to a never-fenced legacy run, its empty epoch-0 token remains
        allowed so coordination does not hide pre-existing rows.
        """

        def _tokens() -> dict[int, list[str]] | None:
            conn = self._connect_sync()
            any_manifest = conn.execute(
                "SELECT 1 FROM manifests WHERE world_id=? AND run_id=? LIMIT 1",
                (world_id, run_id),
            ).fetchone()
            any_claim = conn.execute(
                "SELECT 1 FROM claims WHERE world_id=? AND run_id=? LIMIT 1",
                (world_id, run_id),
            ).fetchone()
            fence = conn.execute(
                "SELECT 1 FROM writer_fence WHERE world_id=?", (world_id,)
            ).fetchone()
            if any_manifest is None and any_claim is None:
                # Distinguish true pre-#273 history (never fenced — implicitly
                # visible) from a coordinated world whose first commit hasn't
                # published (fence exists — nothing is visible yet).
                return None if fence is None else {}
            if ticks is None:
                tick_clause, args = "", []
            else:
                placeholders = ",".join("?" for _ in ticks)
                tick_clause = f" AND tick IN ({placeholders})"
                args = [int(t) for t in ticks]
            visible: dict[int, list[str]] = {}
            if any_manifest is None and fence is None:
                legacy_ticks = [0] if ticks is None else [int(tick) for tick in ticks]
                for tick in legacy_ticks:
                    visible.setdefault(tick, []).append("")
            for row in conn.execute(
                "SELECT tick, commit_token FROM manifests WHERE world_id=? AND run_id=?"
                + tick_clause,
                (world_id, run_id, *args),
            ).fetchall():
                visible.setdefault(int(row["tick"]), []).append(row["commit_token"])
            for row in conn.execute(
                "SELECT tick, commit_token FROM claims "
                "WHERE world_id=? AND run_id=? AND status='COMPLETE'" + tick_clause,
                (world_id, run_id, *args),
            ).fetchall():
                visible.setdefault(int(row["tick"]), []).append(row["commit_token"])
            return visible

        return await self._run(_tokens)

    async def list_manifests(
        self, world_id: str, run_id: str | None = None
    ) -> list[ManifestRecord]:
        def _list() -> list[ManifestRecord]:
            conn = self._connect_sync()
            if run_id is None:
                rows = conn.execute(
                    "SELECT * FROM manifests WHERE world_id=? ORDER BY run_id, tick",
                    (world_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM manifests WHERE world_id=? AND run_id=? ORDER BY tick",
                    (world_id, run_id),
                ).fetchall()
            return [
                ManifestRecord(
                    world_id=r["world_id"],
                    run_id=r["run_id"],
                    tick=int(r["tick"]),
                    commit_token=r["commit_token"],
                    writer_epoch=int(r["writer_epoch"]),
                    table_ids=tuple(json.loads(r["tables_json"])),
                    created_at=r["created_at"],
                )
                for r in rows
            ]

        return await self._run(_list)


def claim_scope_key(world_id: str, run_id: str, producer: str, external_id: str) -> str:
    """Deterministic claim identity: (storage is the catalog itself)."""
    payload = json.dumps(
        {
            "domain": _DIGEST_DOMAIN,
            "kind": "claim-scope",
            "world_id": world_id,
            "run_id": run_id,
            "producer": producer,
            "external_id": external_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def artifact_publication_key(world_id: str, run_id: str, idempotency_key: str) -> str:
    """Deterministic bundle identity within one storage control catalog."""
    payload = json.dumps(
        {
            "domain": _DIGEST_DOMAIN,
            "kind": "artifact-publication",
            "world_id": world_id,
            "run_id": run_id,
            "idempotency_key": idempotency_key,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _claim_from_row(row: sqlite3.Row) -> ClaimRecord:
    return ClaimRecord(
        scope_key=row["scope_key"],
        world_id=row["world_id"],
        run_id=row["run_id"],
        producer=row["producer"],
        external_id=row["external_id"],
        payload_digest=row["payload_digest"],
        status=row["status"],
        commit_token=row["commit_token"],
        tick=int(row["tick"]),
        artifact_entity_id=int(row["artifact_entity_id"]),
        table_id=row["table_id"],
        claimant=row["claimant"],
        lease_expires_at=float(row["lease_expires_at"]),
        fence_epoch=int(row["fence_epoch"]),
    )


def _artifact_publication_from_row(row: sqlite3.Row) -> ArtifactPublicationRecord:
    raw_snapshot_id = row["index_snapshot_id"]
    status = row["status"]
    if not isinstance(status, str) or status not in {
        "PENDING",
        "UPLOADED",
        "INDEXED",
        "EXPIRED",
    }:
        raise RuntimeError(f"local artifact publication has invalid status {status!r}")
    raw_lease_expires_at = row["lease_expires_at"]
    raw_retry_until_ms = row["retry_until_ms"]
    raw_attempt_count = row["attempt_count"]
    if (
        type(raw_lease_expires_at) not in {int, float}
        or not math.isfinite(raw_lease_expires_at)
        or raw_lease_expires_at < 0
        or raw_lease_expires_at > _MAX_PORTABLE_COUNTER
    ):
        raise RuntimeError("local artifact publication has invalid lease_expires_at")
    if (
        type(raw_retry_until_ms) is not int
        or raw_retry_until_ms < 0
        or raw_retry_until_ms > _MAX_PORTABLE_COUNTER
    ):
        raise RuntimeError("local artifact publication has invalid retry_until_ms")
    if (
        type(raw_attempt_count) is not int
        or raw_attempt_count < 1
        or raw_attempt_count > _MAX_PORTABLE_COUNTER
    ):
        raise RuntimeError("local artifact publication has invalid attempt_count")
    if type(raw_snapshot_id) is not int:
        raise RuntimeError("local artifact publication has a lossy snapshot ID")
    if status == "INDEXED":
        if not 1 <= raw_snapshot_id <= MAX_ICEBERG_SNAPSHOT_ID:
            raise RuntimeError("local INDEXED artifact publication snapshot ID is out of range")
    elif raw_snapshot_id != 0:
        raise RuntimeError("local unindexed artifact publication has a nonzero snapshot ID")
    return ArtifactPublicationRecord(
        publication_key=row["publication_key"],
        world_id=row["world_id"],
        run_id=row["run_id"],
        attempt_id=row["attempt_id"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        status=status,
        request_json=row["request_json"],
        records_json=row["records_json"],
        claimant=row["claimant"],
        lease_expires_at=float(raw_lease_expires_at),
        retry_until_ms=raw_retry_until_ms,
        attempt_count=raw_attempt_count,
        index_snapshot_id=raw_snapshot_id,
        manifest_uri=row["manifest_uri"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
    )


def _recover_artifact_publication_row(
    conn: sqlite3.Connection,
    existing: ArtifactPublicationRecord,
    *,
    claimant: str,
    lease_ms: int,
    now_ms: int,
    now_text: str,
) -> tuple[str, ArtifactPublicationRecord]:
    """Apply the exact source-native recovery state machine under one write lock."""

    if existing.status == "INDEXED":
        return "duplicate", existing
    if existing.status == "EXPIRED":
        return "expired", existing
    if existing.status not in {"PENDING", "UPLOADED"}:
        raise ArtifactPublicationConflictError(
            f"artifact publication {existing.publication_key} has invalid status {existing.status}"
        )
    if existing.status == "PENDING" and existing.retry_until_ms <= now_ms:
        conn.execute(
            "UPDATE artifact_publications SET status='EXPIRED', lease_expires_at=0, "
            "last_error=?, updated_at=?, completed_at=? WHERE publication_key=? "
            "AND status='PENDING'",
            (
                _ARTIFACT_RETRY_EXPIRED_DETAIL,
                now_text,
                now_text,
                existing.publication_key,
            ),
        )
        row = conn.execute(
            "SELECT * FROM artifact_publications WHERE publication_key=?",
            (existing.publication_key,),
        ).fetchone()
        assert row is not None
        return "expired", _artifact_publication_from_row(row)
    if existing.lease_expires_at > now_ms / 1000:
        if existing.claimant == claimant:
            updated = conn.execute(
                "UPDATE artifact_publications SET lease_expires_at=?, updated_at=? "
                "WHERE publication_key=? AND status=? AND claimant=?",
                (
                    (now_ms + lease_ms) / 1000,
                    now_text,
                    existing.publication_key,
                    existing.status,
                    claimant,
                ),
            )
            if updated.rowcount != 1:
                raise ArtifactPublicationPendingError(
                    f"artifact publication {existing.publication_key} changed before renewal"
                )
            row = conn.execute(
                "SELECT * FROM artifact_publications WHERE publication_key=?",
                (existing.publication_key,),
            ).fetchone()
            assert row is not None
            return "owned", _artifact_publication_from_row(row)
        raise ArtifactPublicationPendingError(
            f"a live lease holds artifact publication {existing.publication_key}"
        )
    if existing.attempt_count >= _MAX_PORTABLE_COUNTER:
        raise ArtifactPublicationConflictError(
            f"artifact publication {existing.publication_key} exhausted its portable "
            "attempt counter"
        )
    updated = conn.execute(
        "UPDATE artifact_publications SET claimant=?, lease_expires_at=?, "
        "attempt_count=attempt_count+1, updated_at=? WHERE publication_key=? "
        "AND status=? AND lease_expires_at<=?",
        (
            claimant,
            (now_ms + lease_ms) / 1000,
            now_text,
            existing.publication_key,
            existing.status,
            now_ms / 1000,
        ),
    )
    if updated.rowcount != 1:
        raise ArtifactPublicationPendingError(
            f"artifact publication {existing.publication_key} changed before recovery"
        )
    row = conn.execute(
        "SELECT * FROM artifact_publications WHERE publication_key=?",
        (existing.publication_key,),
    ).fetchone()
    assert row is not None
    return "recovered", _artifact_publication_from_row(row)


def _command_from_row(row: sqlite3.Row) -> CommandRecord:
    return CommandRecord(
        command_id=row["command_id"],
        world_id=row["world_id"],
        sequence=int(row["sequence"]),
        scheduled_tick=int(row["scheduled_tick"]),
        priority=int(row["priority"]),
        command_type=row["command_type"],
        payload_json=row["payload_json"],
        payload_digest=row["payload_digest"],
        version=int(row["version"]),
        principal_id=row["principal_id"],
        origin=row["origin"],
        reserved_entity_id=(
            int(row["reserved_entity_id"]) if row["reserved_entity_id"] is not None else None
        ),
        status=row["status"],
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        lease_owner=row["lease_owner"],
        lease_expires_at=(
            float(row["lease_expires_at"]) if row["lease_expires_at"] is not None else None
        ),
        last_error_code=row["last_error_code"],
        last_error_detail=row["last_error_detail"],
        accepted_at=row["accepted_at"],
        updated_at=row["updated_at"],
        applied_tick=int(row["applied_tick"]) if row["applied_tick"] is not None else None,
        commit_token=row["commit_token"],
    )


def _outbox_from_row(row: sqlite3.Row) -> OutboxRecord:
    return OutboxRecord(
        sequence=int(row["sequence"]),
        event_id=row["event_id"],
        world_id=row["world_id"],
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        event_type=row["event_type"],
        command_type=row["command_type"],
        status=row["status"],
        actor_id=row["actor_id"],
        payload_json=row["payload_json"],
        occurred_at=row["occurred_at"],
        projected_at=row["projected_at"],
    )


def _append_command_event(
    conn: sqlite3.Connection,
    *,
    world_id: str,
    command_id: str,
    command_type: str,
    status: str,
    actor_id: str | None,
    payload_json: str,
    occurred_at: str,
) -> None:
    conn.execute(
        "INSERT INTO outbox (event_id, world_id, aggregate_type, aggregate_id, event_type, "
        "command_type, status, actor_id, payload_json, occurred_at) "
        "VALUES (?, ?, 'command', ?, ?, ?, ?, ?, ?, ?)",
        (
            str(uuid7()),
            world_id,
            command_id,
            f"command.{status}",
            command_type,
            status,
            actor_id,
            payload_json,
            occurred_at,
        ),
    )


def _reject_unsettled_commands(
    conn: sqlite3.Connection,
    *,
    world_id: str,
    reason: str,
) -> int:
    """Reject open commands inside the caller's world-state transaction."""
    rows = conn.execute(
        "SELECT * FROM commands WHERE world_id=? "
        "AND status IN ('PENDING', 'RETRYABLE', 'LEASED') ORDER BY sequence",
        (world_id,),
    ).fetchall()
    now = _utcnow()
    for row in rows:
        conn.execute(
            "UPDATE commands SET status='REJECTED', lease_owner=NULL, "
            "lease_expires_at=NULL, last_error_code='world_destroyed', "
            "last_error_detail=?, updated_at=? WHERE command_id=?",
            (reason[:2000], now, row["command_id"]),
        )
        _append_command_event(
            conn,
            world_id=world_id,
            command_id=row["command_id"],
            command_type=row["command_type"],
            status="rejected",
            actor_id=row["principal_id"],
            payload_json=json.dumps({"error_code": "world_destroyed"}),
            occurred_at=now,
        )
    return len(rows)


def _utcnow() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _world_from_row(row: sqlite3.Row) -> WorldRecord:
    return WorldRecord(
        world_id=row["world_id"],
        name=row["name"],
        run_id=row["run_id"],
        parent_world_id=row["parent_world_id"],
        status=row["status"],
        tick_head=int(row["tick_head"]),
    )
