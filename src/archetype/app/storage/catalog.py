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

"""Durable control authority — private implementation resource of StorageService.

The catalog makes world discovery, writer fencing, tick visibility, deferred
commands, command settlement, evaluation execution leases, and the
transactional outbox durable. The process-local registries in WorldService and
the stores remain caches. Domain tables such as artifact indexes and evaluation
results live in Iceberg instead of becoming control-catalog workflow state.

Design rules (issue #272, design review 2026-07-14):

- The catalog location is a **pure function of the storage identity**: the
  same StorageConfig always resolves the same catalog, across processes,
  restarts, and crashes.
- Records are compact control facts: world/signature descriptors, writer
  fences, tick manifests, commands, evaluation leases, and outbox rows. There
  are no entity directories, lineage copies, artifact claims, or domain payload
  indexes.
- Append-only in spirit: worlds transition status; nothing is deleted.
- Same identity + same content → idempotent no-op. Same identity + different
  content → loud ``CatalogConflictError``. Fail closed, never fail quiet.
- SQLite is the reference and default single-host control plane (LanceDB
  ``merge_insert`` is proven non-CAS under concurrency). The Cloudflare Durable
  Object implementation provides the same authority across hosts.

This module is deliberately not a service: it has no distinct authority or
gate surface (``docs/guide/service-protocols.md`` § new-service bar).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa
from uuid_utils import uuid7

from archetype._storage_uri import local_storage_path, normalized_storage_uri
from archetype.app.errors import ConflictError
from archetype.core.config import StorageConfig
from archetype.core.interfaces import StaleWriterError
from archetype.core.paths import require_safe_namespace, resolve_local_root

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 9
_DIGEST_DOMAIN = "archetype.catalog.v1"


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
    tick_head: int  # advisory discovery head derived from published manifests


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


@dataclass(frozen=True)
class EvaluationLease:
    """One durable serialization lease for a potentially expensive grader.

    The result itself remains an Iceberg row. This record only decides which
    process may execute the grader while that row is absent. ``acquired`` is
    the disposition of the current lease request, not persisted state.
    """

    world_id: str
    run_id: str
    evaluation_id: str
    subject_digest: str
    contract_digest: str
    status: str  # "RUNNING" | "RETRYABLE" | "COMPLETE"
    owner: str | None
    lease_expires_at: float | None
    created_at: str
    updated_at: str
    acquired: bool


class ControlCatalog(Protocol):
    """Shared local/remote authority exposed by StorageService."""

    async def register_world(self, record: WorldRecord) -> None: ...
    async def set_world_status(self, world_id: str, status: str) -> None: ...
    async def set_world_run(self, world_id: str, run_id: str) -> None: ...
    async def get_world(self, world_id: str) -> WorldRecord | None: ...
    async def list_worlds(self) -> list[WorldRecord]: ...
    async def register_signature(self, record: SignatureRecord) -> None: ...
    async def list_signatures(self) -> list[SignatureRecord]: ...
    async def acquire_fence(self, world_id: str, holder: str) -> int: ...
    async def current_fence_epoch(self, world_id: str) -> int | None: ...
    async def max_manifest_tick(self, world_id: str, run_id: str) -> int | None: ...
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
    ) -> None: ...
    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]] | None: ...
    async def list_manifests(
        self,
        world_id: str,
        run_id: str | None = None,
    ) -> list[ManifestRecord]: ...
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
    async def lease_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        subject_digest: str,
        contract_digest: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> EvaluationLease: ...
    async def complete_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None: ...
    async def release_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None: ...
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
CREATE TABLE IF NOT EXISTS evaluation_leases (
    world_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    evaluation_id TEXT NOT NULL,
    subject_digest TEXT NOT NULL,
    contract_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    owner TEXT,
    lease_expires_at REAL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (world_id, run_id, evaluation_id)
);
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
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self.path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    # ── connection ─────────────────────────────────────────────────────────

    def _connect_sync(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + max(self._busy_timeout_ms, 0) / 1000
        delay = 0.005
        while True:
            conn = sqlite3.connect(
                self.path,
                timeout=max(self._busy_timeout_ms, 0) / 1000,
                check_same_thread=False,
            )
            try:
                conn.row_factory = sqlite3.Row
                conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
                journal = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).upper()
                if journal != "WAL":
                    logger.warning(
                        "catalog %s: journal_mode=%s (WAL unavailable)", self.path, journal
                    )
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_DDL)
                version = int(
                    conn.execute(
                        "SELECT value FROM catalog_meta WHERE key='schema_version'"
                    ).fetchone()[0]
                )
                if version > _SCHEMA_VERSION:
                    raise CatalogSchemaMismatchError(
                        f"catalog {self.path} has schema_version={version}, "
                        f"this build expects {_SCHEMA_VERSION}"
                    )
                if version < _SCHEMA_VERSION:
                    conn.execute(
                        "UPDATE catalog_meta SET value=? WHERE key='schema_version'",
                        (str(_SCHEMA_VERSION),),
                    )
                conn.commit()
                self._conn = conn
                return conn
            except sqlite3.OperationalError as exc:
                conn.close()
                busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                remaining = deadline - time.monotonic()
                if not busy or remaining <= 0:
                    raise
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.1)
            except BaseException:
                conn.close()
                raise

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

    # ── evaluation execution serialization ────────────────────────────────

    async def lease_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        subject_digest: str,
        contract_digest: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> EvaluationLease:
        """Atomically select the one process allowed to execute a grader.

        Identity mismatches are returned to the evaluation service so it can
        preserve its public ``ValueError`` contract. A live lease owned by a
        different process is observationally ``acquired=False``. Expired and
        explicitly released leases may be taken over; a caller may also renew
        its own lease by calling this method again.
        """
        if not evaluation_id.strip():
            raise ValueError("evaluation_id must be non-empty")
        if not owner.strip():
            raise ValueError("evaluation lease owner must be non-empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        def _lease() -> EvaluationLease:
            conn = self._connect_sync()
            now_seconds = time.time()
            now = _utcnow()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM evaluation_leases WHERE "
                    "world_id=? AND run_id=? AND evaluation_id=?",
                    (world_id, run_id, evaluation_id),
                ).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO evaluation_leases "
                        "(world_id, run_id, evaluation_id, subject_digest, "
                        "contract_digest, status, owner, lease_expires_at, "
                        "created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?)",
                        (
                            world_id,
                            run_id,
                            evaluation_id,
                            subject_digest,
                            contract_digest,
                            owner,
                            now_seconds + lease_seconds,
                            now,
                            now,
                        ),
                    )
                    inserted = conn.execute(
                        "SELECT * FROM evaluation_leases WHERE "
                        "world_id=? AND run_id=? AND evaluation_id=?",
                        (world_id, run_id, evaluation_id),
                    ).fetchone()
                    assert inserted is not None
                    return _evaluation_lease_from_row(inserted, acquired=True)

                existing = _evaluation_lease_from_row(row, acquired=False)
                same_identity = (
                    existing.subject_digest == subject_digest
                    and existing.contract_digest == contract_digest
                )
                if not same_identity or existing.status == "COMPLETE":
                    return existing
                if existing.status not in {"RUNNING", "RETRYABLE"}:
                    raise CatalogSchemaMismatchError(
                        f"evaluation {evaluation_id!r} has invalid lease status "
                        f"{existing.status!r} in catalog {self.path}"
                    )

                available = (
                    existing.status == "RETRYABLE"
                    or existing.owner == owner
                    or (existing.lease_expires_at or 0.0) <= now_seconds
                )
                if not available:
                    return existing
                conn.execute(
                    "UPDATE evaluation_leases SET status='RUNNING', owner=?, "
                    "lease_expires_at=?, updated_at=? WHERE "
                    "world_id=? AND run_id=? AND evaluation_id=?",
                    (
                        owner,
                        now_seconds + lease_seconds,
                        now,
                        world_id,
                        run_id,
                        evaluation_id,
                    ),
                )
                updated = conn.execute(
                    "SELECT * FROM evaluation_leases WHERE "
                    "world_id=? AND run_id=? AND evaluation_id=?",
                    (world_id, run_id, evaluation_id),
                ).fetchone()
                assert updated is not None
                return _evaluation_lease_from_row(updated, acquired=True)

        return await self._run(_lease)

    async def complete_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None:
        """Mark an Iceberg-backed evaluation result as durably available."""

        def _complete() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT status, owner FROM evaluation_leases WHERE "
                    "world_id=? AND run_id=? AND evaluation_id=?",
                    (world_id, run_id, evaluation_id),
                ).fetchone()
                if row is None:
                    raise CatalogConflictError(
                        f"evaluation {evaluation_id!r} has no durable execution lease"
                    )
                if row["status"] == "COMPLETE":
                    return
                if row["status"] != "RUNNING" or row["owner"] != owner:
                    raise CatalogConflictError(
                        f"evaluation {evaluation_id!r} is not leased by {owner}"
                    )
                conn.execute(
                    "UPDATE evaluation_leases SET status='COMPLETE', owner=NULL, "
                    "lease_expires_at=NULL, updated_at=? WHERE "
                    "world_id=? AND run_id=? AND evaluation_id=?",
                    (_utcnow(), world_id, run_id, evaluation_id),
                )

        await self._run(_complete)

    async def release_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None:
        """Make a failed grader execution immediately retryable."""

        def _release() -> None:
            conn = self._connect_sync()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE evaluation_leases SET status='RETRYABLE', owner=NULL, "
                    "lease_expires_at=NULL, updated_at=? WHERE "
                    "world_id=? AND run_id=? AND evaluation_id=? "
                    "AND status='RUNNING' AND owner=?",
                    (_utcnow(), world_id, run_id, evaluation_id, owner),
                )

        await self._run(_release)

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
        """Return manifest commit tokens that make persisted tick rows visible.

        None denotes pre-coordination history with no writer fence. Once a
        world has a fence, an absent manifest means no rows are visible.
        """

        def _tokens() -> dict[int, list[str]] | None:
            conn = self._connect_sync()
            any_manifest = conn.execute(
                "SELECT 1 FROM manifests WHERE world_id=? AND run_id=? LIMIT 1",
                (world_id, run_id),
            ).fetchone()
            fence = conn.execute(
                "SELECT 1 FROM writer_fence WHERE world_id=?", (world_id,)
            ).fetchone()
            if any_manifest is None:
                return None if fence is None else {}
            if ticks is None:
                tick_clause, args = "", []
            else:
                placeholders = ",".join("?" for _ in ticks)
                tick_clause = " AND tick IN (" + placeholders + ")"
                args = [int(tick) for tick in ticks]
            visible: dict[int, list[str]] = {}
            for row in conn.execute(
                "SELECT tick, commit_token FROM manifests WHERE world_id=? AND run_id=?"
                + tick_clause,
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


def _evaluation_lease_from_row(
    row: sqlite3.Row,
    *,
    acquired: bool,
) -> EvaluationLease:
    return EvaluationLease(
        world_id=row["world_id"],
        run_id=row["run_id"],
        evaluation_id=row["evaluation_id"],
        subject_digest=row["subject_digest"],
        contract_digest=row["contract_digest"],
        status=row["status"],
        owner=row["owner"],
        lease_expires_at=(
            float(row["lease_expires_at"]) if row["lease_expires_at"] is not None else None
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        acquired=acquired,
    )


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
