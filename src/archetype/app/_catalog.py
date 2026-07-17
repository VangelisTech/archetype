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
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa

from archetype._storage_uri import local_storage_path, normalized_storage_uri
from archetype.core.config import StorageConfig
from archetype.core.interfaces import StaleWriterError
from archetype.core.paths import require_safe_namespace, resolve_local_root

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 3
_DIGEST_DOMAIN = "archetype.catalog.v1"


class CatalogConflictError(RuntimeError):
    """Same identity registered with different content — never silently resolved."""


class CatalogSchemaMismatchError(RuntimeError):
    """A stored signature descriptor disagrees with the physical table schema."""


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


class ClaimConflictError(RuntimeError):
    """Same external id claimed/completed with a different payload digest."""


class ClaimPendingError(RuntimeError):
    """A live lease holds this claim; back off — never blind-retry."""


@dataclass(frozen=True)
class ClaimRecord:
    """One external-fact claim: the exactly-once-visible authority (issue #274).

    A fact is visible iff its claim is COMPLETE — completion publishes the
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
    fact_entity_id: int
    table_id: str | None
    claimant: str
    lease_expires_at: float
    fence_epoch: int


@dataclass(frozen=True)
class SignatureRecord:
    """Compact durable pointer to one archetype table."""

    table_id: str
    component_names: tuple[str, ...]
    schema_json: str  # canonical arrow_schema_descriptor, JSON-encoded
    fingerprint: str

    def matches(self, schema: pa.Schema) -> bool:
        return self.fingerprint == schema_fingerprint(schema)


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
    fact_entity_id INTEGER NOT NULL DEFAULT 0,
    table_id TEXT,
    claimant TEXT NOT NULL,
    lease_expires_at REAL NOT NULL,
    fence_epoch INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
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
            raise CatalogConflictError(
                f"catalog {self.path} has schema_version={version}, "
                f"this build expects {_SCHEMA_VERSION}"
            )
        if version < _SCHEMA_VERSION:
            # Upgrades are strictly additive (the DDL above already created
            # any missing tables); record the new version.
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
                conn.execute("UPDATE worlds SET status=? WHERE world_id=?", (status, world_id))

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

    # ── fact claims (issue #274) ─────────────────────────────────────────────

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
        - "duplicate": an identical fact is already COMPLETE — the original
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
                token = f"fact-{scope_key[:32]}"
                cursor = conn.execute(
                    "INSERT INTO claims (scope_key, world_id, run_id, producer, external_id, "
                    "payload_digest, status, commit_token, tick, fact_entity_id, table_id, "
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
                # Catalog-allocated fact entity id: unique per storage identity,
                # in the negative metadata band, clear of lineage's small ids.
                # (lastrowid is always set after a successful INSERT.)
                fact_eid = -(100_000 + int(cursor.lastrowid or 0))
                conn.execute(
                    "UPDATE claims SET fact_entity_id=? WHERE scope_key=?",
                    (fact_eid, scope_key),
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
        """Publish the fact's visibility and complete the claim — one CAS.

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
    ) -> None:
        """Publish one tick's manifest — the LAST step of a tick commit.

        One transaction: verify the caller still holds the fence, put-if-
        absent the manifest row, and advance the world's tick head. A stale
        epoch raises StaleWriterError; a different already-published attempt
        for the same tick raises CatalogConflictError. Re-publishing the
        identical attempt is a no-op (idempotent retry).
        """

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
                    if row["commit_token"] == commit_token:
                        return
                    raise CatalogConflictError(
                        f"tick {tick} of world {world_id} already has a published "
                        f"manifest from a different commit attempt"
                    )
                conn.execute(
                    "INSERT INTO manifests "
                    "(world_id, run_id, tick, commit_token, writer_epoch, tables_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
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
                conn.execute(
                    "UPDATE worlds SET tick_head=MAX(tick_head, ?) WHERE world_id=?",
                    (tick, world_id),
                )

        await self._run(_publish)

    async def visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None = None
    ) -> dict[int, list[str]] | None:
        """The reader-side visibility map for one (world, run).

        Unions tick manifests with COMPLETE fact claims (issue #274): a tick
        may carry one manifest token plus any number of fact tokens. None
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
        fact_entity_id=int(row["fact_entity_id"]),
        table_id=row["table_id"],
        claimant=row["claimant"],
        lease_expires_at=float(row["lease_expires_at"]),
        fence_epoch=int(row["fence_epoch"]),
    )


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
