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
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

import pyarrow as pa

from archetype.core.config import StorageConfig

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
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
            "uri": _normalized_uri(config),
            "namespace": config.namespace,
            "backend": config.backend.value,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalized_uri(config: StorageConfig) -> str:
    uri = str(config.uri)
    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        path = parsed.path if parsed.scheme == "file" else uri
        return str(Path(path).expanduser().resolve())
    return uri.rstrip("/")


def catalog_path_for(config: StorageConfig) -> Path:
    """The catalog location as a pure function of the storage identity.

    Local stores keep the record beside the data it is about
    (``<uri>/<namespace>/.archetype-catalog-<backend>.db``). Remote stores
    get a deterministic host-local path keyed by the storage fingerprint —
    the same config always resolves the same catalog on this host
    (single-host authority is the documented v0.3 limit). The backend is
    part of the identity in both forms, mirroring storage_fingerprint.
    """
    uri = str(config.uri)
    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        base = Path(parsed.path if parsed.scheme == "file" else uri).expanduser()
        return base / config.namespace / f".archetype-catalog-{config.backend.value}.db"
    root = Path(os.environ.get("ARCHETYPE_CATALOG_DIR", "~/.archetype/catalogs")).expanduser()
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
    async def set_tick_head(self, world_id: str, tick_head: int, run_id: str | None) -> None: ...
    async def get_world(self, world_id: str) -> WorldRecord | None: ...
    async def list_worlds(self) -> list[WorldRecord]: ...
    async def register_signature(self, record: SignatureRecord) -> None: ...
    async def list_signatures(self) -> list[SignatureRecord]: ...
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
        version = conn.execute(
            "SELECT value FROM catalog_meta WHERE key='schema_version'"
        ).fetchone()[0]
        if int(version) != _SCHEMA_VERSION:
            raise CatalogConflictError(
                f"catalog {self.path} has schema_version={version}, "
                f"this build expects {_SCHEMA_VERSION}"
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

    async def set_tick_head(self, world_id: str, tick_head: int, run_id: str | None) -> None:
        def _set() -> None:
            conn = self._connect_sync()
            with conn:
                if run_id is None:
                    conn.execute(
                        "UPDATE worlds SET tick_head=? WHERE world_id=?",
                        (tick_head, world_id),
                    )
                else:
                    conn.execute(
                        "UPDATE worlds SET tick_head=?, run_id=? WHERE world_id=?",
                        (tick_head, run_id, world_id),
                    )

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


def _world_from_row(row: sqlite3.Row) -> WorldRecord:
    return WorldRecord(
        world_id=row["world_id"],
        name=row["name"],
        run_id=row["run_id"],
        parent_world_id=row["parent_world_id"],
        status=row["status"],
        tick_head=int(row["tick_head"]),
    )
