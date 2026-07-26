# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Control-catalog records and stable identity helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pyarrow as pa

from archetype.core.config import StorageConfig
from archetype.core.paths import normalized_storage_uri
from archetype.errors import ConflictError

_DIGEST_DOMAIN = "archetype.catalog.v1"
WORLD_WRITER_MODES = frozenset({"resumable", "cleanup_only"})


class CatalogConflictError(ConflictError):
    """Same identity registered with different content — never silently resolved."""

    public_detail = "Catalog entry conflicts with existing state"


class CatalogSchemaMismatchError(RuntimeError):
    """Durable catalog schema is incompatible with this build or physical data."""


class CommandConflictError(ConflictError):
    """A command identity was reused with different immutable content."""

    public_detail = "Command conflicts with an existing durable command"


def require_world_writer_mode(value: str) -> str:
    """Validate the finite durable writer-mode vocabulary for new records."""

    if value not in WORLD_WRITER_MODES:
        raise ValueError(
            "world writer_mode must be one of: " + ", ".join(sorted(WORLD_WRITER_MODES))
        )
    return value


def arrow_schema_descriptor(schema: pa.Schema) -> dict[str, object]:
    """Return a JSON-native, order-preserving description of an Arrow schema."""

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
    """Return a domain-separated SHA-256 over the schema's logical shape."""

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
    """Return the stable, credential-free identity for a storage location."""

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


@dataclass(frozen=True)
class WorldRecord:
    """Compact durable pointer to a world in one store.

    ``writer_mode`` is immutable lifecycle identity. ``resumable`` is the
    legacy/default mode; ``cleanup_only`` records a writer that may persist
    evidence but must never be reconstructed as ordinary mutable work.
    """

    world_id: str
    name: str | None
    run_id: str | None
    parent_world_id: str | None
    status: str
    tick_head: int
    writer_mode: str = "resumable"


@dataclass(frozen=True)
class ManifestRecord:
    """One published tick commit: the visibility authority."""

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
    schema_json: str
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
    """One durable serialization lease for a potentially expensive grader."""

    world_id: str
    run_id: str
    evaluation_id: str
    subject_digest: str
    contract_digest: str
    status: str
    owner: str | None
    lease_expires_at: float | None
    created_at: str
    updated_at: str
    acquired: bool
