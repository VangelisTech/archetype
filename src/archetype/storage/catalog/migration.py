# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed administrative state transfer for whole-storage migration.

These contracts are deliberately separate from :class:`ControlCatalog`.
Ordinary catalog mutation methods express live workflow transitions; they are
not valid import APIs.  The local migration slice requires a catalog that can
export exact logical state, reserve one immutable plan, stage that state
without exposing Worlds, and activate the directory last.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import Protocol, runtime_checkable

from archetype.storage.catalog.records import (
    CommandRecord,
    ManifestRecord,
    OutboxRecord,
    SignatureRecord,
    WorldRecord,
)

CONTROL_CATALOG_PROTOCOL_VERSION = 1
CONTROL_SNAPSHOT_FORMAT_VERSION = 1
MIGRATION_PLAN_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class FenceFloorRecord:
    """The minimum writer epoch imported for one World.

    Holder identity is intentionally absent.  Migration preserves the epoch
    floor, never the source process's authority.
    """

    world_id: str
    epoch: int


@dataclass(frozen=True, slots=True)
class EvaluationRecord:
    """Persisted evaluation coordination state without observational fields."""

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


@dataclass(frozen=True, slots=True)
class ControlCatalogSnapshot:
    """Versioned exact logical contents of one control catalog."""

    format_version: int
    catalog_schema_version: int
    catalog_protocol_version: int
    worlds: tuple[WorldRecord, ...]
    signatures: tuple[SignatureRecord, ...]
    manifests: tuple[ManifestRecord, ...]
    commands: tuple[CommandRecord, ...]
    evaluations: tuple[EvaluationRecord, ...]
    outbox: tuple[OutboxRecord, ...]
    fence_floors: tuple[FenceFloorRecord, ...]


@dataclass(frozen=True, slots=True)
class MigrationReservation:
    """Durable destination evidence for one immutable migration plan."""

    migration_id: str
    plan_digest: str
    plan_json: str
    status: str
    control_snapshot_digest: str | None
    receipt_digest: str | None
    receipt_json: str | None
    created_at: str
    updated_at: str


@runtime_checkable
class MigrationControlCatalog(Protocol):
    """Administrative catalog capability required by local migration v1."""

    async def export_migration_snapshot(self) -> ControlCatalogSnapshot: ...

    async def get_migration_reservation(
        self,
        migration_id: str,
    ) -> MigrationReservation | None: ...

    async def list_migration_reservations(self) -> tuple[MigrationReservation, ...]: ...

    async def reserve_migration(
        self,
        migration_id: str,
        plan_digest: str,
        plan_json: str,
    ) -> MigrationReservation: ...

    async def stage_migration_control(
        self,
        migration_id: str,
        plan_digest: str,
        snapshot: ControlCatalogSnapshot,
    ) -> None: ...

    async def activate_migration(
        self,
        migration_id: str,
        plan_digest: str,
        snapshot: ControlCatalogSnapshot,
    ) -> None: ...

    async def complete_migration(
        self,
        migration_id: str,
        plan_digest: str,
        receipt_digest: str,
        receipt_json: str,
    ) -> None: ...


def _json_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: _json_value(item) for key, item in asdict(value).items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def canonical_json(value: object) -> str:
    """Encode bounded migration control values deterministically."""

    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def control_snapshot_digest(snapshot: ControlCatalogSnapshot) -> str:
    """Bind every exported control record to one versioned digest."""

    payload = {
        "domain": "archetype.storage-migration.control.v1",
        "snapshot": _json_value(snapshot),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


__all__ = [
    "CONTROL_CATALOG_PROTOCOL_VERSION",
    "CONTROL_SNAPSHOT_FORMAT_VERSION",
    "MIGRATION_PLAN_FORMAT_VERSION",
    "ControlCatalogSnapshot",
    "EvaluationRecord",
    "FenceFloorRecord",
    "MigrationControlCatalog",
    "MigrationReservation",
    "canonical_json",
    "control_snapshot_digest",
]
