# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Frozen, credential-free contracts for whole-storage migration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from archetype.artifacts.models import ArtifactStoreConfig
from archetype.core.config import StorageConfig
from archetype.storage.catalog import ControlCatalog
from archetype.storage.catalog.migration import (
    CONTROL_SNAPSHOT_FORMAT_VERSION,
    ControlCatalogSnapshot,
    EvaluationRecord,
    FenceFloorRecord,
    MigrationControlCatalog,
    canonical_json,
)
from archetype.storage.catalog.records import (
    CommandRecord,
    ManifestRecord,
    OutboxRecord,
    SignatureRecord,
    WorldRecord,
)
from archetype.storage.interfaces import iStorageService
from archetype.storage.transfer import TableSnapshotEvidence

if TYPE_CHECKING:
    from archetype.migration.interfaces import ColdMigrationVerifier

MIGRATION_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class MigrationEndpoint:
    """One already-composed local migration endpoint.

    Runtime capabilities, paths, I/O configuration, and credentials are never
    represented by a plan or receipt and are omitted from this value's repr.
    Audit identity is explicit because an omitted runtime audit configuration
    means a separate default lakehouse, not co-location.
    """

    storage_config: StorageConfig = field(repr=False, compare=False)
    storage_service: iStorageService = field(repr=False, compare=False)
    control_catalog: ControlCatalog = field(repr=False, compare=False)
    artifact_store_config: ArtifactStoreConfig = field(repr=False, compare=False)
    activity_catalog_path: Path = field(repr=False, compare=False)
    audit_storage_config: StorageConfig = field(repr=False, compare=False)
    cold_verifier: ColdMigrationVerifier | None = field(default=None, repr=False, compare=False)

    @property
    def migration_control_catalog(self) -> MigrationControlCatalog:
        catalog = self.control_catalog
        if not isinstance(catalog, MigrationControlCatalog):
            raise TypeError("migration v1 requires local SQLite control administration")
        return catalog


TableClassification = Literal["ecs", "artifact", "application"]


@dataclass(frozen=True, slots=True)
class MigrationTablePlan:
    """Frozen source and expected destination evidence for one table."""

    name: str
    classification: TableClassification
    source: TableSnapshotEvidence
    destination: TableSnapshotEvidence


@dataclass(frozen=True, slots=True)
class ArtifactPlanEvidence:
    """Bounded inventory facts frozen before any destination mutation."""

    occurrence_count: int
    distinct_content_count: int
    total_bytes: int
    inventory_digest: str


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    """Immutable, resumable plan bound to invocation-scoped endpoints."""

    migration_id: str
    format_version: int
    archetype_version: str
    created_at: str
    source_storage_fingerprint: str
    destination_storage_fingerprint: str
    source_stability_digest: str
    tables: tuple[MigrationTablePlan, ...]
    artifacts: ArtifactPlanEvidence
    control: ControlCatalogSnapshot = field(repr=False)
    plan_digest: str
    source_endpoint: MigrationEndpoint = field(repr=False, compare=False)
    destination_endpoint: MigrationEndpoint = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class ArtifactMigrationReceipt:
    occurrence_count: int
    distinct_content_count: int
    total_verified_bytes: int
    inventory_digest: str


@dataclass(frozen=True, slots=True)
class TableMigrationReceipt:
    name: str
    classification: TableClassification
    source_snapshot_id: int | None
    destination_snapshot_id: int | None
    source_schema_fingerprint: str
    destination_schema_fingerprint: str
    row_count: int
    source_content_digest: str
    destination_content_digest: str


@dataclass(frozen=True, slots=True)
class ControlMigrationReceipt:
    world_count: int
    signature_count: int
    manifest_count: int
    command_count: int
    evaluation_count: int
    outbox_count: int
    fence_floor_count: int
    snapshot_digest: str
    activation_status: str


@dataclass(frozen=True, slots=True)
class ColdVerificationRequest:
    migration_id: str
    destination_storage_fingerprint: str
    tables: tuple[MigrationTablePlan, ...]
    worlds: tuple[WorldRecord, ...]
    fence_floors: tuple[FenceFloorRecord, ...]
    artifacts: ArtifactPlanEvidence


@dataclass(frozen=True, slots=True)
class ColdVerificationEvidence:
    destination_storage_fingerprint: str
    world_count: int
    table_count: int
    artifact_objects_verified: int
    visible_query_verified: bool
    resume_disposition: Literal["verified", "not_applicable"]
    resumed_world_id: str | None
    imported_fence_floor: int | None
    acquired_writer_epoch: int | None
    tick_before: int | None
    tick_after: int | None
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class MigrationReceipt:
    migration_id: str
    format_version: int
    archetype_version: str
    started_at: str
    verified_at: str
    source_storage_fingerprint: str
    destination_storage_fingerprint: str
    plan_digest: str
    source_stability_digest: str
    tables: tuple[TableMigrationReceipt, ...]
    artifacts: ArtifactMigrationReceipt
    control: ControlMigrationReceipt
    activity_disposition: Literal["empty-v1"]
    cold_verification: ColdVerificationEvidence
    receipt_digest: str


def _table_evidence(value: dict[str, Any]) -> TableSnapshotEvidence:
    return TableSnapshotEvidence(**value)


def _control_snapshot(value: dict[str, Any]) -> ControlCatalogSnapshot:
    if int(value["format_version"]) != CONTROL_SNAPSHOT_FORMAT_VERSION:
        raise ValueError("unsupported migration control snapshot format")
    return ControlCatalogSnapshot(
        format_version=int(value["format_version"]),
        catalog_schema_version=int(value["catalog_schema_version"]),
        catalog_protocol_version=int(value["catalog_protocol_version"]),
        worlds=tuple(WorldRecord(**row) for row in value["worlds"]),
        signatures=tuple(
            SignatureRecord(
                table_id=str(row["table_id"]),
                component_names=tuple(str(item) for item in row["component_names"]),
                schema_json=str(row["schema_json"]),
                fingerprint=str(row["fingerprint"]),
            )
            for row in value["signatures"]
        ),
        manifests=tuple(
            ManifestRecord(
                world_id=str(row["world_id"]),
                run_id=str(row["run_id"]),
                tick=int(row["tick"]),
                commit_token=str(row["commit_token"]),
                writer_epoch=int(row["writer_epoch"]),
                table_ids=tuple(str(item) for item in row["table_ids"]),
                created_at=str(row["created_at"]),
            )
            for row in value["manifests"]
        ),
        commands=tuple(CommandRecord(**row) for row in value["commands"]),
        evaluations=tuple(EvaluationRecord(**row) for row in value["evaluations"]),
        outbox=tuple(OutboxRecord(**row) for row in value["outbox"]),
        fence_floors=tuple(FenceFloorRecord(**row) for row in value["fence_floors"]),
    )


def migration_plan_payload(plan: MigrationPlan) -> dict[str, Any]:
    """Return the persistent credential-free plan payload."""

    return {
        "migration_id": plan.migration_id,
        "format_version": plan.format_version,
        "archetype_version": plan.archetype_version,
        "created_at": plan.created_at,
        "source_storage_fingerprint": plan.source_storage_fingerprint,
        "destination_storage_fingerprint": plan.destination_storage_fingerprint,
        "source_stability_digest": plan.source_stability_digest,
        "tables": [asdict(table) for table in plan.tables],
        "artifacts": asdict(plan.artifacts),
        "control": asdict(plan.control),
    }


def migration_plan_json(plan: MigrationPlan) -> str:
    return canonical_json(migration_plan_payload(plan))


def migration_plan_digest(payload: dict[str, Any]) -> str:
    bound = {
        "domain": "archetype.storage-migration.plan.v1",
        "plan": payload,
    }
    return hashlib.sha256(canonical_json(bound).encode("utf-8")).hexdigest()


def load_migration_plan(
    plan_json: str,
    *,
    source: MigrationEndpoint,
    destination: MigrationEndpoint,
    expected_digest: str,
) -> MigrationPlan:
    """Rehydrate one reserved plan and rebind only caller-supplied endpoints."""

    value = json.loads(plan_json)
    if not isinstance(value, dict):
        raise ValueError("migration plan must decode to an object")
    observed = migration_plan_digest(value)
    if observed != expected_digest:
        raise ValueError("reserved migration plan digest does not match its payload")
    if int(value["format_version"]) != MIGRATION_FORMAT_VERSION:
        raise ValueError("unsupported migration plan format")
    tables = tuple(
        MigrationTablePlan(
            name=row["name"],
            classification=row["classification"],
            source=_table_evidence(row["source"]),
            destination=_table_evidence(row["destination"]),
        )
        for row in value["tables"]
    )
    return MigrationPlan(
        migration_id=value["migration_id"],
        format_version=int(value["format_version"]),
        archetype_version=value["archetype_version"],
        created_at=value["created_at"],
        source_storage_fingerprint=value["source_storage_fingerprint"],
        destination_storage_fingerprint=value["destination_storage_fingerprint"],
        source_stability_digest=value["source_stability_digest"],
        tables=tables,
        artifacts=ArtifactPlanEvidence(**value["artifacts"]),
        control=_control_snapshot(value["control"]),
        plan_digest=observed,
        source_endpoint=source,
        destination_endpoint=destination,
    )


def migration_receipt_digest(receipt: MigrationReceipt) -> str:
    payload = asdict(receipt)
    payload.pop("receipt_digest", None)
    bound = {
        "domain": "archetype.storage-migration.receipt.v1",
        "receipt": payload,
    }
    return hashlib.sha256(canonical_json(bound).encode("utf-8")).hexdigest()


def migration_receipt_json(receipt: MigrationReceipt) -> str:
    """Return the canonical, credential-free durable receipt payload."""

    return canonical_json(asdict(receipt))


def load_migration_receipt(
    receipt_json: str,
    *,
    expected_digest: str,
) -> MigrationReceipt:
    """Load and validate one exact canonical receipt from durable evidence."""

    value = json.loads(receipt_json)
    if not isinstance(value, dict):
        raise ValueError("migration receipt must decode to an object")
    if int(value["format_version"]) != MIGRATION_FORMAT_VERSION:
        raise ValueError("unsupported migration receipt format")
    receipt = MigrationReceipt(
        migration_id=str(value["migration_id"]),
        format_version=int(value["format_version"]),
        archetype_version=str(value["archetype_version"]),
        started_at=str(value["started_at"]),
        verified_at=str(value["verified_at"]),
        source_storage_fingerprint=str(value["source_storage_fingerprint"]),
        destination_storage_fingerprint=str(value["destination_storage_fingerprint"]),
        plan_digest=str(value["plan_digest"]),
        source_stability_digest=str(value["source_stability_digest"]),
        tables=tuple(TableMigrationReceipt(**row) for row in value["tables"]),
        artifacts=ArtifactMigrationReceipt(**value["artifacts"]),
        control=ControlMigrationReceipt(**value["control"]),
        activity_disposition=value["activity_disposition"],
        cold_verification=ColdVerificationEvidence(**value["cold_verification"]),
        receipt_digest=str(value["receipt_digest"]),
    )
    if migration_receipt_json(receipt) != receipt_json:
        raise ValueError("stored migration receipt is not canonical or has unknown fields")
    if receipt.receipt_digest != expected_digest:
        raise ValueError("stored migration receipt digest does not match its reservation")
    if migration_receipt_digest(receipt) != expected_digest:
        raise ValueError("stored migration receipt digest is invalid")
    return receipt


def cold_verification_digest(evidence: ColdVerificationEvidence) -> str:
    payload = asdict(evidence)
    payload.pop("evidence_digest", None)
    bound = {
        "domain": "archetype.storage-migration.cold-verification.v1",
        "evidence": payload,
    }
    return hashlib.sha256(canonical_json(bound).encode("utf-8")).hexdigest()


__all__ = [
    "ArtifactMigrationReceipt",
    "ArtifactPlanEvidence",
    "ColdVerificationEvidence",
    "ColdVerificationRequest",
    "ControlMigrationReceipt",
    "MIGRATION_FORMAT_VERSION",
    "MigrationEndpoint",
    "MigrationPlan",
    "MigrationReceipt",
    "MigrationTablePlan",
    "TableClassification",
    "TableMigrationReceipt",
    "cold_verification_digest",
    "load_migration_receipt",
    "load_migration_plan",
    "migration_plan_digest",
    "migration_plan_json",
    "migration_plan_payload",
    "migration_receipt_digest",
    "migration_receipt_json",
]
