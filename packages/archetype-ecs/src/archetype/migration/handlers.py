# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline whole-storage migration orchestration."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import asdict, replace
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import pyarrow as pa

from archetype.artifacts.migration import (
    ARTIFACT_FILES,
    ArtifactInventory,
    capture_artifact_inventory,
    relocate_artifact_objects,
    relocate_artifact_table,
)
from archetype.artifacts.models import resolve_artifact_object_root
from archetype.artifacts.pipeline import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_IMAGES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
)
from archetype.core.config import StorageBackend, StorageConfig
from archetype.core.paths import local_storage_path
from archetype.migration.contracts import (
    MIGRATION_FORMAT_VERSION,
    ArtifactMigrationReceipt,
    ArtifactPlanEvidence,
    ColdVerificationEvidence,
    ColdVerificationRequest,
    ControlMigrationReceipt,
    MigrationEndpoint,
    MigrationPlan,
    MigrationReceipt,
    MigrationTablePlan,
    TableClassification,
    TableMigrationReceipt,
    cold_verification_digest,
    load_migration_plan,
    load_migration_receipt,
    migration_plan_digest,
    migration_plan_json,
    migration_plan_payload,
    migration_receipt_digest,
    migration_receipt_json,
)
from archetype.storage.activity_catalog import inspect_sqlite_activity_catalog
from archetype.storage.catalog import CatalogConflictError, storage_fingerprint
from archetype.storage.catalog.migration import (
    ControlCatalogSnapshot,
    MigrationReservation,
    canonical_json,
    control_snapshot_digest,
)
from archetype.storage.transfer import (
    ImportedTableReceipt,
    logical_arrow_schemas_equal,
    table_evidence,
)

_MIGRATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ARTIFACT_TABLES = frozenset(
    {
        ARTIFACT_FILES,
        ARTIFACT_IMAGES,
        ARTIFACT_AUDIO,
        ARTIFACT_VIDEO,
        ARTIFACT_PDF,
        ARTIFACT_TEXT,
        ARTIFACT_DIFF,
    }
)
_TERMINAL_COMMAND_STATUSES = frozenset({"APPLIED", "REJECTED", "DEAD_LETTER"})
_SUPPORTED_EVALUATION_STATUSES = frozenset({"COMPLETE", "RETRYABLE"})


class StorageMigrationError(RuntimeError):
    """Base class for deterministic migration refusal."""


class MigrationPreflightError(StorageMigrationError):
    """An endpoint or durable plane is outside local v1's declared scope."""


class MigrationDriftError(StorageMigrationError):
    """The offline source changed after its immutable plan was frozen."""


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _archetype_version() -> str:
    try:
        return version("archetype-ecs")
    except PackageNotFoundError:  # pragma: no cover - source tree without install metadata
        return "0.6.3"


def _artifact_plan(inventory: ArtifactInventory) -> ArtifactPlanEvidence:
    return ArtifactPlanEvidence(
        occurrence_count=inventory.occurrence_count,
        distinct_content_count=inventory.distinct_content_count,
        total_bytes=inventory.total_verified_bytes,
        inventory_digest=inventory.inventory_digest,
    )


def _empty_artifact_inventory() -> ArtifactInventory:
    empty = pa.table(
        {
            "artifact_id": pa.array([], type=pa.string()),
            "object_uri": pa.array([], type=pa.string()),
            "sha256": pa.array([], type=pa.string()),
            "xxhash3_64": pa.array([], type=pa.string()),
            "size_bytes": pa.array([], type=pa.int64()),
        }
    )
    return capture_artifact_inventory(empty)


def _classification(name: str, signature_ids: frozenset[str]) -> TableClassification:
    if name in signature_ids:
        return "ecs"
    if name in _ARTIFACT_TABLES:
        return "artifact"
    return "application"


def _source_stability_digest(
    control: ControlCatalogSnapshot,
    tables: tuple[MigrationTablePlan, ...],
    artifacts: ArtifactPlanEvidence,
) -> str:
    payload = {
        "domain": "archetype.storage-migration.source-stability.v1",
        "control_digest": control_snapshot_digest(control),
        "tables": [asdict(table.source) for table in tables],
        "artifacts": asdict(artifacts),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _require_endpoint_scope(endpoint: MigrationEndpoint, *, role: str) -> str:
    config = endpoint.storage_config
    if config.backend != StorageBackend.ICEBERG:
        raise MigrationPreflightError(f"{role} migration endpoint must use Iceberg")
    if local_storage_path(str(config.uri)) is None:
        raise MigrationPreflightError(
            f"{role} Iceberg authority is not local; remote migration is deferred"
        )
    try:
        endpoint.storage_service.require_local_sqlite_iceberg_identity(config)
    except (TypeError, ValueError):
        raise MigrationPreflightError(
            f"{role} migration endpoint must use its configured local SQLite-backed Iceberg catalog"
        ) from None
    if not endpoint.activity_catalog_path.is_absolute():
        raise MigrationPreflightError(f"{role} Activity catalog path must be absolute")
    root = resolve_artifact_object_root(config, endpoint.artifact_store_config)
    if local_storage_path(root) is None:
        raise MigrationPreflightError(
            f"{role} Artifact authority is not local; remote migration is deferred"
        )
    if not isinstance(endpoint.audit_storage_config, StorageConfig):
        raise MigrationPreflightError(f"{role} audit storage identity must be bound explicitly")
    if storage_fingerprint(endpoint.audit_storage_config) != storage_fingerprint(config):
        raise MigrationPreflightError(
            f"{role} audit storage is a different identity; local v1 cannot omit it"
        )
    # Force the separate administrative capability check before any plan work.
    _ = endpoint.migration_control_catalog
    return storage_fingerprint(config)


def _local_artifact_root(endpoint: MigrationEndpoint) -> Path:
    root = local_storage_path(
        resolve_artifact_object_root(
            endpoint.storage_config,
            endpoint.artifact_store_config,
        )
    )
    if root is None:  # already rejected by _require_endpoint_scope
        raise AssertionError("local migration endpoint lost its Artifact authority")
    return root.resolve()


def _require_disjoint_artifact_authorities(
    source: MigrationEndpoint,
    destination: MigrationEndpoint,
) -> None:
    source_root = _local_artifact_root(source)
    destination_root = _local_artifact_root(destination)
    if (
        source_root == destination_root
        or source_root.is_relative_to(destination_root)
        or destination_root.is_relative_to(source_root)
    ):
        raise MigrationPreflightError(
            "source and destination Artifact authorities must be disjoint"
        )


async def _require_empty_activity_history(endpoint: MigrationEndpoint, *, role: str) -> None:
    inventory = await asyncio.to_thread(
        inspect_sqlite_activity_catalog,
        endpoint.activity_catalog_path,
    )
    if not inventory.is_empty:
        raise MigrationPreflightError(
            f"{role} Activity catalog contains durable history; local v1 requires none"
        )


def _require_quiescent_control(snapshot: ControlCatalogSnapshot) -> None:
    unsettled = sorted(
        command.command_id
        for command in snapshot.commands
        if command.status not in _TERMINAL_COMMAND_STATUSES
    )
    if unsettled:
        raise MigrationPreflightError("source has unsettled command history")
    unsupported_evaluations = sorted(
        evaluation.evaluation_id
        for evaluation in snapshot.evaluations
        if evaluation.status not in _SUPPORTED_EVALUATION_STATUSES
    )
    if unsupported_evaluations:
        raise MigrationPreflightError("source has a running or unsupported evaluation lease")


def _snapshot_is_empty(snapshot: ControlCatalogSnapshot) -> bool:
    return not any(
        (
            snapshot.worlds,
            snapshot.signatures,
            snapshot.manifests,
            snapshot.commands,
            snapshot.evaluations,
            snapshot.outbox,
            snapshot.fence_floors,
        )
    )


def _same_nonworld_control(
    expected: ControlCatalogSnapshot,
    observed: ControlCatalogSnapshot,
) -> bool:
    return (
        observed.format_version,
        observed.catalog_schema_version,
        observed.catalog_protocol_version,
        observed.signatures,
        observed.manifests,
        observed.commands,
        observed.evaluations,
        observed.outbox,
        observed.fence_floors,
    ) == (
        expected.format_version,
        expected.catalog_schema_version,
        expected.catalog_protocol_version,
        expected.signatures,
        expected.manifests,
        expected.commands,
        expected.evaluations,
        expected.outbox,
        expected.fence_floors,
    )


def _control_extends_activated_plan(
    expected: ControlCatalogSnapshot,
    observed: ControlCatalogSnapshot,
) -> bool:
    """Allow only the later tick/fence evidence created by cold verification."""

    if (
        observed.format_version,
        observed.catalog_schema_version,
        observed.catalog_protocol_version,
        observed.signatures,
        observed.commands,
        observed.evaluations,
        observed.outbox,
    ) != (
        expected.format_version,
        expected.catalog_schema_version,
        expected.catalog_protocol_version,
        expected.signatures,
        expected.commands,
        expected.evaluations,
        expected.outbox,
    ):
        return False
    expected_worlds = {world.world_id: world for world in expected.worlds}
    observed_worlds = {world.world_id: world for world in observed.worlds}
    if expected_worlds.keys() != observed_worlds.keys():
        return False
    for world_id, planned in expected_worlds.items():
        current = observed_worlds[world_id]
        if (
            current.name,
            current.run_id,
            current.parent_world_id,
            current.status,
            current.writer_mode,
        ) != (
            planned.name,
            planned.run_id,
            planned.parent_world_id,
            planned.status,
            planned.writer_mode,
        ) or current.tick_head < planned.tick_head:
            return False
    expected_manifests = set(expected.manifests)
    if not expected_manifests <= set(observed.manifests):
        return False
    for manifest in observed.manifests:
        world = expected_worlds.get(manifest.world_id)
        if world is None or manifest.run_id != world.run_id:
            return False
    expected_floors = {floor.world_id: floor.epoch for floor in expected.fence_floors}
    observed_floors = {floor.world_id: floor.epoch for floor in observed.fence_floors}
    return expected_floors.keys() == observed_floors.keys() and all(
        observed_floors[world_id] >= epoch for world_id, epoch in expected_floors.items()
    )


def _completed_receipt(
    reservation: MigrationReservation,
    plan: MigrationPlan,
) -> MigrationReceipt:
    """Recover and bind the exact receipt committed with a completed plan."""

    if reservation.receipt_digest is None or reservation.receipt_json is None:
        raise StorageMigrationError("completed migration has no recoverable receipt evidence")
    try:
        receipt = load_migration_receipt(
            reservation.receipt_json,
            expected_digest=reservation.receipt_digest,
        )
    except (KeyError, TypeError, ValueError):
        raise StorageMigrationError("completed migration receipt evidence is invalid") from None
    if (
        receipt.migration_id,
        receipt.format_version,
        receipt.archetype_version,
        receipt.started_at,
        receipt.source_storage_fingerprint,
        receipt.destination_storage_fingerprint,
        receipt.plan_digest,
        receipt.source_stability_digest,
    ) != (
        plan.migration_id,
        plan.format_version,
        plan.archetype_version,
        plan.created_at,
        plan.source_storage_fingerprint,
        plan.destination_storage_fingerprint,
        plan.plan_digest,
        plan.source_stability_digest,
    ):
        raise StorageMigrationError("completed migration receipt is bound to a different plan")
    return receipt


async def _resume_reserved_plan(
    *,
    source: MigrationEndpoint,
    destination: MigrationEndpoint,
    migration_id: str,
    source_fingerprint: str | None,
    destination_fingerprint: str,
) -> MigrationPlan | None:
    admin = destination.migration_control_catalog
    reservations = await admin.list_migration_reservations()
    competing = [item for item in reservations if item.migration_id != migration_id]
    if competing:
        raise MigrationPreflightError("destination has a competing migration reservation")
    reservation = await admin.get_migration_reservation(migration_id)
    if reservation is None:
        return None
    plan = load_migration_plan(
        reservation.plan_json,
        source=source,
        destination=destination,
        expected_digest=reservation.plan_digest,
    )
    if plan.destination_storage_fingerprint != destination_fingerprint:
        raise MigrationPreflightError("reserved migration is bound to different endpoints")
    if source_fingerprint is None and reservation.status not in {"ACTIVATED", "COMPLETE"}:
        return None
    if source_fingerprint is not None and plan.source_storage_fingerprint != source_fingerprint:
        raise MigrationPreflightError("reserved migration is bound to different endpoints")
    actual_tables = set(
        await destination.storage_service.list_table_names(destination.storage_config)
    )
    expected_tables = {table.name for table in plan.tables}
    if not actual_tables <= expected_tables:
        raise MigrationPreflightError("destination contains a table outside the reserved plan")
    observed_control = await admin.export_migration_snapshot()
    if reservation.status == "RESERVED" and not _snapshot_is_empty(observed_control):
        raise MigrationPreflightError("reserved destination control state is not empty")
    if reservation.status == "STAGED":
        if observed_control.worlds or not _same_nonworld_control(plan.control, observed_control):
            raise MigrationPreflightError("staged destination control state conflicts with plan")
    if reservation.status in {"ACTIVATED", "COMPLETE"}:
        if not _control_extends_activated_plan(plan.control, observed_control):
            raise MigrationPreflightError("activated destination control state conflicts with plan")
        if reservation.status == "COMPLETE":
            _completed_receipt(reservation, plan)
    if reservation.status not in {"RESERVED", "STAGED", "ACTIVATED", "COMPLETE"}:
        raise MigrationPreflightError("destination migration has an unknown durable status")
    return plan


async def plan_storage_migration(
    *,
    source: MigrationEndpoint,
    destination: MigrationEndpoint,
    migration_id: str,
) -> MigrationPlan:
    """Freeze and reserve one offline whole-storage migration plan."""

    if not isinstance(migration_id, str) or not _MIGRATION_ID.fullmatch(migration_id):
        raise ValueError("migration_id must be 1-128 portable identifier characters")
    destination_fingerprint = _require_endpoint_scope(destination, role="destination")
    destination_only_resume = await _resume_reserved_plan(
        source=source,
        destination=destination,
        migration_id=migration_id,
        source_fingerprint=None,
        destination_fingerprint=destination_fingerprint,
    )
    if destination_only_resume is not None:
        return destination_only_resume

    source_fingerprint = _require_endpoint_scope(source, role="source")
    if source_fingerprint == destination_fingerprint:
        raise MigrationPreflightError("source and destination storage identities are identical")
    _require_disjoint_artifact_authorities(source, destination)

    # Activity history is a hard unsupported plane.  Check both endpoints
    # before even creating a destination reservation.
    await _require_empty_activity_history(source, role="source")
    await _require_empty_activity_history(destination, role="destination")
    if destination.cold_verifier is None:
        raise MigrationPreflightError("destination requires a fresh destination-only cold verifier")

    resumed = await _resume_reserved_plan(
        source=source,
        destination=destination,
        migration_id=migration_id,
        source_fingerprint=source_fingerprint,
        destination_fingerprint=destination_fingerprint,
    )
    if resumed is not None:
        await _require_source_stability(resumed)
        return resumed

    destination_tables = await destination.storage_service.list_table_names(
        destination.storage_config
    )
    if destination_tables:
        raise MigrationPreflightError("destination Iceberg namespace is not empty")
    destination_control = await destination.migration_control_catalog.export_migration_snapshot()
    if not _snapshot_is_empty(destination_control):
        raise MigrationPreflightError("destination control identity is not empty")

    source_admin = source.migration_control_catalog
    initial_control = await source_admin.export_migration_snapshot()
    _require_quiescent_control(initial_control)
    for world in initial_control.worlds:
        if world.status == "active":
            await source.control_catalog.acquire_fence(
                world.world_id,
                f"migration:{migration_id}",
            )
    control = await source_admin.export_migration_snapshot()
    _require_quiescent_control(control)

    names = await source.storage_service.list_table_names(source.storage_config)
    signature_ids = frozenset(signature.table_id for signature in control.signatures)
    destination_root = resolve_artifact_object_root(
        destination.storage_config,
        destination.artifact_store_config,
    )
    source_root = resolve_artifact_object_root(
        source.storage_config,
        source.artifact_store_config,
    )
    table_plans: list[MigrationTablePlan] = []
    artifact_inventory = _empty_artifact_inventory()
    for name in names:
        evidence = await source.storage_service.capture_table_snapshot(
            source.storage_config,
            name,
        )
        destination_evidence = evidence
        if name == ARTIFACT_FILES:
            payload = await source.storage_service.export_table_snapshot(
                source.storage_config,
                evidence,
            )
            artifact_inventory = capture_artifact_inventory(
                payload,
                source_object_root=source_root,
            )
            relocated = relocate_artifact_table(payload, destination_root)
            destination_evidence = table_evidence(name, evidence.snapshot_id, relocated)
        table_plans.append(
            MigrationTablePlan(
                name=name,
                classification=_classification(name, signature_ids),
                source=evidence,
                destination=destination_evidence,
            )
        )
    tables = tuple(sorted(table_plans, key=lambda item: item.name))
    artifacts = _artifact_plan(artifact_inventory)
    source_stability = _source_stability_digest(control, tables, artifacts)
    provisional = MigrationPlan(
        migration_id=migration_id,
        format_version=MIGRATION_FORMAT_VERSION,
        archetype_version=_archetype_version(),
        created_at=_utcnow(),
        source_storage_fingerprint=source_fingerprint,
        destination_storage_fingerprint=destination_fingerprint,
        source_stability_digest=source_stability,
        tables=tables,
        artifacts=artifacts,
        control=control,
        plan_digest="",
        source_endpoint=source,
        destination_endpoint=destination,
    )
    digest = migration_plan_digest(migration_plan_payload(provisional))
    plan = replace(provisional, plan_digest=digest)
    reservation = await destination.migration_control_catalog.reserve_migration(
        migration_id,
        digest,
        migration_plan_json(plan),
    )
    if reservation.plan_digest != digest:
        raise CatalogConflictError("destination reserved a different migration plan")
    return plan


async def _require_source_stability(plan: MigrationPlan) -> str:
    source = plan.source_endpoint
    control = await source.migration_control_catalog.export_migration_snapshot()
    if control != plan.control:
        raise MigrationDriftError("source control state changed after migration planning")
    for table in plan.tables:
        observed = await source.storage_service.capture_table_snapshot(
            source.storage_config,
            table.name,
        )
        if observed != table.source:
            raise MigrationDriftError(f"source table {table.name!r} changed after planning")
    observed_names = await source.storage_service.list_table_names(source.storage_config)
    if observed_names != tuple(table.name for table in plan.tables):
        raise MigrationDriftError("source table inventory changed after planning")

    artifact_table = next((table for table in plan.tables if table.name == ARTIFACT_FILES), None)
    if artifact_table is None:
        observed_artifacts = _artifact_plan(_empty_artifact_inventory())
    else:
        payload = await source.storage_service.export_table_snapshot(
            source.storage_config,
            artifact_table.source,
        )
        observed_artifacts = _artifact_plan(
            capture_artifact_inventory(
                payload,
                source_object_root=resolve_artifact_object_root(
                    source.storage_config,
                    source.artifact_store_config,
                ),
            )
        )
    if observed_artifacts != plan.artifacts:
        raise MigrationDriftError("source Artifact inventory changed after planning")
    digest = _source_stability_digest(control, plan.tables, observed_artifacts)
    if digest != plan.source_stability_digest:
        raise MigrationDriftError("source stability digest changed after planning")
    return digest


def _require_cold_verification(
    plan: MigrationPlan,
    evidence: ColdVerificationEvidence,
) -> None:
    if cold_verification_digest(evidence) != evidence.evidence_digest:
        raise StorageMigrationError("cold verification evidence digest is invalid")
    if evidence.destination_storage_fingerprint != plan.destination_storage_fingerprint:
        raise StorageMigrationError("cold verification used the wrong destination identity")
    if evidence.world_count != len(plan.control.worlds):
        raise StorageMigrationError("cold verification discovered the wrong World count")
    if evidence.table_count != len(plan.tables):
        raise StorageMigrationError("cold verification discovered the wrong table count")
    if evidence.artifact_objects_verified != plan.artifacts.distinct_content_count:
        raise StorageMigrationError("cold verification did not hash every Artifact object")
    if evidence.visible_query_verified is not True:
        raise StorageMigrationError("cold verification did not prove destination-visible queries")
    eligible = {
        world.world_id: world
        for world in plan.control.worlds
        if world.status == "active" and world.writer_mode == "resumable"
    }
    floors = {floor.world_id: floor.epoch for floor in plan.control.fence_floors}
    if not eligible:
        if evidence.resume_disposition != "not_applicable":
            raise StorageMigrationError("cold verification reported an unexpected resume")
        return
    if evidence.resume_disposition != "verified" or evidence.resumed_world_id not in eligible:
        raise StorageMigrationError("cold verification did not resume an eligible World")
    floor = floors.get(evidence.resumed_world_id)
    if floor is None or evidence.imported_fence_floor != floor:
        raise StorageMigrationError("cold verification used the wrong imported fence floor")
    if evidence.acquired_writer_epoch is None or evidence.acquired_writer_epoch <= floor:
        raise StorageMigrationError("destination writer epoch did not exceed the imported floor")
    if (
        evidence.tick_before is None
        or evidence.tick_after is None
        or evidence.tick_after <= evidence.tick_before
    ):
        raise StorageMigrationError("cold verification did not commit a later tick")


def _historical_table_receipt(
    table: MigrationTablePlan,
    destination_snapshot_id: int | None,
) -> ImportedTableReceipt:
    return ImportedTableReceipt(
        name=table.name,
        source_snapshot_id=table.source.snapshot_id,
        destination_snapshot_id=destination_snapshot_id,
        source_schema_fingerprint=table.source.schema_fingerprint,
        destination_schema_fingerprint=table.destination.schema_fingerprint,
        row_count=table.source.row_count,
        source_content_digest=table.source.content_digest,
        destination_content_digest=table.destination.content_digest,
    )


def _migration_table_receipt(
    receipt: ImportedTableReceipt,
    classification: TableClassification,
) -> TableMigrationReceipt:
    return TableMigrationReceipt(
        name=receipt.name,
        classification=classification,
        source_snapshot_id=receipt.source_snapshot_id,
        destination_snapshot_id=receipt.destination_snapshot_id,
        source_schema_fingerprint=receipt.source_schema_fingerprint,
        destination_schema_fingerprint=receipt.destination_schema_fingerprint,
        row_count=receipt.row_count,
        source_content_digest=receipt.source_content_digest,
        destination_content_digest=receipt.destination_content_digest,
    )


async def migrate_storage(plan: MigrationPlan) -> MigrationReceipt:
    """Execute, activate, cold-verify, and receipt one reserved migration."""

    if not isinstance(plan, MigrationPlan):
        raise TypeError("plan must be a MigrationPlan")
    observed_plan_digest = migration_plan_digest(migration_plan_payload(plan))
    if observed_plan_digest != plan.plan_digest:
        raise MigrationPreflightError("migration plan content does not match its digest")

    destination = plan.destination_endpoint
    if (
        _require_endpoint_scope(destination, role="destination")
        != plan.destination_storage_fingerprint
    ):
        raise MigrationPreflightError("destination endpoint no longer matches the plan")
    admin = destination.migration_control_catalog
    reservation = await admin.get_migration_reservation(plan.migration_id)
    if (
        reservation is None
        or reservation.plan_digest != plan.plan_digest
        or reservation.plan_json != migration_plan_json(plan)
    ):
        raise MigrationPreflightError("destination does not hold the reserved migration plan")
    actual_tables = set(
        await destination.storage_service.list_table_names(destination.storage_config)
    )
    expected_tables = {table.name for table in plan.tables}
    if not actual_tables <= expected_tables:
        raise MigrationPreflightError("destination contains a table outside the reserved plan")
    observed_control = await admin.export_migration_snapshot()
    if reservation.status == "RESERVED" and not _snapshot_is_empty(observed_control):
        raise MigrationPreflightError("reserved destination control state is not empty")
    if reservation.status == "STAGED":
        if observed_control.worlds or not _same_nonworld_control(plan.control, observed_control):
            raise MigrationPreflightError("staged destination control state conflicts with plan")
    if reservation.status in {"ACTIVATED", "COMPLETE"} and not (
        _control_extends_activated_plan(plan.control, observed_control)
    ):
        raise MigrationPreflightError("activated destination control state conflicts with plan")
    if reservation.status not in {"RESERVED", "STAGED", "ACTIVATED", "COMPLETE"}:
        raise MigrationPreflightError("destination migration has an unknown durable status")
    if reservation.status == "COMPLETE":
        return _completed_receipt(reservation, plan)
    if destination.cold_verifier is None:
        raise MigrationPreflightError("destination requires a fresh destination-only cold verifier")
    activated_resume = reservation.status == "ACTIVATED"

    source = plan.source_endpoint
    if not activated_resume:
        if _require_endpoint_scope(source, role="source") != plan.source_storage_fingerprint:
            raise MigrationPreflightError("source endpoint no longer matches the plan")
        _require_disjoint_artifact_authorities(source, destination)
        await _require_empty_activity_history(source, role="source")
        await _require_empty_activity_history(destination, role="destination")

    artifact_receipt = ArtifactMigrationReceipt(
        occurrence_count=plan.artifacts.occurrence_count,
        distinct_content_count=plan.artifacts.distinct_content_count,
        total_verified_bytes=plan.artifacts.total_bytes,
        inventory_digest=plan.artifacts.inventory_digest,
    )
    artifact_payload: pa.Table | None = None
    artifact_table = next((table for table in plan.tables if table.name == ARTIFACT_FILES), None)
    if artifact_table is not None and not activated_resume:
        artifact_payload = await source.storage_service.export_table_snapshot(
            source.storage_config,
            artifact_table.source,
        )
        source_root = resolve_artifact_object_root(
            source.storage_config,
            source.artifact_store_config,
        )
        artifact_inventory = capture_artifact_inventory(
            artifact_payload,
            source_object_root=source_root,
        )
        if _artifact_plan(artifact_inventory) != plan.artifacts:
            raise MigrationDriftError("source Artifact inventory changed after planning")
        relocation = relocate_artifact_objects(
            artifact_payload,
            artifact_inventory,
            resolve_artifact_object_root(
                destination.storage_config,
                destination.artifact_store_config,
            ),
            source_evidence=artifact_table.source,
            source_object_root=source_root,
        )
        if relocation.destination_evidence != artifact_table.destination:
            raise MigrationDriftError("relocated Artifact table differs from its plan")
        artifact_payload = relocation.relocated_table
        artifact_receipt = ArtifactMigrationReceipt(
            occurrence_count=relocation.receipt.occurrence_count,
            distinct_content_count=relocation.receipt.distinct_content_count,
            total_verified_bytes=relocation.receipt.total_verified_bytes,
            inventory_digest=relocation.receipt.inventory_digest,
        )

    receipts: list[ImportedTableReceipt] = []
    ordered = sorted(plan.tables, key=lambda item: (item.name == ARTIFACT_FILES, item.name))
    if activated_resume:
        for table in ordered:
            imported = await destination.storage_service.find_table_snapshot(
                destination.storage_config,
                table.destination,
            )
            if imported is None:
                raise StorageMigrationError(
                    f"activated destination lost imported snapshot {table.name!r}"
                )
            receipts.append(_historical_table_receipt(table, imported.snapshot_id))
    else:
        for table in ordered:
            payload = artifact_payload
            if table.name != ARTIFACT_FILES:
                payload = await source.storage_service.export_table_snapshot(
                    source.storage_config,
                    table.source,
                )
            assert payload is not None
            receipts.append(
                await destination.storage_service.import_table_snapshot(
                    destination.storage_config,
                    table.source,
                    payload,
                    destination_evidence=table.destination,
                )
            )

        for table in plan.tables:
            observed = await destination.storage_service.capture_table_snapshot(
                destination.storage_config,
                table.name,
            )
            if not logical_arrow_schemas_equal(
                observed.arrow_schema,
                table.destination.arrow_schema,
            ) or (
                observed.schema_fingerprint,
                observed.row_count,
                observed.content_digest,
            ) != (
                table.destination.schema_fingerprint,
                table.destination.row_count,
                table.destination.content_digest,
            ):
                raise StorageMigrationError(
                    f"destination table {table.name!r} failed read-back verification"
                )

    stability = (
        plan.source_stability_digest if activated_resume else await _require_source_stability(plan)
    )
    if not activated_resume:
        await admin.stage_migration_control(plan.migration_id, plan.plan_digest, plan.control)
        await admin.activate_migration(plan.migration_id, plan.plan_digest, plan.control)

    verifier = destination.cold_verifier
    assert verifier is not None  # preflighted before any destination data mutation
    request = ColdVerificationRequest(
        migration_id=plan.migration_id,
        destination_storage_fingerprint=plan.destination_storage_fingerprint,
        tables=plan.tables,
        worlds=plan.control.worlds,
        fence_floors=plan.control.fence_floors,
        artifacts=plan.artifacts,
    )
    cold = await verifier(request)
    _require_cold_verification(plan, cold)
    verified_at = _utcnow()
    control_digest = control_snapshot_digest(plan.control)
    table_classification = {table.name: table.classification for table in plan.tables}
    receipt = MigrationReceipt(
        migration_id=plan.migration_id,
        format_version=MIGRATION_FORMAT_VERSION,
        archetype_version=plan.archetype_version,
        started_at=plan.created_at,
        verified_at=verified_at,
        source_storage_fingerprint=plan.source_storage_fingerprint,
        destination_storage_fingerprint=plan.destination_storage_fingerprint,
        plan_digest=plan.plan_digest,
        source_stability_digest=stability,
        tables=tuple(
            _migration_table_receipt(receipt, table_classification[receipt.name])
            for receipt in sorted(receipts, key=lambda item: item.name)
        ),
        artifacts=artifact_receipt,
        control=ControlMigrationReceipt(
            world_count=len(plan.control.worlds),
            signature_count=len(plan.control.signatures),
            manifest_count=len(plan.control.manifests),
            command_count=len(plan.control.commands),
            evaluation_count=len(plan.control.evaluations),
            outbox_count=len(plan.control.outbox),
            fence_floor_count=len(plan.control.fence_floors),
            snapshot_digest=control_digest,
            activation_status="activated",
        ),
        activity_disposition="empty-v1",
        cold_verification=cold,
        receipt_digest="",
    )
    receipt = replace(receipt, receipt_digest=migration_receipt_digest(receipt))
    await admin.complete_migration(
        plan.migration_id,
        plan.plan_digest,
        receipt.receipt_digest,
        migration_receipt_json(receipt),
    )
    return receipt


async def verify_storage_migration(
    receipt: MigrationReceipt,
    *,
    destination: MigrationEndpoint,
) -> ColdVerificationEvidence:
    """Repeat fresh verification from a completed destination reservation."""

    if migration_receipt_digest(receipt) != receipt.receipt_digest:
        raise ValueError("migration receipt digest is invalid")
    admin = destination.migration_control_catalog
    reservation = await admin.get_migration_reservation(receipt.migration_id)
    if reservation is None or reservation.status != "COMPLETE":
        raise MigrationPreflightError("destination has no completed migration reservation")
    if reservation.receipt_digest != receipt.receipt_digest:
        raise CatalogConflictError("destination reservation records a different receipt")
    verifier = destination.cold_verifier
    if verifier is None:
        raise MigrationPreflightError("destination has no cold verifier")
    # Rebind the persisted plan without requiring source access.  The supplied
    # source value is never dereferenced during cold verification.
    plan = load_migration_plan(
        reservation.plan_json,
        source=destination,
        destination=destination,
        expected_digest=reservation.plan_digest,
    )
    request = ColdVerificationRequest(
        migration_id=plan.migration_id,
        destination_storage_fingerprint=plan.destination_storage_fingerprint,
        tables=plan.tables,
        worlds=plan.control.worlds,
        fence_floors=plan.control.fence_floors,
        artifacts=plan.artifacts,
    )
    evidence = await verifier(request)
    _require_cold_verification(plan, evidence)
    return evidence


__all__ = [
    "MigrationDriftError",
    "MigrationPreflightError",
    "StorageMigrationError",
    "migrate_storage",
    "plan_storage_migration",
    "verify_storage_migration",
]
