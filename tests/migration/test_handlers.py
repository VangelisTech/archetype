# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local black-box contracts for whole-storage migration orchestration."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from pathlib import Path

import daft
import pytest
import xxhash

from archetype.artifacts.migration import ArtifactIntegrityError
from archetype.artifacts.models import ArtifactStoreConfig
from archetype.core.config import StorageBackend, StorageConfig
from archetype.migration import (
    ColdVerificationEvidence,
    ColdVerificationRequest,
    MigrationDriftError,
    MigrationEndpoint,
    MigrationPreflightError,
    cold_verification_digest,
    load_migration_receipt,
    migrate_storage,
    plan_storage_migration,
    verify_storage_migration,
)
from archetype.storage.activity_catalog import (
    ActivityAdmissionRecord,
    SqliteActivityCatalog,
    activity_catalog_path_for,
)
from archetype.storage.catalog import CommandAdmission, WorldRecord
from archetype.storage.catalog.records import storage_fingerprint
from archetype.storage.service import StorageService
from archetype.storage.session import configure_session

pytestmark = pytest.mark.asyncio


@dataclass(slots=True)
class _ColdVerifier:
    requests: list[ColdVerificationRequest] = field(default_factory=list)

    async def __call__(self, request: ColdVerificationRequest) -> ColdVerificationEvidence:
        self.requests.append(request)
        evidence = ColdVerificationEvidence(
            destination_storage_fingerprint=request.destination_storage_fingerprint,
            world_count=len(request.worlds),
            table_count=len(request.tables),
            artifact_objects_verified=request.artifacts.distinct_content_count,
            visible_query_verified=True,
            resume_disposition="not_applicable",
            resumed_world_id=None,
            imported_fence_floor=None,
            acquired_writer_epoch=None,
            tick_before=None,
            tick_after=None,
            evidence_digest="",
        )
        return replace(evidence, evidence_digest=cold_verification_digest(evidence))


def _storage(root: Path, name: str) -> StorageConfig:
    return StorageConfig(
        uri=str(root / name),
        namespace="migration",
        backend=StorageBackend.ICEBERG,
    )


def _endpoint(
    root: Path,
    name: str,
    *,
    verifier: _ColdVerifier | None = None,
    with_verifier: bool = True,
) -> MigrationEndpoint:
    storage = _storage(root, name)
    service = StorageService(session=configure_session(storage))
    return MigrationEndpoint(
        storage_config=storage,
        storage_service=service,
        control_catalog=service.get_control_catalog(storage),
        artifact_store_config=ArtifactStoreConfig.local(root / f"{name}-artifact-objects"),
        activity_catalog_path=activity_catalog_path_for(storage),
        audit_storage_config=storage,
        cold_verifier=verifier or (_ColdVerifier() if with_verifier else None),
    )


def _artifact_object_path(endpoint: MigrationEndpoint, payload: bytes) -> Path:
    root = endpoint.artifact_store_config.object_uri
    assert root is not None
    digest = hashlib.sha256(payload).hexdigest()
    return Path(root) / "objects" / "sha256" / digest[:2] / digest


async def _close(*endpoints: MigrationEndpoint) -> None:
    for endpoint in endpoints:
        await endpoint.storage_service.shutdown()


async def _assert_no_reservation(endpoint: MigrationEndpoint) -> None:
    assert await endpoint.migration_control_catalog.list_migration_reservations() == ()


async def test_same_storage_identity_is_rejected_before_catalog_or_table_mutation(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "same")
    destination = _endpoint(tmp_path, "same")
    try:
        with pytest.raises(MigrationPreflightError, match="identical"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="same-identity",
            )

        assert not source.activity_catalog_path.exists()
        assert await source.storage_service.list_table_names(source.storage_config) == ()
    finally:
        await _close(source, destination)


async def test_nonempty_destination_is_rejected_without_merging_or_reserving(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    try:
        await destination.storage_service.append_table(
            destination.storage_config,
            "existing",
            daft.from_pydict({"value": [99]}),
        )

        with pytest.raises(MigrationPreflightError, match="namespace is not empty"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="nonempty-destination",
            )

        rows = await destination.storage_service.read_table(
            destination.storage_config,
            "existing",
        )
        assert rows.to_pydict() == {"value": [99]}
        await _assert_no_reservation(destination)
    finally:
        await _close(source, destination)


async def test_remote_iceberg_and_separate_audit_identity_fail_local_v1(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    try:
        remote = replace(
            source,
            storage_config=StorageConfig(
                uri="s3://example/source",
                namespace="migration",
                backend=StorageBackend.ICEBERG,
            ),
        )
        with pytest.raises(MigrationPreflightError, match="Iceberg authority is not local"):
            await plan_storage_migration(
                source=remote,
                destination=destination,
                migration_id="remote-source",
            )

        split_audit = replace(
            source,
            audit_storage_config=_storage(tmp_path, "different-audit"),
        )
        with pytest.raises(MigrationPreflightError, match="audit storage is a different"):
            await plan_storage_migration(
                source=split_audit,
                destination=destination,
                migration_id="split-audit",
            )

        missing_audit = replace(source, audit_storage_config=None)  # type: ignore[arg-type]
        with pytest.raises(MigrationPreflightError, match="must be bound explicitly"):
            await plan_storage_migration(
                source=missing_audit,
                destination=destination,
                migration_id="missing-audit",
            )
        await _assert_no_reservation(destination)
    finally:
        await _close(source, destination)


async def test_missing_cold_verifier_and_overlapping_artifact_authority_fail_preflight(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination", with_verifier=False)
    try:
        with pytest.raises(MigrationPreflightError, match="cold verifier"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="missing-cold-verifier",
            )
        await _assert_no_reservation(destination)

        overlapping = replace(
            destination,
            artifact_store_config=source.artifact_store_config,
            cold_verifier=_ColdVerifier(),
        )
        with pytest.raises(MigrationPreflightError, match="Artifact authorities must be disjoint"):
            await plan_storage_migration(
                source=source,
                destination=overlapping,
                migration_id="overlapping-artifacts",
            )
        await _assert_no_reservation(destination)
    finally:
        await _close(source, destination)


async def test_any_activity_history_rejects_before_destination_initialization(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    activity_catalog = SqliteActivityCatalog(source.activity_catalog_path)
    try:
        await activity_catalog.admit_activity(
            ActivityAdmissionRecord(
                activity_id="activity-a",
                kind="missions.author",
                source_world_id="world-a",
                source_run_id="run-a",
                source_tick=4,
                source_visibility_token="manifest-4",
                input_ref="artifact://mission-input/a",
                input_digest="input-digest",
            )
        )
        await activity_catalog.close()

        with pytest.raises(MigrationPreflightError, match="Activity catalog contains"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="activity-history",
            )

        assert not destination.activity_catalog_path.exists()
        assert await destination.storage_service.list_table_names(destination.storage_config) == ()
    finally:
        await activity_catalog.close()
        await _close(source, destination)


async def test_unsettled_command_rejects_before_destination_reservation(tmp_path: Path) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    try:
        await source.control_catalog.register_world(
            WorldRecord(
                world_id="world-a",
                name="World A",
                run_id="run-a",
                parent_world_id=None,
                status="active",
                tick_head=0,
            )
        )
        await source.control_catalog.admit_commands(
            "world-a",
            [
                CommandAdmission(
                    command_id="command-a",
                    scheduled_tick=1,
                    priority=10,
                    command_type="spawn",
                    payload_json="{}",
                    payload_digest=hashlib.sha256(b"{}").hexdigest(),
                    version=1,
                    principal_id=None,
                    origin="test",
                )
            ],
        )

        with pytest.raises(MigrationPreflightError, match="unsettled command"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="unsettled-command",
            )

        await _assert_no_reservation(destination)
    finally:
        await _close(source, destination)


async def test_running_evaluation_rejects_before_destination_reservation(tmp_path: Path) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    try:
        lease = await source.control_catalog.lease_evaluation(
            "world-a",
            "run-a",
            "evaluation-a",
            "subject-digest",
            "contract-digest",
            "worker-a",
        )
        assert lease.status == "RUNNING"

        with pytest.raises(MigrationPreflightError, match="running or unsupported evaluation"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="running-evaluation",
            )

        await _assert_no_reservation(destination)
    finally:
        await _close(source, destination)


async def test_missing_artifact_fails_planning_before_destination_reservation(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    expected = b"expected Artifact bytes"
    missing = _artifact_object_path(source, expected)
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "artifact_files",
            daft.from_pydict(
                {
                    "artifact_id": ["artifact-a"],
                    "object_uri": [missing.resolve().as_uri()],
                    "sha256": [hashlib.sha256(expected).hexdigest()],
                    "xxhash3_64": [xxhash.xxh3_64_hexdigest(expected)],
                    "size_bytes": [len(expected)],
                }
            ),
        )

        with pytest.raises(ArtifactIntegrityError, match="unreadable"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="missing-artifact",
            )

        await _assert_no_reservation(destination)
        assert await destination.storage_service.list_table_names(destination.storage_config) == ()
    finally:
        await _close(source, destination)


async def test_corrupt_artifact_fails_planning_before_destination_reservation(
    tmp_path: Path,
) -> None:
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination")
    expected = b"indexed Artifact bytes"
    object_path = _artifact_object_path(source, expected)
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(b"different bytes")
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "artifact_files",
            daft.from_pydict(
                {
                    "artifact_id": ["artifact-a"],
                    "object_uri": [object_path.resolve().as_uri()],
                    "sha256": [hashlib.sha256(expected).hexdigest()],
                    "xxhash3_64": [xxhash.xxh3_64_hexdigest(expected)],
                    "size_bytes": [len(expected)],
                }
            ),
        )

        with pytest.raises(ArtifactIntegrityError, match="disagrees"):
            await plan_storage_migration(
                source=source,
                destination=destination,
                migration_id="corrupt-artifact",
            )

        await _assert_no_reservation(destination)
        assert await destination.storage_service.list_table_names(destination.storage_config) == ()
    finally:
        await _close(source, destination)


async def test_table_only_migration_succeeds_and_repr_redacts_endpoint_paths(
    tmp_path: Path,
) -> None:
    secret_marker = "SECRET_ENDPOINT_MARKER"
    secret_root = tmp_path / secret_marker
    verifier = _ColdVerifier()
    source = _endpoint(secret_root, "source")
    destination = _endpoint(secret_root, "destination", verifier=verifier)
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "unknown_application_table",
            daft.from_pydict({"event_id": ["e1", "e2"], "value": [1, 2]}),
        )

        plan = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id="table-only-success",
        )
        receipt = await migrate_storage(plan)
        repeated_verification = await verify_storage_migration(
            receipt,
            destination=destination,
        )

        assert len(plan.tables) == 1
        assert plan.tables[0].classification == "application"
        assert receipt.control.world_count == 0
        assert receipt.activity_disposition == "empty-v1"
        assert receipt.cold_verification.resume_disposition == "not_applicable"
        assert repeated_verification.resume_disposition == "not_applicable"
        assert len(verifier.requests) == 2
        assert secret_marker not in repr(source)
        assert secret_marker not in repr(plan)
        assert secret_marker not in repr(receipt)
        assert storage_fingerprint(destination.storage_config) in repr(receipt)

        rows = await destination.storage_service.read_table(
            destination.storage_config,
            "unknown_application_table",
        )
        assert rows.to_pydict() == {"event_id": ["e1", "e2"], "value": [1, 2]}
        reservation = await destination.migration_control_catalog.get_migration_reservation(
            plan.migration_id
        )
        assert reservation is not None
        assert reservation.status == "COMPLETE"
        assert reservation.receipt_digest == receipt.receipt_digest
    finally:
        await _close(source, destination)


async def test_retry_after_activation_recovers_imported_historical_snapshot(
    tmp_path: Path,
) -> None:
    verifier = _ColdVerifier()
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination", verifier=verifier)
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "events",
            daft.from_pydict({"value": [1]}),
        )
        plan = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id="activated-recovery",
        )
        table = plan.tables[0]
        payload = await source.storage_service.export_table_snapshot(
            source.storage_config,
            table.source,
        )
        imported = await destination.storage_service.import_table_snapshot(
            destination.storage_config,
            table.source,
            payload,
            destination_evidence=table.destination,
        )
        admin = destination.migration_control_catalog
        await admin.stage_migration_control(plan.migration_id, plan.plan_digest, plan.control)
        await admin.activate_migration(plan.migration_id, plan.plan_digest, plan.control)

        # Simulate a later cold-verification commit whose response was lost
        # before the reservation could record COMPLETE.
        await destination.storage_service.append_table(
            destination.storage_config,
            "events",
            daft.from_pydict({"value": [2]}),
        )

        resumed = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id=plan.migration_id,
        )
        receipt = await migrate_storage(resumed)

        assert receipt.tables[0].destination_snapshot_id == imported.destination_snapshot_id
        rows = await destination.storage_service.read_table(
            destination.storage_config,
            "events",
        )
        assert sorted(rows.to_pydict()["value"]) == [1, 2]
        reservation = await admin.get_migration_reservation(plan.migration_id)
        assert reservation is not None
        assert reservation.status == "COMPLETE"
    finally:
        await _close(source, destination)


async def test_source_drift_after_table_copy_aborts_before_world_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verifier = _ColdVerifier()
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination", verifier=verifier)
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "events",
            daft.from_pydict({"value": [1]}),
        )
        plan = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id="source-drift",
        )
        original_import = destination.storage_service.import_table_snapshot
        drifted = False

        async def import_then_drift(*args, **kwargs):
            nonlocal drifted
            receipt = await original_import(*args, **kwargs)
            if not drifted:
                drifted = True
                await source.storage_service.append_table(
                    source.storage_config,
                    "events",
                    daft.from_pydict({"value": [2]}),
                )
            return receipt

        monkeypatch.setattr(
            destination.storage_service,
            "import_table_snapshot",
            import_then_drift,
        )

        with pytest.raises(MigrationDriftError, match="source table"):
            await migrate_storage(plan)

        reservation = await destination.migration_control_catalog.get_migration_reservation(
            plan.migration_id
        )
        assert reservation is not None
        assert reservation.status == "RESERVED"
        assert await destination.control_catalog.list_worlds() == []
        assert verifier.requests == []
    finally:
        await _close(source, destination)


async def test_tampered_plan_and_activated_control_drift_fail_closed(tmp_path: Path) -> None:
    verifier = _ColdVerifier()
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination", verifier=verifier)
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "events",
            daft.from_pydict({"value": [1]}),
        )
        plan = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id="plan-and-control-binding",
        )
        tampered = replace(
            plan,
            artifacts=replace(plan.artifacts, total_bytes=plan.artifacts.total_bytes + 1),
        )
        with pytest.raises(MigrationPreflightError, match="plan content"):
            await migrate_storage(tampered)

        table = plan.tables[0]
        payload = await source.storage_service.export_table_snapshot(
            source.storage_config,
            table.source,
        )
        await destination.storage_service.import_table_snapshot(
            destination.storage_config,
            table.source,
            payload,
            destination_evidence=table.destination,
        )
        admin = destination.migration_control_catalog
        await admin.stage_migration_control(plan.migration_id, plan.plan_digest, plan.control)
        await admin.activate_migration(plan.migration_id, plan.plan_digest, plan.control)
        await destination.control_catalog.register_world(
            WorldRecord(
                world_id="unexpected-world",
                name="Unexpected",
                run_id="unexpected-run",
                parent_world_id=None,
                status="active",
                tick_head=0,
            )
        )

        with pytest.raises(MigrationPreflightError, match="control state conflicts"):
            await migrate_storage(plan)
        assert verifier.requests == []
    finally:
        await _close(source, destination)


async def test_complete_response_loss_recovers_exact_receipt_without_reverification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verifier = _ColdVerifier()
    source = _endpoint(tmp_path, "source")
    destination = _endpoint(tmp_path, "destination", verifier=verifier)
    try:
        await source.storage_service.append_table(
            source.storage_config,
            "events",
            daft.from_pydict({"value": [1]}),
        )
        plan = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id="complete-response-loss",
        )
        admin = destination.migration_control_catalog
        original_complete = admin.complete_migration

        async def complete_then_lose_response(
            migration_id: str,
            plan_digest: str,
            receipt_digest: str,
            receipt_json: str,
        ) -> None:
            await original_complete(
                migration_id,
                plan_digest,
                receipt_digest,
                receipt_json,
            )
            raise RuntimeError("completion response was lost")

        monkeypatch.setattr(admin, "complete_migration", complete_then_lose_response)
        with pytest.raises(RuntimeError, match="response was lost"):
            await migrate_storage(plan)

        reservation = await admin.get_migration_reservation(plan.migration_id)
        assert reservation is not None
        assert reservation.status == "COMPLETE"
        assert reservation.receipt_digest is not None
        assert reservation.receipt_json is not None
        stored = load_migration_receipt(
            reservation.receipt_json,
            expected_digest=reservation.receipt_digest,
        )
        assert len(verifier.requests) == 1

        monkeypatch.setattr(admin, "complete_migration", original_complete)
        resumed = await plan_storage_migration(
            source=source,
            destination=destination,
            migration_id=plan.migration_id,
        )
        recovered = await migrate_storage(resumed)

        assert recovered == stored
        assert recovered.receipt_digest == reservation.receipt_digest
        assert len(verifier.requests) == 1
    finally:
        await _close(source, destination)
