# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Whole-storage local migration acceptance contract."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
import textwrap
from dataclasses import asdict, dataclass, field
from pathlib import Path

import daft
import pytest
from uuid_utils import uuid7

from archetype.artifacts.handlers import ingest_artifacts
from archetype.artifacts.models import (
    ArtifactSource,
    ArtifactStoreConfig,
    IngestArtifacts,
)
from archetype.artifacts.pipeline import ARTIFACT_FILES, ARTIFACT_TEXT
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.migration import (
    ColdVerificationEvidence,
    ColdVerificationRequest,
    migrate_storage,
    plan_storage_migration,
    verify_storage_migration,
)
from archetype.missions.trajectories import CLAUDE_TRANSCRIPT_TABLE
from archetype.storage.catalog import CommandAdmission, control_snapshot_digest
from archetype.storage.service import StorageService
from archetype.storage.session import configure_session
from archetype.wiring import build_local_migration_endpoint
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


class MigrationScore(Component):
    points: float = 0.0


class MigrationFlag(Component):
    label: str = ""


_COLD_DESTINATION_PROBE = textwrap.dedent(
    r"""
    import asyncio
    import hashlib
    import json
    import sys
    from dataclasses import asdict, replace
    from pathlib import Path
    from urllib.parse import unquote, urlsplit

    import xxhash

    from archetype.artifacts.pipeline import ARTIFACT_FILES
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageBackend, StorageConfig
    from archetype.migration import ColdVerificationEvidence, cold_verification_digest
    from archetype.storage.service import StorageService
    from archetype.storage.session import configure_session
    from archetype.storage.transfer import TableSnapshotEvidence
    from archetype.world.lifecycle import WorldLifecycle
    from archetype.world.registry import WorldRegistry


    class MigrationScore(Component):
        points: float = 0.0


    class MigrationFlag(Component):
        label: str = ""


    def local_path(uri):
        parsed = urlsplit(uri)
        if parsed.scheme != "file":
            raise AssertionError("cold local verification encountered a non-file Artifact URI")
        return Path(unquote(parsed.path)).resolve()


    async def main():
        request = json.loads(sys.stdin.read())
        storage = StorageConfig(
            uri=sys.argv[1],
            namespace=sys.argv[2],
            backend=StorageBackend.ICEBERG,
        )
        artifact_root = Path(sys.argv[3]).resolve()
        service = StorageService(session=configure_session(storage))
        try:
            expected_tables = {row["name"]: row["destination"] for row in request["tables"]}
            names = await service.list_table_names(storage)
            assert tuple(sorted(expected_tables)) == names
            for name, expected in expected_tables.items():
                observed = await service.find_table_snapshot(
                    storage,
                    TableSnapshotEvidence(**expected),
                )
                assert observed is not None

            catalog = service.get_control_catalog(storage)
            worlds = await catalog.list_worlds()
            expected_worlds = sorted(request["worlds"], key=lambda row: row["world_id"])
            observed_worlds = {world.world_id: world for world in worlds}
            assert observed_worlds.keys() == {row["world_id"] for row in expected_worlds}
            for expected in expected_worlds:
                observed = observed_worlds[expected["world_id"]]
                assert (
                    observed.name,
                    observed.run_id,
                    observed.parent_world_id,
                    observed.status,
                    observed.writer_mode,
                ) == (
                    expected["name"],
                    expected["run_id"],
                    expected["parent_world_id"],
                    expected["status"],
                    expected["writer_mode"],
                )
                assert observed.tick_head >= int(expected["tick_head"])

            lifecycle = WorldLifecycle(service, WorldRegistry())
            discovered = await lifecycle.discover_worlds(storage)
            assert sorted(str(world.world_id) for world in discovered) == [
                row["world_id"] for row in expected_worlds
            ]

            artifact_objects = 0
            if ARTIFACT_FILES in expected_tables:
                evidence = await service.capture_table_snapshot(storage, ARTIFACT_FILES)
                rows = (
                    await service.export_table_snapshot(storage, evidence)
                ).to_pylist()
                verified = set()
                for row in rows:
                    path = local_path(str(row["object_uri"]))
                    assert path.is_relative_to(artifact_root)
                    payload = path.read_bytes()
                    assert len(payload) == int(row["size_bytes"])
                    assert hashlib.sha256(payload).hexdigest() == row["sha256"]
                    assert xxhash.xxh3_64_hexdigest(payload) == row["xxhash3_64"]
                    verified.add(str(row["sha256"]))
                artifact_objects = len(verified)

            eligible = sorted(
                (
                    row
                    for row in expected_worlds
                    if row["status"] == "active" and row["writer_mode"] == "resumable"
                ),
                key=lambda row: (row["parent_world_id"] is None, row["world_id"]),
            )
            floors = {row["world_id"]: int(row["epoch"]) for row in request["fence_floors"]}
            if eligible:
                selected = eligible[0]
                world_id = selected["world_id"]
                imported_floor = floors[world_id]
                assert await catalog.current_fence_epoch(world_id) >= imported_floor
                resumed = await lifecycle.open_world_mutable(storage, world_id)
                acquired_epoch = int(resumed.commit_coordinator.epoch)
                tick_before = int(resumed.tick)
                inherited_score = (
                    await resumed.query_archetype((MigrationScore,), ticks=[0])
                ).to_pylist()
                inherited_combined = (
                    await resumed.query_archetype(
                        (MigrationScore, MigrationFlag),
                        ticks=[0],
                    )
                ).to_pylist()
                fork_only = (
                    await resumed.query_archetype((MigrationFlag,), ticks=[1])
                ).to_pylist()
                assert len(inherited_score) == len(inherited_combined) == len(fork_only) == 1
                score_key = next(
                    key for key in inherited_score[0] if key.endswith("__points")
                )
                combined_score_key = next(
                    key for key in inherited_combined[0] if key.endswith("__points")
                )
                combined_flag_key = next(
                    key for key in inherited_combined[0] if key.endswith("__label")
                )
                fork_flag_key = next(key for key in fork_only[0] if key.endswith("__label"))
                assert float(inherited_score[0][score_key]) == 1.0
                assert float(inherited_combined[0][combined_score_key]) == 2.0
                assert str(inherited_combined[0][combined_flag_key]) == "base"
                assert str(fork_only[0][fork_flag_key]) == "fork-only"
                await resumed.step(RunConfig())
                tick_after = int(resumed.tick)
                assert acquired_epoch > imported_floor
                assert tick_after > tick_before
                disposition = "verified"
            else:
                world_id = None
                imported_floor = None
                acquired_epoch = None
                tick_before = None
                tick_after = None
                disposition = "not_applicable"

            # A resumed tick must stay within the frozen inventory. It may append
            # snapshots, but it must not materialize an unplanned logical table.
            assert await service.list_table_names(storage) == names
            evidence = ColdVerificationEvidence(
                destination_storage_fingerprint=request["destination_storage_fingerprint"],
                world_count=len(worlds),
                table_count=len(names),
                artifact_objects_verified=artifact_objects,
                visible_query_verified=True,
                resume_disposition=disposition,
                resumed_world_id=world_id,
                imported_fence_floor=imported_floor,
                acquired_writer_epoch=acquired_epoch,
                tick_before=tick_before,
                tick_after=tick_after,
                evidence_digest="",
            )
            evidence = replace(
                evidence,
                evidence_digest=cold_verification_digest(evidence),
            )
            print(json.dumps(asdict(evidence), sort_keys=True))
        finally:
            await service.shutdown()


    asyncio.run(main())
    """
)


@dataclass(slots=True)
class _DestinationOnlySubprocessVerifier:
    source_service: StorageService = field(repr=False)
    destination_service: StorageService = field(repr=False)
    destination_storage: StorageConfig = field(repr=False)
    destination_artifact_root: Path = field(repr=False)
    source_path_marker: str = field(repr=False)
    invocations: int = 0
    services_shutdown: bool = False
    evidence: ColdVerificationEvidence | None = None

    async def __call__(self, request: ColdVerificationRequest) -> ColdVerificationEvidence:
        self.invocations += 1
        await self.source_service.shutdown()
        await self.destination_service.shutdown()
        self.services_shutdown = True

        command = [
            sys.executable,
            "-c",
            _COLD_DESTINATION_PROBE,
            str(self.destination_storage.uri),
            self.destination_storage.namespace,
            str(self.destination_artifact_root),
        ]
        serialized_request = json.dumps(asdict(request), sort_keys=True)
        assert self.source_path_marker not in "\0".join(command)
        assert self.source_path_marker not in serialized_request
        result = await asyncio.to_thread(
            subprocess.run,
            command,
            input=serialized_request,
            capture_output=True,
            text=True,
            timeout=240,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.evidence = ColdVerificationEvidence(**payload)
        return self.evidence


def _storage(root: Path, name: str) -> StorageConfig:
    return StorageConfig(
        uri=str(root / name),
        namespace="whole_identity",
        backend=StorageBackend.ICEBERG,
    )


def _service(storage: StorageConfig) -> StorageService:
    return StorageService(session=configure_session(storage))


async def test_local_whole_storage_identity_migrates_and_cold_resumes(
    tmp_path: Path,
) -> None:
    source_storage = _storage(tmp_path, "source-storage")
    destination_storage = _storage(tmp_path, "destination-storage")
    source_artifact_root = tmp_path / "source-artifact-objects"
    destination_artifact_root = tmp_path / "destination-artifact-objects"
    source_service = _service(source_storage)
    destination_service = _service(destination_storage)
    fresh_destination: StorageService | None = None
    artifact_payload = b"one immutable Artifact object with two durable occurrences\n"

    try:
        registry = WorldRegistry()
        lifecycle = WorldLifecycle(source_service, registry)
        base = await lifecycle.create_world(
            WorldConfig(name="migration-base"),
            source_storage,
        )
        await base.create_entity([MigrationScore(points=1.0)])
        await base.create_entity([MigrationScore(points=2.0), MigrationFlag(label="base")])
        await base.step(RunConfig())

        fork = await lifecycle.fork_world(
            base.world_id,
            name="migration-fork",
            storage_config=source_storage,
        )
        await fork.create_entity([MigrationFlag(label="fork-only")])
        await fork.step(RunConfig())

        first_artifact = tmp_path / "first-evidence.txt"
        second_artifact = tmp_path / "second-evidence.txt"
        first_artifact.write_bytes(artifact_payload)
        second_artifact.write_bytes(artifact_payload)
        artifact_refs = await ingest_artifacts(
            source_service,
            IngestArtifacts(
                world_id=str(fork.world_id),
                sources=(
                    ArtifactSource(
                        source_uri=str(first_artifact),
                        logical_path="evidence/first.txt",
                    ),
                    ArtifactSource(
                        source_uri=str(second_artifact),
                        logical_path="evidence/second.txt",
                    ),
                ),
                storage_config=source_storage,
            ),
            store_config=ArtifactStoreConfig.local(source_artifact_root),
        )
        assert len(artifact_refs) == 2
        assert len({reference.artifact_id for reference in artifact_refs}) == 2
        assert len({reference.uri for reference in artifact_refs}) == 1

        await source_service.append_table(
            source_storage,
            CLAUDE_TRANSCRIPT_TABLE,
            daft.from_pydict(
                {
                    "session_id": ["session-migration"],
                    "seq": [0],
                    "role": ["assistant"],
                    "content": ["durable transcript evidence"],
                }
            ),
        )
        await source_service.append_table(
            source_storage,
            "audit_rows",
            daft.from_pydict(
                {
                    "event_id": ["audit-migration"],
                    "event_type": ["migration.acceptance.seeded"],
                }
            ),
        )
        await source_service.append_table(
            source_storage,
            "future_family_state",
            daft.from_pydict({"key": ["unknown-v1"], "value": [73]}),
        )

        catalog = source_service.get_control_catalog(source_storage)
        command_payload = '{"operation":"spawn_reserved","entity_id":41}'
        applied_command_id = str(uuid7())
        (admitted,) = await catalog.admit_commands(
            str(base.world_id),
            [
                CommandAdmission(
                    command_id=applied_command_id,
                    scheduled_tick=1,
                    priority=10,
                    command_type="spawn_reserved",
                    payload_json=command_payload,
                    payload_digest=hashlib.sha256(command_payload.encode()).hexdigest(),
                    version=1,
                    principal_id="migration-actor",
                    origin="acceptance",
                    reserved_entity_id=41,
                )
            ],
        )
        assert admitted.reserved_entity_id == 41
        (leased,) = await catalog.lease_commands(
            str(base.world_id),
            1,
            "migration-command-worker",
        )
        assert leased.command_id == applied_command_id
        await catalog.publish_manifest(
            str(base.world_id),
            str(base.run_id),
            1,
            "migration-command-settlement",
            int(base.commit_coordinator.epoch),
            [],
            command_ids=[applied_command_id],
            lease_owner="migration-command-worker",
        )

        rejected_payload = '{"operation":"update","entity_id":404}'
        rejected_command_id = str(uuid7())
        await catalog.admit_commands(
            str(base.world_id),
            [
                CommandAdmission(
                    command_id=rejected_command_id,
                    scheduled_tick=2,
                    priority=20,
                    command_type="update",
                    payload_json=rejected_payload,
                    payload_digest=hashlib.sha256(rejected_payload.encode()).hexdigest(),
                    version=1,
                    principal_id=None,
                    origin="acceptance",
                )
            ],
        )
        await catalog.lease_commands(
            str(base.world_id),
            2,
            "migration-rejection-worker",
        )
        await catalog.fail_command(
            str(base.world_id),
            rejected_command_id,
            "migration-rejection-worker",
            status="REJECTED",
            error_code="acceptance_rejection",
            error_detail="terminal migration fixture",
        )

        completed = await catalog.lease_evaluation(
            str(base.world_id),
            str(base.run_id),
            "migration-evaluation-complete",
            "subject-complete",
            "contract-v1",
            "grader-complete",
        )
        assert completed.acquired
        await catalog.complete_evaluation(
            str(base.world_id),
            str(base.run_id),
            "migration-evaluation-complete",
            "grader-complete",
        )
        retryable = await catalog.lease_evaluation(
            str(base.world_id),
            str(base.run_id),
            "migration-evaluation-retryable",
            "subject-retryable",
            "contract-v1",
            "grader-retryable",
        )
        assert retryable.acquired
        await catalog.release_evaluation(
            str(base.world_id),
            str(base.run_id),
            "migration-evaluation-retryable",
            "grader-retryable",
        )

        outbox = await catalog.read_outbox(str(base.world_id))
        assert len(outbox) == 4
        await catalog.mark_outbox_projected(
            str(base.world_id),
            [outbox[0].event_id],
        )
        assert await catalog.outbox_progress(str(base.world_id)) == (1, 3)
        await lifecycle.destroy_world(base.world_id)

        base_world_id = str(base.world_id)
        fork_world_id = str(fork.world_id)
        del base, fork, lifecycle, registry, catalog
        await source_service.shutdown()
        await destination_service.shutdown()

        # Migration starts from newly composed administrative services, not
        # from the live World/process objects that created the source state.
        source_service = _service(source_storage)
        destination_service = _service(destination_storage)
        verifier = _DestinationOnlySubprocessVerifier(
            source_service=source_service,
            destination_service=destination_service,
            destination_storage=destination_storage,
            destination_artifact_root=destination_artifact_root,
            source_path_marker=str(source_storage.uri),
        )
        source_endpoint = build_local_migration_endpoint(
            source_storage,
            source_service,
            artifact_store_config=ArtifactStoreConfig.local(source_artifact_root),
            audit_storage_config=source_storage,
        )
        destination_endpoint = build_local_migration_endpoint(
            destination_storage,
            destination_service,
            artifact_store_config=ArtifactStoreConfig.local(destination_artifact_root),
            audit_storage_config=destination_storage,
            cold_verifier=verifier,
        )

        assert not source_endpoint.activity_catalog_path.exists()
        assert not destination_endpoint.activity_catalog_path.exists()
        plan = await plan_storage_migration(
            source=source_endpoint,
            destination=destination_endpoint,
            migration_id="local-whole-storage-identity-v1",
        )
        planned_names = tuple(table.name for table in plan.tables)
        assert ARTIFACT_FILES in planned_names
        assert ARTIFACT_TEXT in planned_names
        assert CLAUDE_TRANSCRIPT_TABLE in planned_names
        assert "audit_rows" in planned_names
        assert "future_family_state" in planned_names
        assert len([table for table in plan.tables if table.classification == "ecs"]) >= 3
        assert {world.status for world in plan.control.worlds} == {"active", "destroyed"}
        assert any(world.parent_world_id for world in plan.control.worlds)
        assert {command.status for command in plan.control.commands} == {
            "APPLIED",
            "REJECTED",
        }
        assert {evaluation.status for evaluation in plan.control.evaluations} == {
            "COMPLETE",
            "RETRYABLE",
        }
        assert {event.projected_at is None for event in plan.control.outbox} == {False, True}

        receipt = await migrate_storage(plan)
        assert verifier.services_shutdown
        assert verifier.invocations == 1
        assert verifier.evidence == receipt.cold_verification
        assert receipt.cold_verification.resumed_world_id == fork_world_id
        assert receipt.cold_verification.acquired_writer_epoch is not None
        assert receipt.cold_verification.imported_fence_floor is not None
        assert (
            receipt.cold_verification.acquired_writer_epoch
            > receipt.cold_verification.imported_fence_floor
        )
        assert receipt.cold_verification.tick_after is not None
        assert receipt.cold_verification.tick_before is not None
        assert receipt.cold_verification.tick_after > receipt.cold_verification.tick_before
        assert receipt.artifacts.occurrence_count == 2
        assert receipt.artifacts.distinct_content_count == 1
        assert receipt.artifacts.total_verified_bytes == len(artifact_payload)
        assert receipt.activity_disposition == "empty-v1"
        assert receipt.control.snapshot_digest == control_snapshot_digest(plan.control)
        assert {(table.name, table.classification) for table in receipt.tables} == {
            (table.name, table.classification) for table in plan.tables
        }

        # The trusted verifier is retry-safe after its first later tick: it
        # finds the frozen imported snapshots in history, repeats fork-visible
        # queries, acquires a still-higher fence, and commits another tick.
        reverification = await verify_storage_migration(
            receipt,
            destination=destination_endpoint,
        )
        assert verifier.invocations == 2
        assert reverification.visible_query_verified
        assert reverification.tick_before is not None
        assert receipt.cold_verification.tick_after is not None
        assert reverification.tick_before >= receipt.cold_verification.tick_after

        # Reopen from destination coordinates only. Exact imported snapshots
        # remain addressable even though the cold probe committed a later fork tick.
        fresh_destination = _service(destination_storage)
        fresh_catalog = fresh_destination.get_control_catalog(destination_storage)
        assert await fresh_destination.list_table_names(destination_storage) == planned_names
        receipt_by_name = {table.name: table for table in receipt.tables}
        for table in plan.tables:
            imported = await fresh_destination.find_table_snapshot(
                destination_storage,
                table.destination,
            )
            assert imported is not None, table.name
            imported_receipt = receipt_by_name[table.name]
            assert imported.snapshot_id == imported_receipt.destination_snapshot_id
            assert imported.schema_fingerprint == table.destination.schema_fingerprint
            assert imported.row_count == table.destination.row_count
            assert imported.content_digest == table.destination.content_digest

        destination_control = await fresh_catalog.export_migration_snapshot()
        assert destination_control.signatures == plan.control.signatures
        assert destination_control.commands == plan.control.commands
        assert destination_control.evaluations == plan.control.evaluations
        assert destination_control.outbox == plan.control.outbox
        assert set(plan.control.manifests) < set(destination_control.manifests)
        planned_worlds = {world.world_id: world for world in plan.control.worlds}
        destination_worlds = {world.world_id: world for world in destination_control.worlds}
        assert destination_worlds.keys() == planned_worlds.keys()
        assert destination_worlds[base_world_id] == planned_worlds[base_world_id]
        resumed_world = destination_worlds[fork_world_id]
        planned_fork = planned_worlds[fork_world_id]
        assert (
            resumed_world.name,
            resumed_world.run_id,
            resumed_world.parent_world_id,
            resumed_world.status,
            resumed_world.writer_mode,
        ) == (
            planned_fork.name,
            planned_fork.run_id,
            planned_fork.parent_world_id,
            planned_fork.status,
            planned_fork.writer_mode,
        )
        assert resumed_world.tick_head == reverification.tick_before
        assert await fresh_catalog.max_reserved_entity_id(base_world_id) == 41
        assert await fresh_catalog.outbox_progress(base_world_id) == (1, 3)

        planned_floors = {floor.world_id: floor.epoch for floor in plan.control.fence_floors}
        destination_floors = {
            floor.world_id: floor.epoch for floor in destination_control.fence_floors
        }
        assert destination_floors.keys() == planned_floors.keys()
        assert destination_floors[base_world_id] == planned_floors[base_world_id]
        assert destination_floors[fork_world_id] == (reverification.acquired_writer_epoch)

        artifact_rows = (
            await fresh_destination.read_table(destination_storage, ARTIFACT_FILES)
        ).to_pylist()
        assert len(artifact_rows) == 2
        destination_uris = {str(row["object_uri"]) for row in artifact_rows}
        assert len(destination_uris) == 1
        destination_object = Path(next(iter(destination_uris)).removeprefix("file://"))
        assert destination_object.is_relative_to(destination_artifact_root.resolve())
        assert destination_object.read_bytes() == artifact_payload
        assert {row["sha256"] for row in artifact_rows} == {
            hashlib.sha256(artifact_payload).hexdigest()
        }
        assert (
            await fresh_destination.read_table(destination_storage, ARTIFACT_TEXT)
        ).count_rows() == 2
        assert (
            await fresh_destination.read_table(destination_storage, CLAUDE_TRANSCRIPT_TABLE)
        ).to_pydict()["content"] == ["durable transcript evidence"]
        assert (await fresh_destination.read_table(destination_storage, "audit_rows")).to_pydict()[
            "event_id"
        ] == ["audit-migration"]
        assert (
            await fresh_destination.read_table(destination_storage, "future_family_state")
        ).to_pydict() == {"key": ["unknown-v1"], "value": [73]}

        reservation = await fresh_catalog.get_migration_reservation(plan.migration_id)
        assert reservation is not None
        assert reservation.status == "COMPLETE"
        assert reservation.plan_digest == plan.plan_digest
        assert reservation.receipt_digest == receipt.receipt_digest
    finally:
        if fresh_destination is not None:
            await fresh_destination.shutdown()
        await source_service.shutdown()
        await destination_service.shutdown()
