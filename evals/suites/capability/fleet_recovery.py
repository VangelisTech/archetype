# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free active-subject cold-restart proof for maintenance recovery."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
)
from archetype.app.container import ServiceContainer
from archetype.app.recovery import (
    FleetRecoveryService,
    RecoveryKind,
    RecoveryPolicy,
    RecoverySubject,
)
from archetype.app.storage.catalog import storage_fingerprint
from archetype.core.config import StorageConfig, WorldConfig
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


class _SimulatedProcessLoss(BaseException):
    """Fault injection that bypasses ordinary handler failure settlement."""


class _CrashBeforeArtifactEffect:
    """Lose the process after the active subject is durable but before source I/O."""

    kind = RecoveryKind.ARTIFACT_PUBLICATION

    async def recover(self, subject: RecoverySubject):
        _ = subject
        raise _SimulatedProcessLoss


def task_fleet_recovery_cold_restart() -> list[GraderResult]:
    """Recover an exact artifact after process loss leaves a durable active subject."""

    return asyncio.run(_task_fleet_recovery_cold_restart())


async def _task_fleet_recovery_cold_restart() -> list[GraderResult]:
    with tempfile.TemporaryDirectory(prefix="archetype-fleet-recovery-eval-") as root:
        base = Path(root)
        storage = StorageConfig(uri=base / "worlds", namespace="fleet-eval")
        artifact_config = ArtifactStoreConfig.local(base / "artifacts").model_copy(
            update={"retry_delay_seconds": 0.0}
        )
        source = base / "result.json"
        policy = RecoveryPolicy(
            lease_ms=1_000,
            recurring_delay_ms=0,
            initial_retry_delay_ms=1,
            maximum_retry_delay_ms=1,
            jitter_basis_points=0,
        )

        original = ServiceContainer(artifact_store_config=artifact_config)
        world = await original.world_service.create_world(WorldConfig(name="eval"), storage)
        request = ArtifactBundleRequest(
            world_id=str(world.world_id),
            run_id=str(world.run_id),
            entity_id=1,
            tick=1,
            attempt_id="attempt-1",
            idempotency_key="fleet-recovery-eval",
            checkpoint_ref="eval://checkpoint",
            checkpoint_provider="eval",
            accepted=True,
            retention="run",
            artifacts=(
                ArtifactCandidate(
                    source_ref=str(source),
                    logical_path="result.json",
                    kind="result",
                ),
            ),
        )
        prepared = original.artifact_bundle_service.prepare(request)
        original_catalog = original.storage_service.get_control_catalog(storage)
        await original_catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant="publisher-before-crash",
            retry_window_ms=60_000,
            lease_ms=60_000,
        )
        await original_catalog.fail_artifact_publication(
            request.world_id,
            prepared.publication_key,
            "publisher-before-crash",
            "simulate a publisher crash after claim",
            retry_delay_ms=0,
        )
        first_workflow = original.fleet_recovery_workflow(storage, policy=policy)
        assert first_workflow.artifact_publication is not None
        crash_service = FleetRecoveryService(
            original_catalog,
            storage_fingerprint=storage_fingerprint(storage),
            sources=(first_workflow.artifact_publication,),
            handlers=(_CrashBeforeArtifactEffect(),),
            policy=policy,
        )
        process_lost = False
        try:
            await crash_service.run_once(claimant="worker-before-restart")
        except _SimulatedProcessLoss:
            process_lost = True
        [abandoned] = await crash_service.list_sweeps(world_id=request.world_id)
        safe_abandoned = abandoned.model_dump_json()
        await original.shutdown()

        source.write_text('{"status":"recovered"}')
        await asyncio.sleep(1.05)
        restarted = ServiceContainer(artifact_store_config=artifact_config)
        cold_workflow = restarted.fleet_recovery_workflow(storage, policy=policy)
        second = await cold_workflow.service.run_once(claimant="worker-after-restart")
        cold_catalog = restarted.storage_service.get_control_catalog(storage)
        publication = await cold_catalog.get_artifact_publication(
            request.world_id,
            prepared.publication_key,
        )
        [settled_sweep] = await cold_workflow.service.list_sweeps(world_id=request.world_id)
        exceptions = await cold_workflow.service.list_exceptions(world_id=request.world_id)
        no_model_surface = (
            cold_workflow.artifact_publication is not None
            and not hasattr(cold_workflow.artifact_publication, "recover_model")
            and not hasattr(cold_workflow.service, "recover_model")
        )
        await restarted.shutdown()

    return [
        state_check(
            {
                "process_loss_bypasses_clean_failure_settlement": process_lost,
                "active_subject_is_durable_before_effect": (
                    abandoned.status.value == "leased"
                    and abandoned.active_subject_key == prepared.publication_key
                    and abandoned.cursor == ""
                ),
                "operator_projection_excludes_path_and_request": (
                    "result.json" not in safe_abandoned
                    and "source_ref" not in safe_abandoned
                    and "checkpoint_ref" not in safe_abandoned
                ),
                "maintenance_surface_has_no_model_method": no_model_surface,
            },
            name="fleet_recovery_abandoned_active_subject",
        ),
        state_check(
            {
                "cold_process_recovers_exact_item": (second.completed == 1 and second.failed == 0),
                "artifact_source_row_is_authoritative": (
                    publication is not None
                    and publication.status == "INDEXED"
                    and publication.index_snapshot_id > 0
                ),
                "sweep_settles_without_synthetic_retry": (
                    settled_sweep.status.value == "idle"
                    and settled_sweep.active_subject_key == ""
                    and exceptions == ()
                ),
            },
            name="fleet_recovery_cold_restart",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "fleet_recovery_cold_restart",
        suite=SUITE,
        fn=task_fleet_recovery_cold_restart,
        desc=(
            "A credential-free maintenance surface cold-restarts an abandoned active "
            "subject and indexes its exact durable artifact"
        ),
    )
