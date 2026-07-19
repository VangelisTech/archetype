# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Cold storage-scoped recovery publishes one real artifact end to end."""

import pytest

from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
)
from archetype.app.container import ServiceContainer
from archetype.app.recovery import RecoveryPolicy
from archetype.core.config import StorageConfig, WorldConfig

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("recovery.artifact.item_scoped"),
]


async def test_storage_bound_fleet_recovers_real_artifact_without_model_capability(
    tmp_path,
) -> None:
    storage = StorageConfig(uri=tmp_path / "worlds", namespace="fleet")
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifact-store")
    )
    try:
        world = await container.world_service.create_world(WorldConfig(name="world"), storage)
        source = tmp_path / "result.json"
        source.write_text('{"status":"fixed"}')
        request = ArtifactBundleRequest(
            world_id=str(world.world_id),
            run_id=str(world.run_id),
            entity_id=7,
            tick=3,
            attempt_id="attempt-1",
            idempotency_key="fleet-publication-1",
            checkpoint_ref="test-checkpoint://snapshot-1",
            checkpoint_provider="test",
            checkpoint_restorable=True,
            accepted=True,
            retention="run",
            artifacts=(
                ArtifactCandidate(
                    source_ref=str(source),
                    logical_path="results/result.json",
                    kind="result",
                ),
            ),
        )
        prepared = container.artifact_bundle_service.prepare(request)
        catalog = container.storage_service.get_control_catalog(storage)
        await catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant="crashed-publisher",
            retry_window_ms=60_000,
            lease_ms=60_000,
        )
        await catalog.fail_artifact_publication(
            request.world_id,
            prepared.publication_key,
            "crashed-publisher",
            "simulate a publisher crash after claim",
            retry_delay_ms=0,
        )

        workflow = container.fleet_recovery_workflow(
            storage,
            policy=RecoveryPolicy(recurring_delay_ms=0),
        )
        assert workflow.artifact_publication is not None
        assert not hasattr(workflow.service, "recover_model")

        result = await workflow.service.run_once(claimant="artifact-maintenance-worker")
        assert result.worlds_examined == 1
        assert result.items_examined == 1
        assert result.completed == 1
        assert result.failed == 0

        publication = await catalog.get_artifact_publication(
            request.world_id,
            prepared.publication_key,
        )
        assert publication is not None
        assert publication.status == "INDEXED"
        assert publication.index_snapshot_id > 0
        [sweep] = await workflow.service.list_sweeps(world_id=request.world_id)
        assert sweep.active_subject_key == ""
        assert sweep.status.value == "idle"
    finally:
        await container.shutdown()
