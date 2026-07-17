# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Synchronous runtime parity for sandbox artifact finalization."""

from archetype import (
    ArchetypeRuntime,
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
    Component,
)
from archetype.core.config import StorageConfig


class _SyncArtifactProbe(Component):
    value: int = 0


def test_sync_runtime_publishes_queries_and_reconciles_artifacts(tmp_path):
    source = tmp_path / "sync-result.txt"
    source.write_text("sync artifact")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    artifact_store = ArtifactStoreConfig.local(tmp_path / "artifacts")

    with ArchetypeRuntime.sync(artifact_store=artifact_store) as runtime:
        world = runtime.world("sync-artifacts", storage=storage)
        entity_id = world.spawn(_SyncArtifactProbe(value=1))
        info = world.info()
        request = ArtifactBundleRequest(
            world_id=str(info.world_id),
            run_id=str(info.run_id),
            entity_id=entity_id,
            tick=0,
            attempt_id="sync-attempt",
            idempotency_key="sync-bundle",
            checkpoint_ref="test-checkpoint://sync",
            checkpoint_provider="test",
            artifacts=(
                ArtifactCandidate(
                    source_ref=str(source),
                    logical_path="sync-result.txt",
                ),
            ),
        )

        receipt = world.publish_artifacts(request)
        assert receipt.status == "indexed"
        assert len(world.artifacts(attempt_id="sync-attempt").to_pylist()) == 3
        assert world.reconcile_artifacts().examined == 0
