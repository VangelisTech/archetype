# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application-boundary contracts for authoritative mission artifact publication."""

from __future__ import annotations

import pytest

from archetype.app.application import mission_artifacts
from archetype.app.application.mission_artifacts import MissionArtifactFinalizer
from archetype.app.artifacts.bundle_models import PreparedArtifactBundleRequest
from archetype.app.artifacts.bundle_service import ArtifactBundleService
from archetype.app.container import ServiceContainer
from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.missions.models import (
    AttemptArtifactExpiration,
    MissionArtifactFinalizationExpiredError,
    MissionAttemptRequest,
)
from archetype.app.redaction import RedactionService
from archetype.artifacts.bundles import (
    ArtifactBundleRequest,
    ArtifactPublicationStatus,
    ArtifactPublishReceipt,
    ArtifactStoreConfig,
)
from archetype.core.config import StorageConfig
from archetype.missions.transitions import (
    FinalizationPhase,
    MissionStatus,
    MissionTaskState,
    TaskStatus,
)

pytestmark = pytest.mark.contract("missions.attempt.indexed_finalization")


class _ForbiddenIO:
    def __getattr__(self, name):
        raise AssertionError(f"artifact preparation attempted forbidden I/O through {name}")


class _RecordingArtifactBundles:
    def __init__(self, service: ArtifactBundleService) -> None:
        self._service = service
        self.published: list[PreparedArtifactBundleRequest] = []
        self.receipt_update: dict[str, object] = {}

    @property
    def enabled(self) -> bool:
        return True

    def prepare(self, request: ArtifactBundleRequest) -> PreparedArtifactBundleRequest:
        return self._service.prepare(request)

    async def publish_prepared(
        self,
        prepared: PreparedArtifactBundleRequest,
        *,
        storage_config=None,
    ) -> ArtifactPublishReceipt:
        del storage_config
        self.published.append(prepared)
        request = ArtifactBundleRequest.model_validate_json(prepared.request_json)
        receipt = ArtifactPublishReceipt(
            bundle_id=prepared.publication_key,
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            status=ArtifactPublicationStatus.INDEXED,
            manifest_uri="s3://artifacts/manifest.json",
            index_snapshot_id=42,
            request_digest=prepared.request_digest,
            producer_digest=prepared.producer_digest,
            redaction_policy_id=prepared.redaction_policy_id,
        )
        return receipt.model_copy(update=self.receipt_update)


def _request() -> MissionAttemptRequest:
    return MissionAttemptRequest(
        prompt="Fix the issue",
        validators=({"name": "tests", "command": ["pytest"]},),
        step_name="fix",
        step_index=0,
        attempt_index=1,
        plan_digest="plan-digest",
        max_attempts=3,
        required_finalization_phase=FinalizationPhase.INDEXED,
        idempotency_key="mission-attempt-key",
        mission_id="world-1:run-1:7",
        task_id="task-1",
        attempt_id="attempt-1",
        request_fingerprint="request-fingerprint",
        previous_session_id="",
        previous_validator_details=(),
        correlation={"world_id": "world-1", "run_id": "run-1", "entity_id": "7"},
        source=MissionTaskState(MissionStatus.RUNNING, TaskStatus.READY),
        observation_tick=9,
    )


def _outcome() -> dict[str, object]:
    return {
        "attempt_id": "attempt-1",
        "idempotency_key": "mission-attempt-key",
        "accepted": True,
        "checkpoint_provider": "modal",
        "checkpoint_restorable": True,
        "checkpoint_created_at_ms": 100,
        "checkpoint_expires_at_ms": 200,
        "sandbox_state_ref": "modal-image://checkpoint-1",
        "finalization_manifest_ref": "modal-image://checkpoint-1#/attempt/manifest.json",
        "trace_ref": "modal-image://checkpoint-1#/attempt/trace.jsonl",
        "traces_ref": "modal-image://checkpoint-1#/attempt/traces",
        "live_status_ref": "modal-sandbox://sandbox-1/live/session.json",
        "live_events_ref": "modal-sandbox://sandbox-1/live/events.jsonl",
        "filesystem_start_ref": "modal-image://checkpoint-1#/fs/start.jsonl",
        "filesystem_end_ref": "modal-image://checkpoint-1#/fs/end.jsonl",
        "filesystem_diff_ref": "modal-image://checkpoint-1#/fs/diff.jsonl",
        "git_status_ref": "modal-image://checkpoint-1#/git/status.txt",
        "git_patch_ref": "modal-image://checkpoint-1#/git/worktree.patch",
        "git_bundle_ref": "modal-image://checkpoint-1#/git/repository.bundle",
        "context_ref": "modal-image://checkpoint-1#/.context",
    }


def _finalizer(tmp_path) -> tuple[MissionArtifactFinalizer, _RecordingArtifactBundles, str]:
    redaction = RedactionService()
    service = ArtifactBundleService(
        _ForbiddenIO(),
        _ForbiddenIO(),
        ArtifactStoreConfig.local(tmp_path / "artifacts"),
        _ForbiddenIO(),
        redaction_service=redaction,
    )
    bundles = _RecordingArtifactBundles(service)
    return MissionArtifactFinalizer(bundles), bundles, redaction.policy_id


def test_mission_projection_is_deterministic_and_maps_every_evidence_family(tmp_path):
    finalizer, _bundles, policy_id = _finalizer(tmp_path)

    first = finalizer.prepare(_request(), _outcome(), redaction_policy_id=policy_id)
    second = finalizer.prepare(
        _request(),
        dict(reversed(tuple(_outcome().items()))),
        redaction_policy_id=policy_id,
    )

    assert first == second
    request = ArtifactBundleRequest.model_validate_json(first.request_json)
    assert (request.world_id, request.run_id, request.entity_id, request.tick) == (
        "world-1",
        "run-1",
        7,
        9,
    )
    assert (
        request.checkpoint_ref,
        request.checkpoint_provider,
        request.checkpoint_created_at_ms,
        request.checkpoint_expires_at_ms,
    ) == ("modal-image://checkpoint-1", "modal", 100, 200)
    by_path = {candidate.logical_path: candidate for candidate in request.artifacts}
    assert set(by_path) == {
        "attempt/manifest.json",
        "attempt/agent-output.jsonl",
        "attempt/traces",
        "attempt/live-session.json",
        "attempt/live-events.jsonl",
        "recovery/filesystem-start.jsonl",
        "recovery/filesystem-end.jsonl",
        "recovery/filesystem-diff.jsonl",
        "recovery/git-status.txt",
        "recovery/worktree.patch",
        "recovery/repository.bundle",
        "context",
    }
    assert by_path["attempt/traces"].recursive
    assert not by_path["attempt/traces"].required
    assert by_path["context"].recursive
    assert not by_path["attempt/live-events.jsonl"].required


@pytest.mark.asyncio
async def test_persisted_projection_survives_mapping_code_change(tmp_path, monkeypatch):
    finalizer, bundles, policy_id = _finalizer(tmp_path)
    projection = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )
    changed_map = tuple(
        (field, "changed/trace.jsonl", kind, recursive, required)
        if field == "trace_ref"
        else (field, path, kind, recursive, required)
        for field, path, kind, recursive, required in mission_artifacts._MISSION_ARTIFACT_CANDIDATES
    )
    monkeypatch.setattr(mission_artifacts, "_MISSION_ARTIFACT_CANDIDATES", changed_map)
    remapped = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )
    assert remapped.publication_key == projection.publication_key
    assert remapped.request_digest != projection.request_digest
    assert remapped.producer_digest != projection.producer_digest

    publication = await finalizer.publish(projection)

    assert publication.status is FinalizationPhase.INDEXED
    assert bundles.published[0].request_json == projection.request_json
    persisted = ArtifactBundleRequest.model_validate_json(bundles.published[0].request_json)
    assert "attempt/agent-output.jsonl" in {
        candidate.logical_path for candidate in persisted.artifacts
    }
    assert "changed/trace.jsonl" not in {
        candidate.logical_path for candidate in persisted.artifacts
    }


@pytest.mark.asyncio
async def test_mission_finalizer_requires_the_exact_indexed_receipt(tmp_path):
    finalizer, bundles, policy_id = _finalizer(tmp_path)
    projection = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )
    bundles.receipt_update = {"producer_digest": "f" * 64}

    with pytest.raises(ValueError, match="does not match its staged projection"):
        await finalizer.publish(projection)

    bundles.receipt_update = {"status": ArtifactPublicationStatus.UPLOADED}
    with pytest.raises(RuntimeError, match="authoritative indexed"):
        await finalizer.publish(projection)

    bundles.receipt_update = {"index_snapshot_id": 0}
    with pytest.raises(ValueError, match="index-snapshot evidence"):
        await finalizer.publish(projection)


@pytest.mark.parametrize(
    "snapshot",
    [MAX_ICEBERG_SNAPSHOT_ID + 1, 1.5, True],
)
@pytest.mark.asyncio
async def test_mission_finalizer_revalidates_custom_receipt_snapshot_type_and_range(
    tmp_path,
    snapshot,
):
    finalizer, bundles, policy_id = _finalizer(tmp_path)
    projection = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )
    bundles.receipt_update = {"index_snapshot_id": snapshot}

    with pytest.raises((TypeError, ValueError), match="snapshot"):
        await finalizer.publish(projection)


@pytest.mark.asyncio
async def test_mission_finalizer_translates_exact_expired_receipt(tmp_path):
    finalizer, bundles, policy_id = _finalizer(tmp_path)
    projection = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )
    bundles.receipt_update = {
        "status": ArtifactPublicationStatus.EXPIRED,
        "manifest_uri": "",
        "index_snapshot_id": 0,
    }

    with pytest.raises(MissionArtifactFinalizationExpiredError) as error:
        await finalizer.publish(projection)

    assert error.value.bundle_id == projection.publication_key
    assert error.value.receipt == AttemptArtifactExpiration(
        status="expired",
        bundle_id=projection.publication_key,
        request_digest=projection.request_digest,
        producer_digest=projection.producer_digest,
        redaction_policy_id=projection.redaction_policy_id,
    )


@pytest.mark.asyncio
async def test_mission_finalizer_does_not_translate_mismatched_expired_receipt(tmp_path):
    finalizer, bundles, policy_id = _finalizer(tmp_path)
    projection = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )
    bundles.receipt_update = {
        "status": ArtifactPublicationStatus.EXPIRED,
        "producer_digest": "f" * 64,
        "manifest_uri": "",
        "index_snapshot_id": 0,
    }

    with pytest.raises(ValueError, match="does not match its staged projection"):
        await finalizer.publish(projection)


@pytest.mark.asyncio
async def test_mission_finalizer_returns_exact_indexed_evidence(tmp_path):
    finalizer, _bundles, policy_id = _finalizer(tmp_path)
    projection = finalizer.prepare(
        _request(),
        _outcome(),
        redaction_policy_id=policy_id,
    )

    publication = await finalizer.publish(projection)

    assert publication.status is FinalizationPhase.INDEXED
    assert publication.bundle_id == projection.publication_key
    assert publication.request_digest == projection.request_digest
    assert publication.producer_digest == projection.producer_digest
    assert publication.redaction_policy_id == projection.redaction_policy_id
    assert publication.manifest_uri == "s3://artifacts/manifest.json"
    assert publication.index_snapshot_id == 42


def test_mission_projection_rejects_missing_required_recovery_reference(tmp_path):
    finalizer, _bundles, policy_id = _finalizer(tmp_path)
    outcome = _outcome()
    outcome["git_bundle_ref"] = ""

    with pytest.raises(ValueError, match="requires git_bundle_ref"):
        finalizer.prepare(
            _request(),
            outcome,
            redaction_policy_id=policy_id,
        )


@pytest.mark.asyncio
async def test_container_wires_the_authoritative_mission_artifact_adapter(tmp_path):
    container = ServiceContainer(
        artifact_store_config=ArtifactStoreConfig.local(tmp_path / "artifacts")
    )
    try:
        workflow = container.mission_attempt_workflow(
            StorageConfig(uri=tmp_path / "world", namespace="non-default")
        )
        assert isinstance(workflow.artifact_finalizer, MissionArtifactFinalizer)
    finally:
        await container.shutdown()
