# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Cold-restart crash oracle for mission-owned indexed artifact finalization."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pytest

from archetype.app.artifacts.bundle_models import (
    ArtifactStoreConfig,
    PreparedArtifactBundleRequest,
)
from archetype.app.artifacts.bundle_service import _ARTIFACT_INDEX_TABLE
from archetype.app.container import ServiceContainer
from archetype.app.missions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimStatus,
    AttemptRecoveryAction,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
)
from archetype.app.storage.catalog import (
    SqliteControlCatalog,
    artifact_publication_key,
    catalog_path_for,
)
from archetype.core.config import StorageConfig, WorldConfig
from archetype.missions import AttemptStatus, FinalizationPhase

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("missions.attempt.indexed_finalization"),
]

_FIRST_TICK = 41
_RECOVERY_TICK = 97
_REPLAY_TICK = 113
_ATTEMPT_LEASE_SECONDS = 1.0
_CAPABILITIES = ProviderExecutionCapabilities(
    provider="crash-oracle",
    request_fingerprint="crash-oracle-provider-v1",
)
_BOUNDARIES = (
    "after_prepare",
    "after_stage",
    "after_artifact_claim",
    "during_partial_upload",
    "after_uploaded_metadata",
    "after_iceberg_append",
    "after_artifact_indexed",
    "after_mission_settlement",
)


class _CrashBoundary(RuntimeError):
    pass


class _ExecutionRunner:
    def __init__(self, outcome: dict[str, Any]) -> None:
        self.outcome = outcome
        self.external_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _CAPABILITIES

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        self.external_calls += 1
        await kwargs["authorize_execution"](kwargs["authorization"])
        await kwargs["acknowledge_provider"]("session-crash-oracle", "request-crash-oracle")
        return copy.deepcopy(self.outcome)


class _ReconciliationRunner:
    """Return already-observed provider evidence without another external execution."""

    def __init__(self, outcome: dict[str, Any]) -> None:
        self.outcome = outcome
        self.reconcile_calls = 0
        self.external_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _CAPABILITIES

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        self.reconcile_calls += 1
        assert kwargs["authorization"].action is AttemptRecoveryAction.RECONCILE
        return copy.deepcopy(self.outcome)


class _ForbiddenRunner:
    def __init__(self) -> None:
        self.run_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _CAPABILITIES

    async def run_attempt(self, **_: Any) -> dict[str, Any]:
        self.run_calls += 1
        raise AssertionError("durable FINALIZE/SETTLED recovery invoked external execution")


def _row(world_id: str, run_id: str) -> dict[str, Any]:
    return {
        "world_id": world_id,
        "run_id": run_id,
        "entity_id": 7,
        "mission__plan_json": json.dumps(
            [
                {
                    "name": "fix",
                    "prompt": "Fix the indexed-finalization crash oracle",
                    "validators": [{"name": "tests", "command": ["pytest"]}],
                }
            ]
        ),
        "mission__status": "ready",
        "mission__finished": False,
        "mission__succeeded": False,
        "mission__failure_reason": "",
        "mission__pr_url": "",
        "taskgate__step_index": 0,
        "taskgate__attempts": 0,
        "taskgate__max_attempts": 3,
        "taskgate__status": "ready",
        "taskgate__required_finalization_phase": FinalizationPhase.INDEXED.value,
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
        "frictionlog__entries_json": "[]",
    }


def _source_evidence(root: Path) -> dict[str, str]:
    root.mkdir(parents=True)
    files = {
        "sandbox_state_ref": root / "checkpoint.img",
        "finalization_manifest_ref": root / "attempt-manifest.json",
        "trace_ref": root / "agent-output.jsonl",
        "live_status_ref": root / "live-session.json",
        "live_events_ref": root / "live-events.jsonl",
        "filesystem_start_ref": root / "filesystem-start.jsonl",
        "filesystem_end_ref": root / "filesystem-end.jsonl",
        "filesystem_diff_ref": root / "filesystem-diff.jsonl",
        "git_status_ref": root / "git-status.txt",
        "git_patch_ref": root / "worktree.patch",
        "git_bundle_ref": root / "repository.bundle",
    }
    for field, path in files.items():
        path.write_text(f"durable local evidence for {field}\n")
    traces = root / "traces"
    traces.mkdir()
    (traces / "0001.jsonl").write_text('{"event":"first"}\n')
    (traces / "0002.jsonl").write_text('{"event":"second"}\n')
    context = root / "context"
    (context / "nested").mkdir(parents=True)
    (context / "README.md").write_text("local context\n")
    (context / "nested" / "finding.txt").write_text("indexed crash oracle\n")
    return {
        **{field: str(path) for field, path in files.items()},
        "traces_ref": str(traces),
        "context_ref": str(context),
    }


def _outcome(
    request: Any,
    sources: dict[str, str],
    *,
    status: AttemptStatus = AttemptStatus.ACCEPTED,
) -> dict[str, Any]:
    accepted = status is AttemptStatus.ACCEPTED
    return {
        "attempt_id": request.attempt_id,
        "idempotency_key": request.idempotency_key,
        "attempt_index": request.attempt_index,
        "request_fingerprint": attempt_invocation_fingerprint(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        ),
        "status": status.value,
        "accepted": accepted,
        "harness": "crash-oracle",
        "agent_session_id": "session-crash-oracle",
        "validator_details": [
            {
                "name": "tests",
                "command": ["pytest"],
                "expected_returncode": 0,
                "passed": accepted,
                "returncode": 0 if accepted else 1,
                "stdout": "",
                "stderr": "",
            }
        ],
        "checkpoint_provider": "crash-oracle",
        "checkpoint_status": "ready",
        "checkpoint_restorable": True,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 0,
        "finalization_phase": FinalizationPhase.CHECKPOINTED.value,
        "finalization_error": "",
        "results": {"tests": accepted},
        "friction": [],
        "sha": "abc123" if accepted else "",
        "message": "fix: crash-safe indexed finalization" if accepted else "",
        "pushed": False,
        **sources,
    }


def _object_state(root: Path) -> dict[str, tuple[int, int, str]]:
    if not root.exists():
        return {}
    state: dict[str, tuple[int, int, str]] = {}
    for path in sorted(value for value in root.rglob("*") if value.is_file()):
        stat = path.stat()
        state[path.relative_to(root).as_posix()] = (
            stat.st_size,
            stat.st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    return state


def _local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    assert parsed.scheme == "file"
    return Path(unquote(parsed.path))


def _install_crash(
    boundary: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    workflow: Any,
    bundles: Any,
    catalog: SqliteControlCatalog,
    hits: list[str],
) -> None:
    def crash() -> None:
        hits.append(boundary)
        raise _CrashBoundary(boundary)

    if boundary == "after_prepare":

        async def before_stage(*_: Any, **__: Any) -> None:
            crash()

        monkeypatch.setattr(workflow.claim_service, "stage_finalization", before_stage)
        return

    if boundary == "after_stage":
        real_stage = workflow.claim_service.stage_finalization

        async def after_stage(*args: Any, **kwargs: Any) -> Any:
            await real_stage(*args, **kwargs)
            crash()

        monkeypatch.setattr(workflow.claim_service, "stage_finalization", after_stage)
        return

    if boundary == "after_artifact_claim":
        real_acquire = catalog.acquire_artifact_publication

        async def after_claim(*args: Any, **kwargs: Any) -> Any:
            await real_acquire(*args, **kwargs)
            crash()

        monkeypatch.setattr(catalog, "acquire_artifact_publication", after_claim)
        return

    if boundary == "during_partial_upload":
        real_upload = bundles._upload_files

        def during_upload(rows: list[Any]) -> Any:
            assert len(rows) > 1
            real_upload(rows[:1])
            crash()

        monkeypatch.setattr(bundles, "_upload_files", during_upload)
        return

    if boundary == "after_uploaded_metadata":
        real_record = catalog.record_artifact_uploads

        async def after_uploaded(*args: Any, **kwargs: Any) -> Any:
            await real_record(*args, **kwargs)
            crash()

        monkeypatch.setattr(catalog, "record_artifact_uploads", after_uploaded)
        return

    if boundary == "after_iceberg_append":
        real_index = bundles._index_records

        async def after_append(*args: Any, **kwargs: Any) -> Any:
            await real_index(*args, **kwargs)
            crash()

        monkeypatch.setattr(bundles, "_index_records", after_append)
        return

    if boundary == "after_artifact_indexed":
        real_complete = catalog.complete_artifact_publication

        async def after_indexed(*args: Any, **kwargs: Any) -> Any:
            await real_complete(*args, **kwargs)
            crash()

        monkeypatch.setattr(catalog, "complete_artifact_publication", after_indexed)
        return

    if boundary == "after_mission_settlement":
        real_settle = workflow.claim_service.settle_finalized

        async def after_settlement(*args: Any, **kwargs: Any) -> Any:
            await real_settle(*args, **kwargs)
            crash()

        monkeypatch.setattr(workflow.claim_service, "settle_finalized", after_settlement)
        return

    raise AssertionError(f"unhandled crash boundary {boundary}")


@pytest.mark.parametrize(
    ("provider_status", "settlement_status"),
    [
        (AttemptStatus.ACCEPTED, AttemptStatus.INCOMPLETE),
        (AttemptStatus.REJECTED, AttemptStatus.REJECTED),
    ],
)
async def test_mid_upload_expiry_settles_in_the_same_execution_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider_status: AttemptStatus,
    settlement_status: AttemptStatus,
) -> None:
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifact-store")
    storage = StorageConfig(
        uri=tmp_path / "world-store",
        namespace="mid-upload-expiry",
    )
    source_evidence = _source_evidence(tmp_path / "source-evidence")
    container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await container.world_service.create_world(
            WorldConfig(name=f"mid-upload-expiry-{provider_status.value}"),
            storage,
        )
        row = _row(str(world.world_id), str(world.run_id))
        workflow = container.mission_attempt_workflow(storage)
        request = workflow.mission_service.prepare_attempt(row, tick=_FIRST_TICK)
        assert request is not None
        runner = _ExecutionRunner(_outcome(request, source_evidence, status=provider_status))
        catalog = container.storage_service.get_control_catalog(storage)
        assert isinstance(catalog, SqliteControlCatalog)
        real_record = catalog.record_artifact_uploads
        record_calls = 0

        async def expire_before_record(
            world_id: str,
            publication_key: str,
            claimant: str,
            records_json: str,
            manifest_uri: str,
        ) -> None:
            nonlocal record_calls
            record_calls += 1
            await catalog.expire_artifact_publication(
                world_id,
                publication_key,
                claimant,
                "deterministic expiry after upload and before metadata commit",
            )
            await real_record(
                world_id,
                publication_key,
                claimant,
                records_json,
                manifest_uri,
            )

        monkeypatch.setattr(catalog, "record_artifact_uploads", expire_before_record)

        completed = await workflow.execution_service.run(
            row,
            tick=_FIRST_TICK,
            claimant="mid-upload-expiry-worker",
            runner=runner,
            lease_seconds=_ATTEMPT_LEASE_SECONDS,
        )

        assert completed is not None
        assert completed.acquisition.outcome is AttemptClaimAcquireOutcome.ACQUIRED
        assert completed.decision.action is AttemptRecoveryAction.EXECUTE
        assert completed.replayed is False
        assert completed.claim.status is AttemptClaimStatus.SETTLED
        assert completed.claim.settlement_status == settlement_status.value
        assert completed.outcome["finalization_error"] == "artifact_publication_expired"
        assert completed.outcome["finalization_phase"] == FinalizationPhase.CHECKPOINTED.value
        assert completed.updated_row["attempt__status"] == settlement_status.value
        assert completed.updated_row["taskgate__status"] == "retryable"
        assert runner.external_calls == 1
        assert record_calls == 1

        publication = await catalog.get_artifact_publication(
            str(world.world_id),
            completed.claim.artifact_publication_key,
        )
        assert publication is not None
        assert publication.status == "EXPIRED"
        assert publication.records_json == "[]"
        assert publication.manifest_uri == ""
    finally:
        await container.shutdown()


@pytest.mark.parametrize("boundary", _BOUNDARIES)
async def test_indexed_finalization_cold_restart_crash_oracle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifact-store").model_copy(
        update={"lease_seconds": 0.05, "retry_delay_seconds": 0.0}
    )
    storage = StorageConfig(
        uri=tmp_path / "non-default-world-store",
        namespace="indexed-crash-oracle",
    )
    source_evidence = _source_evidence(tmp_path / "source-evidence")
    first = ServiceContainer(artifact_store_config=artifact_config)
    row: dict[str, Any]
    request: Any
    original_row: dict[str, Any]
    publication_key: str
    first_claim: Any
    first_publication: Any
    first_query_rows: list[dict[str, Any]]
    try:
        world = await first.world_service.create_world(WorldConfig(name=boundary), storage)
        row = _row(str(world.world_id), str(world.run_id))
        original_row = copy.deepcopy(row)
        workflow = first.mission_attempt_workflow(storage)
        request = workflow.mission_service.prepare_attempt(row, tick=_FIRST_TICK)
        assert request is not None
        outcome = _outcome(request, source_evidence)
        runner = _ExecutionRunner(outcome)
        catalog = first.storage_service.get_control_catalog(storage)
        assert isinstance(catalog, SqliteControlCatalog)
        publication_key = artifact_publication_key(
            str(world.world_id), str(world.run_id), request.idempotency_key
        )
        prepare_calls = 0
        real_prepare = workflow.artifact_finalizer.prepare

        def tracked_prepare(*args: Any, **kwargs: Any) -> Any:
            nonlocal prepare_calls
            prepare_calls += 1
            return real_prepare(*args, **kwargs)

        monkeypatch.setattr(workflow.artifact_finalizer, "prepare", tracked_prepare)
        hits: list[str] = []
        _install_crash(
            boundary,
            monkeypatch,
            workflow=workflow,
            bundles=first.artifact_bundle_service,
            catalog=catalog,
            hits=hits,
        )

        with pytest.raises(_CrashBoundary, match=boundary):
            await workflow.execution_service.run(
                row,
                tick=_FIRST_TICK,
                claimant=f"first-{boundary}",
                runner=runner,
                lease_seconds=_ATTEMPT_LEASE_SECONDS,
            )

        assert hits == [boundary]
        assert runner.external_calls == 1
        assert prepare_calls == 1
        assert row == original_row
        claim_key = workflow.claim_service.claim_key(
            world_id=str(world.world_id),
            mission_id=request.mission_id,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
        )
        first_claim = await workflow.claim_service.get(str(world.world_id), claim_key)
        assert first_claim is not None
        first_publication = await catalog.get_artifact_publication(
            str(world.world_id), publication_key
        )
        first_query = await first.artifact_bundle_service.query(
            str(world.world_id), str(world.run_id), attempt_id=request.attempt_id
        )
        first_query_rows = first_query.collect().to_pylist()

        expected_claim_status = (
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED
            if boundary == "after_prepare"
            else AttemptClaimStatus.SETTLED
            if boundary == "after_mission_settlement"
            else AttemptClaimStatus.FINALIZING
        )
        assert first_claim.status is expected_claim_status
        expected_publication_status = {
            "after_prepare": None,
            "after_stage": None,
            "after_artifact_claim": "PENDING",
            "during_partial_upload": "PENDING",
            "after_uploaded_metadata": "UPLOADED",
            "after_iceberg_append": "UPLOADED",
            "after_artifact_indexed": "INDEXED",
            "after_mission_settlement": "INDEXED",
        }[boundary]
        assert (first_publication.status if first_publication is not None else None) == (
            expected_publication_status
        )
        index_was_visible = boundary in {
            "after_iceberg_append",
            "after_artifact_indexed",
            "after_mission_settlement",
        }
        assert bool(first_query_rows) is index_was_visible
        if boundary == "during_partial_upload":
            assert _object_state(Path(artifact_config.object_uri))
            assert first_publication is not None and first_publication.records_json == "[]"
    finally:
        await first.shutdown()

    lease_deadline = max(
        first_claim.lease_expires_at,
        first_publication.lease_expires_at if first_publication is not None else 0.0,
    )
    await asyncio.sleep(max(0.0, lease_deadline - time.time() + 0.03))

    cold = ServiceContainer(artifact_store_config=artifact_config)
    try:
        workflow = cold.mission_attempt_workflow(storage)
        cold_prepare_calls = 0
        cold_publish_calls = 0
        real_prepare = workflow.artifact_finalizer.prepare
        real_publish = workflow.artifact_finalizer.publish

        def tracked_prepare(*args: Any, **kwargs: Any) -> Any:
            nonlocal cold_prepare_calls
            cold_prepare_calls += 1
            return real_prepare(*args, **kwargs)

        async def tracked_publish(*args: Any, **kwargs: Any) -> Any:
            nonlocal cold_publish_calls
            cold_publish_calls += 1
            return await real_publish(*args, **kwargs)

        monkeypatch.setattr(workflow.artifact_finalizer, "prepare", tracked_prepare)
        monkeypatch.setattr(workflow.artifact_finalizer, "publish", tracked_publish)
        if boundary == "after_prepare":
            recovery_runner: Any = _ReconciliationRunner(_outcome(request, source_evidence))
        else:
            recovery_runner = _ForbiddenRunner()

        completed = await workflow.execution_service.run(
            row,
            tick=_RECOVERY_TICK,
            claimant=f"recovery-{boundary}",
            runner=recovery_runner,
            lease_seconds=1.0,
        )

        assert completed is not None
        expected_action = (
            AttemptRecoveryAction.RECONCILE
            if boundary == "after_prepare"
            else AttemptRecoveryAction.SETTLED
            if boundary == "after_mission_settlement"
            else AttemptRecoveryAction.FINALIZE
        )
        assert completed.decision.action is expected_action
        assert completed.request.observation_tick == _FIRST_TICK
        assert completed.claim.status is AttemptClaimStatus.SETTLED
        assert completed.claim.settlement_status == AttemptStatus.ACCEPTED.value
        assert completed.updated_row["mission__status"] == "succeeded"
        assert completed.updated_row["mission__finished"] is True
        assert completed.updated_row["mission__succeeded"] is True
        assert completed.updated_row["finalization__phase"] == FinalizationPhase.INDEXED.value
        assert completed.updated_row["finalization__legacy_unbound"] is False
        assert row == original_row
        assert cold_prepare_calls == (1 if boundary == "after_prepare" else 0)
        assert cold_publish_calls == (0 if boundary == "after_mission_settlement" else 1)
        if boundary == "after_prepare":
            assert recovery_runner.reconcile_calls == 1
            assert recovery_runner.external_calls == 0
        else:
            assert recovery_runner.run_calls == 0

        catalog = cold.storage_service.get_control_catalog(storage)
        persisted_claim = await workflow.claim_service.get(
            str(world.world_id), completed.claim.claim_key
        )
        publication = await catalog.get_artifact_publication(str(world.world_id), publication_key)
        assert persisted_claim == completed.claim
        assert publication is not None and publication.status == "INDEXED"
        assert publication.publication_key == completed.claim.artifact_publication_key
        assert publication.publication_key == completed.outcome["finalization_bundle_id"]
        assert publication.index_snapshot_id == completed.outcome["finalization_index_snapshot_id"]
        assert publication.manifest_uri == completed.outcome["finalization_manifest_ref"]

        queried = await cold.artifact_bundle_service.query(
            str(world.world_id), str(world.run_id), attempt_id=request.attempt_id
        )
        queried_rows = queried.collect().to_pylist()
        durable_records = json.loads(publication.records_json)
        assert {row["artifact_id"] for row in queried_rows} == {
            record["artifact_id"] for record in durable_records
        }
        assert {row["tick"] for row in queried_rows} == {_FIRST_TICK}
        manifest = json.loads(_local_path(publication.manifest_uri).read_text())
        assert manifest["bundle_id"] == publication.publication_key
        assert manifest["world_id"] == str(world.world_id)
        assert manifest["run_id"] == str(world.run_id)
        assert manifest["attempt_id"] == request.attempt_id
        assert manifest["tick"] == _FIRST_TICK
        assert manifest["artifacts"]

        iceberg = await cold.storage_service.get_iceberg_context(artifact_config.index_storage)
        table = iceberg.get_table(_ARTIFACT_INDEX_TABLE)
        physical_rows = iceberg.read(table).to_pylist()
        assert len(physical_rows) == len(durable_records)
        assert {row["artifact_id"] for row in physical_rows} == {
            record["artifact_id"] for record in durable_records
        }
        snapshot_id = iceberg.current_snapshot_id(table)
        object_state = _object_state(Path(artifact_config.object_uri))
        assert snapshot_id == publication.index_snapshot_id

        with sqlite3.connect(catalog_path_for(storage)) as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM mission_attempt_claims").fetchone()[0] == 1
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM artifact_publications").fetchone()[0] == 1
            )

        prepared = PreparedArtifactBundleRequest(
            request_json=completed.claim.artifact_request_json,
            request_digest=completed.claim.artifact_request_digest,
            publication_key=completed.claim.artifact_publication_key,
            producer_digest=str(completed.outcome["artifact_producer_digest"]),
            redaction_policy_id=str(completed.outcome["artifact_redaction_policy_id"]),
        )
        duplicate = await cold.artifact_bundle_service.publish_prepared(
            prepared,
            storage_config=storage,
        )
        assert duplicate.duplicate is True
        assert duplicate.bundle_id == publication.publication_key
        assert duplicate.index_snapshot_id == publication.index_snapshot_id
        assert duplicate.records == tuple(
            type(duplicate.records[0]).model_validate(record) for record in durable_records
        )
        assert iceberg.current_snapshot_id(table) == snapshot_id
        assert _object_state(Path(artifact_config.object_uri)) == object_state

        def forbidden_prepare(*_: Any, **__: Any) -> Any:
            raise AssertionError("terminal mission replay prepared artifacts")

        async def forbidden_publish(*_: Any, **__: Any) -> Any:
            raise AssertionError("terminal mission replay published artifacts")

        monkeypatch.setattr(cold.artifact_bundle_service, "prepare", forbidden_prepare)
        monkeypatch.setattr(cold.artifact_bundle_service, "publish_prepared", forbidden_publish)
        replay_runner = _ForbiddenRunner()
        replay = await workflow.execution_service.run(
            row,
            tick=_REPLAY_TICK,
            claimant=f"replay-{boundary}",
            runner=replay_runner,
            lease_seconds=1.0,
        )
        assert replay is not None
        assert replay.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
        assert replay.decision.action is AttemptRecoveryAction.SETTLED
        assert replay.claim == completed.claim
        assert replay.outcome == completed.outcome
        assert replay.updated_row == completed.updated_row
        assert replay.request.observation_tick == _FIRST_TICK
        assert replay_runner.run_calls == 0
        assert iceberg.current_snapshot_id(table) == snapshot_id
        assert _object_state(Path(artifact_config.object_uri)) == object_state
        assert await catalog.get_artifact_publication(str(world.world_id), publication_key) == (
            publication
        )
        with sqlite3.connect(catalog_path_for(storage)) as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM mission_attempt_claims").fetchone()[0] == 1
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM artifact_publications").fetchone()[0] == 1
            )
    finally:
        await cold.shutdown()
