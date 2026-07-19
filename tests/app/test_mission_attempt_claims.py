# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable pre-execution claims and possibly-submitted recovery (#501)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from dataclasses import replace
from typing import Any

import pytest

from archetype.app.artifacts.bundle_models import ArtifactBundleRequest, ArtifactStoreConfig
from archetype.app.container import ServiceContainer
from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.missions import (
    ATTEMPT_CLAIM_TRANSITION_GRAPH,
    AttemptArtifactExpiration,
    AttemptArtifactProjection,
    AttemptArtifactPublication,
    AttemptClaimAcquireOutcome,
    AttemptClaimEvent,
    AttemptClaimStatus,
    AttemptClaimTransitionGraph,
    AttemptRecoveryAction,
    MissionArtifactFinalizationExpiredError,
    MissionAttemptClaimService,
    MissionAttemptExecutionService,
    MissionService,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
)
from archetype.app.redaction.models import (
    RedactionPolicyConfig,
    SecretQuarantineError,
)
from archetype.app.redaction.service import RedactionService
from archetype.app.storage.catalog import (
    AttemptClaimConflictError,
    AttemptClaimPendingError,
    AttemptClaimStaleError,
    SqliteControlCatalog,
    artifact_publication_key,
)
from archetype.core.config import StorageConfig, WorldConfig
from archetype.missions import AttemptStatus, FinalizationPhase

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("missions.attempt.claim_fenced"),
]

_SYNTHETIC_SECRET = "sk-proj-" + "A" * 32


def _claim_service(
    catalog: Any,
    redaction_service: RedactionService | None = None,
) -> MissionAttemptClaimService:
    return MissionAttemptClaimService(
        catalog,
        redaction_service=redaction_service or RedactionService(),
    )


def _row() -> dict[str, Any]:
    plan = [
        {
            "name": "fix",
            "prompt": "Fix the reported bug",
            "validators": [{"name": "tests", "command": ["pytest"]}],
        }
    ]
    return {
        "world_id": "world-1",
        "run_id": "run-1",
        "entity_id": 7,
        "mission__plan_json": json.dumps(plan),
        "mission__status": "ready",
        "mission__finished": False,
        "mission__succeeded": False,
        "mission__failure_reason": "",
        "mission__pr_url": "",
        "taskgate__step_index": 0,
        "taskgate__attempts": 0,
        "taskgate__max_attempts": 3,
        "taskgate__status": "ready",
        "taskgate__required_finalization_phase": "checkpointed",
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
        "frictionlog__entries_json": "[]",
    }


def _request(*, tick: int = 11):
    request = MissionService().prepare_attempt(_row(), tick=tick)
    assert request is not None
    return request


def _indexed_row() -> dict[str, Any]:
    row = _row()
    row["taskgate__required_finalization_phase"] = FinalizationPhase.INDEXED.value
    return row


def _indexed_request(*, tick: int = 11):
    request = MissionService().prepare_attempt(_indexed_row(), tick=tick)
    assert request is not None
    return request


def _artifact_projection(request: Any, policy_id: str) -> AttemptArtifactProjection:
    world_id = str(request.correlation["world_id"])
    run_id = str(request.correlation["run_id"])
    request_json = json.dumps(
        {
            "attempt_id": request.attempt_id,
            "idempotency_key": request.idempotency_key,
            "redaction_policy_id": policy_id,
            "run_id": run_id,
            "source": "fake://checkpoint",
            "world_id": world_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return AttemptArtifactProjection(
        request_json=request_json,
        request_digest=hashlib.sha256(request_json.encode()).hexdigest(),
        publication_key=artifact_publication_key(
            world_id,
            run_id,
            request.idempotency_key,
        ),
        producer_digest="c" * 64,
        redaction_policy_id=policy_id,
    )


def _artifact_publication(
    projection: AttemptArtifactProjection,
) -> AttemptArtifactPublication:
    return AttemptArtifactPublication(
        status=FinalizationPhase.INDEXED,
        bundle_id=projection.publication_key,
        manifest_uri="s3://artifacts/manifest.json",
        index_snapshot_id=17,
        request_digest=projection.request_digest,
        producer_digest=projection.producer_digest,
        redaction_policy_id=projection.redaction_policy_id,
    )


def _unchecked_artifact_publication(
    projection: AttemptArtifactProjection,
    *,
    index_snapshot_id: Any,
) -> AttemptArtifactPublication:
    """Bypass the frozen model to emulate a dishonest custom finalizer."""

    publication = _artifact_publication(projection)
    object.__setattr__(publication, "index_snapshot_id", index_snapshot_id)
    return publication


async def _advance_artifact_publication(
    catalog: Any,
    projection: AttemptArtifactProjection,
    *,
    status: str,
    index_snapshot_id: int = 17,
    claimant: str = "artifact-publisher",
) -> Any:
    """Advance the exact staged request through the real durable outbox API."""

    request = json.loads(projection.request_json)
    world_id = str(request["world_id"])
    publication = await catalog.get_artifact_publication(
        world_id,
        projection.publication_key,
    )
    if publication is None:
        _, publication = await catalog.acquire_artifact_publication(
            world_id=world_id,
            run_id=str(request["run_id"]),
            attempt_id=str(request["attempt_id"]),
            idempotency_key=str(request["idempotency_key"]),
            request_digest=projection.producer_digest,
            request_json=projection.request_json,
            claimant=claimant,
            retry_window_ms=60_000,
            lease_ms=60_000,
        )
    assert publication.publication_key == projection.publication_key

    if status == "PENDING":
        return publication
    if status == "EXPIRED":
        if publication.status != "EXPIRED":
            await catalog.expire_artifact_publication(
                world_id,
                projection.publication_key,
                claimant,
                "test retry window elapsed",
            )
        return await catalog.get_artifact_publication(world_id, projection.publication_key)
    if publication.status == "PENDING":
        await catalog.record_artifact_uploads(
            world_id,
            projection.publication_key,
            claimant,
            '[{"uri":"s3://artifacts/result.json"}]',
            "s3://artifacts/manifest.json",
        )
        publication = await catalog.get_artifact_publication(
            world_id,
            projection.publication_key,
        )
    if status == "UPLOADED":
        return publication
    if status != "INDEXED":
        raise AssertionError(f"unsupported artifact publication status: {status}")
    if publication is not None and publication.status != "INDEXED":
        await catalog.complete_artifact_publication(
            world_id,
            projection.publication_key,
            claimant,
            index_snapshot_id,
        )
    return await catalog.get_artifact_publication(world_id, projection.publication_key)


async def _stage_indexed_claim(
    service: MissionAttemptClaimService,
    *,
    claimant: str = "worker",
    lease_seconds: float = 900.0,
    status: AttemptStatus = AttemptStatus.ACCEPTED,
    outcome_extra: dict[str, Any] | None = None,
) -> tuple[Any, AttemptArtifactProjection]:
    request = _indexed_request()
    acquired = await service.acquire(
        request,
        _capabilities(),
        claimant=claimant,
        lease_seconds=lease_seconds,
    )
    decision = await service.decide_recovery(
        acquired.claim,
        lease_seconds=lease_seconds,
    )
    consumed = await service.consume_execution(decision.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-indexed",
        provider_request_id="request-indexed",
    )
    outcome = _outcome(
        request=request,
        status=status,
        agent_session_id="session-indexed",
    )
    outcome.update(outcome_extra or {})
    durable = service.prepare_durable_outcome(acknowledged, outcome)
    projection = _artifact_projection(request, acknowledged.redaction_policy_id)
    staged = await service.stage_finalization(
        acknowledged,
        outcome=durable,
        projection=projection,
    )
    return staged, projection


async def _seed_unbound_settled_claim(
    path: Any,
    *,
    phase: FinalizationPhase,
    as_v7: bool,
    required_phase: FinalizationPhase = FinalizationPhase.INDEXED,
    authority_extras: bool = False,
) -> tuple[Any, dict[str, Any]]:
    """Persist unbound evidence through the raw catalog, optionally as real v7."""

    redaction = RedactionService()
    catalog = SqliteControlCatalog(path)
    service = _claim_service(catalog, redaction)
    row = _row()
    row["taskgate__required_finalization_phase"] = required_phase.value
    request = MissionService().prepare_attempt(row, tick=11)
    assert request is not None
    acquired = await service.acquire(
        request,
        _capabilities(),
        claimant="v7-worker",
    )
    decision = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(decision.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-v7",
        provider_request_id="request-v7",
    )
    outcome = _outcome(
        request=request,
        status=AttemptStatus.ACCEPTED,
        agent_session_id="session-v7",
        finalization_phase=phase.value,
    )
    if authority_extras:
        outcome.update(
            artifact_publication_key="b" * 64,
            artifact_request_digest="c" * 64,
            artifact_producer_digest="d" * 64,
            artifact_redaction_policy_id=redaction.policy_id,
            finalization_bundle_id="b" * 64,
            finalization_request_digest="c" * 64,
            finalization_producer_digest="d" * 64,
            finalization_redaction_policy_id=redaction.policy_id,
            finalization_index_snapshot_id=17,
        )
    redacted = redaction.redact_record(outcome, scope="mission-attempt-outcome")
    assert redacted.value == outcome
    error = redaction.redact_text("", scope="mission-attempt-last-error")
    evidence_json = service._updated_redaction_evidence(
        acknowledged,
        outcome=redacted.receipt,
        last_error=error.receipt,
    )
    legacy_request_json = ""
    if as_v7:
        legacy_request = json.loads(acknowledged.request_json)
        assert legacy_request.pop("claim_contract_version") == 8
        assert legacy_request.pop("observation_tick") == request.observation_tick
        legacy_request_json = service._json(legacy_request)
        request_receipt = redaction.redact_record(
            legacy_request,
            scope="mission-attempt-request",
        ).receipt
        legacy_evidence = json.loads(evidence_json)
        legacy_evidence["request"] = request_receipt.model_dump(mode="json")
        evidence_json = service._json(legacy_evidence)
    outcome_json = service._json(outcome)
    await catalog.transition_attempt_claim(
        acknowledged.world_id,
        acknowledged.claim_key,
        acknowledged.claimant,
        acknowledged.fence_epoch,
        expected_status=AttemptClaimStatus.PROVIDER_ACKNOWLEDGED.value,
        target_status=AttemptClaimStatus.SETTLED.value,
        redaction_evidence_json=evidence_json,
        settlement_status=AttemptStatus.ACCEPTED.value,
        outcome_digest=hashlib.sha256(outcome_json.encode()).hexdigest(),
        outcome_json=outcome_json,
        last_error="",
    )
    await catalog.close()

    if as_v7:
        connection = sqlite3.connect(path)
        connection.execute(
            "UPDATE mission_attempt_claims SET request_json=?",
            (legacy_request_json,),
        )
        for column in (
            "artifact_request_json",
            "artifact_request_digest",
            "artifact_publication_key",
            "finalizing_at",
            "legacy_unbound_eligible",
        ):
            connection.execute(f"ALTER TABLE mission_attempt_claims DROP COLUMN {column}")
        connection.execute("UPDATE catalog_meta SET value='7' WHERE key='schema_version'")
        connection.commit()
        connection.close()
    return request, outcome


def _strip_claim_contract_marker(path: Any) -> None:
    """Mimic a post-v8 raw mutation without granting migration provenance."""

    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "SELECT request_json, redaction_evidence_json FROM mission_attempt_claims"
    ).fetchone()
    assert row is not None
    request_json = json.loads(row["request_json"])
    assert request_json.pop("claim_contract_version") == 8
    assert request_json.pop("observation_tick") == 11
    evidence = json.loads(row["redaction_evidence_json"])
    receipt = (
        RedactionService()
        .redact_record(
            request_json,
            scope="mission-attempt-request",
        )
        .receipt
    )
    evidence["request"] = receipt.model_dump(mode="json")
    connection.execute(
        "UPDATE mission_attempt_claims SET request_json=?, redaction_evidence_json=?",
        (
            json.dumps(request_json, sort_keys=True, separators=(",", ":")),
            json.dumps(evidence, sort_keys=True, separators=(",", ":")),
        ),
    )
    connection.commit()
    connection.close()


class _NoRunRunner:
    def __init__(self) -> None:
        self.run_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _capabilities()

    async def run_attempt(self, **_: Any) -> dict[str, Any]:
        self.run_calls += 1
        raise AssertionError("FINALIZE recovery must not invoke the sandbox runner")


class _RecoveryFinalizer:
    def __init__(
        self,
        catalog: Any,
        projection: AttemptArtifactProjection,
        publication: AttemptArtifactPublication,
        *,
        durable_status: str = "INDEXED",
        durable_snapshot_id: int = 17,
    ) -> None:
        self.catalog = catalog
        self.projection = projection
        self.publication = publication
        self.durable_status = durable_status
        self.durable_snapshot_id = durable_snapshot_id
        self.prepare_calls = 0
        self.publish_calls = 0

    def prepare(self, *_: Any, **__: Any) -> AttemptArtifactProjection:
        self.prepare_calls += 1
        raise AssertionError("FINALIZE recovery must reuse the staged artifact request")

    async def publish(
        self,
        projection: AttemptArtifactProjection,
    ) -> AttemptArtifactPublication:
        self.publish_calls += 1
        assert projection == self.projection
        if self.durable_status != "MISSING":
            await _advance_artifact_publication(
                self.catalog,
                projection,
                status=self.durable_status,
                index_snapshot_id=self.durable_snapshot_id,
            )
        return self.publication


class _ExpiredFinalizer:
    def __init__(
        self,
        catalog: Any,
        projection: AttemptArtifactProjection,
        *,
        bundle_id: str | None = None,
    ) -> None:
        self.catalog = catalog
        self.projection = projection
        self.bundle_id = bundle_id or projection.publication_key
        self.prepare_calls = 0
        self.publish_calls = 0

    def prepare(self, *_: Any, **__: Any) -> AttemptArtifactProjection:
        self.prepare_calls += 1
        raise AssertionError("FINALIZE recovery must reuse the staged artifact request")

    async def publish(
        self,
        projection: AttemptArtifactProjection,
    ) -> AttemptArtifactPublication:
        self.publish_calls += 1
        assert projection == self.projection
        await _advance_artifact_publication(
            self.catalog,
            projection,
            status="EXPIRED",
        )
        raise MissionArtifactFinalizationExpiredError(
            AttemptArtifactExpiration(
                status="expired",
                bundle_id=self.bundle_id,
                request_digest=projection.request_digest,
                producer_digest=projection.producer_digest,
                redaction_policy_id=projection.redaction_policy_id,
            )
        )


class _IndexedRunner:
    def __init__(self, request: Any, *, status: AttemptStatus = AttemptStatus.ACCEPTED) -> None:
        self.request = request
        self.status = status
        self.run_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _capabilities()

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        self.run_calls += 1
        await kwargs["authorize_execution"](kwargs["authorization"])
        await kwargs["acknowledge_provider"]("session-indexed", "request-indexed")
        return _outcome(
            request=self.request,
            status=self.status,
            agent_session_id="session-indexed",
        )


class _StageObservingFinalizer:
    def __init__(
        self,
        service: MissionAttemptClaimService,
        catalog: Any,
        request: Any,
    ) -> None:
        self.service = service
        self.catalog = catalog
        self.request = request
        self.prepare_calls = 0
        self.publish_calls = 0
        self.observed_status: AttemptClaimStatus | None = None

    def prepare(
        self,
        request: Any,
        outcome: Any,
        *,
        redaction_policy_id: str,
    ) -> AttemptArtifactProjection:
        self.prepare_calls += 1
        assert request == self.request
        assert outcome["finalization_phase"] == "checkpointed"
        assert request.observation_tick == self.request.observation_tick
        return _artifact_projection(request, redaction_policy_id)

    async def publish(
        self,
        projection: AttemptArtifactProjection,
    ) -> AttemptArtifactPublication:
        self.publish_calls += 1
        claim_key = self.service.claim_key(
            world_id="world-1",
            mission_id=self.request.mission_id,
            task_id=self.request.task_id,
            attempt_id=self.request.attempt_id,
        )
        staged = await self.service.get("world-1", claim_key)
        assert staged is not None
        self.observed_status = staged.status
        assert staged.status is AttemptClaimStatus.FINALIZING
        assert staged.artifact_request_json == projection.request_json
        await _advance_artifact_publication(
            self.catalog,
            projection,
            status="INDEXED",
        )
        return _artifact_publication(projection)


def _capabilities(**overrides: Any) -> ProviderExecutionCapabilities:
    values: dict[str, Any] = {
        "provider": "fake",
        "request_fingerprint": "modal-spec-and-harness-fingerprint",
    }
    values.update(overrides)
    return ProviderExecutionCapabilities(**values)


def _outcome(
    *,
    status: AttemptStatus = AttemptStatus.REJECTED,
    request=None,
    **extra: Any,
) -> dict[str, Any]:
    request = request or _request()
    value = {
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
        "accepted": status is AttemptStatus.ACCEPTED,
        "harness": "fake",
        "agent_session_id": "",
        "validator_details": [
            {
                "name": "tests",
                "command": ["pytest"],
                "expected_returncode": 0,
                "passed": status is AttemptStatus.ACCEPTED,
                "returncode": 0 if status is AttemptStatus.ACCEPTED else 1,
                "stdout": "",
                "stderr": "",
            }
        ],
        "checkpoint_provider": "fake",
        "checkpoint_status": "ready",
        "checkpoint_restorable": True,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 2,
        "sandbox_state_ref": "fake://checkpoint",
        "finalization_phase": "checkpointed",
        "finalization_manifest_ref": "fake://manifest",
        "finalization_error": "",
        "results": {"tests": status is AttemptStatus.ACCEPTED},
        "trace_ref": "fake://trace",
        "traces_ref": "fake://traces",
        "filesystem_start_ref": "fake://filesystem-start",
        "filesystem_end_ref": "fake://filesystem-end",
        "filesystem_diff_ref": "fake://filesystem-diff",
        "git_status_ref": "fake://git-status",
        "git_patch_ref": "fake://git-patch",
        "git_bundle_ref": "fake://git-bundle",
        "context_ref": "fake://context",
        "friction": [],
        "sha": "abc123" if status is AttemptStatus.ACCEPTED else "",
        "message": "fix: repair" if status is AttemptStatus.ACCEPTED else "",
        "pushed": False,
    }
    value.update(extra)
    return value


@pytest.mark.parametrize("location", ["prompt", "validator"])
async def test_secret_bearing_request_is_quarantined_before_claim_commit(
    tmp_path,
    location: str,
) -> None:
    row = _row()
    plan = json.loads(row["mission__plan_json"])
    if location == "prompt":
        plan[0]["prompt"] = f"debug with {_SYNTHETIC_SECRET}"
    else:
        plan[0]["validators"][0]["command"] = ["tool", "--api-key", _SYNTHETIC_SECRET]
    row["mission__plan_json"] = json.dumps(plan)
    request = MissionService().prepare_attempt(row, tick=1)
    assert request is not None
    catalog = SqliteControlCatalog(tmp_path / f"{location}.db")
    service = _claim_service(catalog)

    with pytest.raises(SecretQuarantineError, match="secret-bearing payload quarantined"):
        await service.acquire(request, _capabilities(), claimant="worker")

    claim_key = service.claim_key(
        world_id="world-1",
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    assert await service.get("world-1", claim_key) is None
    await catalog.close()


async def test_secret_bearing_provider_identity_is_quarantined_before_ack_commit(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(_request(), _capabilities(), claimant="worker")
    decision = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(decision.authorization)

    with pytest.raises(SecretQuarantineError, match="secret-bearing payload quarantined"):
        await service.acknowledge_provider(
            consumed,
            provider_session_id=_SYNTHETIC_SECRET,
        )

    persisted = await service.get(consumed.world_id, consumed.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    assert persisted.provider_session_id == ""
    assert json.loads(persisted.redaction_evidence_json)["acknowledgement"] is None
    await catalog.close()


async def test_secret_bearing_provider_capability_is_quarantined_before_claim_commit(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    request = _request()

    with pytest.raises(SecretQuarantineError, match="secret-bearing payload quarantined"):
        await service.acquire(
            request,
            _capabilities(provider=_SYNTHETIC_SECRET),
            claimant="worker",
        )

    claim_key = service.claim_key(
        world_id="world-1",
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    assert await service.get("world-1", claim_key) is None
    await catalog.close()


async def test_outcome_and_error_are_redacted_before_projection_and_settlement(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    request = _request()
    acquired = await service.acquire(request, _capabilities(), claimant="worker")
    decision = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(decision.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-1",
    )
    raw_outcome = _outcome(
        request=request,
        agent_session_id="session-1",
        message=f"provider diagnostic {_SYNTHETIC_SECRET}",
        friction=[{"message": f"retry {_SYNTHETIC_SECRET}"}],
    )
    raw_outcome["validator_details"][0]["stdout"] = _SYNTHETIC_SECRET

    durable = service.prepare_durable_outcome(acknowledged, raw_outcome)
    serialized = json.dumps(durable.value, sort_keys=True)
    assert _SYNTHETIC_SECRET not in serialized
    assert durable.receipt.status == "redacted"
    assert "openai-api-key" in durable.receipt.rule_ids
    projected = MissionService().apply_attempt(_row(), request, durable.value)
    assert _SYNTHETIC_SECRET not in projected["attempt__validator_details_json"]
    assert _SYNTHETIC_SECRET not in projected["frictionlog__entries_json"]

    settled = await service.settle(
        acknowledged,
        attempt_status=AttemptStatus.REJECTED,
        outcome=durable,
        last_error=f"validator failed with {_SYNTHETIC_SECRET}",
    )
    assert _SYNTHETIC_SECRET not in settled.outcome_json
    assert _SYNTHETIC_SECRET not in settled.last_error
    evidence = json.loads(settled.redaction_evidence_json)
    assert evidence["outcome"]["status"] == "redacted"
    assert "openai-api-key" in evidence["outcome"]["rule_ids"]
    assert evidence["last_error"]["status"] == "redacted"
    replayed = service.settled_outcome(settled)
    assert MissionService().apply_attempt(_row(), request, replayed) == projected
    await catalog.close()


@pytest.mark.parametrize(
    ("field", "unsafe_ref"),
    [
        (
            "trace_ref",
            f"https://trace.example/item?token={_SYNTHETIC_SECRET}",
        ),
        ("context_ref", "sandbox://root/.codex/auth.json"),
    ],
)
async def test_secret_bearing_semantic_ref_is_quarantined_without_settlement(
    tmp_path,
    field: str,
    unsafe_ref: str,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(_request(), _capabilities(), claimant="worker")
    decision = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(decision.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-1",
    )
    outcome = _outcome(agent_session_id="session-1", **{field: unsafe_ref})

    with pytest.raises(SecretQuarantineError, match="secret-bearing payload quarantined"):
        service.prepare_durable_outcome(acknowledged, outcome)

    persisted = await service.get(acknowledged.world_id, acknowledged.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.PROVIDER_ACKNOWLEDGED
    assert persisted.outcome_json == ""
    await catalog.close()


async def test_policy_drift_fails_closed_for_live_claim_but_terminal_replay_is_readable(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    original = _claim_service(catalog)
    request = _request()
    acquired = await original.acquire(request, _capabilities(), claimant="worker")
    before = await original.get(acquired.claim.world_id, acquired.claim.claim_key)
    changed_policy = RedactionService(RedactionPolicyConfig(scan_chunk_bytes=8192))
    changed = _claim_service(catalog, changed_policy)
    observed = await changed.get(acquired.claim.world_id, acquired.claim.claim_key)

    with pytest.raises(ValueError, match="redaction policy differs"):
        await changed.renew(observed or acquired.claim)

    assert await original.get(acquired.claim.world_id, acquired.claim.claim_key) == before
    # Legacy PUBLISHED evidence remains replayable under its compatible gate,
    # even after the active redaction policy changes.
    outcome = _outcome(finalization_phase=FinalizationPhase.PUBLISHED.value)
    settled = await original.settle(
        acquired.claim,
        attempt_status=AttemptStatus.REJECTED,
        outcome=outcome,
        last_error=f"validator failed with {_SYNTHETIC_SECRET}",
    )
    assert _SYNTHETIC_SECRET not in settled.last_error
    duplicate = await changed.acquire(request, _capabilities(), claimant="other-worker")
    assert duplicate.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert duplicate.claim == settled
    assert changed.settled_outcome(duplicate.claim) == outcome
    assert (
        await changed.settle(
            duplicate.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=outcome,
            last_error=f"validator failed with {_SYNTHETIC_SECRET}",
        )
        == settled
    )
    with pytest.raises(ValueError, match="settlement changed on replay"):
        await changed.settle(
            duplicate.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=dict(outcome, message="different outcome"),
            last_error=f"validator failed with {_SYNTHETIC_SECRET}",
        )
    await catalog.close()


@pytest.mark.parametrize("required_phase", list(FinalizationPhase))
async def test_unbound_raw_indexed_outcome_fails_closed_for_every_gate_policy(
    tmp_path,
    required_phase: FinalizationPhase,
) -> None:
    row = _row()
    row["taskgate__required_finalization_phase"] = required_phase.value
    request = MissionService().prepare_attempt(row, tick=1)
    assert request is not None
    catalog = SqliteControlCatalog(tmp_path / f"unbound-{required_phase.value}.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(request, _capabilities(), claimant="worker")
    decision = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(decision.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-unbound",
        provider_request_id="request-unbound",
    )
    raw_indexed = _outcome(
        request=request,
        status=AttemptStatus.REJECTED,
        agent_session_id="session-unbound",
        finalization_phase=FinalizationPhase.INDEXED.value,
        finalization_manifest_ref="s3://artifacts/manifest.json",
        finalization_bundle_id="b" * 64,
        finalization_request_digest="c" * 64,
        finalization_producer_digest="d" * 64,
        finalization_redaction_policy_id=acknowledged.redaction_policy_id,
        finalization_index_snapshot_id=17,
    )

    with pytest.raises(ValueError, match="staged authority|staged artifact request"):
        service.prepare_durable_outcome(acknowledged, raw_indexed)

    persisted = await service.get(acknowledged.world_id, acknowledged.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.PROVIDER_ACKNOWLEDGED
    assert persisted.outcome_json == ""
    await catalog.close()


async def test_live_published_evidence_cannot_create_accepted_indexed_settlement(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "published-indexed-live.db")
    service = _claim_service(catalog)
    request = _indexed_request(tick=23)
    acquired = await service.acquire(request, _capabilities(), claimant="worker")
    decision = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(decision.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-live-published",
        provider_request_id="request-live-published",
    )
    published = service.prepare_durable_outcome(
        acknowledged,
        _outcome(
            request=request,
            status=AttemptStatus.ACCEPTED,
            agent_session_id="session-live-published",
            finalization_phase=FinalizationPhase.PUBLISHED.value,
        ),
    )

    with pytest.raises(ValueError, match="settlement status disagrees"):
        await service.settle(
            acknowledged,
            attempt_status=AttemptStatus.ACCEPTED,
            outcome=published,
        )

    persisted = await service.get(acknowledged.world_id, acknowledged.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.PROVIDER_ACKNOWLEDGED
    await catalog.close()


async def test_attempt_claim_graph_is_complete_and_rejects_every_absent_edge() -> None:
    graph = AttemptClaimTransitionGraph()
    assert len(ATTEMPT_CLAIM_TRANSITION_GRAPH) == 7
    for (source, event), target in ATTEMPT_CLAIM_TRANSITION_GRAPH.items():
        transition = graph.transition(source.value, event.value)
        assert transition.source is source
        assert transition.event is event
        assert transition.target is target

    all_pairs = {(source, event) for source in AttemptClaimStatus for event in AttemptClaimEvent}
    for source, event in all_pairs - set(ATTEMPT_CLAIM_TRANSITION_GRAPH):
        with pytest.raises(ValueError, match="illegal attempt claim transition"):
            graph.transition(source, event)


async def test_stage_finalization_persists_exact_outcome_and_request_before_io(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "stage.db")
    service = _claim_service(catalog)
    staged, projection = await _stage_indexed_claim(service)

    assert staged.status is AttemptClaimStatus.FINALIZING
    assert staged.finalizing_at
    assert staged.settled_at is None
    assert staged.artifact_request_json == projection.request_json
    assert staged.artifact_request_digest == projection.request_digest
    assert staged.artifact_publication_key == projection.publication_key
    assert service.staged_artifact_projection(staged) == projection
    staged_outcome = json.loads(staged.outcome_json)
    assert staged_outcome["finalization_phase"] == "checkpointed"
    assert staged_outcome["artifact_publication_key"] == projection.publication_key
    assert staged_outcome["artifact_request_digest"] == projection.request_digest
    assert staged_outcome["artifact_producer_digest"] == projection.producer_digest
    assert staged_outcome["artifact_redaction_policy_id"] == projection.redaction_policy_id
    assert (await service.decide_recovery(staged)).action is AttemptRecoveryAction.FINALIZE
    await catalog.close()


async def test_execution_stages_accepted_attempt_before_artifact_publication_io(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "execute-indexed.db")
    claims = _claim_service(catalog)
    request = _indexed_request(tick=23)
    runner = _IndexedRunner(request)
    finalizer = _StageObservingFinalizer(claims, catalog, request)

    execution = await MissionAttemptExecutionService(
        claims,
        MissionService(),
        finalizer,
    ).run(
        _indexed_row(),
        tick=23,
        claimant="worker",
        runner=runner,
        lease_seconds=1,
    )

    assert execution is not None
    assert execution.decision.action is AttemptRecoveryAction.EXECUTE
    assert execution.replayed is False
    assert execution.claim.status is AttemptClaimStatus.SETTLED
    assert execution.updated_row["mission__status"] == "succeeded"
    assert execution.updated_row["finalization__phase"] == "indexed"
    assert execution.updated_row["finalization__bundle_id"] == artifact_publication_key(
        "world-1",
        "run-1",
        request.idempotency_key,
    )
    assert execution.updated_row["finalization__legacy_unbound"] is False
    assert execution.claim.legacy_unbound is False
    assert runner.run_calls == 1
    assert finalizer.prepare_calls == 1
    assert finalizer.publish_calls == 1
    assert finalizer.observed_status is AttemptClaimStatus.FINALIZING
    await catalog.close()


async def test_recovered_execution_preserves_first_observation_tick_before_staging(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "observation-tick-recovery.db")
    claims = _claim_service(catalog)
    first_request = _indexed_request(tick=11)
    acquired = await claims.acquire(
        first_request,
        _capabilities(),
        claimant="crashed-before-execution",
        lease_seconds=0.05,
    )
    assert json.loads(acquired.claim.request_json)["observation_tick"] == 11
    await asyncio.sleep(0.06)

    runner = _IndexedRunner(first_request)
    finalizer = _StageObservingFinalizer(claims, catalog, first_request)
    execution = await MissionAttemptExecutionService(
        claims,
        MissionService(),
        finalizer,
    ).run(
        _indexed_row(),
        tick=99,
        claimant="recovery-worker",
        runner=runner,
        lease_seconds=1,
    )

    assert execution is not None
    assert execution.acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert execution.request.observation_tick == 11
    assert finalizer.prepare_calls == 1
    assert runner.run_calls == 1
    await catalog.close()


async def test_stale_stage_response_cannot_publish_or_settle_after_finalizer_takeover(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "stale-stage-response.db"
    stale_catalog = SqliteControlCatalog(path)
    stale_claims = _claim_service(stale_catalog)
    request = _indexed_request(tick=23)
    runner = _IndexedRunner(request)
    stale_finalizer = _StageObservingFinalizer(stale_claims, stale_catalog, request)
    stage_committed = asyncio.Event()
    release_stage_response = asyncio.Event()
    staged_records: list[Any] = []
    transition = stale_catalog.transition_attempt_claim

    async def delay_finalizing_response(*args: Any, **kwargs: Any) -> Any:
        record = await transition(*args, **kwargs)
        if kwargs.get("target_status") == AttemptClaimStatus.FINALIZING.value:
            staged_records.append(record)
            stage_committed.set()
            await release_stage_response.wait()
        return record

    monkeypatch.setattr(stale_catalog, "transition_attempt_claim", delay_finalizing_response)
    stale_run = asyncio.create_task(
        MissionAttemptExecutionService(
            stale_claims,
            MissionService(),
            stale_finalizer,
        ).run(
            _indexed_row(),
            tick=23,
            claimant="stale-worker",
            runner=runner,
            lease_seconds=0.08,
        )
    )

    await asyncio.wait_for(stage_committed.wait(), timeout=2)
    assert len(staged_records) == 1
    stale_claim = await stale_claims.get("world-1", staged_records[0].claim_key)
    assert stale_claim is not None
    assert stale_claim.status is AttemptClaimStatus.FINALIZING
    projection = stale_claims.staged_artifact_projection(stale_claim)
    while time.time() <= stale_claim.lease_expires_at:
        await asyncio.sleep(0.005)

    recovery_catalog = SqliteControlCatalog(path)
    recovery_claims = _claim_service(recovery_catalog)
    recovery_runner = _NoRunRunner()
    recovery_finalizer = _RecoveryFinalizer(
        recovery_catalog,
        projection,
        _artifact_publication(projection),
    )
    try:
        recovery = await MissionAttemptExecutionService(
            recovery_claims,
            MissionService(),
            recovery_finalizer,
        ).run(
            _indexed_row(),
            tick=24,
            claimant="recovery-worker",
            runner=recovery_runner,
            lease_seconds=1,
        )
    finally:
        release_stage_response.set()

    assert recovery is not None
    assert recovery.acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert recovery.decision.action is AttemptRecoveryAction.FINALIZE
    assert recovery.claim.status is AttemptClaimStatus.SETTLED
    with pytest.raises(AttemptClaimStaleError):
        await stale_run
    with pytest.raises(ValueError, match="taken over"):
        await stale_claims.settle(
            stale_claim,
            attempt_status=AttemptStatus.ACCEPTED,
            outcome=recovery.outcome,
        )

    assert runner.run_calls == 1
    assert stale_finalizer.prepare_calls == 1
    assert stale_finalizer.publish_calls == 0
    assert recovery_runner.run_calls == 0
    assert recovery_finalizer.prepare_calls == 0
    assert recovery_finalizer.publish_calls == 1
    await stale_catalog.close()
    await recovery_catalog.close()


async def test_only_durable_indexed_row_can_upgrade_a_staged_outcome(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "durable-authority.db")
    service = _claim_service(catalog)
    staged, projection = await _stage_indexed_claim(service)
    staged_outcome_receipt = json.loads(staged.redaction_evidence_json)["outcome"]

    # A custom finalizer can fabricate this process-local DTO, but the claim
    # API no longer accepts it as authority.
    forged_receipt = _artifact_publication(projection)
    prepare_with_receipt: Any = service.prepare_artifact_finalization_outcome
    with pytest.raises(TypeError):
        await prepare_with_receipt(staged, forged_receipt)

    with pytest.raises(ValueError, match="authority is missing"):
        await service.prepare_artifact_finalization_outcome(staged)
    for durable_status in ("PENDING", "UPLOADED"):
        publication = await _advance_artifact_publication(
            catalog,
            projection,
            status=durable_status,
        )
        assert publication is not None and publication.status == durable_status
        with pytest.raises(RuntimeError, match="has not reached INDEXED or EXPIRED"):
            await service.prepare_artifact_finalization_outcome(staged)
        persisted = await service.get(staged.world_id, staged.claim_key)
        assert persisted is not None
        assert persisted.status is AttemptClaimStatus.FINALIZING

    forged = json.loads(staged.outcome_json)
    forged.update(
        finalization_phase=FinalizationPhase.INDEXED.value,
        finalization_manifest_ref="s3://forged/manifest.json",
        finalization_bundle_id=projection.publication_key,
        finalization_request_digest=projection.request_digest,
        finalization_producer_digest=projection.producer_digest,
        finalization_redaction_policy_id=projection.redaction_policy_id,
        finalization_index_snapshot_id=17,
        finalization_error="",
    )
    forged_redacted = service.prepare_durable_outcome(staged, forged)
    for arbitrary in (forged, forged_redacted):
        with pytest.raises(ValueError, match="prepared finalization settlement"):
            await service.settle(
                staged,
                attempt_status=AttemptStatus.ACCEPTED,
                outcome=arbitrary,
            )
        persisted = await service.get(staged.world_id, staged.claim_key)
        assert persisted is not None
        assert persisted.status is AttemptClaimStatus.FINALIZING

    indexed = await _advance_artifact_publication(
        catalog,
        projection,
        status="INDEXED",
    )
    assert indexed is not None and indexed.status == "INDEXED"
    finalized = await service.prepare_artifact_finalization_outcome(staged)
    expected_scanned_bytes = len(service._json(finalized.value).encode())
    assert finalized.receipt.scanned_bytes == expected_scanned_bytes
    assert finalized.receipt.scanned_bytes > staged_outcome_receipt["scanned_bytes"]
    with pytest.raises(ValueError, match="claim-bound settled projection"):
        MissionService().apply_attempt(_indexed_row(), _indexed_request(), finalized.value)
    with pytest.raises(ValueError, match="prepared finalization settlement"):
        await service.settle(
            staged,
            attempt_status=AttemptStatus.ACCEPTED,
            outcome=finalized,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="seal"):
        await service.settle_finalized(
            staged,
            replace(finalized, attempt_status=AttemptStatus.REJECTED),
        )
    settled = await service.settle_finalized(
        staged,
        finalized,
    )
    assert settled.status is AttemptClaimStatus.SETTLED
    assert service.settled_outcome(settled) == finalized.value
    settled_outcome_receipt = json.loads(settled.redaction_evidence_json)["outcome"]
    assert settled_outcome_receipt == finalized.receipt.model_dump(mode="json")
    canonical, canonical_outcome, updated = await MissionAttemptExecutionService(
        service,
        MissionService(),
    )._project_settled(
        _indexed_row(),
        _indexed_request(),
        settled,
    )
    assert canonical == settled
    assert canonical_outcome == finalized.value
    assert updated["attempt__status"] == "accepted"
    assert updated["mission__status"] == "succeeded"
    await catalog.close()


async def test_indexed_terminal_receipt_preserves_findings_and_covers_final_bytes(
    tmp_path,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / "indexed-redaction-receipt.db")
    service = _claim_service(catalog)
    staged, projection = await _stage_indexed_claim(
        service,
        outcome_extra={"message": f"provider diagnostic {_SYNTHETIC_SECRET}"},
    )
    staged_receipt = json.loads(staged.redaction_evidence_json)["outcome"]
    assert staged_receipt["status"] == "redacted"
    assert "openai-api-key" in staged_receipt["rule_ids"]

    await _advance_artifact_publication(catalog, projection, status="INDEXED")
    finalized = await service.prepare_artifact_finalization_outcome(staged)
    assert finalized.receipt.status == "redacted"
    assert finalized.receipt.redaction_count == staged_receipt["redaction_count"]
    assert list(finalized.receipt.rule_ids) == staged_receipt["rule_ids"]
    assert finalized.receipt.scanned_bytes == len(service._json(finalized.value).encode())

    settled = await service.settle_finalized(staged, finalized)
    settled_receipt = json.loads(settled.redaction_evidence_json)["outcome"]
    assert settled_receipt == finalized.receipt.model_dump(mode="json")
    await catalog.close()


async def test_cold_finalization_rejects_a_lossy_local_snapshot_id(tmp_path) -> None:
    path = tmp_path / "lossy-local-snapshot.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(
        first,
        claimant="crashed-worker",
        lease_seconds=0.05,
    )
    publication = await _advance_artifact_publication(
        first_catalog,
        projection,
        status="INDEXED",
    )
    assert publication is not None and publication.index_snapshot_id == 17
    await first_catalog.close()

    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE artifact_publications SET index_snapshot_id=? WHERE publication_key=?",
        (17.5, projection.publication_key),
    )
    stored = connection.execute(
        "SELECT typeof(index_snapshot_id), index_snapshot_id "
        "FROM artifact_publications WHERE publication_key=?",
        (projection.publication_key,),
    ).fetchone()
    connection.commit()
    connection.close()
    assert stored == ("real", 17.5)

    await asyncio.sleep(0.06)
    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    acquisition = await cold.acquire(
        _indexed_request(),
        _capabilities(),
        claimant="cold-worker",
        lease_seconds=1,
    )
    assert acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert acquisition.claim.status is AttemptClaimStatus.FINALIZING
    with pytest.raises(RuntimeError, match="lossy snapshot ID"):
        await cold.prepare_artifact_finalization_outcome(acquisition.claim)
    persisted = await cold.get(acquisition.claim.world_id, acquisition.claim.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.FINALIZING
    await cold_catalog.close()


async def test_replaced_settled_claim_dto_is_not_projection_authority(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "forged-settled-projection.db")
    claims = _claim_service(catalog)
    staged, projection = await _stage_indexed_claim(claims)
    forged_outcome = json.loads(staged.outcome_json)
    forged_outcome.update(
        finalization_phase=FinalizationPhase.INDEXED.value,
        finalization_manifest_ref="s3://forged/manifest.json",
        finalization_bundle_id=projection.publication_key,
        finalization_request_digest=projection.request_digest,
        finalization_producer_digest=projection.producer_digest,
        finalization_redaction_policy_id=projection.redaction_policy_id,
        finalization_index_snapshot_id=17,
        finalization_error="",
    )
    forged_json = json.dumps(forged_outcome, sort_keys=True, separators=(",", ":"))
    forged_claim = replace(
        staged,
        status=AttemptClaimStatus.SETTLED,
        settlement_status=AttemptStatus.ACCEPTED.value,
        outcome_json=forged_json,
        outcome_digest=hashlib.sha256(forged_json.encode()).hexdigest(),
        settled_at="2026-07-18T00:00:00+00:00",
    )

    missions = MissionService()
    assert not hasattr(missions, "apply_settled_attempt")
    execution = MissionAttemptExecutionService(claims, missions)
    with pytest.raises(ValueError, match="not durably settled"):
        await execution._project_settled(
            _indexed_row(),
            _indexed_request(),
            forged_claim,
        )

    persisted = await claims.get(staged.world_id, staged.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.FINALIZING
    assert (
        await catalog.get_artifact_publication(staged.world_id, projection.publication_key) is None
    )
    await catalog.close()


@pytest.mark.parametrize(
    ("durable_status", "error_type", "message"),
    [
        ("MISSING", ValueError, "authority is missing"),
        ("PENDING", RuntimeError, "has not reached INDEXED or EXPIRED"),
        ("UPLOADED", RuntimeError, "has not reached INDEXED or EXPIRED"),
    ],
)
async def test_forgeable_finalizer_dto_cannot_settle_without_terminal_durable_row(
    tmp_path,
    durable_status: str,
    error_type: type[Exception],
    message: str,
) -> None:
    path = tmp_path / f"forged-finalizer-{durable_status.lower()}.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(
        first,
        claimant="crashed-worker",
        lease_seconds=0.05,
    )
    await first_catalog.close()
    await asyncio.sleep(0.06)

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    runner = _NoRunRunner()
    finalizer = _RecoveryFinalizer(
        cold_catalog,
        projection,
        _artifact_publication(projection),
        durable_status=durable_status,
    )
    with pytest.raises(error_type, match=message):
        await MissionAttemptExecutionService(
            cold,
            MissionService(),
            finalizer,
        ).run(
            _indexed_row(),
            tick=103,
            claimant="recovery-worker",
            runner=runner,
            lease_seconds=1,
        )

    persisted = await cold.get(staged.world_id, staged.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.FINALIZING
    assert persisted.outcome_json == staged.outcome_json
    assert runner.run_calls == 0
    assert finalizer.publish_calls == 1
    await cold_catalog.close()


async def test_attempt_artifact_publication_requires_exact_signed_64_bit_snapshot() -> None:
    projection = _artifact_projection(_indexed_request(), RedactionService().policy_id)
    publication = _artifact_publication(projection)
    assert replace(publication, index_snapshot_id=MAX_ICEBERG_SNAPSHOT_ID).index_snapshot_id == (
        MAX_ICEBERG_SNAPSHOT_ID
    )
    for invalid in (MAX_ICEBERG_SNAPSHOT_ID + 1, 1.5, True):
        with pytest.raises((TypeError, ValueError), match="snapshot"):
            replace(publication, index_snapshot_id=invalid)


@pytest.mark.parametrize(
    "forged_snapshot",
    [MAX_ICEBERG_SNAPSHOT_ID + 1, 1.5, True],
)
async def test_custom_finalizer_snapshot_dto_cannot_override_durable_authority(
    tmp_path,
    forged_snapshot: Any,
) -> None:
    path = tmp_path / f"custom-snapshot-{forged_snapshot!s}.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(
        first,
        claimant="crashed-worker",
        lease_seconds=0.05,
    )
    await first_catalog.close()
    await asyncio.sleep(0.06)

    forged_publication = _unchecked_artifact_publication(
        projection,
        index_snapshot_id=forged_snapshot,
    )
    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    runner = _NoRunRunner()
    execution_service = MissionAttemptExecutionService(
        cold,
        MissionService(),
        _RecoveryFinalizer(
            cold_catalog,
            projection,
            forged_publication,
            durable_snapshot_id=MAX_ICEBERG_SNAPSHOT_ID,
        ),
    )
    execution = await execution_service.run(
        _indexed_row(),
        tick=103,
        claimant="snapshot-recovery-worker",
        runner=runner,
        lease_seconds=1,
    )
    assert execution is not None
    assert execution.claim.status is AttemptClaimStatus.SETTLED
    assert execution.updated_row["finalization__index_snapshot_id"] == MAX_ICEBERG_SNAPSHOT_ID
    assert runner.run_calls == 0
    await cold_catalog.close()


async def test_cold_finalizing_recovery_indexes_rejected_attempt_without_runner(tmp_path) -> None:
    path = tmp_path / "cold-finalizing.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(
        first,
        claimant="crashed-worker",
        lease_seconds=0.05,
        status=AttemptStatus.REJECTED,
    )
    assert staged.status is AttemptClaimStatus.FINALIZING
    await first_catalog.close()
    await asyncio.sleep(0.06)

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    runner = _NoRunRunner()
    finalizer = _RecoveryFinalizer(
        cold_catalog,
        projection,
        _artifact_publication(projection),
    )
    execution = await MissionAttemptExecutionService(
        cold,
        MissionService(),
        finalizer,
    ).run(
        _indexed_row(),
        tick=99,
        claimant="recovery-worker",
        runner=runner,
        lease_seconds=1,
    )

    assert execution is not None
    assert execution.acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert execution.decision.action is AttemptRecoveryAction.FINALIZE
    assert execution.replayed is True
    assert execution.claim.status is AttemptClaimStatus.SETTLED
    assert execution.updated_row["attempt__status"] == "rejected"
    assert execution.updated_row["taskgate__status"] == "retryable"
    assert runner.run_calls == 0
    assert finalizer.prepare_calls == 0
    assert finalizer.publish_calls == 1
    await cold_catalog.close()


@pytest.mark.parametrize(
    ("provider_status", "settlement_status"),
    [
        (AttemptStatus.ACCEPTED, AttemptStatus.INCOMPLETE),
        (AttemptStatus.REJECTED, AttemptStatus.REJECTED),
    ],
)
async def test_cold_expired_finalization_settles_without_rerunning_same_attempt(
    tmp_path,
    provider_status: AttemptStatus,
    settlement_status: AttemptStatus,
) -> None:
    path = tmp_path / f"expired-{provider_status.value}.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(
        first,
        claimant="crashed-worker",
        lease_seconds=0.05,
        status=provider_status,
    )
    assert staged.status is AttemptClaimStatus.FINALIZING
    await first_catalog.close()
    await asyncio.sleep(0.06)

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    runner = _NoRunRunner()
    expired = _ExpiredFinalizer(cold_catalog, projection)
    execution = await MissionAttemptExecutionService(
        cold,
        MissionService(),
        expired,
    ).run(
        _indexed_row(),
        tick=101,
        claimant="expiry-recovery-worker",
        runner=runner,
        lease_seconds=1,
    )

    assert execution is not None
    assert execution.acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert execution.decision.action is AttemptRecoveryAction.FINALIZE
    assert execution.claim.status is AttemptClaimStatus.SETTLED
    assert execution.claim.settlement_status == settlement_status.value
    assert execution.claim.legacy_unbound is False
    assert execution.outcome["finalization_phase"] == FinalizationPhase.CHECKPOINTED.value
    assert execution.outcome["finalization_error"] == "artifact_publication_expired"
    assert execution.updated_row["attempt__status"] == settlement_status.value
    assert execution.updated_row["taskgate__status"] == "retryable"
    assert execution.updated_row["finalization__legacy_unbound"] is False
    assert runner.run_calls == 0
    assert expired.prepare_calls == 0
    assert expired.publish_calls == 1
    evidence = json.loads(execution.claim.redaction_evidence_json)
    assert evidence["outcome"]["status"] == "clean"

    replay_runner = _NoRunRunner()
    replay = await MissionAttemptExecutionService(cold, MissionService()).run(
        _indexed_row(),
        tick=102,
        claimant="expiry-replay-worker",
        runner=replay_runner,
    )
    assert replay is not None
    assert replay.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert replay.decision.action is AttemptRecoveryAction.SETTLED
    assert replay.claim == execution.claim
    assert replay.outcome == execution.outcome
    assert replay.updated_row == execution.updated_row
    assert replay_runner.run_calls == 0

    if provider_status is AttemptStatus.ACCEPTED:
        retry_request = MissionService().prepare_attempt(execution.updated_row, tick=103)
        assert retry_request is not None
        retry_runner = _IndexedRunner(retry_request, status=AttemptStatus.FAILED)
        retried = await MissionAttemptExecutionService(cold, MissionService()).run(
            execution.updated_row,
            tick=103,
            claimant="next-attempt-worker",
            runner=retry_runner,
        )
        assert retried is not None
        assert retried.request.attempt_index == 2
        assert retried.claim.claim_key != execution.claim.claim_key
        assert retried.updated_row["attempt__status"] == AttemptStatus.FAILED.value
        assert retried.updated_row["taskgate__status"] == "retryable"
        assert retry_runner.run_calls == 1

    await cold_catalog.close()


async def test_forged_expiration_dto_cannot_override_durable_expired_row(tmp_path) -> None:
    path = tmp_path / "expired-wrong-bundle.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(
        first,
        claimant="crashed-worker",
        lease_seconds=0.05,
    )
    await first_catalog.close()
    await asyncio.sleep(0.06)

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    runner = _NoRunRunner()
    expired = _ExpiredFinalizer(cold_catalog, projection, bundle_id="e" * 64)
    execution = await MissionAttemptExecutionService(
        cold,
        MissionService(),
        expired,
    ).run(
        _indexed_row(),
        tick=102,
        claimant="expiry-recovery-worker",
        runner=runner,
        lease_seconds=1,
    )

    assert execution is not None
    assert execution.claim.status is AttemptClaimStatus.SETTLED
    assert execution.claim.settlement_status == AttemptStatus.INCOMPLETE.value
    assert execution.outcome["finalization_error"] == "artifact_publication_expired"
    assert execution.outcome.get("finalization_bundle_id", "") == ""
    assert runner.run_calls == 0
    assert expired.publish_calls == 1
    await cold_catalog.close()


async def test_expired_settlement_requires_the_exact_durable_artifact_row(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "durable-expiration-authority.db")
    claims = _claim_service(catalog)
    staged, projection = await _stage_indexed_claim(claims)

    with pytest.raises(ValueError, match="authority is missing"):
        await claims.prepare_artifact_finalization_outcome(staged)
    persisted = await claims.get(staged.world_id, staged.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.FINALIZING

    publication = await _advance_artifact_publication(
        catalog,
        projection,
        status="EXPIRED",
    )
    assert publication is not None and publication.status == "EXPIRED"
    prepared = await claims.prepare_artifact_finalization_outcome(staged)
    assert prepared.kind == "expired"
    assert prepared.value["finalization_error"] == "artifact_publication_expired"
    settled = await claims.settle_finalized(staged, prepared)
    assert settled.status is AttemptClaimStatus.SETTLED
    await catalog.close()


async def test_actual_expired_artifact_publication_cold_settles_and_replays(
    tmp_path,
) -> None:
    artifact_config = ArtifactStoreConfig.local(tmp_path / "artifacts")
    storage = StorageConfig(uri=tmp_path / "world", namespace="world")
    row = _indexed_row()
    first_container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        world = await first_container.world_service.create_world(
            WorldConfig(name="mission-expiry-world"),
            storage,
        )
        row.update(
            world_id=str(world.world_id),
            run_id=str(world.run_id),
            entity_id=7,
        )
        request = MissionService().prepare_attempt(row, tick=107)
        assert request is not None
        catalog = first_container.storage_service.get_control_catalog(storage)
        first = _claim_service(catalog, first_container.redaction_service)
        acquired = await first.acquire(
            request,
            _capabilities(),
            claimant="crashed-worker",
            lease_seconds=0.05,
        )
        decision = await first.decide_recovery(
            acquired.claim,
            lease_seconds=0.05,
        )
        consumed = await first.consume_execution(decision.authorization)
        acknowledged = await first.acknowledge_provider(
            consumed,
            provider_session_id="session-indexed",
            provider_request_id="request-indexed",
        )
        durable = first.prepare_durable_outcome(
            acknowledged,
            _outcome(
                request=request,
                status=AttemptStatus.ACCEPTED,
                agent_session_id="session-indexed",
            ),
        )
        first_workflow = first_container.mission_attempt_workflow(storage)
        finalizer = first_workflow.artifact_finalizer
        projection = finalizer.prepare(
            request,
            durable.value,
            redaction_policy_id=acknowledged.redaction_policy_id,
        )
        staged = await first.stage_finalization(
            acknowledged,
            outcome=durable,
            projection=projection,
        )
        bundle_request = ArtifactBundleRequest.model_validate_json(projection.request_json)
        assert bundle_request.tick == 107
        artifact_outcome, artifact_claim = await catalog.acquire_artifact_publication(
            world_id=bundle_request.world_id,
            run_id=bundle_request.run_id,
            attempt_id=bundle_request.attempt_id,
            idempotency_key=bundle_request.idempotency_key,
            request_digest=projection.producer_digest,
            request_json=projection.request_json,
            claimant="expiry-seed",
            retry_window_ms=60_000,
            lease_ms=1_000,
        )
        assert artifact_outcome == "acquired"
        assert artifact_claim.status == "PENDING"
        await catalog.expire_artifact_publication(
            bundle_request.world_id,
            projection.publication_key,
            "expiry-seed",
            "test retry window elapsed",
        )
        expired_artifact = await catalog.get_artifact_publication(
            bundle_request.world_id,
            projection.publication_key,
        )
        assert expired_artifact is not None
        assert expired_artifact.status == "EXPIRED"
        assert expired_artifact.records_json == "[]"
        assert expired_artifact.manifest_uri == ""
        assert staged.status is AttemptClaimStatus.FINALIZING
    finally:
        await first_container.shutdown()

    await asyncio.sleep(0.06)
    cold_container = ServiceContainer(artifact_store_config=artifact_config)
    try:
        assert not cold_container.world_service.has_world(row["world_id"])
        cold_catalog = cold_container.storage_service.get_control_catalog(storage)
        cold_workflow = cold_container.mission_attempt_workflow(storage)
        runner = _NoRunRunner()
        execution = await cold_workflow.execution_service.run(
            row,
            tick=108,
            claimant="expiry-recovery-worker",
            runner=runner,
            lease_seconds=1,
        )

        assert execution is not None
        assert execution.acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
        assert execution.decision.action is AttemptRecoveryAction.FINALIZE
        assert execution.claim.status is AttemptClaimStatus.SETTLED
        assert execution.claim.settlement_status == AttemptStatus.INCOMPLETE.value
        assert execution.request.observation_tick == 107
        assert execution.outcome["finalization_error"] == "artifact_publication_expired"
        assert execution.outcome["finalization_phase"] == FinalizationPhase.CHECKPOINTED.value
        assert execution.updated_row["taskgate__status"] == "retryable"
        assert execution.updated_row["finalization__legacy_unbound"] is False
        assert runner.run_calls == 0

        replay_runner = _NoRunRunner()
        replay = await cold_workflow.execution_service.run(
            row,
            tick=109,
            claimant="expiry-replay-worker",
            runner=replay_runner,
        )
        assert replay is not None
        assert replay.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
        assert replay.decision.action is AttemptRecoveryAction.SETTLED
        assert replay.claim == execution.claim
        assert replay.outcome == execution.outcome
        assert replay.updated_row == execution.updated_row
        assert replay_runner.run_calls == 0

        persisted_artifact = await cold_catalog.get_artifact_publication(
            bundle_request.world_id,
            projection.publication_key,
        )
        assert persisted_artifact == expired_artifact
    finally:
        await cold_container.shutdown()


async def test_cold_replay_after_indexed_settlement_applies_without_runner_or_finalizer(
    tmp_path,
) -> None:
    path = tmp_path / "indexed-settled-before-world-commit.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    staged, projection = await _stage_indexed_claim(first)
    publication = await _advance_artifact_publication(
        first_catalog,
        projection,
        status="INDEXED",
    )
    assert publication is not None and publication.status == "INDEXED"
    finalized = await first.prepare_artifact_finalization_outcome(staged)
    settled = await first.settle_finalized(
        staged,
        finalized,
    )
    assert settled.status is AttemptClaimStatus.SETTLED
    (
        canonical,
        canonical_outcome,
        projected_but_not_committed,
    ) = await MissionAttemptExecutionService(first, MissionService())._project_settled(
        _indexed_row(),
        _indexed_request(),
        settled,
    )
    assert canonical == settled
    assert canonical_outcome == finalized.value
    assert projected_but_not_committed["mission__status"] == "succeeded"
    await first_catalog.close()

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    runner = _NoRunRunner()
    replay = await MissionAttemptExecutionService(cold, MissionService()).run(
        _indexed_row(),
        tick=100,
        claimant="cold-replay-worker",
        runner=runner,
    )

    assert replay is not None
    assert replay.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert replay.decision.action is AttemptRecoveryAction.SETTLED
    assert replay.replayed is True
    assert replay.updated_row == projected_but_not_committed
    assert replay.updated_row["finalization__legacy_unbound"] is False
    assert replay.claim.legacy_unbound is False
    assert runner.run_calls == 0
    await cold_catalog.close()


@pytest.mark.parametrize(
    "phase",
    [FinalizationPhase.PUBLISHED, FinalizationPhase.INDEXED],
)
@pytest.mark.parametrize("authority_extras", [False, True])
async def test_v7_unbound_terminal_claim_cold_replays_with_explicit_compatibility_marker(
    tmp_path,
    phase: FinalizationPhase,
    authority_extras: bool,
) -> None:
    path = tmp_path / f"v7-{phase.value}-{authority_extras}.db"
    request, stored_outcome = await _seed_unbound_settled_claim(
        path,
        phase=phase,
        as_v7=True,
        authority_extras=authority_extras,
    )
    changed_policy = RedactionService(RedactionPolicyConfig(scan_chunk_bytes=8192))
    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog, changed_policy)

    projected = await cold.get(
        "world-1",
        cold.claim_key(
            world_id="world-1",
            mission_id=request.mission_id,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
        ),
    )
    assert projected is not None
    assert projected.status is AttemptClaimStatus.SETTLED
    assert projected.contract_version == 7
    assert projected.settlement_status == AttemptStatus.ACCEPTED.value
    assert projected.legacy_unbound_eligible is True
    assert projected.legacy_unbound is True
    assert projected.artifact_request_json == ""
    assert cold.recover_request(projected).observation_tick == 0
    assert cold.settled_outcome(projected) == stored_outcome

    runner = _NoRunRunner()
    replay = await MissionAttemptExecutionService(cold, MissionService()).run(
        _indexed_row(),
        tick=100,
        claimant="cold-v8-worker",
        runner=runner,
    )

    assert replay is not None
    assert replay.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert replay.decision.action is AttemptRecoveryAction.SETTLED
    assert replay.replayed is True
    assert replay.outcome == stored_outcome
    if authority_extras:
        assert replay.outcome["artifact_publication_key"] == "b" * 64
        assert replay.outcome["finalization_bundle_id"] == "b" * 64
        assert replay.outcome["finalization_index_snapshot_id"] == 17
    else:
        assert "artifact_publication_key" not in replay.outcome
        assert "finalization_bundle_id" not in replay.outcome
    assert replay.updated_row["mission__status"] == "succeeded"
    assert replay.updated_row["attempt__status"] == AttemptStatus.ACCEPTED.value
    assert replay.updated_row["finalization__phase"] == phase.value
    assert replay.updated_row["finalization__legacy_unbound"] is True
    assert replay.updated_row["finalization__bundle_id"] == ""
    assert replay.updated_row["finalization__request_digest"] == ""
    assert replay.updated_row["finalization__producer_digest"] == ""
    assert replay.updated_row["finalization__redaction_policy_id"] == ""
    assert replay.updated_row["finalization__index_snapshot_id"] == 0
    assert runner.run_calls == 0
    with pytest.raises(ValueError, match="lacks migration eligibility"):
        MissionService()._apply_settled_attempt(
            _indexed_row(),
            request,
            stored_outcome,
            replace(replay.claim, legacy_unbound_eligible=False),
        )
    assert (
        await cold.settle(
            replay.claim,
            attempt_status=AttemptStatus.ACCEPTED,
            outcome=stored_outcome,
        )
        == replay.claim
    )
    await cold_catalog.close()


@pytest.mark.parametrize(
    "phase",
    [
        FinalizationPhase.CAPTURED,
        FinalizationPhase.CHECKPOINTED,
        FinalizationPhase.PUBLISHED,
    ],
)
async def test_v7_nonindexed_terminal_claim_is_not_legacy_unbound_after_migration(
    tmp_path,
    phase: FinalizationPhase,
) -> None:
    path = tmp_path / f"v7-nonindexed-{phase.value}.db"
    request, stored_outcome = await _seed_unbound_settled_claim(
        path,
        phase=phase,
        required_phase=phase,
        as_v7=True,
    )
    claim_key = MissionAttemptClaimService.claim_key(
        world_id="world-1",
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )

    catalog = SqliteControlCatalog(path)
    raw = await catalog.get_attempt_claim("world-1", claim_key)
    assert raw is not None
    assert raw.legacy_unbound_eligible is False
    projected = await _claim_service(catalog).get("world-1", claim_key)
    assert projected is not None
    assert projected.legacy_unbound_eligible is False
    assert projected.legacy_unbound is False
    assert _claim_service(catalog).settled_outcome(projected) == stored_outcome
    await catalog.close()

    # A catalog already upgraded by the original phase-agnostic migration may
    # retain the overbroad durable bit. The read boundary must still normalize
    # it instead of routing this ordinary legacy claim into INDEXED authority.
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE mission_attempt_claims SET legacy_unbound_eligible=1 WHERE claim_key=?",
        (claim_key,),
    )
    connection.commit()
    connection.close()

    overmarked_catalog = SqliteControlCatalog(path)
    overmarked_raw = await overmarked_catalog.get_attempt_claim("world-1", claim_key)
    assert overmarked_raw is not None
    assert overmarked_raw.legacy_unbound_eligible is True
    normalized = await _claim_service(overmarked_catalog).get("world-1", claim_key)
    assert normalized is not None
    assert normalized.legacy_unbound_eligible is False
    assert normalized.legacy_unbound is False
    assert _claim_service(overmarked_catalog).settled_outcome(normalized) == stored_outcome
    await overmarked_catalog.close()


@pytest.mark.parametrize(
    ("contract_version", "case"),
    [
        pytest.param(7, "explicit-integer", id="explicit-7"),
        pytest.param(7.0, "explicit-float", id="explicit-7.0"),
        pytest.param("legacy", "malformed", id="malformed-version"),
    ],
)
async def test_v7_migration_rejects_noncanonical_contract_version_markers(
    tmp_path,
    contract_version: object,
    case: str,
) -> None:
    path = tmp_path / f"v7-noncanonical-version-{case}.db"
    request, _ = await _seed_unbound_settled_claim(
        path,
        phase=FinalizationPhase.PUBLISHED,
        as_v7=True,
    )
    claim_key = MissionAttemptClaimService.claim_key(
        world_id="world-1",
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    connection = sqlite3.connect(path)
    row = connection.execute(
        "SELECT request_json FROM mission_attempt_claims WHERE claim_key=?",
        (claim_key,),
    ).fetchone()
    assert row is not None
    request_json = json.loads(row[0])
    request_json["claim_contract_version"] = contract_version
    connection.execute(
        "UPDATE mission_attempt_claims SET request_json=? WHERE claim_key=?",
        (json.dumps(request_json), claim_key),
    )
    connection.commit()
    connection.close()

    catalog = SqliteControlCatalog(path)
    migrated = await catalog.get_attempt_claim("world-1", claim_key)
    assert migrated is not None
    assert migrated.legacy_unbound_eligible is False
    await catalog.close()


@pytest.mark.parametrize(
    "phase",
    [FinalizationPhase.PUBLISHED, FinalizationPhase.INDEXED],
)
async def test_v8_raw_catalog_row_cannot_claim_v7_terminal_compatibility(
    tmp_path,
    phase: FinalizationPhase,
) -> None:
    path = tmp_path / f"v8-unbound-{phase.value}.db"
    request, _ = await _seed_unbound_settled_claim(
        path,
        phase=phase,
        as_v7=False,
    )
    _strip_claim_contract_marker(path)
    catalog = SqliteControlCatalog(path)
    service = _claim_service(catalog)
    claim_key = service.claim_key(
        world_id="world-1",
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    raw = await catalog.get_attempt_claim("world-1", claim_key)
    assert raw is not None
    assert raw.legacy_unbound_eligible is False

    with pytest.raises(
        ValueError,
        match="indexed attempt outcome|authoritative outcome",
    ):
        await service.get("world-1", claim_key)

    await catalog.close()


@pytest.mark.parametrize(
    "phase",
    [FinalizationPhase.PUBLISHED, FinalizationPhase.INDEXED],
)
async def test_v8_authority_named_extras_without_staged_claim_fail_closed(
    tmp_path,
    phase: FinalizationPhase,
) -> None:
    path = tmp_path / f"v8-authority-extras-{phase.value}.db"
    request, stored_outcome = await _seed_unbound_settled_claim(
        path,
        phase=phase,
        as_v7=False,
        authority_extras=True,
    )
    assert stored_outcome["finalization_bundle_id"] == "b" * 64
    catalog = SqliteControlCatalog(path)
    service = _claim_service(catalog)
    claim_key = service.claim_key(
        world_id="world-1",
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )
    raw = await catalog.get_attempt_claim("world-1", claim_key)
    assert raw is not None
    assert raw.legacy_unbound_eligible is False
    assert json.loads(raw.request_json)["claim_contract_version"] == 8

    with pytest.raises(
        ValueError,
        match="staged artifact request|authoritative outcome",
    ):
        await service.get("world-1", claim_key)

    await catalog.close()


async def test_claim_is_durable_before_submission_is_armed(tmp_path) -> None:
    path = tmp_path / "catalog.db"
    first_catalog = SqliteControlCatalog(path)
    service = _claim_service(first_catalog)
    acquired = await service.acquire(
        _request(),
        _capabilities(),
        claimant="worker-a",
    )

    assert acquired.outcome is AttemptClaimAcquireOutcome.ACQUIRED
    assert acquired.claim.status is AttemptClaimStatus.CLAIMED
    assert acquired.claim.fence_epoch == 1
    assert acquired.claim.contract_version == 8
    assert acquired.claim.legacy_unbound_eligible is False
    assert json.loads(acquired.claim.request_json)["claim_contract_version"] == 8
    assert acquired.claim.possibly_submitted_at is None

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    discovered = await cold.get(acquired.claim.world_id, acquired.claim.claim_key)
    assert discovered == acquired.claim

    decision = await service.decide_recovery(acquired.claim)
    assert decision.action is AttemptRecoveryAction.EXECUTE
    assert decision.claim.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    assert decision.claim.possibly_submitted_at
    assert decision.authorization.fence_epoch == 1

    await first_catalog.close()
    await cold_catalog.close()


async def test_same_attempt_reacquires_across_observation_ticks(tmp_path) -> None:
    first_request = MissionService().prepare_attempt(_row(), tick=11)
    later_request = MissionService().prepare_attempt(_row(), tick=12)
    assert first_request is not None
    assert later_request is not None
    assert replace(later_request, observation_tick=11) == first_request
    assert later_request.request_fingerprint == first_request.request_fingerprint
    assert "tick" not in first_request.correlation

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(
        first_request,
        _capabilities(),
        claimant="worker-a",
        lease_seconds=0,
    )
    recovered = await service.acquire(
        later_request,
        _capabilities(),
        claimant="worker-b",
    )

    assert acquired.outcome is AttemptClaimAcquireOutcome.ACQUIRED
    assert recovered.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert recovered.claim.claim_key == acquired.claim.claim_key
    assert recovered.claim.fence_epoch == acquired.claim.fence_epoch + 1
    assert service.recover_request(recovered.claim).observation_tick == 11
    await catalog.close()


async def test_concurrent_workers_get_one_fenced_executor(tmp_path) -> None:
    path = tmp_path / "catalog.db"
    left_catalog = SqliteControlCatalog(path)
    right_catalog = SqliteControlCatalog(path)
    left = _claim_service(left_catalog)
    right = _claim_service(right_catalog)
    request = _request()
    # First-use schema initialization is a separate catalog lifecycle concern;
    # prime both connections before racing the claim CAS itself.
    assert await left.get("world-1", "missing") is None
    assert await right.get("world-1", "missing") is None
    start = asyncio.Event()

    async def acquire(service: MissionAttemptClaimService, claimant: str):
        await start.wait()
        return await service.acquire(request, _capabilities(), claimant=claimant)

    tasks = [
        asyncio.create_task(acquire(left, "worker-left")),
        asyncio.create_task(acquire(right, "worker-right")),
    ]
    start.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    winners = [result for result in results if not isinstance(result, BaseException)]
    losers = [result for result in results if isinstance(result, BaseException)]
    assert len(winners) == 1
    assert winners[0].outcome is AttemptClaimAcquireOutcome.ACQUIRED
    assert len(losers) == 1
    assert isinstance(losers[0], AttemptClaimPendingError)
    await left_catalog.close()
    await right_catalog.close()


async def test_concurrent_decisions_issue_exactly_one_execute_authorization(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(
        _request(),
        _capabilities(),
        claimant="one-worker-incarnation",
    )

    decisions = await asyncio.gather(
        service.decide_recovery(acquired.claim),
        service.decide_recovery(acquired.claim),
    )

    assert [decision.action for decision in decisions].count(AttemptRecoveryAction.EXECUTE) == 1
    assert [decision.action for decision in decisions].count(AttemptRecoveryAction.RECONCILE) == 1
    assert all(
        decision.claim.status is AttemptClaimStatus.POSSIBLY_SUBMITTED for decision in decisions
    )
    await catalog.close()


async def test_same_attempt_cannot_fork_a_claim_by_changing_provider_input(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    request = _request()
    acquired = await service.acquire(
        request,
        _capabilities(),
        claimant="worker-a",
        lease_seconds=0,
    )

    with pytest.raises(AttemptClaimConflictError, match="different immutable input"):
        await service.acquire(
            request,
            _capabilities(request_fingerprint="changed-provider-request"),
            claimant="worker-b",
        )

    persisted = await service.get(acquired.claim.world_id, acquired.claim.claim_key)
    assert persisted == acquired.claim
    await catalog.close()


async def test_invalid_request_fingerprint_is_rejected_before_persistence(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    request = _request()
    poisoned = replace(request, request_fingerprint="corrupt")
    claim_key = service.claim_key(
        world_id=request.correlation["world_id"],
        mission_id=request.mission_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
    )

    with pytest.raises(ValueError, match="request fingerprint is invalid"):
        await service.acquire(poisoned, _capabilities(), claimant="worker")

    assert await service.get(request.correlation["world_id"], claim_key) is None
    await catalog.close()


async def test_restart_discovers_due_claim_and_fences_the_dead_worker(tmp_path) -> None:
    path = tmp_path / "catalog.db"
    original_catalog = SqliteControlCatalog(path)
    original = _claim_service(original_catalog)
    acquired = await original.acquire(
        _request(),
        _capabilities(),
        claimant="dead-worker",
        lease_seconds=0.1,
    )
    uncertain = await original.decide_recovery(acquired.claim, lease_seconds=0.05)
    await asyncio.sleep(0.06)

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    due = await cold.list_due("world-1", now=time.time() + 1)
    assert [claim.claim_key for claim in due] == [uncertain.claim.claim_key]
    assert due[0].status is AttemptClaimStatus.POSSIBLY_SUBMITTED

    recovered = await cold.acquire(
        _request(),
        _capabilities(),
        claimant="recovery-worker",
    )
    assert recovered.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert recovered.claim.fence_epoch == 2
    assert recovered.claim.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    decision = await cold.decide_recovery(recovered.claim)
    assert decision.action is AttemptRecoveryAction.RECONCILE

    with pytest.raises(ValueError, match="taken over"):
        await original.settle(
            uncertain.claim,
            attempt_status=AttemptStatus.FAILED,
            outcome=_outcome(status=AttemptStatus.FAILED, worker="old"),
        )
    with pytest.raises(AttemptClaimStaleError):
        await original_catalog.transition_attempt_claim(
            uncertain.claim.world_id,
            uncertain.claim.claim_key,
            uncertain.claim.claimant,
            uncertain.claim.fence_epoch,
            expected_status="possibly_submitted",
            target_status="settled",
            redaction_evidence_json="{}",
            settlement_status="failed",
            outcome_digest=hashlib.sha256(b"{}").hexdigest(),
            outcome_json="{}",
        )

    await original_catalog.close()
    await cold_catalog.close()


@pytest.mark.parametrize(
    ("crash_point", "expected_status", "expected_action"),
    [
        (
            "after_claim_before_call",
            AttemptClaimStatus.CLAIMED,
            AttemptRecoveryAction.EXECUTE,
        ),
        (
            "after_send_before_ack",
            AttemptClaimStatus.POSSIBLY_SUBMITTED,
            AttemptRecoveryAction.RECONCILE,
        ),
        (
            "after_ack_before_finalization",
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
            AttemptRecoveryAction.RECONCILE,
        ),
    ],
)
async def test_pre_finalization_crash_matrix_recovers_with_one_new_fence(
    tmp_path,
    crash_point: str,
    expected_status: AttemptClaimStatus,
    expected_action: AttemptRecoveryAction,
) -> None:
    path = tmp_path / f"{crash_point}.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    acquired = await first.acquire(
        _request(),
        _capabilities(),
        claimant="crashed-worker",
        lease_seconds=0 if crash_point == "after_claim_before_call" else 0.2,
    )
    crashed = acquired.claim
    if crash_point != "after_claim_before_call":
        decision = await first.decide_recovery(crashed, lease_seconds=0.1)
        crashed = decision.claim
    if crash_point == "after_ack_before_finalization":
        crashed = await first.consume_execution(decision.authorization)
        crashed = await first.acknowledge_provider(
            crashed,
            provider_request_id="provider-request-1",
        )
    assert crashed.status is expected_status
    await asyncio.sleep(0.11)

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    recovered = await cold.acquire(
        _request(),
        _capabilities(),
        claimant="recovery-worker",
    )
    assert recovered.outcome is AttemptClaimAcquireOutcome.RECOVERED
    assert recovered.claim.fence_epoch == acquired.claim.fence_epoch + 1
    decision = await cold.decide_recovery(recovered.claim)
    assert decision.action is expected_action

    with pytest.raises(ValueError, match="taken over"):
        await first.settle(
            crashed,
            attempt_status=AttemptStatus.FAILED,
            outcome=_outcome(status=AttemptStatus.FAILED, worker="stale"),
        )

    await first_catalog.close()
    await cold_catalog.close()


async def test_post_finalization_crash_replays_terminal_outcome_without_execution(tmp_path) -> None:
    path = tmp_path / "post-finalization.db"
    first_catalog = SqliteControlCatalog(path)
    first = _claim_service(first_catalog)
    acquired = await first.acquire(
        _request(),
        _capabilities(),
        claimant="crashed-worker",
    )
    uncertain = await first.decide_recovery(acquired.claim)
    consumed = await first.consume_execution(uncertain.authorization)
    outcome = _outcome(status=AttemptStatus.ACCEPTED)
    settled = await first.settle(
        consumed,
        attempt_status=AttemptStatus.ACCEPTED,
        outcome=outcome,
    )
    await first_catalog.close()

    cold_catalog = SqliteControlCatalog(path)
    cold = _claim_service(cold_catalog)
    duplicate = await cold.acquire(
        _request(),
        _capabilities(),
        claimant="recovery-worker",
    )
    assert duplicate.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert duplicate.claim.fence_epoch == settled.fence_epoch
    decision = await cold.decide_recovery(duplicate.claim)
    assert decision.action is AttemptRecoveryAction.SETTLED
    assert cold.settled_outcome(decision.claim) == outcome
    assert cold.recover_request(decision.claim) == _request()
    await cold_catalog.close()


async def test_uncertain_attempt_never_blindly_replays_without_capability(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(
        _request(),
        _capabilities(),
        claimant="worker-a",
    )
    first = await service.decide_recovery(acquired.claim)
    second = await service.decide_recovery(first.claim)

    assert first.action is AttemptRecoveryAction.EXECUTE
    assert second.action is AttemptRecoveryAction.RECONCILE
    assert second.claim.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    await catalog.close()


async def test_provider_capability_metadata_never_substitutes_for_a_transport(tmp_path) -> None:
    with pytest.raises(ValueError, match="idempotency key"):
        _capabilities(supports_idempotent_replay=True)
    with pytest.raises(ValueError, match="exactly one"):
        _capabilities(provider_idempotency_key="unexpected")

    replay_catalog = SqliteControlCatalog(tmp_path / "replay.db")
    replay_service = _claim_service(replay_catalog)
    replay_capabilities = _capabilities(
        supports_idempotent_replay=True,
        provider_idempotency_key="provider-operation-1",
    )
    acquired = await replay_service.acquire(
        _request(),
        replay_capabilities,
        claimant="worker-a",
    )
    uncertain = await replay_service.decide_recovery(acquired.claim)
    replay = await replay_service.decide_recovery(uncertain.claim)
    assert replay.action is AttemptRecoveryAction.RECONCILE
    assert replay.claim.supports_idempotent_replay is True
    assert replay.authorization.provider_idempotency_key == "provider-operation-1"

    resume_catalog = SqliteControlCatalog(tmp_path / "resume.db")
    resume_service = _claim_service(resume_catalog)
    acquired = await resume_service.acquire(
        _request(),
        _capabilities(supports_session_resume=True),
        claimant="worker-b",
    )
    uncertain = await resume_service.decide_recovery(acquired.claim)
    assert (await resume_service.decide_recovery(uncertain.claim)).action is (
        AttemptRecoveryAction.RECONCILE
    )
    consumed = await resume_service.consume_execution(uncertain.authorization)
    acknowledged = await resume_service.acknowledge_provider(
        consumed,
        provider_session_id="session-1",
        provider_request_id="request-1",
    )
    resume = await resume_service.decide_recovery(acknowledged)
    assert resume.action is AttemptRecoveryAction.RECONCILE
    assert resume.claim.supports_session_resume is True
    assert resume.authorization.provider_session_id == "session-1"

    await replay_catalog.close()
    await resume_catalog.close()


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: {**value, "attempt_id": "other"}, "attempt_id"),
        (lambda value: {**value, "idempotency_key": "other"}, "idempotency key"),
        (lambda value: {**value, "attempt_index": 99}, "attempt_index"),
        (lambda value: {**value, "request_fingerprint": "other"}, "sandbox request"),
        (lambda value: {**value, "accepted": True}, "accepted sandbox outcome"),
        (
            lambda value: {
                **value,
                "status": "accepted",
                "accepted": True,
                "sha": "abc123",
            },
            "validator evidence",
        ),
        (lambda value: {**value, "checkpoint_status": "bogus"}, "checkpoint status"),
        (lambda value: {**value, "results": {"tests": True}}, "results"),
        (
            lambda value: {
                **value,
                "validator_details": [
                    {**value["validator_details"][0], "command": ["different-command"]}
                ],
            },
            "requested command",
        ),
        (lambda value: {**value, "checkpoint_provider": "other"}, "checkpoint provider"),
    ],
)
async def test_terminal_outcome_is_bound_to_claim_identity_and_status(
    tmp_path,
    mutate,
    message: str,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / f"{message.replace(' ', '-')}.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(_request(), _capabilities(), claimant="worker")
    uncertain = await service.decide_recovery(acquired.claim)

    with pytest.raises(ValueError, match=message):
        await service.settle(
            uncertain.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=mutate(_outcome()),
        )

    persisted = await service.get(acquired.claim.world_id, acquired.claim.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    await catalog.close()


async def test_concurrent_conflicting_settlements_preserve_exactly_one_outcome(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(_request(), _capabilities(), claimant="worker")
    uncertain = await service.decide_recovery(acquired.claim)
    left = _outcome(winner="left")
    right = _outcome(winner="right")

    results = await asyncio.gather(
        service.settle(
            uncertain.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=left,
        ),
        service.settle(
            uncertain.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=right,
        ),
        return_exceptions=True,
    )

    winners = [value for value in results if not isinstance(value, BaseException)]
    losers = [value for value in results if isinstance(value, BaseException)]
    assert len(winners) == len(losers) == 1
    assert isinstance(losers[0], ValueError)
    assert service.settled_outcome(winners[0]) in (left, right)
    persisted = await service.get(acquired.claim.world_id, acquired.claim.claim_key)
    assert persisted == winners[0]
    await catalog.close()


async def test_accepted_outcome_requires_a_consumed_execution_grant(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(_request(), _capabilities(), claimant="worker")
    armed = await service.decide_recovery(acquired.claim)

    with pytest.raises(ValueError, match="consumed execution grant"):
        await service.settle(
            armed.claim,
            attempt_status=AttemptStatus.ACCEPTED,
            outcome=_outcome(status=AttemptStatus.ACCEPTED),
        )

    persisted = await service.get(armed.claim.world_id, armed.claim.claim_key)
    assert persisted is not None
    assert persisted.status is AttemptClaimStatus.POSSIBLY_SUBMITTED
    assert persisted.execution_consumed_at is None
    await catalog.close()


async def test_settlement_binds_provider_acknowledgement_to_outcome_session(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(_request(), _capabilities(), claimant="worker")
    armed = await service.decide_recovery(acquired.claim)
    consumed = await service.consume_execution(armed.authorization)
    acknowledged = await service.acknowledge_provider(
        consumed,
        provider_session_id="session-1",
    )

    with pytest.raises(ValueError, match="provider acknowledgement"):
        await service.settle(
            acknowledged,
            attempt_status=AttemptStatus.REJECTED,
            outcome=_outcome(agent_session_id="session-2"),
        )

    await catalog.close()


async def test_only_complete_mission_replayable_outcomes_can_settle(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    service = _claim_service(catalog)
    request = _request()
    acquired = await service.acquire(request, _capabilities(), claimant="worker")
    uncertain = await service.decide_recovery(acquired.claim)
    complete = _outcome(request=request)
    minimal = {
        key: complete[key]
        for key in (
            "attempt_id",
            "idempotency_key",
            "attempt_index",
            "request_fingerprint",
            "status",
            "accepted",
        )
    }

    with pytest.raises(ValueError, match="not replayable; missing fields"):
        await service.settle(
            uncertain.claim,
            attempt_status=AttemptStatus.REJECTED,
            outcome=minimal,
        )

    accepted_without_commit = _outcome(
        status=AttemptStatus.ACCEPTED,
        request=request,
        sha="",
    )
    with pytest.raises(ValueError, match="commit SHA"):
        await service.settle(
            uncertain.claim,
            attempt_status=AttemptStatus.INCOMPLETE,
            outcome=accepted_without_commit,
        )

    updated = MissionService().apply_attempt(_row(), request, complete)
    settled = await service.settle(
        uncertain.claim,
        attempt_status=AttemptStatus(updated["attempt__status"]),
        outcome=complete,
    )
    replayed = service.settled_outcome(settled)
    assert MissionService().apply_attempt(_row(), request, replayed) == updated
    await catalog.close()


@pytest.mark.parametrize(
    ("path", "expected_possible", "expected_acknowledged"),
    [
        ("never-submitted", False, False),
        ("uncertain", True, False),
        ("acknowledged", True, True),
    ],
)
async def test_each_terminal_path_retains_submission_evidence(
    tmp_path,
    path: str,
    expected_possible: bool,
    expected_acknowledged: bool,
) -> None:
    catalog = SqliteControlCatalog(tmp_path / f"{path}.db")
    service = _claim_service(catalog)
    acquired = await service.acquire(
        _request(),
        _capabilities(supports_session_resume=True),
        claimant="worker",
    )
    claim = acquired.claim
    if path != "never-submitted":
        decision = await service.decide_recovery(claim)
        claim = decision.claim
    if path == "acknowledged":
        claim = await service.consume_execution(decision.authorization)
        claim = await service.acknowledge_provider(
            claim,
            provider_session_id="session-1",
        )

    outcome = _outcome(
        path=path,
        agent_session_id="session-1" if path == "acknowledged" else "",
    )
    digest = service.outcome_digest(outcome)
    settled = await service.settle(
        claim,
        attempt_status=AttemptStatus.REJECTED,
        outcome=outcome,
        last_error="validator rejected the change",
    )

    assert settled.status is AttemptClaimStatus.SETTLED
    assert settled.settlement_status == "rejected"
    assert settled.outcome_digest == digest
    assert service.settled_outcome(settled) == outcome
    assert service.recover_request(settled) == _request()
    assert bool(settled.possibly_submitted_at) is expected_possible
    assert bool(settled.acknowledged_at) is expected_acknowledged
    assert settled.settled_at
    assert await service.list_due("world-1", now=time.time() + 10) == []

    duplicate = await service.acquire(
        _request(),
        _capabilities(supports_session_resume=True),
        claimant="another-worker",
    )
    assert duplicate.outcome is AttemptClaimAcquireOutcome.DUPLICATE
    assert duplicate.claim == settled
    await catalog.close()
