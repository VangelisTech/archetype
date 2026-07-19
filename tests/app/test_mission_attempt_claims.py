# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable pre-execution claims and possibly-submitted recovery (#501)."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from typing import Any

import pytest

from archetype.app.missions import (
    ATTEMPT_CLAIM_TRANSITION_GRAPH,
    AttemptClaimAcquireOutcome,
    AttemptClaimEvent,
    AttemptClaimStatus,
    AttemptClaimTransitionGraph,
    AttemptRecoveryAction,
    AttemptStatus,
    MissionAttemptClaimService,
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
)

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


def _request():
    request = MissionService().prepare_attempt(_row(), tick=11)
    assert request is not None
    return request


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
    outcome = _outcome()
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


async def test_attempt_claim_graph_is_complete_and_rejects_every_absent_edge() -> None:
    graph = AttemptClaimTransitionGraph()
    assert len(ATTEMPT_CLAIM_TRANSITION_GRAPH) == 5
    for (source, event), target in ATTEMPT_CLAIM_TRANSITION_GRAPH.items():
        transition = graph.transition(source.value, event.value)
        assert transition.source is source
        assert transition.event is event
        assert transition.target is target

    all_pairs = {(source, event) for source in AttemptClaimStatus for event in AttemptClaimEvent}
    for source, event in all_pairs - set(ATTEMPT_CLAIM_TRANSITION_GRAPH):
        with pytest.raises(ValueError, match="illegal attempt claim transition"):
            graph.transition(source, event)


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
    assert later_request == first_request
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
            settlement_status="failed",
            outcome_digest="stale",
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
