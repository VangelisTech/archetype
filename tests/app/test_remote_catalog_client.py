# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transport behavior for the remote control-catalog client."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from archetype.app.storage import remote_catalog as _remote_catalog
from archetype.app.storage.catalog import (
    AttemptClaimConflictError,
    AttemptClaimStaleError,
    CommandAdmission,
    RecoverySweepStaleError,
)
from archetype.app.storage.catalog import artifact_publication_key
from archetype.app.storage.remote_catalog import RemoteControlCatalog
from archetype.app.storage.service import StorageService
from archetype.core.config import StorageConfig

pytestmark = pytest.mark.asyncio

_ARTIFACT_KEY = artifact_publication_key("world-1", "run-1", "bundle-1")


async def _catalog_with(
    responses: list[httpx.Response], requests: list[httpx.Request] | None = None
) -> RemoteControlCatalog:
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    def handler(_request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(_request)
        return responses.pop(0)

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return catalog


def _attempt_protocol_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "status": "active",
            "catalog_protocol_version": 4,
            "capabilities": ["attempt_claim_execution_v2"],
        },
    )


def _artifact_protocol_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "status": "active",
            "catalog_protocol_version": 6,
            "capabilities": [
                "artifact_snapshot_decimal_v1",
                "artifact_publication_server_clock_v1",
            ],
        },
    )


def _recovery_protocol_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "status": "active",
            "catalog_protocol_version": 5,
            "capabilities": ["fleet_recovery_v1"],
        },
    )


_RECOVERY_CURSOR = "f" * 64


def _recovery_sweep_row(**overrides):
    row = {
        "sweep_key": "b" * 64,
        "storage_fingerprint": "a" * 64,
        "world_id": "world-1",
        "kind": "mission_model_recovery",
        "status": "leased",
        "cursor": _RECOVERY_CURSOR,
        "cycle": 1,
        "claimant": "worker-1",
        "lease_expires_at_ms": 1_000_100,
        "fence_epoch": 1,
        "active_subject_key": "c" * 64,
        "consecutive_failures": 0,
        "max_consecutive_failures": 3,
        "next_due_at_ms": 1_000_000,
        "last_error_code": "",
        "last_error_detail": "",
        "created_at_ms": 999_000,
        "updated_at_ms": 1_000_000,
        "paused_at_ms": None,
    }
    row.update(overrides)
    return row


def _recovery_exception_row(**overrides):
    row = {
        "exception_key": "d" * 64,
        "sweep_key": "b" * 64,
        "storage_fingerprint": "a" * 64,
        "world_id": "world-1",
        "kind": "mission_model_recovery",
        "subject_key": "c" * 64,
        "authority_key": "e" * 64,
        "status": "retry_wait",
        "attempt_count": 1,
        "max_attempts": 3,
        "retry_at_ms": 1_000_020,
        "last_error_code": "handler_failed",
        "last_error_detail": "provider timeout",
        "created_at_ms": 1_000_000,
        "updated_at_ms": 1_000_000,
        "resolved_at_ms": None,
        "dead_lettered_at_ms": None,
    }
    row.update(overrides)
    return row


async def test_remote_catalog_configuration_requires_token(monkeypatch):
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", "https://catalog.invalid")
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", raising=False)

    service = StorageService()
    with pytest.raises(RuntimeError, match="ARCHETYPE_CONTROL_CATALOG_TOKEN is required"):
        service.get_control_catalog(StorageConfig())


async def test_get_world_retries_transient_server_errors(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(_remote_catalog.asyncio, "sleep", sleep)
    catalog = await _catalog_with(
        [
            httpx.Response(503),
            httpx.Response(
                200,
                json={
                    "world_id": "w1",
                    "name": "alpha",
                    "run_id": "r1",
                    "parent_world_id": None,
                    "status": "active",
                    "tick_head": 3,
                },
            ),
        ]
    )
    try:
        world = await catalog.get_world("w1")
        assert world is not None and world.tick_head == 3
        sleep.assert_awaited_once_with(0.5)
    finally:
        await catalog.close()


async def test_get_claim_retries_then_treats_not_found_as_terminal(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(_remote_catalog.asyncio, "sleep", sleep)
    catalog = await _catalog_with([httpx.Response(500), httpx.Response(404)])
    try:
        assert await catalog.get_claim("w1", "missing") is None
        sleep.assert_awaited_once_with(0.5)
    finally:
        await catalog.close()


async def test_rearm_claim_returns_the_rotated_remote_record():
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={
                    "scope_key": "scope",
                    "run_id": "r1",
                    "producer": "p",
                    "external_id": "e1",
                    "payload_digest": "digest",
                    "status": "PENDING",
                    "commit_token": "fresh-token",
                    "tick": 0,
                    "artifact_entity_id": -100001,
                    "table_id": None,
                    "claimant": "recovery",
                    "lease_expires_at": 10.0,
                    "fence_epoch": 1,
                },
            )
        ]
    )
    try:
        claim = await catalog.rearm_claim("w1", "scope", "recovery", "fresh-token")
        assert claim.commit_token == "fresh-token"
        assert claim.table_id is None
    finally:
        await catalog.close()


def _command_row(**overrides):
    row = {
        "command_id": "c1",
        "sequence": 1,
        "scheduled_tick": 2,
        "priority": 3,
        "command_type": "custom",
        "payload_json": "{}",
        "payload_digest": "digest",
        "version": 1,
        "principal_id": "actor-1",
        "origin": "gateway",
        "reserved_entity_id": 7,
        "status": "PENDING",
        "attempts": 0,
        "max_attempts": 3,
        "lease_owner": None,
        "lease_expires_at": None,
        "last_error_code": None,
        "last_error_detail": None,
        "accepted_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "applied_tick": None,
        "commit_token": None,
    }
    row.update(overrides)
    return row


def _outbox_row():
    return {
        "sequence": 4,
        "event_id": "event-1",
        "aggregate_type": "command",
        "aggregate_id": "c1",
        "event_type": "command_queued",
        "command_type": "custom",
        "status": "queued",
        "actor_id": "actor-1",
        "payload_json": "{}",
        "occurred_at": "2026-01-01T00:00:00Z",
        "projected_at": None,
    }


def _artifact_publication_row(**overrides):
    row = {
        "publication_key": _ARTIFACT_KEY,
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "idempotency_key": "bundle-1",
        "request_digest": "digest-1",
        "status": "PENDING",
        "request_json": "{}",
        "records_json": "[]",
        "claimant": "owner-1",
        "lease_expires_at": 30.0,
        "retry_until_ms": 60_000,
        "attempt_count": 1,
        "index_snapshot_id": "0",
        "manifest_uri": "",
        "last_error": "",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "completed_at": None,
    }
    row.update(overrides)
    return row


async def _acquire_test_artifact(catalog: RemoteControlCatalog) -> None:
    await catalog.acquire_artifact_publication(
        world_id="world-1",
        run_id="run-1",
        attempt_id="attempt-1",
        idempotency_key="bundle-1",
        request_digest="digest-1",
        request_json="{}",
        claimant="owner-1",
        retry_window_ms=60_000,
    )


def _attempt_claim_row(**overrides):
    row = {
        "claim_key": "claim-1",
        "run_id": "run-1",
        "mission_id": "mission-1",
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "idempotency_key": "idempotency-1",
        "request_fingerprint": "request-fingerprint-1",
        "request_json": "{}",
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "status": "claimed",
        "provider": "modal",
        "provider_request_fingerprint": "provider-fingerprint-1",
        "supports_idempotent_replay": 0,
        "supports_session_resume": 1,
        "provider_idempotency_key": "",
        "claimant": "worker-1",
        "lease_expires_at": 30.0,
        "fence_epoch": 1,
        "execution_nonce": "",
        "execution_consumed_at": None,
        "provider_session_id": "",
        "provider_request_id": "",
        "settlement_status": "",
        "outcome_digest": "",
        "outcome_json": "",
        "artifact_request_json": "",
        "artifact_request_digest": "",
        "artifact_publication_key": "",
        "legacy_unbound_eligible": 0,
        "last_error": "",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "possibly_submitted_at": None,
        "acknowledged_at": None,
        "finalizing_at": None,
        "settled_at": None,
    }
    row.update(overrides)
    return row


async def _acquire_test_attempt(catalog: RemoteControlCatalog) -> None:
    await catalog.acquire_attempt_claim(
        claim_key="claim-1",
        world_id="world-1",
        run_id="run-1",
        mission_id="mission-1",
        task_id="task-1",
        attempt_id="attempt-1",
        idempotency_key="idempotency-1",
        request_fingerprint="request-fingerprint-1",
        request_json="{}",
        redaction_policy_id="redaction-v1",
        redaction_evidence_json='{"phase":"acquired"}',
        provider="modal",
        provider_request_fingerprint="provider-fingerprint-1",
        supports_idempotent_replay=False,
        supports_session_resume=True,
        provider_idempotency_key="",
        claimant="worker-1",
    )


async def test_attempt_claim_transport_is_typed_and_scoped():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            _attempt_protocol_response(),
            httpx.Response(
                200,
                json={"outcome": "acquired", "claim": _attempt_claim_row()},
            ),
            _attempt_protocol_response(),
            httpx.Response(
                200,
                json=_attempt_claim_row(
                    status="possibly_submitted",
                    execution_nonce="execution-1",
                    redaction_evidence_json='{"phase":"armed"}',
                    possibly_submitted_at="2026-01-01T00:00:01+00:00",
                ),
            ),
            _attempt_protocol_response(),
            httpx.Response(
                200,
                json=_attempt_claim_row(
                    status="possibly_submitted",
                    execution_nonce="execution-1",
                    execution_consumed_at="2026-01-01T00:00:02+00:00",
                ),
            ),
            httpx.Response(200, json=_attempt_claim_row(lease_expires_at=90.0)),
            httpx.Response(200, json=_attempt_claim_row(status="provider_acknowledged")),
            httpx.Response(200, json=[_attempt_claim_row(status="possibly_submitted")]),
            httpx.Response(404),
        ],
        requests,
    )
    try:
        outcome, claim = await catalog.acquire_attempt_claim(
            claim_key="claim-1",
            world_id="world-1",
            run_id="run-1",
            mission_id="mission-1",
            task_id="task-1",
            attempt_id="attempt-1",
            idempotency_key="idempotency-1",
            request_fingerprint="request-fingerprint-1",
            request_json="{}",
            redaction_policy_id="redaction-v1",
            redaction_evidence_json='{"phase":"acquired"}',
            provider="modal",
            provider_request_fingerprint="provider-fingerprint-1",
            supports_idempotent_replay=False,
            supports_session_resume=True,
            provider_idempotency_key="",
            claimant="worker-1",
            lease_seconds=15.0,
        )
        uncertain = await catalog.transition_attempt_claim(
            "world-1",
            claim.claim_key,
            "worker-1",
            1,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="execution-1",
            redaction_evidence_json='{"phase":"armed"}',
        )
        consumed = await catalog.consume_attempt_execution(
            "world-1",
            claim.claim_key,
            "worker-1",
            1,
            "execution-1",
        )
        renewed = await catalog.renew_attempt_claim(
            "world-1",
            claim.claim_key,
            "worker-1",
            1,
            lease_seconds=60.0,
        )
        fetched = await catalog.get_attempt_claim("world-1", claim.claim_key)
        due = await catalog.list_due_attempt_claims("world-1", now=5.0, limit=7)
        missing = await catalog.get_attempt_claim("world-1", "missing")

        assert outcome == "acquired" and claim.world_id == "world-1"
        assert claim.redaction_policy_id == "redaction-v1"
        assert claim.redaction_evidence_json == '{"phase":"acquired"}'
        assert uncertain.execution_nonce == "execution-1"
        assert uncertain.redaction_evidence_json == '{"phase":"armed"}'
        assert consumed.execution_consumed_at == "2026-01-01T00:00:02+00:00"
        assert renewed.lease_expires_at == 90.0
        assert fetched is not None and fetched.status == "provider_acknowledged"
        assert [record.status for record in due] == ["possibly_submitted"]
        assert missing is None
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/acquire-v2",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/claim-1/transition-v2",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/claim-1/consume-v2",
            "/ns/test/w/world-1/attempt-claims/claim-1/renew",
            "/ns/test/w/world-1/attempt-claims/claim-1",
            "/ns/test/w/world-1/attempt-claims",
            "/ns/test/w/world-1/attempt-claims/missing",
        ]
        assert dict(requests[-2].url.params) == {"due": "5.0", "limit": "7"}
        assert b'"execution_nonce":"execution-1"' in requests[3].content
        assert b'"redaction_policy_id":"redaction-v1"' in requests[1].content
        assert b'"redaction_evidence_json":"{\\"phase\\":\\"acquired\\"}"' in requests[1].content
        assert b'"redaction_evidence_json":"{\\"phase\\":\\"armed\\"}"' in requests[3].content
        assert b'"execution_nonce":"execution-1"' in requests[5].content
    finally:
        await catalog.close()


async def test_attempt_claim_redaction_receipts_reject_blank_transport_input():
    catalog = await _catalog_with([])

    async def acquire(policy_id: str, evidence_json: str):
        return await catalog.acquire_attempt_claim(
            claim_key="claim-1",
            world_id="world-1",
            run_id="run-1",
            mission_id="mission-1",
            task_id="task-1",
            attempt_id="attempt-1",
            idempotency_key="idempotency-1",
            request_fingerprint="request-fingerprint-1",
            request_json="{}",
            redaction_policy_id=policy_id,
            redaction_evidence_json=evidence_json,
            provider="modal",
            provider_request_fingerprint="provider-fingerprint-1",
            supports_idempotent_replay=False,
            supports_session_resume=True,
            provider_idempotency_key="",
            claimant="worker-1",
        )

    try:
        with pytest.raises(ValueError, match="redaction_policy_id"):
            await acquire("  ", '{"phase":"acquired"}')
        with pytest.raises(ValueError, match="redaction_evidence_json"):
            await acquire("redaction-v1", "  ")
        with pytest.raises(ValueError, match="redaction evidence update"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="possibly_submitted",
                target_status="provider_acknowledged",
                redaction_evidence_json="  ",
            )
        with pytest.raises(ValueError, match="complete artifact request"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="provider_acknowledged",
                target_status="finalizing",
                redaction_evidence_json='{"phase":"finalizing"}',
                outcome_digest="outcome-1",
                outcome_json='{"status":"accepted"}',
                artifact_request_json='{"attempt_id":"attempt-1"}',
                artifact_request_digest="artifact-request-1",
            )
        with pytest.raises(ValueError, match="complete durable outcome"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="provider_acknowledged",
                target_status="finalizing",
                redaction_evidence_json='{"phase":"finalizing"}',
                artifact_request_json='{"attempt_id":"attempt-1"}',
                artifact_request_digest="artifact-request-1",
                artifact_publication_key="publication-1",
            )
        complete_terminal = {
            "redaction_evidence_json": '{"phase":"settled"}',
            "settlement_status": "failed",
            "outcome_digest": "outcome-1",
            "outcome_json": '{"status":"failed"}',
        }
        for omitted in complete_terminal:
            with pytest.raises(ValueError, match=omitted):
                await catalog.transition_attempt_claim(
                    "world-1",
                    "claim-1",
                    "worker-1",
                    1,
                    expected_status="claimed",
                    target_status="settled",
                    **{**complete_terminal, omitted: ""},
                )
        with pytest.raises(ValueError, match="illegal attempt claim transition"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="settled",
                target_status="settled",
                **complete_terminal,
            )
    finally:
        await catalog.close()


async def test_attempt_claim_acquire_missing_capability_fails_before_mutation():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [httpx.Response(200, json={"status": "active"})],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="attempt-claim execution v2"):
            await _acquire_test_attempt(catalog)
        assert [request.url.path for request in requests] == ["/ns/test/w/world-1/status"]
    finally:
        await catalog.close()


async def test_attempt_claim_acquire_v2_route_404_fails_closed_after_probe():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [_attempt_protocol_response(), httpx.Response(404, json={"error": "bad_route"})],
        requests,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await _acquire_test_attempt(catalog)
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/acquire-v2",
        ]
    finally:
        await catalog.close()


async def test_attempt_claim_finalizing_transport_persists_typed_outbox_payload():
    requests: list[httpx.Request] = []
    finalizing_at = "2026-01-01T00:00:03+00:00"
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={
                    "status": "active",
                    "catalog_protocol_version": 4,
                    "capabilities": ["attempt_claim_execution_v2"],
                },
            ),
            httpx.Response(
                200,
                json=_attempt_claim_row(
                    status="finalizing",
                    outcome_digest="outcome-1",
                    outcome_json='{"status":"accepted"}',
                    artifact_request_json='{"attempt_id":"attempt-1"}',
                    artifact_request_digest="artifact-request-1",
                    artifact_publication_key="publication-1",
                    finalizing_at=finalizing_at,
                ),
            ),
        ],
        requests,
    )
    try:
        staged = await catalog.transition_attempt_claim(
            "world-1",
            "claim-1",
            "worker-1",
            1,
            expected_status="provider_acknowledged",
            target_status="finalizing",
            redaction_evidence_json='{"phase":"finalizing"}',
            outcome_digest="outcome-1",
            outcome_json='{"status":"accepted"}',
            artifact_request_json='{"attempt_id":"attempt-1"}',
            artifact_request_digest="artifact-request-1",
            artifact_publication_key="publication-1",
        )
        assert staged.status == "finalizing"
        assert staged.outcome_json == '{"status":"accepted"}'
        assert staged.artifact_request_json == '{"attempt_id":"attempt-1"}'
        assert staged.artifact_request_digest == "artifact-request-1"
        assert staged.artifact_publication_key == "publication-1"
        assert staged.finalizing_at == finalizing_at
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/claim-1/transition-v2",
        ]
        payload = json.loads(requests[1].content)
        assert payload["expected_status"] == "provider_acknowledged"
        assert payload["target_status"] == "finalizing"
        assert payload["outcome_digest"] == "outcome-1"
        assert payload["outcome_json"] == '{"status":"accepted"}'
        assert payload["artifact_request_json"] == '{"attempt_id":"attempt-1"}'
        assert payload["artifact_request_digest"] == "artifact-request-1"
        assert payload["artifact_publication_key"] == "publication-1"
    finally:
        await catalog.close()


async def test_attempt_claim_execution_protocol_missing_capability_fails_before_write():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [httpx.Response(200, json={"status": "active"})],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="attempt-claim execution v2"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="claimed",
                target_status="settled",
                redaction_evidence_json='{"phase":"settled"}',
                settlement_status="failed",
                outcome_digest="outcome-1",
                outcome_json='{"status":"failed"}',
            )
        assert [request.url.path for request in requests] == ["/ns/test/w/world-1/status"]
    finally:
        await catalog.close()


async def test_attempt_claim_transition_v2_route_404_fails_closed_after_probe():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={
                    "status": "active",
                    "catalog_protocol_version": 4,
                    "capabilities": ["attempt_claim_execution_v2"],
                },
            ),
            httpx.Response(404, json={"error": "bad_route"}),
        ],
        requests,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="execution-1",
            )
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/claim-1/transition-v2",
        ]
    finally:
        await catalog.close()


async def test_attempt_claim_transition_conflict_is_typed():
    catalog = await _catalog_with(
        [
            _attempt_protocol_response(),
            httpx.Response(
                409,
                json={
                    "error": "attempt_claim_conflict",
                    "message": "attempt claim claim-1 is possibly_submitted, expected claimed",
                },
            ),
        ]
    )
    try:
        with pytest.raises(AttemptClaimConflictError, match="expected claimed"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="execution-conflict",
            )
    finally:
        await catalog.close()


async def test_attempt_execution_consume_stale_is_typed():
    catalog = await _catalog_with(
        [
            _attempt_protocol_response(),
            httpx.Response(
                412,
                json={
                    "error": "attempt_claim_stale",
                    "message": "attempt execution grant claim-1 is already consumed",
                },
            ),
        ]
    )
    try:
        with pytest.raises(AttemptClaimStaleError, match="already consumed"):
            await catalog.consume_attempt_execution(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                "execution-1",
            )
    finally:
        await catalog.close()


async def test_attempt_execution_consume_v2_route_404_fails_closed_after_probe():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [_attempt_protocol_response(), httpx.Response(404, json={"error": "bad_route"})],
        requests,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await catalog.consume_attempt_execution(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                "execution-1",
            )
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/attempt-claims/claim-1/consume-v2",
        ]
    finally:
        await catalog.close()


async def test_expired_attempt_renew_and_transition_are_typed_as_stale():
    catalog = await _catalog_with(
        [
            httpx.Response(
                412,
                json={
                    "error": "attempt_claim_stale",
                    "message": "attempt claim lease expired before renewal",
                },
            ),
            _attempt_protocol_response(),
            httpx.Response(
                412,
                json={
                    "error": "attempt_claim_stale",
                    "message": "attempt claim lease expired before transition",
                },
            ),
        ]
    )
    try:
        with pytest.raises(AttemptClaimStaleError, match="before renewal"):
            await catalog.renew_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                lease_seconds=30,
            )
        with pytest.raises(AttemptClaimStaleError, match="before transition"):
            await catalog.transition_attempt_claim(
                "world-1",
                "claim-1",
                "worker-1",
                1,
                expected_status="possibly_submitted",
                target_status="provider_acknowledged",
                redaction_evidence_json='{"phase":"acknowledged"}',
                provider_request_id="request-1",
            )
    finally:
        await catalog.close()


async def test_command_ledger_transport_round_trip_is_typed_and_scoped():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json=[_command_row()]),
            httpx.Response(
                200,
                json=[
                    _command_row(
                        status="LEASED",
                        attempts=1,
                        lease_owner="worker-1",
                        lease_expires_at=30.0,
                    )
                ],
            ),
            httpx.Response(
                200,
                json=_command_row(
                    status="REJECTED",
                    attempts=1,
                    last_error_code="poison",
                    last_error_detail="invalid payload",
                ),
            ),
            httpx.Response(204),
            httpx.Response(200, json=[_command_row()]),
            httpx.Response(200, json={"count": 1}),
            httpx.Response(200, json={"entity_id": 7}),
            httpx.Response(200, json={"count": 1}),
        ],
        requests,
    )
    admission = CommandAdmission(
        command_id="c1",
        scheduled_tick=2,
        priority=3,
        command_type="custom",
        payload_json="{}",
        payload_digest="digest",
        version=1,
        principal_id="actor-1",
        origin="gateway",
        reserved_entity_id=7,
    )
    try:
        (admitted,) = await catalog.admit_commands("world-1", [admission])
        (leased,) = await catalog.lease_commands(
            "world-1", 2, "worker-1", lease_seconds=30.0, limit=4
        )
        failed = await catalog.fail_command(
            "world-1",
            "c1",
            "worker-1",
            status="REJECTED",
            error_code="poison",
            error_detail="invalid payload",
        )
        await catalog.release_commands("world-1", ["c1"], "worker-1")
        listed = await catalog.list_commands("world-1", status="PENDING", limit=5)
        pending = await catalog.pending_command_count("world-1")
        reserved = await catalog.max_reserved_entity_id("world-1")
        cancelled = await catalog.cancel_commands("world-1", reason="destroyed")

        assert admitted.world_id == "world-1" and admitted.reserved_entity_id == 7
        assert leased.status == "LEASED" and leased.lease_owner == "worker-1"
        assert failed.status == "REJECTED" and failed.last_error_code == "poison"
        assert [record.command_id for record in listed] == ["c1"]
        assert (pending, reserved, cancelled) == (1, 7, 1)
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/commands/admit",
            "/ns/test/w/world-1/commands/lease",
            "/ns/test/w/world-1/commands/c1/fail",
            "/ns/test/w/world-1/commands/release",
            "/ns/test/w/world-1/commands",
            "/ns/test/w/world-1/commands/pending-count",
            "/ns/test/w/world-1/commands/max-reserved",
            "/ns/test/w/world-1/commands/cancel",
        ]
        assert dict(requests[4].url.params) == {"limit": "5", "status": "PENDING"}
    finally:
        await catalog.close()


async def test_outbox_transport_preserves_order_and_projection_progress():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json=[_outbox_row()]),
            httpx.Response(204),
            httpx.Response(200, json={"watermark": 4, "pending": 0}),
        ],
        requests,
    )
    try:
        (event,) = await catalog.read_outbox("world-1", limit=8)
        await catalog.mark_outbox_projected("world-1", [event.event_id])
        progress = await catalog.outbox_progress("world-1")

        assert event.world_id == "world-1"
        assert event.sequence == 4 and event.status == "queued"
        assert progress == (4, 0)
        assert dict(requests[0].url.params) == {"limit": "8"}
        assert requests[1].method == "POST"
        assert requests[1].content == b'{"event_ids":["event-1"]}'
    finally:
        await catalog.close()


async def test_artifact_publication_transport_is_typed_and_scoped():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            _artifact_protocol_response(),
            httpx.Response(
                200,
                json={
                    "outcome": "acquired",
                    "publication": _artifact_publication_row(),
                },
            ),
            _artifact_protocol_response(),
            httpx.Response(200, json=_artifact_publication_row(lease_expires_at=90.0)),
            _artifact_protocol_response(),
            httpx.Response(204),
            _artifact_protocol_response(),
            httpx.Response(204),
            _artifact_protocol_response(),
            httpx.Response(204),
            _artifact_protocol_response(),
            httpx.Response(204),
            httpx.Response(404),
            httpx.Response(
                200,
                json=_artifact_publication_row(status="INDEXED", index_snapshot_id="42"),
            ),
            _artifact_protocol_response(),
            httpx.Response(200, json=[{"publication_key": _ARTIFACT_KEY}]),
        ],
        requests,
    )
    try:
        outcome, publication = await catalog.acquire_artifact_publication(
            world_id="world-1",
            run_id="run-1",
            attempt_id="attempt-1",
            idempotency_key="bundle-1",
            request_digest="digest-1",
            request_json="{}",
            claimant="owner-1",
            retry_window_ms=60_000,
            lease_ms=15_000,
        )
        renewed = await catalog.renew_artifact_publication(
            "world-1", publication.publication_key, "owner-1", lease_seconds=60.0
        )
        await catalog.record_artifact_uploads(
            "world-1", publication.publication_key, "owner-1", "[]", "s3://manifest"
        )
        await catalog.complete_artifact_publication(
            "world-1", publication.publication_key, "owner-1", 42
        )
        await catalog.fail_artifact_publication(
            "world-1",
            publication.publication_key,
            "owner-1",
            "retry",
            retry_delay_ms=3_000,
        )
        await catalog.expire_artifact_publication(
            "world-1", publication.publication_key, "owner-1", "expired"
        )
        missing = await catalog.get_artifact_publication("world-1", "missing")
        indexed = await catalog.get_artifact_publication("world-1", publication.publication_key)
        due = await catalog.list_due_artifact_publications(
            "world-1",
            limit=7,
            after_publication_key="1" * 64,
        )

        assert outcome == "acquired" and publication.world_id == "world-1"
        assert renewed.lease_expires_at == 90.0
        assert missing is None
        assert indexed is not None and indexed.status == "INDEXED"
        assert [record.publication_key for record in due] == [_ARTIFACT_KEY]
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/artifact-publications/acquire-v3",
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}/renew-v2",
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}/uploads-v2",
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}/complete-v2",
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}/fail-v3",
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}/expire-v2",
            "/ns/test/w/world-1/artifact-publications/missing",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/artifact-publications/due-v1",
        ]
        assert dict(requests[-1].url.params) == {
            "limit": "7",
            "after_publication_key": "1" * 64,
        }
        assert json.loads(requests[7].content)["index_snapshot_id"] == "42"
    finally:
        await catalog.close()


async def test_exact_artifact_recovery_sends_only_claimant_and_lease_duration():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            _artifact_protocol_response(),
            httpx.Response(
                200,
                json={
                    "outcome": "owned",
                    "publication": _artifact_publication_row(),
                },
            ),
        ],
        requests,
    )
    try:
        outcome, publication = await catalog.recover_artifact_publication(
            "world-1", _ARTIFACT_KEY, "owner-1", lease_ms=15_000
        )
        assert outcome == "owned" and publication is not None
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/artifact-publications/{_ARTIFACT_KEY}/recover-v1",
        ]
        assert json.loads(requests[-1].content) == {
            "claimant": "owner-1",
            "lease_ms": 15_000,
        }
    finally:
        await catalog.close()


async def test_exact_artifact_recovery_rejects_a_different_response_key():
    catalog = await _catalog_with(
        [
            _artifact_protocol_response(),
            httpx.Response(
                200,
                json={
                    "outcome": "owned",
                    "publication": _artifact_publication_row(publication_key="b" * 64),
                },
            ),
        ]
    )
    try:
        with pytest.raises(RuntimeError, match="different publication"):
            await catalog.recover_artifact_publication(
                "world-1", _ARTIFACT_KEY, "owner-1", lease_ms=15_000
            )
    finally:
        await catalog.close()


async def test_remote_due_discovery_rejects_full_replay_rows():
    catalog = await _catalog_with(
        [
            _artifact_protocol_response(),
            httpx.Response(200, json=[_artifact_publication_row()]),
        ]
    )
    try:
        with pytest.raises(RuntimeError, match="digest-only"):
            await catalog.list_due_artifact_publications("world-1")
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        ("owned", "INDEXED"),
        ("recovered", "EXPIRED"),
        ("duplicate", "PENDING"),
        ("expired", "UPLOADED"),
    ],
)
async def test_remote_exact_recovery_rejects_contradictory_outcome_status(outcome, status):
    catalog = await _catalog_with(
        [
            _artifact_protocol_response(),
            httpx.Response(
                200,
                json={
                    "outcome": outcome,
                    "publication": _artifact_publication_row(
                        status=status,
                        index_snapshot_id="1" if status == "INDEXED" else "0",
                    ),
                },
            ),
        ]
    )
    try:
        with pytest.raises(RuntimeError, match="contradicts"):
            await catalog.recover_artifact_publication(
                "world-1", _ARTIFACT_KEY, "owner-1", lease_ms=15_000
            )
    finally:
        await catalog.close()


async def test_remote_artifact_decoder_requires_attempt_count():
    row = _artifact_publication_row()
    del row["attempt_count"]
    catalog = await _catalog_with([httpx.Response(200, json=row)])
    try:
        with pytest.raises(RuntimeError, match="attempt_count"):
            await catalog.get_artifact_publication("world-1", _ARTIFACT_KEY)
    finally:
        await catalog.close()


@pytest.mark.parametrize("lease_ms", [0, True, 1.0, "1"])
async def test_remote_artifact_lease_duration_is_strictly_positive(lease_ms):
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([], requests)
    try:
        with pytest.raises((TypeError, ValueError)):
            await catalog.recover_artifact_publication(
                "world-1", _ARTIFACT_KEY, "owner-1", lease_ms=lease_ms
            )
        assert requests == []
    finally:
        await catalog.close()


@pytest.mark.parametrize("lease_seconds", [True, 0, -1, float("inf"), float("nan"), 86_401])
async def test_remote_artifact_renewal_duration_is_finite_and_bounded(lease_seconds):
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([], requests)
    try:
        with pytest.raises((TypeError, ValueError)):
            await catalog.renew_artifact_publication(
                "world-1",
                _ARTIFACT_KEY,
                "owner-1",
                lease_seconds=lease_seconds,
            )
        assert requests == []
    finally:
        await catalog.close()


async def test_artifact_publication_read_rejects_old_numeric_indexed_snapshot():
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json=_artifact_publication_row(
                    status="INDEXED",
                    index_snapshot_id=8_123_456_789_012_346_000,
                ),
            )
        ]
    )
    try:
        with pytest.raises(RuntimeError, match="lossy snapshot ID"):
            await catalog.get_artifact_publication("world-1", "publication-1")
    finally:
        await catalog.close()


async def test_artifact_acquire_missing_capability_fails_before_mutation():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [httpx.Response(200, json={"status": "active"})],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="artifact publication server-clock v1"):
            await _acquire_test_artifact(catalog)
        assert [request.url.path for request in requests] == ["/ns/test/w/world-1/status"]
    finally:
        await catalog.close()


async def test_artifact_acquire_v3_route_404_fails_closed_after_probe():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [_artifact_protocol_response(), httpx.Response(404, json={"error": "bad_route"})],
        requests,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await _acquire_test_artifact(catalog)
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/artifact-publications/acquire-v3",
        ]
    finally:
        await catalog.close()


@pytest.mark.parametrize("snapshot_id", [True, 1.5, "1", 0, -1, 1 << 63])
async def test_artifact_publication_completion_rejects_nonpositive_noninteger_snapshot(
    snapshot_id,
):
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([], requests)
    try:
        with pytest.raises(ValueError, match="positive integer"):
            await catalog.complete_artifact_publication(
                "world-1",
                "publication-1",
                "owner-1",
                snapshot_id,
            )
        assert requests == []
    finally:
        await catalog.close()


async def test_artifact_mutation_protocol_missing_capability_fails_before_write():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={
                    "status": "active",
                    "catalog_protocol_version": 2,
                    "capabilities": ["attempt_claim_finalization_v2"],
                },
            )
        ],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="lease-fenced artifact mutation v2"):
            await catalog.complete_artifact_publication("world-1", "publication-1", "owner-1", 42)
        assert [request.url.path for request in requests] == ["/ns/test/w/world-1/status"]
    finally:
        await catalog.close()


async def test_artifact_mutation_v2_route_404_fails_closed_after_probe():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={
                    "status": "active",
                    "catalog_protocol_version": 6,
                    "capabilities": [
                        "artifact_snapshot_decimal_v1",
                        "artifact_publication_server_clock_v1",
                    ],
                },
            ),
            httpx.Response(404, json={"error": "bad_route"}),
        ],
        requests,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await catalog.complete_artifact_publication("world-1", "publication-1", "owner-1", 42)
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/artifact-publications/publication-1/complete-v2",
        ]
    finally:
        await catalog.close()


async def test_remote_world_discovery_page_encodes_cursor_and_limit():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            _recovery_protocol_response(),
            httpx.Response(
                200,
                json=[
                    {
                        "world_id": "world/2",
                        "name": "alpha",
                        "run_id": "run-1",
                        "parent_world_id": None,
                        "status": "destroyed",
                        "tick_head": 2,
                    }
                ],
            ),
        ],
        requests,
    )
    try:
        page = await catalog.list_worlds_page(after_world_id="world/1", limit=7)
        assert [record.world_id for record in page] == ["world/2"]
        assert page[0].status == "destroyed"
        assert [request.url.path for request in requests] == [
            "/ns/test/w/__fleet_recovery_protocol__/status",
            "/ns/test/worlds",
        ]
        assert dict(requests[1].url.params) == {
            "after_world_id": "world/1",
            "limit": "7",
        }
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("rows", "after_world_id", "limit", "message"),
    [
        (
            [
                {"world_id": "world-1", "status": "active"},
                {"world_id": "world-2", "status": "active"},
            ],
            "",
            1,
            "page size",
        ),
        (
            [
                {"world_id": "world-2", "status": "active"},
                {"world_id": "world-1", "status": "active"},
            ],
            "",
            2,
            "unordered",
        ),
        (
            [{"world_id": "world-1", "status": "active"}],
            "world-1",
            2,
            "out-of-cursor",
        ),
    ],
)
async def test_remote_world_discovery_rejects_untrusted_pages(rows, after_world_id, limit, message):
    catalog = await _catalog_with([_recovery_protocol_response(), httpx.Response(200, json=rows)])
    try:
        with pytest.raises(RuntimeError, match=message):
            await catalog.list_worlds_page(
                after_world_id=after_world_id,
                limit=limit,
            )
    finally:
        await catalog.close()


async def test_remote_world_discovery_requires_fleet_capability_before_read():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(
                404,
                json={
                    "error": "not_found",
                    "catalog_protocol_version": 4,
                    "capabilities": [],
                },
            )
        ],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="fleet recovery v1"):
            await catalog.list_worlds_page(limit=10)
        assert [request.url.path for request in requests] == [
            "/ns/test/w/__fleet_recovery_protocol__/status"
        ]
    finally:
        await catalog.close()


async def test_remote_recovery_transport_is_versioned_typed_and_scoped():
    requests: list[httpx.Request] = []
    created = _recovery_sweep_row(
        status="idle",
        cursor="",
        cycle=0,
        claimant="",
        lease_expires_at_ms=0,
        fence_epoch=0,
        active_subject_key="",
    )
    leased = _recovery_sweep_row()
    checkpointed = _recovery_sweep_row(cursor=_RECOVERY_CURSOR)
    exception = _recovery_exception_row()
    catalog = await _catalog_with(
        [
            _recovery_protocol_response(),
            httpx.Response(200, json=created),
            _recovery_protocol_response(),
            httpx.Response(200, json={"outcome": "acquired", "sweep": leased}),
            _recovery_protocol_response(),
            httpx.Response(200, json=checkpointed),
            _recovery_protocol_response(),
            httpx.Response(200, json=exception),
            _recovery_protocol_response(),
            httpx.Response(200, json=exception),
            _recovery_protocol_response(),
            httpx.Response(200, json=[exception]),
        ],
        requests,
    )
    try:
        ensured = await catalog.ensure_recovery_sweep(
            "a" * 64,
            "world-1",
            "mission_model_recovery",
            max_consecutive_failures=3,
        )
        outcome, acquired = await catalog.lease_recovery_sweep(
            "world-1",
            "mission_model_recovery",
            "worker-1",
            lease_ms=100,
        )
        saved = await catalog.checkpoint_recovery_sweep(
            "world-1",
            "mission_model_recovery",
            "worker-1",
            acquired.fence_epoch,
            cursor=_RECOVERY_CURSOR,
            active_subject_key="c" * 64,
        )
        retried = await catalog.retry_recovery_exception(
            "world-1",
            "mission_model_recovery",
            "worker-1",
            acquired.fence_epoch,
            subject_key="c" * 64,
            authority_key="e" * 64,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="provider timeout",
            retry_delay_ms=20,
            max_attempts=3,
        )
        fetched = await catalog.get_recovery_exception(
            "world-1", "mission_model_recovery", retried.exception_key
        )
        due = await catalog.list_recovery_exceptions(
            "world-1",
            kind="mission_model_recovery",
            status="retry_wait",
            due_only=True,
            limit=7,
        )

        assert ensured.status == "idle"
        assert outcome == "acquired" and acquired.fence_epoch == 1
        assert saved.cursor == _RECOVERY_CURSOR
        assert fetched == retried
        assert due == [retried]
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/recovery/sweeps/ensure-v1",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/recovery/sweeps/lease-v1",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/recovery/sweeps/checkpoint-v1",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/recovery/exceptions/retry-v1",
            "/ns/test/w/world-1/status",
            f"/ns/test/w/world-1/recovery/exceptions/{'d' * 64}",
            "/ns/test/w/world-1/status",
            "/ns/test/w/world-1/recovery/exceptions",
        ]
        assert dict(requests[-1].url.params) == {
            "limit": "7",
            "kind": "mission_model_recovery",
            "status": "retry_wait",
            "due_only": "1",
        }
        retry_payload = json.loads(requests[7].content)
        assert retry_payload["expected_attempt_count"] == 0
        assert retry_payload["subject_key"] == "c" * 64
    finally:
        await catalog.close()


async def test_remote_recovery_capability_and_stale_fence_fail_closed():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={
                    "status": "active",
                    "catalog_protocol_version": 4,
                    "capabilities": [],
                },
            )
        ],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="fleet recovery v1"):
            await catalog.lease_recovery_sweep(
                "world-1", "mission_model_recovery", "worker", lease_ms=100
            )
        assert [request.url.path for request in requests] == ["/ns/test/w/world-1/status"]
    finally:
        await catalog.close()

    validation_requests: list[httpx.Request] = []
    validation_catalog = await _catalog_with([], validation_requests)
    try:
        with pytest.raises(ValueError, match="lowercase SHA-256"):
            await validation_catalog.checkpoint_recovery_sweep(
                "world-1",
                "mission_model_recovery",
                "worker",
                1,
                cursor="raw-page-token",
            )
        assert validation_requests == []
    finally:
        await validation_catalog.close()

    stale_catalog = await _catalog_with(
        [
            _recovery_protocol_response(),
            httpx.Response(
                412,
                json={
                    "error": "recovery_sweep_stale",
                    "message": "lease expired before checkpoint",
                },
            ),
        ]
    )
    try:
        with pytest.raises(RecoverySweepStaleError, match="lease expired"):
            await stale_catalog.checkpoint_recovery_sweep(
                "world-1",
                "mission_model_recovery",
                "worker",
                1,
                cursor=_RECOVERY_CURSOR,
            )
    finally:
        await stale_catalog.close()


async def test_remote_recovery_mutations_reject_transition_target_drift():
    checkpoint_catalog = await _catalog_with(
        [
            _recovery_protocol_response(),
            httpx.Response(200, json=_recovery_sweep_row(status="paused")),
        ]
    )
    try:
        with pytest.raises(RuntimeError, match="outside the transition graph"):
            await checkpoint_catalog.checkpoint_recovery_sweep(
                "world-1",
                "mission_model_recovery",
                "worker-1",
                1,
                cursor=_RECOVERY_CURSOR,
            )
    finally:
        await checkpoint_catalog.close()

    retry_catalog = await _catalog_with(
        [
            _recovery_protocol_response(),
            httpx.Response(200, json=_recovery_exception_row(status="resolved")),
        ]
    )
    try:
        with pytest.raises(RuntimeError, match="outside the transition graph"):
            await retry_catalog.retry_recovery_exception(
                "world-1",
                "mission_model_recovery",
                "worker-1",
                1,
                subject_key="c" * 64,
                authority_key="e" * 64,
                expected_attempt_count=0,
                error_code="handler_failed",
                error_detail="provider timeout",
                retry_delay_ms=20,
                max_attempts=3,
            )
    finally:
        await retry_catalog.close()


@pytest.mark.parametrize(
    ("invalid_fence", "error_type"),
    [
        (True, TypeError),
        (1.5, TypeError),
        ("1", TypeError),
        (-1, ValueError),
        (1 << 53, ValueError),
        (1 << 63, ValueError),
    ],
)
async def test_every_remote_recovery_mutation_rejects_non_portable_fences(
    invalid_fence, error_type
):
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([], requests)
    calls = (
        lambda: catalog.renew_recovery_sweep(
            "world-1", "artifact_publication", "worker", invalid_fence, lease_ms=100
        ),
        lambda: catalog.checkpoint_recovery_sweep(
            "world-1", "artifact_publication", "worker", invalid_fence, cursor=""
        ),
        lambda: catalog.yield_recovery_sweep(
            "world-1",
            "artifact_publication",
            "worker",
            invalid_fence,
            next_delay_ms=0,
        ),
        lambda: catalog.fail_recovery_sweep(
            "world-1",
            "artifact_publication",
            "worker",
            invalid_fence,
            error_code="handler_failed",
            error_detail="",
            retry_delay_ms=0,
        ),
        lambda: catalog.pause_recovery_sweep(
            "world-1",
            "artifact_publication",
            "worker",
            invalid_fence,
            error_code="capability_unavailable",
            error_detail="",
        ),
        lambda: catalog.redrive_recovery_sweep(
            "world-1",
            "artifact_publication",
            expected_fence_epoch=invalid_fence,
        ),
        lambda: catalog.retry_recovery_exception(
            "world-1",
            "artifact_publication",
            "worker",
            invalid_fence,
            subject_key="c" * 64,
            authority_key="e" * 64,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="",
            retry_delay_ms=0,
            max_attempts=3,
        ),
        lambda: catalog.resolve_recovery_exception(
            "world-1", "artifact_publication", "worker", invalid_fence, "d" * 64
        ),
        lambda: catalog.redrive_recovery_exception(
            "world-1",
            "artifact_publication",
            "worker",
            invalid_fence,
            "d" * 64,
            expected_attempt_count=1,
        ),
    )
    try:
        for call in calls:
            with pytest.raises(error_type):
                await call()
        assert requests == []
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("body", "limit", "message"),
    [
        ({"not": "a-list"}, 2, "page size"),
        ([_recovery_exception_row(), _recovery_exception_row()], 1, "page size"),
        (
            [
                _recovery_exception_row(exception_key="d" * 64),
                _recovery_exception_row(exception_key="c" * 64),
            ],
            2,
            "strictly ordered",
        ),
        ([_recovery_exception_row(exception_key="raw-key")], 2, "invalid exception_key"),
    ],
)
async def test_remote_recovery_exception_lists_reject_untrusted_pages(body, limit, message):
    catalog = await _catalog_with([_recovery_protocol_response(), httpx.Response(200, json=body)])
    try:
        with pytest.raises(RuntimeError, match=message):
            await catalog.list_recovery_exceptions("world-1", limit=limit)
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ([_recovery_sweep_row()] * 8, "closed kind set"),
        ([_recovery_sweep_row(kind="eighth_recovery_kind")], "invalid kind"),
        ([_recovery_sweep_row(cycle=1 << 53)], "out-of-range cycle"),
        (
            [
                _recovery_sweep_row(kind="mission_model_recovery", sweep_key="d" * 64),
                _recovery_sweep_row(kind="artifact_publication", sweep_key="c" * 64),
            ],
            "strictly ordered",
        ),
    ],
)
async def test_remote_recovery_sweep_lists_reject_untrusted_closed_set(body, message):
    catalog = await _catalog_with([_recovery_protocol_response(), httpx.Response(200, json=body)])
    try:
        with pytest.raises(RuntimeError, match=message):
            await catalog.list_recovery_sweeps("world-1")
    finally:
        await catalog.close()


async def test_remote_recovery_kind_set_is_closed_before_transport():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([], requests)
    try:
        with pytest.raises(ValueError, match="unsupported recovery kind"):
            await catalog.lease_recovery_sweep(
                "world-1", "eighth_recovery_kind", "worker", lease_ms=100
            )
        assert requests == []
    finally:
        await catalog.close()


async def test_remote_recovery_durable_inputs_require_exact_safe_types():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([], requests)
    try:
        with pytest.raises(ValueError, match="lowercase SHA-256"):
            await catalog.ensure_recovery_sweep(
                "a" * 62 + "  ",
                "world-1",
                "artifact_publication",
                max_consecutive_failures=3,
            )
        with pytest.raises(TypeError, match="permanent must be a boolean"):
            await catalog.retry_recovery_exception(
                "world-1",
                "artifact_publication",
                "worker",
                1,
                subject_key="c" * 64,
                authority_key="e" * 64,
                expected_attempt_count=0,
                error_code="handler_failed",
                error_detail="",
                retry_delay_ms=0,
                max_attempts=3,
                permanent=1,
            )
        for error_code, error_detail in ((1, ""), ("handler_failed", 1)):
            with pytest.raises(TypeError, match="must be a string"):
                await catalog.fail_recovery_sweep(
                    "world-1",
                    "artifact_publication",
                    "worker",
                    1,
                    error_code=error_code,
                    error_detail=error_detail,
                    retry_delay_ms=0,
                )
        assert requests == []
    finally:
        await catalog.close()
