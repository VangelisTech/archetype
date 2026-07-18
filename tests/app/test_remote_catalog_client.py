# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transport behavior for the remote control-catalog client."""

from unittest.mock import AsyncMock

import httpx
import pytest

from archetype.app.storage import remote_catalog as _remote_catalog
from archetype.app.storage.catalog import (
    AttemptClaimConflictError,
    AttemptClaimStaleError,
    CommandAdmission,
)
from archetype.app.storage.remote_catalog import RemoteControlCatalog
from archetype.app.storage.service import StorageService
from archetype.core.config import StorageConfig

pytestmark = pytest.mark.asyncio


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
        "publication_key": "publication-1",
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
        "index_snapshot_id": 0,
        "manifest_uri": "",
        "last_error": "",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "completed_at": None,
    }
    row.update(overrides)
    return row


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
        "last_error": "",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "possibly_submitted_at": None,
        "acknowledged_at": None,
        "settled_at": None,
    }
    row.update(overrides)
    return row


async def test_attempt_claim_transport_is_typed_and_scoped():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(
                200,
                json={"outcome": "acquired", "claim": _attempt_claim_row()},
            ),
            httpx.Response(
                200,
                json=_attempt_claim_row(
                    status="possibly_submitted",
                    execution_nonce="execution-1",
                    redaction_evidence_json='{"phase":"armed"}',
                    possibly_submitted_at="2026-01-01T00:00:01+00:00",
                ),
            ),
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
            "/ns/test/w/world-1/attempt-claims/acquire",
            "/ns/test/w/world-1/attempt-claims/claim-1/transition",
            "/ns/test/w/world-1/attempt-claims/claim-1/consume",
            "/ns/test/w/world-1/attempt-claims/claim-1/renew",
            "/ns/test/w/world-1/attempt-claims/claim-1",
            "/ns/test/w/world-1/attempt-claims",
            "/ns/test/w/world-1/attempt-claims/missing",
        ]
        assert dict(requests[-2].url.params) == {"due": "5.0", "limit": "7"}
        assert b'"execution_nonce":"execution-1"' in requests[1].content
        assert b'"redaction_policy_id":"redaction-v1"' in requests[0].content
        assert b'"redaction_evidence_json":"{\\"phase\\":\\"acquired\\"}"' in requests[0].content
        assert b'"redaction_evidence_json":"{\\"phase\\":\\"armed\\"}"' in requests[1].content
        assert b'"execution_nonce":"execution-1"' in requests[2].content
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
    finally:
        await catalog.close()


async def test_attempt_claim_transition_conflict_is_typed():
    catalog = await _catalog_with(
        [
            httpx.Response(
                409,
                json={
                    "error": "attempt_claim_conflict",
                    "message": "attempt claim claim-1 is possibly_submitted, expected claimed",
                },
            )
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
                last_error="conflicting evidence",
            )
    finally:
        await catalog.close()


async def test_attempt_execution_consume_stale_is_typed():
    catalog = await _catalog_with(
        [
            httpx.Response(
                412,
                json={
                    "error": "attempt_claim_stale",
                    "message": "attempt execution grant claim-1 is already consumed",
                },
            )
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
            httpx.Response(
                200,
                json={
                    "outcome": "acquired",
                    "publication": _artifact_publication_row(),
                },
            ),
            httpx.Response(200, json=_artifact_publication_row(lease_expires_at=90.0)),
            httpx.Response(204),
            httpx.Response(204),
            httpx.Response(204),
            httpx.Response(204),
            httpx.Response(404),
            httpx.Response(200, json=_artifact_publication_row(status="INDEXED")),
            httpx.Response(200, json=[_artifact_publication_row(status="UPLOADED")]),
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
            retry_until_ms=60_000,
            lease_seconds=15.0,
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
            "world-1", publication.publication_key, "owner-1", "retry", retry_at=3.0
        )
        await catalog.expire_artifact_publication(
            "world-1", publication.publication_key, "owner-1", "expired"
        )
        missing = await catalog.get_artifact_publication("world-1", "missing")
        indexed = await catalog.get_artifact_publication("world-1", publication.publication_key)
        due = await catalog.list_due_artifact_publications("world-1", now=5.0, limit=7)

        assert outcome == "acquired" and publication.world_id == "world-1"
        assert renewed.lease_expires_at == 90.0
        assert missing is None
        assert indexed is not None and indexed.status == "INDEXED"
        assert [record.status for record in due] == ["UPLOADED"]
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/artifact-publications/acquire",
            "/ns/test/w/world-1/artifact-publications/publication-1/renew",
            "/ns/test/w/world-1/artifact-publications/publication-1/uploads",
            "/ns/test/w/world-1/artifact-publications/publication-1/complete",
            "/ns/test/w/world-1/artifact-publications/publication-1/fail",
            "/ns/test/w/world-1/artifact-publications/publication-1/expire",
            "/ns/test/w/world-1/artifact-publications/missing",
            "/ns/test/w/world-1/artifact-publications/publication-1",
            "/ns/test/w/world-1/artifact-publications",
        ]
        assert dict(requests[-1].url.params) == {"due": "5.0", "limit": "7"}
    finally:
        await catalog.close()
