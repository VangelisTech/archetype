# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Remote control catalog parity (issue #281).

The DO worker must be observationally identical to SqliteControlCatalog —
the reference implementation — for every operation the system performs:
identity conflicts, fence semantics, manifest CAS, the three-state
visibility map, and the full claim lifecycle. The harness runs the worker
locally under ``wrangler dev`` (skipped cleanly when node/wrangler is
unavailable), then drives BOTH catalogs through identical sequences and
asserts identical outcomes, including exception types.

The final test runs the real service stack — coordinator, ingestion,
receipts — against the remote catalog via ARCHETYPE_CONTROL_CATALOG_URL.
"""

import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from archetype.app.storage.catalog import (
    ArtifactPublicationConflictError,
    ArtifactPublicationPendingError,
    AttemptClaimConflictError,
    AttemptClaimPendingError,
    AttemptClaimStaleError,
    CatalogConflictError,
    ClaimConflictError,
    ClaimPendingError,
    CommandAdmission,
    CommandConflictError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
)
from archetype.core.interfaces import StaleWriterError

pytestmark = pytest.mark.asyncio

WORKER_DIR = Path(__file__).resolve().parents[2] / "infra" / "control-catalog"
WORKER_TOKEN = "archetype-parity-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def worker_url():
    if shutil.which("npx") is None:
        pytest.skip("npx unavailable; wrangler dev harness skipped")
    port = _free_port()
    worker_log = tempfile.TemporaryFile(mode="w+")
    proc = subprocess.Popen(
        [
            "npx",
            "--yes",
            "wrangler",
            "dev",
            "--local",
            "--port",
            str(port),
            "--var",
            f"CATALOG_TOKEN:{WORKER_TOKEN}",
        ],
        cwd=WORKER_DIR,
        stdout=worker_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        import httpx

        deadline = time.time() + 120
        while time.time() < deadline:
            if proc.poll() is not None:
                worker_log.seek(0)
                out = worker_log.read()
                pytest.skip(f"wrangler dev exited early: {out[-400:]}")
            try:
                response = httpx.get(
                    f"{url}/ns/probe/worlds",
                    headers={"authorization": f"Bearer {WORKER_TOKEN}"},
                    timeout=2.0,
                )
                if response.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1.0)
        else:
            pytest.skip("wrangler dev did not become ready in 120s")
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        worker_log.close()


def _remote(worker_url: str):
    from archetype.app.storage.remote_catalog import RemoteControlCatalog

    # Fresh namespace per catalog instance: parity runs are independent.
    return RemoteControlCatalog(
        worker_url,
        f"parity-{uuid.uuid4().hex[:12]}",
        token=WORKER_TOKEN,
    )


def _sqlite(tmp_path) -> SqliteControlCatalog:
    return SqliteControlCatalog(tmp_path / f"parity-{uuid.uuid4().hex[:8]}.db")


def _world(wid: str = "w1", **overrides) -> WorldRecord:
    base = dict(
        world_id=wid, name="alpha", run_id="r1", parent_world_id=None, status="active", tick_head=0
    )
    base.update(overrides)
    return WorldRecord(**base)


async def _both(tmp_path, worker_url):
    return _sqlite(tmp_path), _remote(worker_url)


async def test_worker_requires_bearer_token(worker_url):
    import httpx

    response = httpx.get(f"{worker_url}/ns/probe/worlds")
    assert response.status_code == 401


async def test_world_registration_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        await catalog.register_world(_world())  # idempotent
        with pytest.raises(CatalogConflictError):
            await catalog.register_world(_world(name="impostor"))
        record = await catalog.get_world("w1")
        assert record is not None and record.name == "alpha"
        assert await catalog.get_world("missing") is None
        await catalog.set_world_status("w1", "destroyed")
        assert (await catalog.get_world("w1")).status == "destroyed"
        listed = await catalog.list_worlds()
        assert [w.world_id for w in listed] == ["w1"]
        await catalog.close()


async def test_signature_registration_parity(tmp_path, worker_url):
    rec = SignatureRecord(
        table_id="t1", component_names=("A", "B"), schema_json='{"fields":[]}', fingerprint="f1"
    )
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_signature(rec)
        await catalog.register_signature(rec)  # idempotent
        with pytest.raises(CatalogConflictError):
            await catalog.register_signature(
                SignatureRecord(
                    table_id="t1", component_names=("A",), schema_json="{}", fingerprint="f2"
                )
            )
        listed = await catalog.list_signatures()
        assert len(listed) == 1 and listed[0].component_names == ("A", "B")
        await catalog.close()


async def test_fence_and_manifest_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        assert await catalog.current_fence_epoch("w1") is None
        assert await catalog.acquire_fence("w1", "h1") == 1
        assert await catalog.acquire_fence("w1", "h2") == 2
        assert await catalog.current_fence_epoch("w1") == 2
        assert await catalog.max_manifest_tick("w1", "r1") is None

        # Stale epoch fails closed.
        with pytest.raises(StaleWriterError):
            await catalog.publish_manifest("w1", "r1", 0, "tok-a", 1, ["t1"])
        # Live epoch publishes; identical retry is a no-op; different attempt conflicts.
        await catalog.publish_manifest("w1", "r1", 0, "tok-a", 2, ["t1"])
        await catalog.publish_manifest("w1", "r1", 0, "tok-a", 2, ["t1"])
        with pytest.raises(CatalogConflictError):
            await catalog.publish_manifest("w1", "r1", 0, "tok-b", 2, ["t1"])
        await catalog.publish_manifest("w1", "r1", 1, "tok-c", 2, ["t2", "t1"])

        manifests = await catalog.list_manifests("w1", "r1")
        assert [(m.tick, m.commit_token) for m in manifests] == [(0, "tok-a"), (1, "tok-c")]
        assert manifests[1].table_ids == ("t1", "t2")
        assert await catalog.max_manifest_tick("w1", "r1") == 1
        # Publication advances the durable head on both backends.
        assert (await catalog.get_world("w1")).tick_head == 1
        await catalog.close()


async def test_visibility_three_state_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        # Never fenced: legacy, unfiltered.
        assert await catalog.visible_tokens("w1", "r1") is None
        # Once a claim exists, its pending rows must not leak through the
        # legacy-unfiltered state even before this world has a writer fence.
        await catalog.acquire_claim(
            world_id="w1",
            run_id="r1",
            producer="p",
            external_id="pending",
            payload_digest="pending-digest",
            claimant="pending-writer",
            tick=0,
        )
        assert await catalog.visible_tokens("w1", "r1") == {0: [""]}
        assert await catalog.visible_tokens("w1", "r1", []) == {}
        assert await catalog.visible_tokens("w1", "r1", [3]) == {3: [""]}
        # Fenced, nothing published: nothing visible.
        await catalog.acquire_fence("w1", "h1")
        assert await catalog.visible_tokens("w1", "r1") == {}
        # Manifests appear in the map; tick filter applies.
        await catalog.publish_manifest("w1", "r1", 0, "tok-a", 1, ["t1"])
        await catalog.publish_manifest("w1", "r1", 1, "tok-b", 1, ["t1"])
        assert await catalog.visible_tokens("w1", "r1") == {0: ["tok-a"], 1: ["tok-b"]}
        assert await catalog.visible_tokens("w1", "r1", [1]) == {1: ["tok-b"]}
        await catalog.close()


async def test_claim_lifecycle_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        await catalog.acquire_fence("w1", "h1")

        outcome, claim = await catalog.acquire_claim(
            world_id="w1",
            run_id="r1",
            producer="p",
            external_id="e1",
            payload_digest="d1",
            claimant="c1",
            tick=0,
            lease_seconds=30.0,
        )
        assert outcome == "acquired"
        assert claim.artifact_entity_id < 0, "artifacts live in the negative id band"
        assert claim.fence_epoch == 1

        # Live lease blocks other claimants; same digest.
        with pytest.raises(ClaimPendingError):
            await catalog.acquire_claim(
                world_id="w1",
                run_id="r1",
                producer="p",
                external_id="e1",
                payload_digest="d1",
                claimant="c2",
                tick=0,
            )
        # Different digest conflicts loudly regardless of lease.
        with pytest.raises(ClaimConflictError):
            await catalog.acquire_claim(
                world_id="w1",
                run_id="r1",
                producer="p",
                external_id="e1",
                payload_digest="d2",
                claimant="c2",
                tick=0,
            )

        await catalog.record_claim_table("w1", claim.scope_key, "t9")
        # Wrong claimant cannot complete.
        with pytest.raises(ClaimPendingError):
            await catalog.complete_claim("w1", claim.scope_key, "someone-else", "t9")
        await catalog.complete_claim("w1", claim.scope_key, "c1", "t9")
        await catalog.complete_claim("w1", claim.scope_key, "c1", "t9")  # idempotent

        settled = await catalog.get_claim("w1", claim.scope_key)
        assert settled.status == "COMPLETE" and settled.table_id == "t9"

        # Duplicate submission returns the original.
        outcome2, claim2 = await catalog.acquire_claim(
            world_id="w1",
            run_id="r1",
            producer="p",
            external_id="e1",
            payload_digest="d1",
            claimant="c3",
            tick=0,
        )
        assert outcome2 == "duplicate" and claim2.commit_token == claim.commit_token

        # Completed claims join the visible set at their tick.
        visible = await catalog.visible_tokens("w1", "r1", [0])
        assert claim.commit_token in visible.get(0, [])

        # An expired empty claim rotates away from the stale writer's token
        # before a recovery may append.
        _, empty = await catalog.acquire_claim(
            world_id="w1",
            run_id="r1",
            producer="p",
            external_id="empty",
            payload_digest="d-empty",
            claimant="expired",
            tick=0,
            lease_seconds=0.0,
        )
        await catalog.record_claim_table("w1", empty.scope_key, "stale-table")
        outcome3, recovered = await catalog.acquire_claim(
            world_id="w1",
            run_id="r1",
            producer="p",
            external_id="empty",
            payload_digest="d-empty",
            claimant="recovery",
            tick=0,
        )
        assert outcome3 == "recovered"
        with pytest.raises(ClaimPendingError):
            await catalog.rearm_claim("w1", empty.scope_key, "expired", "fresh-token")
        rearmed = await catalog.rearm_claim("w1", recovered.scope_key, "recovery", "fresh-token")
        assert rearmed.commit_token != empty.commit_token
        assert rearmed.table_id is None
        await catalog.close()


async def test_command_ledger_and_outbox_parity(tmp_path, worker_url):
    admissions = [
        CommandAdmission(
            command_id="c-a",
            scheduled_tick=0,
            priority=10,
            command_type="spawn",
            payload_json='{"entity_id":1}',
            payload_digest="digest-a",
            version=1,
            principal_id="actor-1",
            origin="gateway",
            reserved_entity_id=1,
        ),
        CommandAdmission(
            command_id="c-b",
            scheduled_tick=0,
            priority=0,
            command_type="custom",
            payload_json="{}",
            payload_digest="digest-b",
            version=1,
            principal_id=None,
            origin="local",
        ),
        CommandAdmission(
            command_id="c-c",
            scheduled_tick=2,
            priority=0,
            command_type="message",
            payload_json="{}",
            payload_digest="digest-c",
            version=1,
            principal_id=None,
            origin="local",
        ),
    ]
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        admitted = await catalog.admit_commands("w1", admissions)
        assert [record.command_id for record in admitted] == ["c-a", "c-b", "c-c"]
        assert await catalog.pending_command_count("w1") == 3
        assert await catalog.max_reserved_entity_id("w1") == 1

        replay = await catalog.admit_commands("w1", [admissions[0]])
        assert replay[0].sequence == admitted[0].sequence
        changed = CommandAdmission(**{**admissions[0].__dict__, "payload_digest": "changed"})
        with pytest.raises(CommandConflictError):
            await catalog.admit_commands("w1", [changed])

        leased = await catalog.lease_commands("w1", 0, "worker-a")
        assert [record.command_id for record in leased] == ["c-b", "c-a"]
        rejected = await catalog.fail_command(
            "w1",
            "c-b",
            "worker-a",
            status="REJECTED",
            error_code="ValueError",
            error_detail="poison",
        )
        assert rejected.status == "REJECTED"
        await catalog.release_commands("w1", ["c-a"], "worker-a")
        (leased_a,) = await catalog.lease_commands("w1", 0, "worker-b")
        assert leased_a.command_id == "c-a" and leased_a.attempts == 1

        epoch = await catalog.acquire_fence("w1", "writer")
        await catalog.publish_manifest(
            "w1",
            "r1",
            0,
            "tick-token",
            epoch,
            ["t1"],
            command_ids=["c-a"],
            lease_owner="worker-b",
        )
        records = await catalog.list_commands("w1", limit=10)
        assert [(record.command_id, record.status) for record in records] == [
            ("c-a", "APPLIED"),
            ("c-b", "REJECTED"),
            ("c-c", "PENDING"),
        ]
        assert records[0].commit_token == "tick-token" and records[0].applied_tick == 0

        events = await catalog.read_outbox("w1")
        assert [event.status for event in events] == [
            "queued",
            "queued",
            "queued",
            "rejected",
            "applied",
        ]
        assert await catalog.outbox_progress("w1") == (0, 5)
        await catalog.mark_outbox_projected("w1", [event.event_id for event in events])
        assert await catalog.outbox_progress("w1") == (5, 0)

        assert await catalog.cancel_commands("w1", reason="destroyed") == 1
        assert await catalog.pending_command_count("w1") == 0
        await catalog.close()


async def test_world_deactivation_rejects_open_and_future_commands(tmp_path, worker_url):
    admission = CommandAdmission(
        command_id="c-before-destroy",
        scheduled_tick=0,
        priority=0,
        command_type="custom",
        payload_json="{}",
        payload_digest="before-destroy",
        version=1,
        principal_id=None,
        origin="local",
    )
    after_destroy = CommandAdmission(
        **{
            **admission.__dict__,
            "command_id": "c-after-destroy",
            "payload_digest": "after-destroy",
        }
    )

    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        await catalog.admit_commands("w1", [admission])

        # Status and cancellation share one catalog transaction. A remote
        # admission racing the transition is therefore either cancelled by
        # it or rejected after it; it cannot remain pending.
        await catalog.set_world_status("w1", "destroyed")
        assert await catalog.pending_command_count("w1") == 0
        (record,) = await catalog.list_commands("w1")
        assert record.status == "REJECTED"
        assert record.last_error_code == "world_destroyed"

        with pytest.raises(CommandConflictError, match="not active"):
            await catalog.admit_commands("w1", [after_destroy])
        with pytest.raises(CommandConflictError, match="not active"):
            await catalog.lease_commands("w1", 0, "worker-after-destroy")
        await catalog.close()


async def test_mission_attempt_claim_lifecycle_parity(tmp_path, worker_url):
    base = {
        "claim_key": "claim-1",
        "world_id": "w1",
        "run_id": "r1",
        "mission_id": "mission-1",
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "idempotency_key": "mission-idempotency-1",
        "request_fingerprint": "request-fingerprint-1",
        "request_json": '{"request_fingerprint":"mission-request-1"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-request-1",
        "supports_idempotent_replay": False,
        "supports_session_resume": True,
        "provider_idempotency_key": "",
    }
    for catalog in await _both(tmp_path, worker_url):
        outcome, claim = await catalog.acquire_attempt_claim(
            **base,
            claimant="worker-1",
            lease_seconds=30.0,
        )
        assert outcome == "acquired"
        assert claim.status == "claimed" and claim.fence_epoch == 1
        assert claim.redaction_policy_id == "redaction-v1"
        assert claim.redaction_evidence_json == '{"phase":"acquired"}'
        assert not hasattr(claim, "redaction_acquisition_evidence_json")

        # A caller-supplied claim key cannot create a second row for the same
        # world/mission/task/attempt identity.
        with pytest.raises(AttemptClaimConflictError):
            await catalog.acquire_attempt_claim(
                **{**base, "claim_key": "claim-shadow"},
                claimant="worker-shadow",
            )

        outcome, owned = await catalog.acquire_attempt_claim(
            **base,
            claimant="worker-1",
            lease_seconds=30.0,
        )
        assert outcome == "owned" and owned.fence_epoch == 1
        with pytest.raises(AttemptClaimPendingError):
            await catalog.acquire_attempt_claim(
                **base,
                claimant="worker-2",
            )
        with pytest.raises(AttemptClaimConflictError):
            await catalog.acquire_attempt_claim(
                **{**base, "request_fingerprint": "changed"},
                claimant="worker-2",
            )
        with pytest.raises(AttemptClaimConflictError):
            await catalog.acquire_attempt_claim(
                **{**base, "redaction_policy_id": "redaction-v2"},
                claimant="worker-1",
            )
        with pytest.raises(AttemptClaimConflictError):
            await catalog.acquire_attempt_claim(
                **{**base, "redaction_evidence_json": '{"phase":"different"}'},
                claimant="worker-1",
            )

        uncertain = await catalog.transition_attempt_claim(
            "w1",
            claim.claim_key,
            "worker-1",
            1,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="execution-lifecycle-1",
            redaction_evidence_json='{"phase":"armed"}',
        )
        assert uncertain.status == "possibly_submitted"
        assert uncertain.redaction_evidence_json == '{"phase":"armed"}'
        assert uncertain.possibly_submitted_at
        with pytest.raises(AttemptClaimConflictError):
            await catalog.transition_attempt_claim(
                "w1",
                claim.claim_key,
                "worker-1",
                1,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="execution-lifecycle-1",
                last_error="conflicting same-target evidence",
            )
        unchanged = await catalog.get_attempt_claim("w1", claim.claim_key)
        assert unchanged is not None and unchanged.last_error == ""
        renewed = await catalog.renew_attempt_claim(
            "w1",
            claim.claim_key,
            "worker-1",
            1,
            lease_seconds=60.0,
        )
        assert renewed.lease_expires_at > time.time()
        acknowledged = await catalog.transition_attempt_claim(
            "w1",
            claim.claim_key,
            "worker-1",
            1,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            provider_session_id="session-1",
            provider_request_id="request-1",
        )
        assert acknowledged.provider_session_id == "session-1"
        settled = await catalog.transition_attempt_claim(
            "w1",
            claim.claim_key,
            "worker-1",
            1,
            expected_status="provider_acknowledged",
            target_status="settled",
            settlement_status="accepted",
            outcome_digest="outcome-1",
            outcome_json='{"status":"accepted"}',
        )
        assert settled.status == "settled" and settled.settled_at

        outcome, duplicate = await catalog.acquire_attempt_claim(
            **base,
            claimant="worker-2",
        )
        assert outcome == "duplicate" and duplicate.outcome_digest == "outcome-1"
        assert duplicate.redaction_evidence_json == '{"phase":"armed"}'
        assert await catalog.list_due_attempt_claims("w1", now=time.time() + 100) == []

        expiring = {**base, "claim_key": "claim-2", "attempt_id": "attempt-2"}
        _, dead = await catalog.acquire_attempt_claim(
            **expiring,
            claimant="dead-worker",
            lease_seconds=0,
        )
        due = await catalog.list_due_attempt_claims("w1", now=time.time() + 1)
        assert [record.claim_key for record in due] == ["claim-2"]
        outcome, recovered = await catalog.acquire_attempt_claim(
            **expiring,
            claimant="recovery-worker",
        )
        assert outcome == "recovered" and recovered.fence_epoch == 2
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                "w1",
                dead.claim_key,
                "dead-worker",
                1,
                expected_status="claimed",
                target_status="settled",
            )
        await catalog.close()


async def test_mission_attempt_claim_same_target_race_parity(tmp_path, worker_url):
    base = {
        "claim_key": "claim-race",
        "world_id": "w-race",
        "run_id": "r-race",
        "mission_id": "mission-race",
        "task_id": "task-race",
        "attempt_id": "attempt-race",
        "idempotency_key": "mission-idempotency-race",
        "request_fingerprint": "request-fingerprint-race",
        "request_json": '{"request_fingerprint":"mission-request-race"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-request-race",
        "supports_idempotent_replay": False,
        "supports_session_resume": False,
        "provider_idempotency_key": "",
    }
    for catalog in await _both(tmp_path, worker_url):
        _, claim = await catalog.acquire_attempt_claim(
            **base,
            claimant="race-worker",
        )

        results = await asyncio.gather(
            catalog.transition_attempt_claim(
                "w-race",
                claim.claim_key,
                "race-worker",
                claim.fence_epoch,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="execution-race-1",
                last_error="evidence-left",
            ),
            catalog.transition_attempt_claim(
                "w-race",
                claim.claim_key,
                "race-worker",
                claim.fence_epoch,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="execution-race-1",
                last_error="evidence-right",
            ),
            return_exceptions=True,
        )
        successes = [result for result in results if not isinstance(result, BaseException)]
        failures = [result for result in results if isinstance(result, BaseException)]
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], AttemptClaimConflictError)

        persisted = await catalog.get_attempt_claim("w-race", claim.claim_key)
        assert persisted is not None
        assert persisted.status == "possibly_submitted"
        assert persisted.last_error == successes[0].last_error
        assert persisted.last_error in {"evidence-left", "evidence-right"}
        await catalog.close()


async def test_mission_attempt_execution_grant_single_consume_parity(tmp_path, worker_url):
    base = {
        "claim_key": "claim-execution-grant",
        "world_id": "w-execution-grant",
        "run_id": "r-execution-grant",
        "mission_id": "mission-execution-grant",
        "task_id": "task-execution-grant",
        "attempt_id": "attempt-execution-grant",
        "idempotency_key": "idempotency-execution-grant",
        "request_fingerprint": "request-execution-grant",
        "request_json": '{"request_fingerprint":"mission-execution-grant"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-execution-grant",
        "supports_idempotent_replay": False,
        "supports_session_resume": False,
        "provider_idempotency_key": "",
    }
    for catalog in await _both(tmp_path, worker_url):
        _, claim = await catalog.acquire_attempt_claim(
            **base,
            claimant="execution-worker",
            lease_seconds=30.0,
        )
        with pytest.raises(ValueError, match="execution nonce"):
            await catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                "execution-worker",
                claim.fence_epoch,
                expected_status="claimed",
                target_status="possibly_submitted",
            )
        armed = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            "execution-worker",
            claim.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="execution-nonce-1",
        )
        assert armed.execution_nonce == "execution-nonce-1"
        assert armed.execution_consumed_at is None
        for claimant, fence_epoch, nonce in (
            ("wrong-worker", claim.fence_epoch, "execution-nonce-1"),
            ("execution-worker", claim.fence_epoch + 1, "execution-nonce-1"),
            ("execution-worker", claim.fence_epoch, "wrong-nonce"),
        ):
            with pytest.raises(AttemptClaimStaleError):
                await catalog.consume_attempt_execution(
                    base["world_id"],
                    claim.claim_key,
                    claimant,
                    fence_epoch,
                    nonce,
                )

        results = await asyncio.gather(
            catalog.consume_attempt_execution(
                base["world_id"],
                claim.claim_key,
                "execution-worker",
                claim.fence_epoch,
                "execution-nonce-1",
            ),
            catalog.consume_attempt_execution(
                base["world_id"],
                claim.claim_key,
                "execution-worker",
                claim.fence_epoch,
                "execution-nonce-1",
            ),
            return_exceptions=True,
        )
        consumed = [result for result in results if not isinstance(result, BaseException)]
        rejected = [result for result in results if isinstance(result, BaseException)]
        assert len(consumed) == 1
        assert consumed[0].execution_consumed_at
        assert len(rejected) == 1
        assert isinstance(rejected[0], AttemptClaimStaleError)

        persisted = await catalog.get_attempt_claim(base["world_id"], claim.claim_key)
        assert persisted is not None
        assert persisted.execution_consumed_at == consumed[0].execution_consumed_at
        settled = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            "execution-worker",
            claim.fence_epoch,
            expected_status="possibly_submitted",
            target_status="settled",
            settlement_status="failed",
            outcome_digest="outcome-execution-grant",
            outcome_json='{"status":"failed"}',
        )
        assert settled.status == "settled"
        assert settled.execution_consumed_at == persisted.execution_consumed_at
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                "execution-worker",
                claim.fence_epoch,
                expected_status="settled",
                target_status="settled",
                settlement_status="failed",
                outcome_digest="changed-outcome",
                outcome_json='{"status":"changed"}',
            )
        with pytest.raises(AttemptClaimStaleError):
            await catalog.consume_attempt_execution(
                base["world_id"],
                claim.claim_key,
                "execution-worker",
                claim.fence_epoch,
                "execution-nonce-1",
            )

        await catalog.close()


async def test_mission_attempt_expired_lease_rejects_every_mutation_parity(
    tmp_path,
    worker_url,
):
    base = {
        "world_id": "w-expired-attempt",
        "run_id": "r-expired-attempt",
        "mission_id": "mission-expired-attempt",
        "task_id": "task-expired-attempt",
        "idempotency_key": "idempotency-expired-attempt",
        "request_fingerprint": "request-expired-attempt",
        "request_json": '{"request_fingerprint":"mission-expired-attempt"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-expired-attempt",
        "supports_idempotent_replay": False,
        "supports_session_resume": False,
        "provider_idempotency_key": "",
    }

    async def acquire(catalog, suffix: str, lease_seconds: float):
        _, claim = await catalog.acquire_attempt_claim(
            **base,
            claim_key=f"claim-{suffix}",
            attempt_id=f"attempt-{suffix}",
            claimant=f"worker-{suffix}",
            lease_seconds=lease_seconds,
        )
        return claim

    for catalog in await _both(tmp_path, worker_url):
        expired_renew = await acquire(catalog, "expired-renew", 0)
        with pytest.raises(AttemptClaimStaleError):
            await catalog.renew_attempt_claim(
                base["world_id"],
                expired_renew.claim_key,
                expired_renew.claimant,
                expired_renew.fence_epoch,
                lease_seconds=30,
            )

        expired_arm = await acquire(catalog, "expired-arm", 0)
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                base["world_id"],
                expired_arm.claim_key,
                expired_arm.claimant,
                expired_arm.fence_epoch,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="nonce-expired-arm",
            )

        expiring_ack = await acquire(catalog, "expired-ack", 1.0)
        expiring_ack = await catalog.transition_attempt_claim(
            base["world_id"],
            expiring_ack.claim_key,
            expiring_ack.claimant,
            expiring_ack.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-expired-ack",
        )
        expiring_settle = await acquire(catalog, "expired-settle", 1.0)
        expiring_settle = await catalog.transition_attempt_claim(
            base["world_id"],
            expiring_settle.claim_key,
            expiring_settle.claimant,
            expiring_settle.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-expired-settle",
        )
        expiring_settle = await catalog.transition_attempt_claim(
            base["world_id"],
            expiring_settle.claim_key,
            expiring_settle.claimant,
            expiring_settle.fence_epoch,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            provider_request_id="request-expired-settle",
        )
        expiring_consume = await acquire(catalog, "expired-consume", 1.0)
        expiring_consume = await catalog.transition_attempt_claim(
            base["world_id"],
            expiring_consume.claim_key,
            expiring_consume.claimant,
            expiring_consume.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-expired-consume",
        )

        deadline = max(
            expiring_ack.lease_expires_at,
            expiring_settle.lease_expires_at,
            expiring_consume.lease_expires_at,
        )
        await asyncio.sleep(max(0.0, deadline - time.time()) + 0.05)

        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                base["world_id"],
                expiring_ack.claim_key,
                expiring_ack.claimant,
                expiring_ack.fence_epoch,
                expected_status="possibly_submitted",
                target_status="provider_acknowledged",
                provider_request_id="request-expired-ack",
            )
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                base["world_id"],
                expiring_settle.claim_key,
                expiring_settle.claimant,
                expiring_settle.fence_epoch,
                expected_status="provider_acknowledged",
                target_status="settled",
                settlement_status="failed",
                outcome_digest="outcome-expired-settle",
                outcome_json='{"status":"failed"}',
            )
        with pytest.raises(AttemptClaimStaleError):
            await catalog.consume_attempt_execution(
                base["world_id"],
                expiring_consume.claim_key,
                expiring_consume.claimant,
                expiring_consume.fence_epoch,
                expiring_consume.execution_nonce,
            )

        unchanged_ack = await catalog.get_attempt_claim(base["world_id"], expiring_ack.claim_key)
        unchanged_settle = await catalog.get_attempt_claim(
            base["world_id"], expiring_settle.claim_key
        )
        assert unchanged_ack is not None and unchanged_ack.status == "possibly_submitted"
        assert unchanged_ack.acknowledged_at is None
        assert unchanged_settle is not None and unchanged_settle.status == "provider_acknowledged"
        assert unchanged_settle.settled_at is None
        await catalog.close()


async def test_artifact_publication_lifecycle_parity(tmp_path, worker_url):
    request_json = '{"world_id":"w1","run_id":"r1"}'
    retry_until_ms = int(time.time() * 1000) + 60_000
    for catalog in await _both(tmp_path, worker_url):
        missing = "missing-publication"
        with pytest.raises(ArtifactPublicationConflictError):
            await catalog.renew_artifact_publication("w1", missing, "nobody", lease_seconds=30.0)
        with pytest.raises(ArtifactPublicationConflictError):
            await catalog.record_artifact_uploads(
                "w1", missing, "nobody", "[]", "s3://bucket/missing"
            )
        with pytest.raises(ArtifactPublicationConflictError):
            await catalog.complete_artifact_publication("w1", missing, "nobody", 1)
        with pytest.raises(ArtifactPublicationConflictError):
            await catalog.expire_artifact_publication("w1", missing, "nobody", "expired")
        # Failure release is deliberately idempotent for a missing row.
        await catalog.fail_artifact_publication(
            "w1", missing, "nobody", "nothing to release", retry_at=0.0
        )

        outcome, publication = await catalog.acquire_artifact_publication(
            world_id="w1",
            run_id="r1",
            attempt_id="a1",
            idempotency_key="bundle-1",
            request_digest="digest-1",
            request_json=request_json,
            claimant="owner-1",
            retry_until_ms=retry_until_ms,
            lease_seconds=30.0,
        )
        assert outcome == "acquired" and publication.status == "PENDING"

        with pytest.raises(ArtifactPublicationPendingError):
            await catalog.acquire_artifact_publication(
                world_id="w1",
                run_id="r1",
                attempt_id="a1",
                idempotency_key="bundle-1",
                request_digest="digest-1",
                request_json=request_json,
                claimant="owner-2",
                retry_until_ms=retry_until_ms,
            )
        with pytest.raises(ArtifactPublicationConflictError):
            await catalog.acquire_artifact_publication(
                world_id="w1",
                run_id="r1",
                attempt_id="a1",
                idempotency_key="bundle-1",
                request_digest="different",
                request_json=request_json,
                claimant="owner-2",
                retry_until_ms=retry_until_ms,
            )

        await catalog.record_artifact_uploads(
            "w1",
            publication.publication_key,
            "owner-1",
            '[{"artifact_id":"x"}]',
            "s3://bucket/manifest",
        )
        await catalog.fail_artifact_publication(
            "w1",
            publication.publication_key,
            "owner-1",
            "index unavailable",
            retry_at=0.0,
        )
        due = await catalog.list_due_artifact_publications("w1", now=time.time(), limit=10)
        assert len(due) == 1 and due[0].status == "UPLOADED"

        outcome, recovered = await catalog.acquire_artifact_publication(
            world_id="w1",
            run_id="r1",
            attempt_id="a1",
            idempotency_key="bundle-1",
            request_digest="digest-1",
            request_json=request_json,
            claimant="reconciler",
            retry_until_ms=retry_until_ms,
        )
        assert outcome == "recovered" and recovered.attempt_count == 2
        renewed = await catalog.renew_artifact_publication(
            "w1",
            recovered.publication_key,
            "reconciler",
            lease_seconds=60.0,
        )
        assert renewed.lease_expires_at > time.time()
        await catalog.complete_artifact_publication(
            "w1", recovered.publication_key, "reconciler", 99
        )
        outcome, duplicate = await catalog.acquire_artifact_publication(
            world_id="w1",
            run_id="r1",
            attempt_id="a1",
            idempotency_key="bundle-1",
            request_digest="digest-1",
            request_json=request_json,
            claimant="later",
            retry_until_ms=retry_until_ms,
        )
        assert outcome == "duplicate"
        assert duplicate.index_snapshot_id == 99
        await catalog.close()


async def test_service_stack_runs_against_remote_catalog(tmp_path, worker_url, monkeypatch):
    """The integration proof: coordinator + ingestion + receipts through the
    remote catalog with zero changes above the protocol."""
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", worker_url)
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", WORKER_TOKEN)

    from archetype.app.container import ServiceContainer
    from archetype.app.evaluation.models import GraderContract, Outcome
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig

    class Probe(Component):
        value: float = 0.0

    c = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await c.world_service.create_world(WorldConfig(name="remote-w"), storage)
        assert world.commit_coordinator is not None
        await c.mutation_service.create_entity(world.world_id, [Probe(value=1.0)])
        await c.simulation_service.step(world.world_id, RunConfig())
        await c.simulation_service.step(world.world_id, RunConfig())
        wid, rid = str(world.world_id), str(world.run_id)

        receipt = await c.artifact_service.publish(
            wid, [Probe(value=42.0)], external_id="remote-artifact-1", producer="probe"
        )
        assert not receipt.duplicate

        eval_receipt = await c.command_gateway.evaluate(
            __import__("archetype.app.gateway.auth.models", fromlist=["ActorCtx"]).ActorCtx(
                id=__import__("uuid_utils").uuid7(), roles={"operator"}
            ),
            wid,
            [Probe],
            contract=GraderContract(grader_id="probe-v1", implementation_version="1"),
            grader=lambda df: Outcome(status="pass", score=1.0),
            evaluation_id="remote-eval-1",
        )
        assert not eval_receipt.duplicate

        # Cold discovery through the remote catalog from a FRESH container.
        await c.shutdown()
        fresh = ServiceContainer()
        try:
            infos = await fresh.world_service.discover_worlds(storage)
            assert wid in [str(i.world_id) for i in infos]
            df = await fresh.query_service.query_components([Probe], wid, rid, storage)
            rows = df.to_pylist()
            assert {r["tick"] for r in rows} >= {0, 1}, "stepped history visible cold"
        finally:
            await fresh.shutdown()
    finally:
        try:
            await c.shutdown()
        except Exception:
            pass
