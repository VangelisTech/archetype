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
import hashlib
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
    ArtifactPublicationExpiredError,
    ArtifactPublicationPendingError,
    AttemptClaimConflictError,
    AttemptClaimPendingError,
    AttemptClaimStaleError,
    CatalogConflictError,
    ClaimConflictError,
    ClaimPendingError,
    CommandAdmission,
    CommandConflictError,
    RecoveryExceptionConflictError,
    RecoverySweepConflictError,
    RecoverySweepPendingError,
    RecoverySweepStaleError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
    artifact_publication_key,
    recovery_exception_key,
    recovery_sweep_key,
)
from archetype.core.interfaces import StaleWriterError

pytestmark = pytest.mark.asyncio

WORKER_DIR = Path(__file__).resolve().parents[2] / "infra" / "control-catalog"
WORKER_TOKEN = "archetype-parity-token"

_INTERRUPTED_LEGACY_WORKER_SOURCE = r"""
interface Env {
  WORLD: DurableObjectNamespace;
  DIRECTORY: DurableObjectNamespace;
  CATALOG_TOKEN: string;
}

const JSON_HEADERS = { "content-type": "application/json" };

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.CATALOG_TOKEN}`) {
      return json({ error: "unauthorized" }, 401);
    }
    const parts = new URL(request.url).pathname.split("/").filter(Boolean);
    if (parts[0] !== "ns" || parts.length < 3) return json({ error: "bad_route" }, 404);
    if (parts[2] === "worlds") return json([]);
    if (parts[2] !== "w" || parts.length < 4) return json({ error: "bad_route" }, 404);
    const stub = env.WORLD.get(env.WORLD.idFromName(`${parts[1]}:${parts[3]}`));
    return stub.fetch(request);
  },
};

export class CatalogDirectoryDO implements DurableObject {
  async fetch(): Promise<Response> {
    return json([]);
  }
}

export class WorldCommitDO implements DurableObject {
  private sql: SqlStorage;

  constructor(state: DurableObjectState) {
    this.sql = state.storage.sql;
    // This is the state left by the old non-atomic migration after every
    // ALTER committed but before its eligibility backfill ran.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS mission_attempt_claims (
        claim_key TEXT PRIMARY KEY, run_id TEXT NOT NULL,
        mission_id TEXT NOT NULL, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL, request_fingerprint TEXT NOT NULL,
        request_json TEXT NOT NULL, redaction_policy_id TEXT NOT NULL DEFAULT '',
        redaction_acquisition_evidence_json TEXT NOT NULL DEFAULT '',
        redaction_evidence_json TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL, provider TEXT NOT NULL,
        provider_request_fingerprint TEXT NOT NULL,
        supports_idempotent_replay INTEGER NOT NULL,
        supports_session_resume INTEGER NOT NULL,
        provider_idempotency_key TEXT NOT NULL,
        claimant TEXT NOT NULL, lease_expires_at REAL NOT NULL,
        fence_epoch INTEGER NOT NULL, execution_nonce TEXT NOT NULL DEFAULT '',
        execution_consumed_at TEXT, provider_session_id TEXT NOT NULL DEFAULT '',
        provider_request_id TEXT NOT NULL DEFAULT '',
        settlement_status TEXT NOT NULL DEFAULT '',
        outcome_digest TEXT NOT NULL DEFAULT '', outcome_json TEXT NOT NULL DEFAULT '',
        artifact_request_json TEXT NOT NULL DEFAULT '',
        artifact_request_digest TEXT NOT NULL DEFAULT '',
        artifact_publication_key TEXT NOT NULL DEFAULT '',
        legacy_unbound_eligible INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        possibly_submitted_at TEXT, acknowledged_at TEXT,
        finalizing_at TEXT, settled_at TEXT
      );
    `);
  }

  async fetch(request: Request): Promise<Response> {
    const route = new URL(request.url).pathname.split("/").filter(Boolean).slice(4);
    if (route[0] !== "seed" || request.method !== "POST") {
      return json({ error: "bad_route" }, 404);
    }
    const now = new Date().toISOString();
    this.sql.exec(
      "INSERT INTO mission_attempt_claims (claim_key, run_id, mission_id, task_id, " +
        "attempt_id, idempotency_key, request_fingerprint, request_json, " +
        "redaction_policy_id, redaction_evidence_json, status, provider, " +
        "provider_request_fingerprint, supports_idempotent_replay, " +
        "supports_session_resume, provider_idempotency_key, claimant, " +
        "lease_expires_at, fence_epoch, settlement_status, outcome_digest, " +
        "outcome_json, created_at, updated_at, acknowledged_at, settled_at) " +
        "VALUES ('claim-v7-interrupted', 'run-v7', 'mission-v7', 'task-v7', " +
        "'attempt-v7', 'idempotency-v7', 'request-v7', " +
        "'{\"required_finalization_phase\":\"indexed\"}', 'redaction-v1', " +
        "'{\"phase\":\"settled\"}', 'settled', 'modal', 'provider-v7', " +
        "0, 1, '', 'worker-v7', 1, 1, 'accepted', 'outcome-v7', " +
        "'{\"finalization_phase\":\"indexed\",\"status\":\"accepted\"}', " +
        "?, ?, ?, ?)",
      now,
      now,
      now,
      now,
    );
    this.sql.exec(
      "INSERT INTO mission_attempt_claims (claim_key, run_id, mission_id, task_id, " +
        "attempt_id, idempotency_key, request_fingerprint, request_json, " +
        "redaction_policy_id, redaction_evidence_json, status, provider, " +
        "provider_request_fingerprint, supports_idempotent_replay, " +
        "supports_session_resume, provider_idempotency_key, claimant, " +
        "lease_expires_at, fence_epoch, settlement_status, outcome_digest, " +
        "outcome_json, created_at, updated_at, acknowledged_at, settled_at) " +
        "VALUES ('claim-v7-nonindexed', 'run-v7', 'mission-v7-nonindexed', " +
        "'task-v7-nonindexed', 'attempt-v7-nonindexed', 'idempotency-v7-nonindexed', " +
        "'request-v7-nonindexed', " +
        "'{\"required_finalization_phase\":\"checkpointed\"}', 'redaction-v1', " +
        "'{\"phase\":\"settled\"}', 'settled', 'modal', 'provider-v7', " +
        "0, 1, '', 'worker-v7', 1, 1, 'accepted', 'outcome-v7-nonindexed', " +
        "'{\"finalization_phase\":\"checkpointed\",\"status\":\"accepted\"}', " +
        "?, ?, ?, ?)",
      now,
      now,
      now,
      now,
    );
    this.sql.exec(
      "INSERT INTO mission_attempt_claims (claim_key, run_id, mission_id, task_id, " +
        "attempt_id, idempotency_key, request_fingerprint, request_json, " +
        "redaction_policy_id, redaction_evidence_json, status, provider, " +
        "provider_request_fingerprint, supports_idempotent_replay, " +
        "supports_session_resume, provider_idempotency_key, claimant, " +
        "lease_expires_at, fence_epoch, settlement_status, outcome_digest, " +
        "outcome_json, created_at, updated_at, acknowledged_at, settled_at) " +
        "VALUES ('claim-v7-explicit-float', 'run-v7', 'mission-v7-explicit-float', " +
        "'task-v7-explicit-float', 'attempt-v7-explicit-float', " +
        "'idempotency-v7-explicit-float', 'request-v7-explicit-float', " +
        "'{\"claim_contract_version\":7.0,\"required_finalization_phase\":\"indexed\"}', " +
        "'redaction-v1', '{\"phase\":\"settled\"}', 'settled', 'modal', " +
        "'provider-v7', 0, 1, '', 'worker-v7', 1, 1, 'accepted', " +
        "'outcome-v7-explicit-float', " +
        "'{\"finalization_phase\":\"published\",\"status\":\"accepted\"}', " +
        "?, ?, ?, ?)",
      now,
      now,
      now,
      now,
    );
    return json({ ok: true });
  }
}
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def worker_url():
    if shutil.which("npx") is None:
        pytest.skip("npx unavailable; wrangler dev harness skipped")
    port = _free_port()
    inspector_port = _free_port()
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
            "--inspector-port",
            str(inspector_port),
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


async def test_worker_recovers_interrupted_legacy_eligibility_backfill(tmp_path):
    """A schema-complete but unbackfilled v7 row is recovered exactly once."""

    if shutil.which("npx") is None:
        pytest.skip("npx unavailable; wrangler migration harness skipped")
    import httpx

    worker_dir = tmp_path / "migration-worker"
    source_dir = worker_dir / "src"
    source_dir.mkdir(parents=True)
    shutil.copyfile(WORKER_DIR / "wrangler.toml", worker_dir / "wrangler.toml")
    source_path = source_dir / "index.ts"
    source_path.write_text(_INTERRUPTED_LEGACY_WORKER_SOURCE)
    persistence = tmp_path / "wrangler-state"

    def start_worker():
        port = _free_port()
        inspector_port = _free_port()
        worker_log = tempfile.TemporaryFile(mode="w+")
        process = subprocess.Popen(
            [
                "npx",
                "--yes",
                "wrangler",
                "dev",
                "--local",
                "--port",
                str(port),
                "--inspector-port",
                str(inspector_port),
                "--persist-to",
                str(persistence),
                "--var",
                f"CATALOG_TOKEN:{WORKER_TOKEN}",
            ],
            cwd=worker_dir,
            stdout=worker_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        url = f"http://127.0.0.1:{port}"
        deadline = time.time() + 120
        while time.time() < deadline:
            if process.poll() is not None:
                break
            try:
                response = httpx.get(
                    f"{url}/ns/probe/worlds",
                    headers={"authorization": f"Bearer {WORKER_TOKEN}"},
                    timeout=2.0,
                )
                if response.status_code == 200:
                    return process, worker_log, url
            except Exception:
                pass
            time.sleep(0.5)
        worker_log.seek(0)
        output = worker_log.read()
        process.terminate()
        process.wait(timeout=10)
        worker_log.close()
        pytest.fail(f"migration worker did not become ready: {output[-1000:]}")

    def stop_worker(process, worker_log):
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        worker_log.close()

    legacy_process, legacy_log, legacy_url = start_worker()
    try:
        seeded = httpx.post(
            f"{legacy_url}/ns/migration/w/world-v7/seed",
            headers={"authorization": f"Bearer {WORKER_TOKEN}"},
            timeout=10.0,
        )
        assert seeded.status_code == 200, seeded.text
    finally:
        stop_worker(legacy_process, legacy_log)

    shutil.copyfile(WORKER_DIR / "src" / "index.ts", source_path)
    upgraded_process, upgraded_log, upgraded_url = start_worker()
    try:
        from archetype.app.storage.remote_catalog import RemoteControlCatalog

        # Use the exact namespace that seeded the persistent Durable Object.
        catalog = RemoteControlCatalog(upgraded_url, "migration", token=WORKER_TOKEN)
        try:
            restored = await catalog.get_attempt_claim("world-v7", "claim-v7-interrupted")
            assert restored is not None
            assert restored.status == "settled"
            assert restored.legacy_unbound_eligible is True
            assert restored.artifact_request_json == ""
            assert restored.artifact_request_digest == ""
            assert restored.artifact_publication_key == ""
            nonindexed = await catalog.get_attempt_claim("world-v7", "claim-v7-nonindexed")
            assert nonindexed is not None
            assert nonindexed.status == "settled"
            assert nonindexed.legacy_unbound_eligible is False
            explicit_float = await catalog.get_attempt_claim("world-v7", "claim-v7-explicit-float")
            assert explicit_float is not None
            assert explicit_float.status == "settled"
            assert explicit_float.legacy_unbound_eligible is False
        finally:
            await catalog.close()
    finally:
        stop_worker(upgraded_process, upgraded_log)


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
        assert claim.legacy_unbound_eligible is False
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
                redaction_evidence_json='{"phase":"changed"}',
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
            redaction_evidence_json='{"phase":"acknowledged"}',
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
            redaction_evidence_json='{"phase":"settled"}',
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
        assert duplicate.redaction_evidence_json == '{"phase":"settled"}'
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
                redaction_evidence_json='{"phase":"settled"}',
                settlement_status="failed",
                outcome_digest="outcome-stale",
                outcome_json='{"status":"failed"}',
            )
        await catalog.close()


async def test_mission_attempt_finalization_outbox_lifecycle_parity(tmp_path, worker_url):
    base = {
        "claim_key": "claim-finalizing",
        "world_id": "w-finalizing",
        "run_id": "r-finalizing",
        "mission_id": "mission-finalizing",
        "task_id": "task-finalizing",
        "attempt_id": "attempt-finalizing",
        "idempotency_key": "idempotency-finalizing",
        "request_fingerprint": "request-finalizing",
        "request_json": '{"request_fingerprint":"mission-finalizing"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-finalizing",
        "supports_idempotent_replay": False,
        "supports_session_resume": True,
        "provider_idempotency_key": "",
    }
    staged = {
        "redaction_evidence_json": '{"phase":"finalizing"}',
        "outcome_digest": "outcome-finalizing",
        "outcome_json": '{"status":"accepted"}',
        "artifact_request_json": '{"attempt_id":"attempt-finalizing"}',
        "artifact_request_digest": "artifact-request-finalizing",
        "artifact_publication_key": "publication-finalizing",
    }
    finalized = {
        "redaction_evidence_json": '{"phase":"settled"}',
        "settlement_status": "accepted",
        "outcome_digest": "outcome-indexed",
        "outcome_json": '{"finalization_phase":"indexed","status":"accepted"}',
    }
    for catalog in await _both(tmp_path, worker_url):
        _, claim = await catalog.acquire_attempt_claim(
            **base,
            claimant="finalizing-worker",
            lease_seconds=30.0,
        )
        armed = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-finalizing",
        )
        acknowledged = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            redaction_evidence_json='{"phase":"acknowledged"}',
            provider_request_id="provider-request-finalizing",
        )
        assert acknowledged.status == "provider_acknowledged"
        assert acknowledged.execution_nonce == armed.execution_nonce

        with pytest.raises(ValueError, match="complete artifact request"):
            await catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="provider_acknowledged",
                target_status="finalizing",
                redaction_evidence_json=staged["redaction_evidence_json"],
                outcome_digest=staged["outcome_digest"],
                outcome_json=staged["outcome_json"],
            )
        finalizing = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="provider_acknowledged",
            target_status="finalizing",
            **staged,
        )
        assert finalizing.status == "finalizing"
        assert finalizing.settlement_status == ""
        assert finalizing.outcome_digest == staged["outcome_digest"]
        assert finalizing.outcome_json == staged["outcome_json"]
        assert finalizing.artifact_request_json == staged["artifact_request_json"]
        assert finalizing.artifact_request_digest == staged["artifact_request_digest"]
        assert finalizing.artifact_publication_key == staged["artifact_publication_key"]
        assert finalizing.finalizing_at
        assert finalizing.settled_at is None

        # A lost success response can replay the exact completed CAS. Changed
        # staging is rejected and cannot mutate the durable outbox.
        replayed = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="provider_acknowledged",
            target_status="finalizing",
            **staged,
        )
        assert replayed == finalizing
        with pytest.raises(AttemptClaimConflictError):
            await catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="provider_acknowledged",
                target_status="finalizing",
                **{**staged, "artifact_request_digest": "changed-request"},
            )

        short_lease = await catalog.renew_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            lease_seconds=0.1,
        )
        await asyncio.sleep(max(0.0, short_lease.lease_expires_at - time.time()) + 0.05)
        due = await catalog.list_due_attempt_claims(base["world_id"], now=time.time())
        assert [record.claim_key for record in due] == [claim.claim_key]
        outcome, recovered = await catalog.acquire_attempt_claim(
            **base,
            claimant="finalizing-recovery-worker",
        )
        assert outcome == "recovered"
        assert recovered.status == "finalizing"
        assert recovered.fence_epoch == claim.fence_epoch + 1
        assert recovered.outcome_json == staged["outcome_json"]
        assert recovered.artifact_request_json == staged["artifact_request_json"]
        assert recovered.finalizing_at == finalizing.finalizing_at

        with pytest.raises(AttemptClaimConflictError):
            await catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                recovered.claimant,
                recovered.fence_epoch,
                expected_status="finalizing",
                target_status="settled",
                **finalized,
                artifact_request_json=staged["artifact_request_json"],
                artifact_request_digest="changed-request",
                artifact_publication_key=staged["artifact_publication_key"],
            )
        settled = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            recovered.claimant,
            recovered.fence_epoch,
            expected_status="finalizing",
            target_status="settled",
            **finalized,
        )
        assert settled.status == "settled"
        assert settled.settled_at
        assert settled.finalizing_at == finalizing.finalizing_at
        assert settled.outcome_digest == finalized["outcome_digest"]
        assert settled.outcome_json == finalized["outcome_json"]
        assert settled.artifact_publication_key == staged["artifact_publication_key"]
        replayed_settlement = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            recovered.claimant,
            recovered.fence_epoch,
            expected_status="finalizing",
            target_status="settled",
            **finalized,
        )
        assert replayed_settlement == settled

        outcome, duplicate = await catalog.acquire_attempt_claim(
            **base,
            claimant="finalizing-duplicate-worker",
        )
        assert outcome == "duplicate"
        assert duplicate == settled
        assert (
            await catalog.list_due_attempt_claims(base["world_id"], now=time.time() + 1_000) == []
        )
        await catalog.close()


async def test_mission_attempt_settlement_receipt_replay_is_exact_parity(tmp_path, worker_url):
    def request(suffix: str) -> dict:
        return {
            "claim_key": f"claim-settlement-exact-{suffix}",
            "world_id": "w-settlement-exact",
            "run_id": "r-settlement-exact",
            "mission_id": "mission-settlement-exact",
            "task_id": "task-settlement-exact",
            "attempt_id": f"attempt-settlement-exact-{suffix}",
            "idempotency_key": f"idempotency-settlement-exact-{suffix}",
            "request_fingerprint": f"request-settlement-exact-{suffix}",
            "request_json": "{}",
            "redaction_policy_id": "redaction-v1",
            "redaction_evidence_json": '{"phase":"acquired"}',
            "provider": "modal",
            "provider_request_fingerprint": f"provider-settlement-exact-{suffix}",
            "supports_idempotent_replay": False,
            "supports_session_resume": False,
            "provider_idempotency_key": "",
        }

    terminal = {
        "redaction_evidence_json": '{"phase":"settled"}',
        "settlement_status": "failed",
        "outcome_digest": "outcome-settlement-exact",
        "outcome_json": '{"status":"failed"}',
    }
    for catalog in await _both(tmp_path, worker_url):
        _, empty_error_claim = await catalog.acquire_attempt_claim(
            **request("empty-error"), claimant="settlement-worker"
        )
        for omitted in terminal:
            with pytest.raises(ValueError, match=omitted):
                await catalog.transition_attempt_claim(
                    empty_error_claim.world_id,
                    empty_error_claim.claim_key,
                    empty_error_claim.claimant,
                    empty_error_claim.fence_epoch,
                    expected_status="claimed",
                    target_status="settled",
                    **{**terminal, omitted: ""},
                )
        with pytest.raises(ValueError, match="complete terminal evidence"):
            await catalog.transition_attempt_claim(
                empty_error_claim.world_id,
                empty_error_claim.claim_key,
                empty_error_claim.claimant,
                empty_error_claim.fence_epoch,
                expected_status="claimed",
                target_status="settled",
            )
        unchanged = await catalog.get_attempt_claim(
            empty_error_claim.world_id, empty_error_claim.claim_key
        )
        assert unchanged is not None and unchanged.status == "claimed"

        settled_empty = await catalog.transition_attempt_claim(
            empty_error_claim.world_id,
            empty_error_claim.claim_key,
            empty_error_claim.claimant,
            empty_error_claim.fence_epoch,
            expected_status="claimed",
            target_status="settled",
            **terminal,
            last_error="",
        )
        replayed_empty = await catalog.transition_attempt_claim(
            empty_error_claim.world_id,
            empty_error_claim.claim_key,
            empty_error_claim.claimant,
            empty_error_claim.fence_epoch,
            expected_status="claimed",
            target_status="settled",
            **terminal,
            last_error="",
        )
        assert replayed_empty == settled_empty
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                empty_error_claim.world_id,
                empty_error_claim.claim_key,
                empty_error_claim.claimant,
                empty_error_claim.fence_epoch,
                expected_status="possibly_submitted",
                target_status="settled",
                **terminal,
                last_error="",
            )

        _, error_claim = await catalog.acquire_attempt_claim(
            **request("with-error"), claimant="settlement-worker"
        )
        settled_error = await catalog.transition_attempt_claim(
            error_claim.world_id,
            error_claim.claim_key,
            error_claim.claimant,
            error_claim.fence_epoch,
            expected_status="claimed",
            target_status="settled",
            **terminal,
            last_error="winner error",
        )
        assert settled_error.last_error == "winner error"
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                error_claim.world_id,
                error_claim.claim_key,
                error_claim.claimant,
                error_claim.fence_epoch,
                expected_status="claimed",
                target_status="settled",
                **terminal,
                last_error="",
            )
        replayed_error = await catalog.transition_attempt_claim(
            error_claim.world_id,
            error_claim.claim_key,
            error_claim.claimant,
            error_claim.fence_epoch,
            expected_status="claimed",
            target_status="settled",
            **terminal,
            last_error="winner error",
        )
        assert replayed_error == settled_error
        await catalog.close()


async def test_attempt_claim_catalog_rejects_illegal_edges_and_misplaced_evidence_parity(
    tmp_path,
    worker_url,
):
    request = {
        "claim_key": "claim-edge-validation",
        "world_id": "w-edge-validation",
        "run_id": "r-edge-validation",
        "mission_id": "mission-edge-validation",
        "task_id": "task-edge-validation",
        "attempt_id": "attempt-edge-validation",
        "idempotency_key": "idempotency-edge-validation",
        "request_fingerprint": "request-edge-validation",
        "request_json": "{}",
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-edge-validation",
        "supports_idempotent_replay": False,
        "supports_session_resume": False,
        "provider_idempotency_key": "",
    }
    for catalog in await _both(tmp_path, worker_url):
        _, claim = await catalog.acquire_attempt_claim(**request, claimant="edge-worker")
        with pytest.raises(ValueError, match="illegal attempt claim transition"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="claimed",
                target_status="provider_acknowledged",
                redaction_evidence_json='{"phase":"acknowledged"}',
                provider_request_id="provider-request",
            )
        with pytest.raises(ValueError, match="provider identity"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="nonce-edge-validation",
                provider_request_id="too-early",
            )
        armed = await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-edge-validation",
        )
        with pytest.raises(ValueError, match="redaction evidence"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="possibly_submitted",
                target_status="provider_acknowledged",
                provider_request_id="provider-request",
            )
        with pytest.raises(ValueError, match="provider identity"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="possibly_submitted",
                target_status="provider_acknowledged",
                redaction_evidence_json='{"phase":"acknowledged"}',
            )
        misplaced = {
            "settlement_status": "failed",
            "outcome_digest": "outcome-too-early",
            "outcome_json": "{}",
            "artifact_request_json": "{}",
            "last_error": "too early",
        }
        for field, value in misplaced.items():
            with pytest.raises(ValueError, match="terminal evidence"):
                await catalog.transition_attempt_claim(
                    claim.world_id,
                    claim.claim_key,
                    claim.claimant,
                    claim.fence_epoch,
                    expected_status="possibly_submitted",
                    target_status="provider_acknowledged",
                    redaction_evidence_json='{"phase":"acknowledged"}',
                    provider_request_id="provider-request",
                    **{field: value},
                )
        acknowledged = await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            redaction_evidence_json='{"phase":"acknowledged"}',
            provider_request_id="provider-request",
        )
        staged = {
            "redaction_evidence_json": '{"phase":"finalizing"}',
            "outcome_digest": "outcome-staged",
            "outcome_json": '{"finalization_phase":"checkpointed"}',
            "artifact_request_json": "{}",
            "artifact_request_digest": "artifact-request",
            "artifact_publication_key": "artifact-publication",
        }
        with pytest.raises(ValueError, match="provider identity"):
            await catalog.transition_attempt_claim(
                acknowledged.world_id,
                acknowledged.claim_key,
                acknowledged.claimant,
                acknowledged.fence_epoch,
                expected_status="provider_acknowledged",
                target_status="finalizing",
                provider_request_id="overwrite",
                **staged,
            )
        unchanged = await catalog.get_attempt_claim(claim.world_id, claim.claim_key)
        assert unchanged == acknowledged
        assert unchanged.provider_request_id == "provider-request"
        assert unchanged.status == "provider_acknowledged"
        assert armed.execution_nonce == "nonce-edge-validation"
        await catalog.close()


async def test_mission_attempt_finalizing_cannot_settle_provisional_evidence_parity(
    tmp_path,
    worker_url,
):
    base = {
        "claim_key": "claim-finalizing-completeness",
        "world_id": "w-finalizing-completeness",
        "run_id": "r-finalizing-completeness",
        "mission_id": "mission-finalizing-completeness",
        "task_id": "task-finalizing-completeness",
        "attempt_id": "attempt-finalizing-completeness",
        "idempotency_key": "idempotency-finalizing-completeness",
        "request_fingerprint": "request-finalizing-completeness",
        "request_json": "{}",
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-finalizing-completeness",
        "supports_idempotent_replay": False,
        "supports_session_resume": False,
        "provider_idempotency_key": "",
    }
    staged = {
        "redaction_evidence_json": '{"phase":"finalizing"}',
        "outcome_digest": "outcome-provisional",
        "outcome_json": '{"finalization_phase":"checkpointed"}',
        "artifact_request_json": '{"attempt_id":"attempt-finalizing-completeness"}',
        "artifact_request_digest": "artifact-request-finalizing-completeness",
        "artifact_publication_key": "publication-finalizing-completeness",
    }
    terminal = {
        "redaction_evidence_json": '{"phase":"settled"}',
        "settlement_status": "accepted",
        "outcome_digest": "outcome-indexed",
        "outcome_json": '{"finalization_phase":"indexed"}',
    }
    for catalog in await _both(tmp_path, worker_url):
        _, claim = await catalog.acquire_attempt_claim(**base, claimant="finalizing-worker")
        await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-finalizing-completeness",
        )
        await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            redaction_evidence_json='{"phase":"acknowledged"}',
            provider_request_id="provider-finalizing-completeness",
        )
        with pytest.raises(ValueError, match="illegal attempt claim transition"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="claimed",
                target_status="finalizing",
                **staged,
            )
        finalizing = await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="provider_acknowledged",
            target_status="finalizing",
            **staged,
        )
        with pytest.raises(ValueError, match="outcome_digest"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="finalizing",
                target_status="settled",
                redaction_evidence_json=terminal["redaction_evidence_json"],
                settlement_status=terminal["settlement_status"],
                outcome_json=terminal["outcome_json"],
            )
        with pytest.raises(AttemptClaimConflictError, match="replace provisional outcome"):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="finalizing",
                target_status="settled",
                redaction_evidence_json=terminal["redaction_evidence_json"],
                settlement_status=terminal["settlement_status"],
                outcome_digest=staged["outcome_digest"],
                outcome_json=staged["outcome_json"],
            )
        unchanged = await catalog.get_attempt_claim(claim.world_id, claim.claim_key)
        assert unchanged == finalizing
        settled = await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="finalizing",
            target_status="settled",
            **terminal,
        )
        with pytest.raises(AttemptClaimStaleError):
            await catalog.transition_attempt_claim(
                claim.world_id,
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status="provider_acknowledged",
                target_status="settled",
                **terminal,
            )
        assert (await catalog.get_attempt_claim(claim.world_id, claim.claim_key)) == settled
        await catalog.close()


async def test_mission_attempt_finalization_staging_race_parity(tmp_path, worker_url):
    base = {
        "claim_key": "claim-finalizing-race",
        "world_id": "w-finalizing-race",
        "run_id": "r-finalizing-race",
        "mission_id": "mission-finalizing-race",
        "task_id": "task-finalizing-race",
        "attempt_id": "attempt-finalizing-race",
        "idempotency_key": "idempotency-finalizing-race",
        "request_fingerprint": "request-finalizing-race",
        "request_json": '{"request_fingerprint":"mission-finalizing-race"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"acquired"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-finalizing-race",
        "supports_idempotent_replay": False,
        "supports_session_resume": False,
        "provider_idempotency_key": "",
    }
    for catalog in await _both(tmp_path, worker_url):
        _, claim = await catalog.acquire_attempt_claim(**base, claimant="finalizing-race-worker")
        await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-finalizing-race",
        )
        acknowledged = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            redaction_evidence_json='{"phase":"acknowledged"}',
            provider_request_id="provider-finalizing-race",
        )
        common = {
            "redaction_evidence_json": '{"phase":"finalizing"}',
            "outcome_digest": "outcome-finalizing-race",
            "outcome_json": '{"status":"accepted"}',
        }
        left = {
            **common,
            "artifact_request_json": '{"winner":"left"}',
            "artifact_request_digest": "request-left",
            "artifact_publication_key": "publication-left",
        }
        right = {
            **common,
            "artifact_request_json": '{"winner":"right"}',
            "artifact_request_digest": "request-right",
            "artifact_publication_key": "publication-right",
        }
        results = await asyncio.gather(
            catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status=acknowledged.status,
                target_status="finalizing",
                **left,
            ),
            catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                claim.claimant,
                claim.fence_epoch,
                expected_status=acknowledged.status,
                target_status="finalizing",
                **right,
            ),
            return_exceptions=True,
        )
        successes = [result for result in results if not isinstance(result, BaseException)]
        failures = [result for result in results if isinstance(result, BaseException)]
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], AttemptClaimConflictError)
        persisted = await catalog.get_attempt_claim(base["world_id"], claim.claim_key)
        assert persisted == successes[0]
        assert persisted is not None and persisted.status == "finalizing"
        assert (
            persisted.artifact_request_digest,
            persisted.artifact_publication_key,
        ) in {
            ("request-left", "publication-left"),
            ("request-right", "publication-right"),
        }
        winner = left if persisted.artifact_request_digest == "request-left" else right
        exact_replay = await catalog.transition_attempt_claim(
            base["world_id"],
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status=acknowledged.status,
            target_status="finalizing",
            **winner,
        )
        assert exact_replay == persisted
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
                redaction_evidence_json='{"winner":"left"}',
            ),
            catalog.transition_attempt_claim(
                "w-race",
                claim.claim_key,
                "race-worker",
                claim.fence_epoch,
                expected_status="claimed",
                target_status="possibly_submitted",
                execution_nonce="execution-race-1",
                redaction_evidence_json='{"winner":"right"}',
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
        assert persisted.redaction_evidence_json == successes[0].redaction_evidence_json
        assert persisted.redaction_evidence_json in {'{"winner":"left"}', '{"winner":"right"}'}
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
            redaction_evidence_json='{"phase":"settled"}',
            settlement_status="failed",
            outcome_digest="outcome-execution-grant",
            outcome_json='{"status":"failed"}',
        )
        assert settled.status == "settled"
        assert settled.execution_consumed_at == persisted.execution_consumed_at
        with pytest.raises(ValueError, match="illegal attempt claim transition"):
            await catalog.transition_attempt_claim(
                base["world_id"],
                claim.claim_key,
                "execution-worker",
                claim.fence_epoch,
                expected_status="settled",
                target_status="settled",
                redaction_evidence_json='{"phase":"settled"}',
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
            redaction_evidence_json='{"phase":"acknowledged"}',
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
                redaction_evidence_json='{"phase":"acknowledged"}',
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
                redaction_evidence_json='{"phase":"settled"}',
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
    for catalog in await _both(tmp_path, worker_url):
        missing = "f" * 64
        for invalid_snapshot in (True, 1.5, "1", 0, -1, 1 << 63):
            with pytest.raises(ValueError, match="positive integer"):
                await catalog.complete_artifact_publication(
                    "w1", missing, "nobody", invalid_snapshot
                )
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
            "w1", missing, "nobody", "nothing to release", retry_delay_ms=0
        )

        outcome, publication = await catalog.acquire_artifact_publication(
            world_id="w1",
            run_id="r1",
            attempt_id="a1",
            idempotency_key="bundle-1",
            request_digest="digest-1",
            request_json=request_json,
            claimant="owner-1",
            retry_window_ms=60_000,
            lease_ms=30_000,
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
                retry_window_ms=60_000,
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
                retry_window_ms=60_000,
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
            retry_delay_ms=0,
        )
        due = await catalog.list_due_artifact_publications("w1", limit=10)
        assert [candidate.publication_key for candidate in due] == [publication.publication_key]

        outcome, recovered = await catalog.recover_artifact_publication(
            "w1",
            publication.publication_key,
            "reconciler",
            lease_ms=900_000,
        )
        assert recovered is not None
        assert outcome == "recovered" and recovered.attempt_count == 2
        renewed = await catalog.renew_artifact_publication(
            "w1",
            recovered.publication_key,
            "reconciler",
            lease_seconds=60.0,
        )
        assert renewed.lease_expires_at > time.time()
        snapshot_id = 8_123_456_789_012_345_678
        await catalog.complete_artifact_publication(
            "w1", recovered.publication_key, "reconciler", snapshot_id
        )
        await catalog.complete_artifact_publication(
            "w1", recovered.publication_key, "later", snapshot_id
        )
        with pytest.raises(ArtifactPublicationConflictError, match=str(snapshot_id + 1)):
            await catalog.complete_artifact_publication(
                "w1", recovered.publication_key, "later", snapshot_id + 1
            )
        outcome, duplicate = await catalog.acquire_artifact_publication(
            world_id="w1",
            run_id="r1",
            attempt_id="a1",
            idempotency_key="bundle-1",
            request_digest="digest-1",
            request_json=request_json,
            claimant="later",
            retry_window_ms=60_000,
        )
        assert outcome == "duplicate"
        assert duplicate.index_snapshot_id == snapshot_id
        await catalog.close()


async def test_due_artifact_publication_cursor_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        try:
            publications = []
            for index in range(3):
                _, publication = await catalog.acquire_artifact_publication(
                    world_id="w-cursor",
                    run_id="r-cursor",
                    attempt_id=f"a-{index}",
                    idempotency_key=f"bundle-{index}",
                    request_digest=f"digest-{index}",
                    request_json="{}",
                    claimant=f"owner-{index}",
                    retry_window_ms=60_000,
                    lease_ms=60_000,
                )
                await catalog.fail_artifact_publication(
                    "w-cursor",
                    publication.publication_key,
                    f"owner-{index}",
                    "make publication immediately due",
                    retry_delay_ms=0,
                )
                publications.append(publication)

            expected_keys = sorted(row.publication_key for row in publications)
            first = await catalog.list_due_artifact_publications("w-cursor", limit=2)
            second = await catalog.list_due_artifact_publications(
                "w-cursor",
                limit=2,
                after_publication_key=first[-1].publication_key,
            )
            assert [row.publication_key for row in first] == expected_keys[:2]
            assert [row.publication_key for row in second] == expected_keys[2:]
            with pytest.raises(ValueError, match="lowercase SHA-256"):
                await catalog.list_due_artifact_publications(
                    "w-cursor",
                    after_publication_key="raw-publication-id",
                )
        finally:
            await catalog.close()


async def test_exact_artifact_recovery_state_machine_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        try:
            outcome, missing = await catalog.recover_artifact_publication(
                "w-exact", "f" * 64, "worker", lease_ms=100
            )
            assert outcome == "obsolete" and missing is None

            _, publication = await catalog.acquire_artifact_publication(
                world_id="w-exact",
                run_id="r-exact",
                attempt_id="a-owned",
                idempotency_key="same-owner",
                request_digest="digest-owned",
                request_json="{}",
                claimant="owner",
                retry_window_ms=10_000,
                lease_ms=10_000,
            )
            outcome, owned = await catalog.recover_artifact_publication(
                "w-exact", publication.publication_key, "owner", lease_ms=10_000
            )
            assert outcome == "owned" and owned is not None
            assert owned.attempt_count == 1
            assert owned.lease_expires_at > publication.lease_expires_at
            with pytest.raises(ArtifactPublicationPendingError):
                await catalog.recover_artifact_publication(
                    "w-exact", publication.publication_key, "other", lease_ms=100
                )
            await catalog.fail_artifact_publication(
                "w-exact",
                publication.publication_key,
                "owner",
                "release owned lease",
                retry_delay_ms=0,
            )
            outcome, recovered = await catalog.recover_artifact_publication(
                "w-exact", publication.publication_key, "other", lease_ms=500
            )
            assert outcome == "recovered" and recovered is not None
            assert recovered.attempt_count == 2

            _, deadline = await catalog.acquire_artifact_publication(
                world_id="w-exact",
                run_id="r-exact",
                attempt_id="a-deadline",
                idempotency_key="deadline-before-live-owner",
                request_digest="digest-deadline",
                request_json="{}",
                claimant="deadline-owner",
                retry_window_ms=25,
                lease_ms=1_000,
            )
            await asyncio.sleep(0.05)
            outcome, expired = await catalog.recover_artifact_publication(
                "w-exact", deadline.publication_key, "different-owner", lease_ms=100
            )
            assert outcome == "expired" and expired is not None
            assert expired.status == "EXPIRED"

            _, uploaded = await catalog.acquire_artifact_publication(
                world_id="w-exact",
                run_id="r-exact",
                attempt_id="a-uploaded",
                idempotency_key="uploaded-past-deadline",
                request_digest="digest-uploaded",
                request_json="{}",
                claimant="uploader",
                retry_window_ms=50,
                lease_ms=500,
            )
            await catalog.record_artifact_uploads(
                "w-exact", uploaded.publication_key, "uploader", "[]", "file:///manifest"
            )
            await catalog.fail_artifact_publication(
                "w-exact",
                uploaded.publication_key,
                "uploader",
                "release uploaded lease",
                retry_delay_ms=0,
            )
            await asyncio.sleep(0.06)
            outcome, uploaded_recovery = await catalog.recover_artifact_publication(
                "w-exact", uploaded.publication_key, "indexer", lease_ms=500
            )
            assert outcome == "recovered" and uploaded_recovery is not None
            assert uploaded_recovery.status == "UPLOADED"
            await catalog.complete_artifact_publication(
                "w-exact", uploaded.publication_key, "indexer", 42
            )
            outcome, duplicate = await catalog.recover_artifact_publication(
                "w-exact", uploaded.publication_key, "later", lease_ms=100
            )
            assert outcome == "duplicate" and duplicate is not None

            _, explicitly_expired = await catalog.acquire_artifact_publication(
                world_id="w-exact",
                run_id="r-exact",
                attempt_id="a-explicit",
                idempotency_key="explicit-expiry",
                request_digest="digest-explicit",
                request_json="{}",
                claimant="expirer",
                retry_window_ms=10_000,
                lease_ms=500,
            )
            await catalog.expire_artifact_publication(
                "w-exact", explicitly_expired.publication_key, "expirer", "manual"
            )
            outcome, terminal = await catalog.recover_artifact_publication(
                "w-exact", explicitly_expired.publication_key, "later", lease_ms=100
            )
            assert outcome == "expired" and terminal is not None
        finally:
            await catalog.close()


async def test_artifact_source_mutations_require_live_lease_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        try:
            _, pending = await catalog.acquire_artifact_publication(
                world_id="w-source-fence",
                run_id="r1",
                attempt_id="a-pending",
                idempotency_key="stale-pending",
                request_digest="digest-pending",
                request_json="{}",
                claimant="owner",
                retry_window_ms=10_000,
                lease_ms=40,
            )
            await asyncio.sleep(0.07)
            with pytest.raises(ArtifactPublicationPendingError):
                await catalog.renew_artifact_publication(
                    "w-source-fence",
                    pending.publication_key,
                    "owner",
                    lease_seconds=1.0,
                )
            with pytest.raises(ArtifactPublicationPendingError):
                await catalog.record_artifact_uploads(
                    "w-source-fence", pending.publication_key, "owner", "[]", "file:///m"
                )
            with pytest.raises(ArtifactPublicationPendingError):
                await catalog.fail_artifact_publication(
                    "w-source-fence",
                    pending.publication_key,
                    "owner",
                    "late",
                    retry_delay_ms=0,
                )
            with pytest.raises(ArtifactPublicationPendingError):
                await catalog.expire_artifact_publication(
                    "w-source-fence", pending.publication_key, "owner", "late"
                )

            _, uploaded = await catalog.acquire_artifact_publication(
                world_id="w-source-fence",
                run_id="r1",
                attempt_id="a-uploaded",
                idempotency_key="stale-complete",
                request_digest="digest-uploaded",
                request_json="{}",
                claimant="owner",
                retry_window_ms=10_000,
                lease_ms=40,
            )
            await catalog.record_artifact_uploads(
                "w-source-fence", uploaded.publication_key, "owner", "[]", "file:///m"
            )
            await asyncio.sleep(0.07)
            with pytest.raises(ArtifactPublicationPendingError):
                await catalog.complete_artifact_publication(
                    "w-source-fence", uploaded.publication_key, "owner", 42
                )

            _, deadline = await catalog.acquire_artifact_publication(
                world_id="w-source-fence",
                run_id="r1",
                attempt_id="a-deadline",
                idempotency_key="deadline-upload",
                request_digest="digest-deadline",
                request_json="{}",
                claimant="owner",
                retry_window_ms=25,
                lease_ms=500,
            )
            await asyncio.sleep(0.05)
            with pytest.raises(ArtifactPublicationExpiredError):
                await catalog.record_artifact_uploads(
                    "w-source-fence", deadline.publication_key, "owner", "[]", "file:///m"
                )
            expired = await catalog.get_artifact_publication(
                "w-source-fence", deadline.publication_key
            )
            assert expired is not None and expired.status == "EXPIRED"
        finally:
            await catalog.close()


async def test_artifact_publication_key_unicode_parity(tmp_path, worker_url):
    world_id = "wörld-🚀"
    run_id = "rün-雪"
    idempotency_key = "bundlé-🧪"
    expected = artifact_publication_key(world_id, run_id, idempotency_key)
    for catalog in await _both(tmp_path, worker_url):
        try:
            outcome, publication = await catalog.acquire_artifact_publication(
                world_id=world_id,
                run_id=run_id,
                attempt_id="attempt-unicode",
                idempotency_key=idempotency_key,
                request_digest="digest-unicode",
                request_json="{}",
                claimant="owner",
                retry_window_ms=10_000,
                lease_ms=500,
            )
            assert outcome == "acquired"
            assert publication.publication_key == expected
        finally:
            await catalog.close()


async def test_worker_rejects_open_or_lossy_artifact_mutation_bodies(worker_url):
    catalog = _remote(worker_url)
    try:
        _, publication = await catalog.acquire_artifact_publication(
            world_id="w-mutation-wire",
            run_id="r1",
            attempt_id="a1",
            idempotency_key="mutation-wire",
            request_digest="digest",
            request_json="{}",
            claimant="owner",
            retry_window_ms=10_000,
            lease_ms=1_000,
        )
        base = (
            f"{catalog._base}/w/w-mutation-wire/artifact-publications/{publication.publication_key}"
        )
        invalid_renewals = [
            await catalog._client.post(
                f"{base}/renew-v2",
                json={"claimant": "owner", "lease_seconds": value},
            )
            for value in ("1", True, 0, -1, 86_401)
        ]
        open_renewal = await catalog._client.post(
            f"{base}/renew-v2",
            json={"claimant": "owner", "lease_seconds": 1, "now": time.time()},
        )
        open_upload = await catalog._client.post(
            f"{base}/uploads-v2",
            json={
                "claimant": "owner",
                "records_json": "[]",
                "manifest_uri": "file:///m",
                "request_json": "{}",
            },
        )
        open_completion = await catalog._client.post(
            f"{base}/complete-v2",
            json={"claimant": "owner", "index_snapshot_id": "1", "retry_at": 0},
        )
        open_expiry = await catalog._client.post(
            f"{base}/expire-v2",
            json={"claimant": "owner", "error": "manual", "operator": True},
        )

        assert all(response.status_code == 400 for response in invalid_renewals)
        assert open_renewal.status_code == 400
        assert open_upload.status_code == 400
        assert open_completion.status_code == 400
        assert open_expiry.status_code == 400
    finally:
        await catalog.close()


async def test_worker_rejects_raw_durable_cursors(worker_url):
    catalog = _remote(worker_url)
    try:
        await catalog.register_world(_world("w-cursor-validation"))
        await catalog.ensure_recovery_sweep(
            hashlib.sha256(b"cursor-validation-storage").hexdigest(),
            "w-cursor-validation",
            "artifact_publication",
            max_consecutive_failures=2,
        )
        _, sweep = await catalog.lease_recovery_sweep(
            "w-cursor-validation",
            "artifact_publication",
            "worker",
            lease_ms=10_000,
        )
        invalid_fence_responses = [
            await catalog._client.post(
                f"{catalog._base}/w/w-cursor-validation/recovery/sweeps/checkpoint-v1",
                json={
                    "kind": "artifact_publication",
                    "claimant": "worker",
                    "fence_epoch": invalid_fence,
                    "cursor": "",
                    "active_subject_key": "",
                },
            )
            for invalid_fence in (True, 1.5, "1", -1, 1 << 53, 1 << 63)
        ]
        checkpoint = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/recovery/sweeps/checkpoint-v1",
            json={
                "kind": "artifact_publication",
                "claimant": "worker",
                "fence_epoch": sweep.fence_epoch,
                "cursor": "raw-page-token",
                "active_subject_key": "",
            },
        )
        invalid_identity_responses = [
            await catalog._client.post(
                f"{catalog._base}/w/w-cursor-validation/recovery/sweeps/checkpoint-v1",
                json={
                    "kind": invalid_kind,
                    "claimant": invalid_claimant,
                    "fence_epoch": sweep.fence_epoch,
                    "cursor": "",
                    "active_subject_key": "",
                },
            )
            for invalid_kind, invalid_claimant in (
                (1, "worker"),
                ("eighth_recovery_kind", "worker"),
                ("artifact_publication", 1),
            )
        ]
        invalid_error_responses = [
            await catalog._client.post(
                f"{catalog._base}/w/w-cursor-validation/recovery/sweeps/fail-v1",
                json={
                    "kind": "artifact_publication",
                    "claimant": "worker",
                    "fence_epoch": sweep.fence_epoch,
                    "error_code": error_code,
                    "error_detail": error_detail,
                    "retry_delay_ms": 0,
                },
            )
            for error_code, error_detail in (
                (1, ""),
                ("handler_failed", 1),
                ("handler_failed", None),
                ("failed", ""),
            )
        ]
        invalid_permanent = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/recovery/exceptions/retry-v1",
            json={
                "kind": "artifact_publication",
                "claimant": "worker",
                "fence_epoch": sweep.fence_epoch,
                "subject_key": hashlib.sha256(b"invalid-permanent-subject").hexdigest(),
                "authority_key": hashlib.sha256(b"invalid-permanent-authority").hexdigest(),
                "expected_attempt_count": 0,
                "error_code": "handler_failed",
                "error_detail": "",
                "retry_delay_ms": 0,
                "max_attempts": 3,
                "permanent": 1,
            },
        )
        sensitive_claimant = "sensitive-worker-label"
        stale = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/recovery/sweeps/checkpoint-v1",
            json={
                "kind": "artifact_publication",
                "claimant": sensitive_claimant,
                "fence_epoch": sweep.fence_epoch,
                "cursor": "",
                "active_subject_key": "",
            },
        )
        publication_raw_cursor = await catalog._client.get(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/due-v1",
            params={
                "limit": 10,
                "after_publication_key": "raw-publication-id",
            },
        )
        publication_caller_clock = await catalog._client.get(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/due-v1",
            params={"due": time.time(), "limit": 10},
        )
        publication_noncanonical_limit = await catalog._client.get(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/due-v1",
            params={"limit": "1e2"},
        )
        raw_publication_key = artifact_publication_key(
            "w-cursor-validation", "raw-run", "raw-idempotency"
        )
        acquisition_with_caller_clock = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/acquire-v3",
            json={
                "publication_key": raw_publication_key,
                "run_id": "raw-run",
                "attempt_id": "raw-attempt",
                "idempotency_key": "raw-idempotency",
                "request_digest": "raw-digest",
                "request_json": "{}",
                "claimant": "raw-worker",
                "retry_window_ms": 1_000,
                "lease_ms": 100,
                "retry_until_ms": int(time.time() * 1000) + 1_000,
            },
        )
        recovery_with_source_echo = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/{'f' * 64}/recover-v1",
            json={"claimant": "raw-worker", "lease_ms": 100, "request_json": "{}"},
        )
        failure_with_caller_clock = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/{'f' * 64}/fail-v3",
            json={
                "claimant": "raw-worker",
                "error": "failed",
                "retry_delay_ms": 0,
                "retry_at": time.time(),
            },
        )
        zero_lease_recovery = await catalog._client.post(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications/{'f' * 64}/recover-v1",
            json={"claimant": "raw-worker", "lease_ms": 0},
        )
        legacy_due = await catalog._client.get(
            f"{catalog._base}/w/w-cursor-validation/artifact-publications",
            params={"due": time.time()},
        )
        assert all(response.status_code == 400 for response in invalid_fence_responses)
        assert all(response.status_code == 400 for response in invalid_identity_responses)
        assert all(response.status_code == 400 for response in invalid_error_responses)
        assert invalid_permanent.status_code == 400
        assert checkpoint.status_code == 400
        assert publication_raw_cursor.status_code == 400
        assert publication_caller_clock.status_code == 400
        assert publication_noncanonical_limit.status_code == 400
        assert acquisition_with_caller_clock.status_code == 400
        assert recovery_with_source_echo.status_code == 400
        assert failure_with_caller_clock.status_code == 400
        assert zero_lease_recovery.status_code == 400
        assert legacy_due.status_code == 426
        assert stale.status_code == 412
        assert sensitive_claimant not in stale.text
        assert "lowercase SHA-256" in checkpoint.json()["message"]
        assert "lowercase SHA-256" in publication_raw_cursor.json()["message"]
        assert "does not accept a caller clock" in publication_caller_clock.json()["message"]
        assert "canonical decimal text" in publication_noncanonical_limit.json()["message"]
    finally:
        await catalog.close()


async def test_worker_retry_hashes_before_lease_authority_and_never_yields_afterward():
    source = (WORKER_DIR / "src" / "index.ts").read_text()
    start = source.index('if (operation === "retry-v1")')
    end = source.index("const exceptionKey = recoverySha256(p.exception_key", start)
    retry_block = source[start:end]
    hash_position = retry_block.index("const exceptionKey = await recoveryKey")
    clock_position = retry_block.index("const nowMs = Date.now()")
    authority_position = retry_block.index("const live = this.liveRecoverySweep")

    assert hash_position < clock_position < authority_position
    assert "await " not in retry_block[clock_position:]


async def test_worker_artifact_key_hashes_before_clock_and_due_is_digest_only():
    source = (WORKER_DIR / "src" / "index.ts").read_text()
    start = source.index('route[1] === "acquire-v3"')
    end = source.index('route[1] === "due-v1"', start)
    acquire_block = source[start:end]
    hash_position = acquire_block.index("await artifactPublicationKey")
    clock_position = acquire_block.index("Date.now()", hash_position)

    assert hash_position < clock_position
    assert "await " not in acquire_block[clock_position:]
    assert "SELECT publication_key FROM artifact_publications" in source
    assert "function artifactPublicationAuthorityError(" in source
    assert source.count("artifactPublicationAuthorityError(row,") >= 3


async def test_worker_guards_closed_recovery_kinds_and_portable_lease_counters():
    source = (WORKER_DIR / "src" / "index.ts").read_text()
    lease_start = source.index('if (operation === "lease-v1")')
    lease_end = source.index("const updated = this.sql.exec(", lease_start)
    lease_guard = source[lease_start:lease_end]

    assert "const kind = recoveryKind(p.kind)" in lease_guard
    assert "currentFence >= Number.MAX_SAFE_INTEGER" in lease_guard
    assert "currentCycle >= Number.MAX_SAFE_INTEGER" in lease_guard
    assert "const RECOVERY_KINDS = new Set([" in source


async def test_worker_rejects_invalid_snapshot_before_indexed_replay(worker_url):
    catalog = _remote(worker_url)
    try:
        _, publication = await catalog.acquire_artifact_publication(
            world_id="w-snapshot-wire",
            run_id="r-snapshot-wire",
            attempt_id="a-snapshot-wire",
            idempotency_key="bundle-snapshot-wire",
            request_digest="digest-snapshot-wire",
            request_json="{}",
            claimant="snapshot-worker",
            retry_window_ms=60_000,
        )
        await catalog.record_artifact_uploads(
            publication.world_id,
            publication.publication_key,
            publication.claimant,
            "[]",
            "s3://bucket/snapshot-wire",
        )
        snapshot_id = 8_123_456_789_012_345_678
        await catalog.complete_artifact_publication(
            publication.world_id,
            publication.publication_key,
            publication.claimant,
            snapshot_id,
        )
        path = (
            f"{catalog._base}/w/{publication.world_id}/artifact-publications/"
            f"{publication.publication_key}/complete-v2"
        )
        for invalid_snapshot in (
            True,
            1.5,
            7,
            0,
            -1,
            "07",
            "0",
            "-1",
            "7.0",
            str(1 << 63),
        ):
            response = await catalog._client.post(
                path,
                json={
                    "claimant": publication.claimant,
                    "index_snapshot_id": invalid_snapshot,
                },
            )
            assert response.status_code == 400
            assert response.json()["error"] == "invalid"
        exact_replay = await catalog._client.post(
            path,
            json={
                "claimant": publication.claimant,
                "index_snapshot_id": str(snapshot_id),
            },
        )
        assert exact_replay.status_code == 200
        persisted = await catalog.get_artifact_publication(
            publication.world_id, publication.publication_key
        )
        assert persisted is not None and persisted.index_snapshot_id == snapshot_id

        _, legacy = await catalog.acquire_artifact_publication(
            world_id="w-snapshot-wire",
            run_id="r-snapshot-wire",
            attempt_id="a-snapshot-wire-legacy",
            idempotency_key="bundle-snapshot-wire-legacy",
            request_digest="digest-snapshot-wire-legacy",
            request_json="{}",
            claimant="snapshot-worker",
            retry_window_ms=60_000,
        )
        await catalog.record_artifact_uploads(
            legacy.world_id,
            legacy.publication_key,
            legacy.claimant,
            "[]",
            "s3://bucket/snapshot-wire-legacy",
        )
        legacy_path = (
            f"{catalog._base}/w/{legacy.world_id}/artifact-publications/"
            f"{legacy.publication_key}/complete"
        )
        legacy_complete = await catalog._client.post(
            legacy_path,
            json={"claimant": legacy.claimant, "index_snapshot_id": 7},
        )
        assert legacy_complete.status_code == 200
        for invalid_legacy_snapshot in ("7", 2**53):
            response = await catalog._client.post(
                legacy_path,
                json={
                    "claimant": legacy.claimant,
                    "index_snapshot_id": invalid_legacy_snapshot,
                },
            )
            assert response.status_code == 400
        legacy_persisted = await catalog.get_artifact_publication(
            legacy.world_id, legacy.publication_key
        )
        assert legacy_persisted is not None and legacy_persisted.index_snapshot_id == 7
    finally:
        await catalog.close()


async def test_worker_rejects_illegal_attempt_edges_and_misplaced_evidence(worker_url):
    catalog = _remote(worker_url)
    try:
        _, claim = await catalog.acquire_attempt_claim(
            claim_key="claim-edge-wire",
            world_id="w-edge-wire",
            run_id="r-edge-wire",
            mission_id="mission-edge-wire",
            task_id="task-edge-wire",
            attempt_id="attempt-edge-wire",
            idempotency_key="idempotency-edge-wire",
            request_fingerprint="request-edge-wire",
            request_json="{}",
            redaction_policy_id="redaction-v1",
            redaction_evidence_json='{"phase":"acquired"}',
            provider="modal",
            provider_request_fingerprint="provider-edge-wire",
            supports_idempotent_replay=False,
            supports_session_resume=False,
            provider_idempotency_key="",
            claimant="edge-worker",
        )
        path = f"{catalog._base}/w/{claim.world_id}/attempt-claims/{claim.claim_key}/transition"

        async def post(*, versioned: bool = False, **payload):
            return await catalog._client.post(
                f"{path}-v2" if versioned else path,
                json={
                    "claimant": claim.claimant,
                    "fence_epoch": claim.fence_epoch,
                    **payload,
                },
            )

        illegal = await post(
            expected_status="claimed",
            target_status="provider_acknowledged",
            redaction_evidence_json='{"phase":"acknowledged"}',
            provider_request_id="provider-request",
        )
        assert illegal.status_code == 400
        assert "illegal attempt claim transition" in illegal.json()["message"]
        misplaced_provider = await post(
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-edge-wire",
            provider_request_id="too-early",
        )
        assert misplaced_provider.status_code == 400
        assert "provider identity" in misplaced_provider.json()["message"]

        armed = await catalog.transition_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            expected_status="claimed",
            target_status="possibly_submitted",
            execution_nonce="nonce-edge-wire",
        )
        for payload in (
            {
                "provider_request_id": "provider-request",
            },
            {
                "redaction_evidence_json": '{"phase":"acknowledged"}',
            },
            {
                "redaction_evidence_json": '{"phase":"acknowledged"}',
                "provider_request_id": "provider-request",
                "last_error": "too early",
            },
        ):
            response = await post(
                expected_status="possibly_submitted",
                target_status="provider_acknowledged",
                **payload,
            )
            assert response.status_code == 400
        acknowledged = await catalog.transition_attempt_claim(
            armed.world_id,
            armed.claim_key,
            armed.claimant,
            armed.fence_epoch,
            expected_status="possibly_submitted",
            target_status="provider_acknowledged",
            redaction_evidence_json='{"phase":"acknowledged"}',
            provider_request_id="provider-request",
        )
        overwrite = await post(
            versioned=True,
            expected_status="provider_acknowledged",
            target_status="finalizing",
            redaction_evidence_json='{"phase":"finalizing"}',
            provider_request_id="overwrite",
            outcome_digest="outcome-staged",
            outcome_json='{"finalization_phase":"checkpointed"}',
            artifact_request_json="{}",
            artifact_request_digest="artifact-request",
            artifact_publication_key="artifact-publication",
        )
        assert overwrite.status_code == 400
        assert "provider identity" in overwrite.json()["message"]
        terminal = {
            "expected_status": "provider_acknowledged",
            "target_status": "settled",
            "redaction_evidence_json": '{"phase":"settled"}',
            "settlement_status": "failed",
            "outcome_digest": "outcome-terminal",
            "outcome_json": '{"status":"failed"}',
            "last_error": "",
        }
        for omitted in (
            "redaction_evidence_json",
            "settlement_status",
            "outcome_digest",
            "outcome_json",
        ):
            response = await post(
                versioned=True,
                **{key: value for key, value in terminal.items() if key != omitted},
            )
            assert response.status_code == 400
            assert omitted in response.json()["message"]
        persisted = await catalog.get_attempt_claim(claim.world_id, claim.claim_key)
        assert persisted == acknowledged

        settled = await post(versioned=True, **terminal)
        assert settled.status_code == 200
        replayed = await post(versioned=True, **terminal)
        assert replayed.status_code == 200
        changed_error = await post(versioned=True, **{**terminal, "last_error": "changed"})
        assert changed_error.status_code == 412
        wrong_source = await post(
            versioned=True,
            **{**terminal, "expected_status": "possibly_submitted"},
        )
        assert wrong_source.status_code == 412
        persisted = await catalog.get_attempt_claim(claim.world_id, claim.claim_key)
        assert persisted is not None and persisted.last_error == ""
    finally:
        await catalog.close()


async def test_worker_rejects_post_upgrade_legacy_unbound_indexed_settlement(worker_url):
    catalog = _remote(worker_url)
    world_id = "w-legacy-indexed-wire"
    claim_key = "claim-legacy-indexed-wire"
    try:
        acquire = await catalog._client.post(
            f"{catalog._base}/w/{world_id}/attempt-claims/acquire",
            json={
                "claim_key": claim_key,
                "run_id": "run-legacy-indexed-wire",
                "mission_id": "mission-legacy-indexed-wire",
                "task_id": "task-legacy-indexed-wire",
                "attempt_id": "attempt-legacy-indexed-wire",
                "idempotency_key": "idempotency-legacy-indexed-wire",
                "request_fingerprint": "request-legacy-indexed-wire",
                "request_json": '{"required_finalization_phase":"indexed"}',
                "redaction_policy_id": "redaction-v1",
                "redaction_evidence_json": '{"phase":"acquired"}',
                "provider": "modal",
                "provider_request_fingerprint": "provider-legacy-indexed-wire",
                "supports_idempotent_replay": False,
                "supports_session_resume": False,
                "provider_idempotency_key": "",
                "claimant": "legacy-worker",
                "lease_seconds": 30.0,
            },
        )
        assert acquire.status_code == 200
        claim = acquire.json()["claim"]

        settle = await catalog._client.post(
            f"{catalog._base}/w/{world_id}/attempt-claims/{claim_key}/transition",
            json={
                "claimant": claim["claimant"],
                "fence_epoch": claim["fence_epoch"],
                "expected_status": "claimed",
                "target_status": "settled",
                "redaction_evidence_json": '{"phase":"settled"}',
                "settlement_status": "accepted",
                "outcome_digest": "legacy-unbound-outcome",
                "outcome_json": '{"finalization_phase":"indexed","status":"accepted"}',
            },
        )
        assert settle.status_code == 409
        assert "must bind artifact authority" in settle.json()["message"]

        persisted = await catalog.get_attempt_claim(world_id, claim_key)
        assert persisted is not None
        assert persisted.status == "claimed"
        assert persisted.settlement_status == ""
        assert persisted.outcome_digest == ""
        assert persisted.legacy_unbound_eligible is False
    finally:
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


async def test_world_discovery_page_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        try:
            for world_id in ("world-3", "world-1", "world-4", "world-2"):
                await catalog.register_world(_world(world_id))
            await catalog.set_world_status("world-2", "destroyed")

            first = await catalog.list_worlds_page(limit=2)
            second = await catalog.list_worlds_page(
                after_world_id=first[-1].world_id,
                limit=2,
            )
            assert [record.world_id for record in first] == ["world-1", "world-2"]
            assert first[1].status == "destroyed"
            assert [record.world_id for record in second] == ["world-3", "world-4"]
            assert await catalog.list_worlds_page(after_world_id="world-4", limit=2) == []
        finally:
            await catalog.close()


async def test_fleet_recovery_state_machine_parity(tmp_path, worker_url):
    fingerprint = hashlib.sha256(b"storage-config").hexdigest()
    other_fingerprint = hashlib.sha256(b"other-storage-config").hexdigest()
    subject_key = hashlib.sha256(b"attempt-1").hexdigest()
    authority_key = hashlib.sha256(b"attempt-authority-1").hexdigest()
    kind = "artifact_publication"

    for catalog in await _both(tmp_path, worker_url):
        try:
            await catalog.register_world(_world("world-1"))
            sweep = await catalog.ensure_recovery_sweep(
                fingerprint,
                "world-1",
                kind,
                max_consecutive_failures=2,
            )
            assert sweep.sweep_key == recovery_sweep_key(fingerprint, "world-1", kind)
            assert (
                await catalog.ensure_recovery_sweep(
                    fingerprint,
                    "world-1",
                    kind,
                    max_consecutive_failures=2,
                )
                == sweep
            )
            with pytest.raises(RecoverySweepConflictError):
                await catalog.ensure_recovery_sweep(
                    other_fingerprint,
                    "world-1",
                    kind,
                    max_consecutive_failures=2,
                )

            outcome, leased = await catalog.lease_recovery_sweep(
                "world-1", kind, "worker-1", lease_ms=1_000
            )
            assert outcome == "acquired"
            assert leased.fence_epoch == 1
            with pytest.raises(RecoverySweepPendingError):
                await catalog.lease_recovery_sweep("world-1", kind, "worker-2", lease_ms=10_000)
            checkpointed = await catalog.checkpoint_recovery_sweep(
                "world-1",
                kind,
                "worker-1",
                leased.fence_epoch,
                cursor=hashlib.sha256(b"page-7").hexdigest(),
                active_subject_key=subject_key,
            )
            assert checkpointed.active_subject_key == subject_key

            await asyncio.sleep(1.05)
            outcome, recovered = await catalog.lease_recovery_sweep(
                "world-1", kind, "worker-2", lease_ms=10_000
            )
            assert outcome == "recovered"
            assert recovered.fence_epoch == leased.fence_epoch + 1
            assert recovered.cursor == checkpointed.cursor
            assert recovered.active_subject_key == subject_key
            with pytest.raises(RecoverySweepStaleError):
                await catalog.checkpoint_recovery_sweep(
                    "world-1",
                    kind,
                    "worker-1",
                    leased.fence_epoch,
                    cursor="",
                )

            exception = await catalog.retry_recovery_exception(
                "world-1",
                kind,
                "worker-2",
                recovered.fence_epoch,
                subject_key=subject_key,
                authority_key=authority_key,
                expected_attempt_count=0,
                error_code="handler_failed",
                error_detail="TimeoutError",
                retry_delay_ms=0,
                max_attempts=2,
            )
            assert exception.exception_key == recovery_exception_key(sweep.sweep_key, subject_key)
            assert exception.status == "retry_wait"
            assert (
                await catalog.get_recovery_exception("world-1", kind, exception.exception_key)
                == exception
            )
            assert (
                await catalog.retry_recovery_exception(
                    "world-1",
                    kind,
                    "worker-2",
                    recovered.fence_epoch,
                    subject_key=subject_key,
                    authority_key=authority_key,
                    expected_attempt_count=0,
                    error_code="handler_failed",
                    error_detail="TimeoutError",
                    retry_delay_ms=0,
                    max_attempts=2,
                )
                == exception
            )
            with pytest.raises(RecoveryExceptionConflictError):
                await catalog.retry_recovery_exception(
                    "world-1",
                    kind,
                    "worker-2",
                    recovered.fence_epoch,
                    subject_key=subject_key,
                    authority_key=hashlib.sha256(b"wrong-authority").hexdigest(),
                    expected_attempt_count=1,
                    error_code="handler_failed",
                    error_detail="TimeoutError",
                    retry_delay_ms=0,
                    max_attempts=2,
                )
            dead = await catalog.retry_recovery_exception(
                "world-1",
                kind,
                "worker-2",
                recovered.fence_epoch,
                subject_key=subject_key,
                authority_key=authority_key,
                expected_attempt_count=1,
                error_code="handler_failed",
                error_detail="TimeoutError",
                retry_delay_ms=0,
                max_attempts=2,
            )
            assert dead.status == "dead_letter"
            redriven_exception = await catalog.redrive_recovery_exception(
                "world-1",
                kind,
                "worker-2",
                recovered.fence_epoch,
                dead.exception_key,
                expected_attempt_count=dead.attempt_count,
            )
            assert redriven_exception.status == "retry_wait"
            resolved = await catalog.resolve_recovery_exception(
                "world-1",
                kind,
                "worker-2",
                recovered.fence_epoch,
                dead.exception_key,
            )
            assert resolved.status == "resolved"
            assert await catalog.list_recovery_exceptions(
                "world-1", kind=kind, status="resolved"
            ) == [resolved]

            first_failure = await catalog.fail_recovery_sweep(
                "world-1",
                kind,
                "worker-2",
                recovered.fence_epoch,
                error_code="discovery_failed",
                error_detail="TimeoutError",
                retry_delay_ms=0,
            )
            assert first_failure.status == "retry_wait"
            _, retry_lease = await catalog.lease_recovery_sweep(
                "world-1", kind, "worker-2", lease_ms=10_000
            )
            assert retry_lease.active_subject_key == subject_key
            assert retry_lease.cursor == checkpointed.cursor
            paused = await catalog.fail_recovery_sweep(
                "world-1",
                kind,
                "worker-2",
                retry_lease.fence_epoch,
                error_code="discovery_failed",
                error_detail="TimeoutError",
                retry_delay_ms=0,
            )
            assert paused.status == "paused"
            redriven = await catalog.redrive_recovery_sweep(
                "world-1",
                kind,
                expected_fence_epoch=paused.fence_epoch,
            )
            assert redriven.status == "idle"
            assert redriven.fence_epoch == paused.fence_epoch + 1
            assert redriven.active_subject_key == subject_key
            assert redriven.cursor == checkpointed.cursor
            assert await catalog.list_recovery_sweeps("world-1", status="idle") == [redriven]
        finally:
            await catalog.close()
