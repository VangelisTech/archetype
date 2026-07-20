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
import os
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
    CatalogConflictError,
    ClaimConflictError,
    ClaimPendingError,
    CommandAdmission,
    CommandConflictError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
    artifact_publication_key,
)
from archetype.core.interfaces import StaleWriterError

pytestmark = pytest.mark.asyncio

WORKER_DIR = Path(__file__).resolve().parents[2] / "infra" / "control-catalog"
WORKER_TOKEN = "archetype-parity-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _isolated_npx_env(cache_path: Path) -> dict[str, str]:
    """Keep concurrent xdist workers from racing in npx's install cache."""

    cache_path.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.pop("NPM_CONFIG_CACHE", None)
    env["npm_config_cache"] = str(cache_path)
    return env


@pytest.fixture(scope="module")
def worker_url(tmp_path_factory):
    if shutil.which("npx") is None:
        pytest.skip("npx unavailable; wrangler dev harness skipped")
    port = _free_port()
    inspector_port = _free_port()
    worker_log = tempfile.TemporaryFile(mode="w+")
    npx_env = _isolated_npx_env(tmp_path_factory.mktemp("wrangler-npm-cache"))
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
        env=npx_env,
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


async def test_world_unicode_identity_parity(tmp_path, worker_url):
    world_id = "wörld-🚀"
    for catalog in await _both(tmp_path, worker_url):
        try:
            await catalog.register_world(_world(world_id))
            assert (await catalog.get_world(world_id)).world_id == world_id

            await catalog.set_world_status(world_id, "destroyed")
            await catalog.set_world_run(world_id, "rün-雪")

            record = await catalog.get_world(world_id)
            assert record is not None
            assert record.status == "destroyed"
            assert record.run_id == "rün-雪"
            assert [world.world_id for world in await catalog.list_worlds()] == [world_id]
        finally:
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
            # The remote catalog clock is millisecond-granular. Acquisition
            # and same-owner recovery can therefore observe the same catalog
            # millisecond and produce an equal (never regressed) expiry.
            # The deterministic clock test in test_artifact_publication_catalog
            # advances time explicitly and proves that renewal extends from a
            # later catalog instant.
            assert owned.lease_expires_at >= publication.lease_expires_at
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
    run_id = "rün-\x7f-雪"
    idempotency_key = "bundlé-\x7f-🧪"
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
    assert source.count('(route.length === 1 && method === "GET")') == 1
    assert "SELECT * FROM artifact_publications WHERE status IN" not in source


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


async def test_service_stack_runs_against_remote_catalog(tmp_path, worker_url, monkeypatch):
    """The integration proof: coordinator + ingestion + receipts through the
    remote catalog with zero changes above the protocol."""
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", worker_url)
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", WORKER_TOKEN)

    from archetype.app.container import ServiceContainer
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig
    from archetype.evaluation.contracts import GraderContract, Outcome

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
