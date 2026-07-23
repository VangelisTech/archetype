# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Remote control catalog parity (issue #281).

The DO worker must be observationally identical to SqliteControlCatalog —
the reference implementation — for every operation the system performs:
identity conflicts, fence semantics, manifest CAS, and the three-state
visibility map. The harness runs the worker
locally under ``wrangler dev`` (skipped cleanly when node/wrangler is
unavailable), then drives BOTH catalogs through identical sequences and
asserts identical outcomes, including exception types.

The final test runs the real service stack — coordinator, ingestion,
receipts — against the remote catalog via ARCHETYPE_CONTROL_CATALOG_URL.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from archetype.core.interfaces import StaleWriterError
from archetype.storage.catalog import (
    CatalogConflictError,
    CommandAdmission,
    CommandConflictError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
)

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
    from archetype.storage.catalog.remote import RemoteControlCatalog

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
        # Never fenced: legacy history remains implicitly visible.
        assert await catalog.visible_tokens("w1", "r1") is None
        # Fenced, nothing published: nothing is visible.
        await catalog.acquire_fence("w1", "h1")
        assert await catalog.visible_tokens("w1", "r1") == {}
        # Manifests appear in the map; tick filtering applies.
        await catalog.publish_manifest("w1", "r1", 0, "tok-a", 1, ["t1"])
        await catalog.publish_manifest("w1", "r1", 1, "tok-b", 1, ["t1"])
        assert await catalog.visible_tokens("w1", "r1") == {0: ["tok-a"], 1: ["tok-b"]}
        assert await catalog.visible_tokens("w1", "r1", [1]) == {1: ["tok-b"]}
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


async def test_evaluation_lease_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        first = await catalog.lease_evaluation(
            "w1",
            "r1",
            "eval-1",
            "subject-a",
            "contract-a",
            "owner-a",
            lease_seconds=30,
        )
        waiting = await catalog.lease_evaluation(
            "w1",
            "r1",
            "eval-1",
            "subject-a",
            "contract-a",
            "owner-b",
            lease_seconds=30,
        )
        mismatch = await catalog.lease_evaluation(
            "w1",
            "r1",
            "eval-1",
            "subject-b",
            "contract-a",
            "owner-b",
            lease_seconds=30,
        )
        assert first.acquired and first.owner == "owner-a"
        assert not waiting.acquired and waiting.owner == "owner-a"
        assert not mismatch.acquired and mismatch.subject_digest == "subject-a"

        await catalog.release_evaluation("w1", "r1", "eval-1", "owner-a")
        recovered = await catalog.lease_evaluation(
            "w1",
            "r1",
            "eval-1",
            "subject-a",
            "contract-a",
            "owner-b",
        )
        assert recovered.acquired and recovered.owner == "owner-b"

        await catalog.complete_evaluation("w1", "r1", "eval-1", "owner-b")
        complete = await catalog.lease_evaluation(
            "w1",
            "r1",
            "eval-1",
            "subject-a",
            "contract-a",
            "owner-c",
        )
        assert complete.status == "COMPLETE"
        assert not complete.acquired and complete.owner is None
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


async def test_service_stack_runs_against_remote_catalog(tmp_path, worker_url, monkeypatch):
    """The integration proof: coordinator + ingestion + receipts through the
    remote catalog with zero changes above the protocol."""
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", worker_url)
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", WORKER_TOKEN)

    from archetype.app.container import ServiceContainer
    from archetype.artifacts import ArtifactSource
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
    from archetype.evaluation.contracts import GraderContract, Outcome

    class Probe(Component):
        value: float = 0.0

    c = ServiceContainer()
    try:
        storage = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.ICEBERG,
        )
        world = await c.world_service.create_world(WorldConfig(name="remote-w"), storage)
        assert world.commit_coordinator is not None
        await c.mutation_service.create_entity(world.world_id, [Probe(value=1.0)])
        await c.simulation_service.step(world.world_id, RunConfig())
        await c.simulation_service.step(world.world_id, RunConfig())
        wid, rid = str(world.world_id), str(world.run_id)

        output = tmp_path / "remote-artifact.txt"
        output.write_text("probe output")
        (artifact,) = await c.artifact_service.ingest(
            wid,
            ArtifactSource(
                source_uri=str(output),
                logical_path="results/remote-artifact.txt",
            ),
        )
        assert artifact.logical_path == "results/remote-artifact.txt"

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
        assert eval_receipt.outcome == "pass"

        # Cold discovery through the remote catalog from a FRESH container.
        await c.shutdown()
        fresh = ServiceContainer()
        try:
            infos = await fresh.world_service.discover_worlds(storage)
            assert wid in [str(i.world_id) for i in infos]
            df = await fresh.query_service.query_components([Probe], wid, rid, storage)
            rows = df.to_pylist()
            assert {r["tick"] for r in rows} >= {0, 1}, "stepped history visible cold"
            artifacts = await fresh.artifact_service.index(wid, storage_config=storage)
            assert artifacts.select("artifact_id").to_pylist() == [
                {"artifact_id": artifact.artifact_id}
            ]
        finally:
            await fresh.shutdown()
    finally:
        try:
            await c.shutdown()
        except Exception:
            pass
