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
import time
import uuid
from pathlib import Path

import pytest

from archetype.app._catalog import (
    CatalogConflictError,
    ClaimConflictError,
    ClaimPendingError,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
)
from archetype.core.interfaces import StaleWriterError

pytestmark = pytest.mark.asyncio

WORKER_DIR = Path(__file__).resolve().parents[2] / "infra" / "control-catalog"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def worker_url():
    if shutil.which("npx") is None:
        pytest.skip("npx unavailable; wrangler dev harness skipped")
    port = _free_port()
    proc = subprocess.Popen(
        ["npx", "--yes", "wrangler", "dev", "--local", "--port", str(port)],
        cwd=WORKER_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        import httpx

        deadline = time.time() + 120
        while time.time() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                pytest.skip(f"wrangler dev exited early: {out[-400:]}")
            try:
                response = httpx.get(f"{url}/ns/probe/worlds", timeout=2.0)
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


def _remote(worker_url: str):
    from archetype.app._remote_catalog import RemoteControlCatalog

    # Fresh namespace per catalog instance: parity runs are independent.
    return RemoteControlCatalog(worker_url, f"parity-{uuid.uuid4().hex[:12]}")


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

        # Stale epoch fails closed.
        with pytest.raises(StaleWriterError):
            await catalog.publish_manifest("w1", "r1", 0, "tok-a", 1, ["t1"])
        # Live epoch publishes; identical retry is a no-op; different attempt conflicts.
        await catalog.publish_manifest("w1", "r1", 0, "tok-a", 2, ["t1"])
        await catalog.publish_manifest("w1", "r1", 0, "tok-a", 2, ["t1"])
        with pytest.raises(CatalogConflictError):
            await catalog.publish_manifest("w1", "r1", 0, "tok-b", 2, ["t1"])
        await catalog.publish_manifest("w1", "r1", 1, "tok-c", 2, ["t1", "t2"])

        manifests = await catalog.list_manifests("w1", "r1")
        assert [(m.tick, m.commit_token) for m in manifests] == [(0, "tok-a"), (1, "tok-c")]
        # Publication advances the durable head on both backends.
        assert (await catalog.get_world("w1")).tick_head == 1
        await catalog.close()


async def test_visibility_three_state_parity(tmp_path, worker_url):
    for catalog in await _both(tmp_path, worker_url):
        await catalog.register_world(_world())
        # Never fenced: legacy, unfiltered.
        assert await catalog.visible_tokens("w1", "r1") is None
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
        assert claim.fact_entity_id < 0, "facts live in the negative id band"
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
        await catalog.close()


async def test_service_stack_runs_against_remote_catalog(tmp_path, worker_url, monkeypatch):
    """The integration proof: coordinator + ingestion + receipts through the
    remote catalog with zero changes above the protocol."""
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", worker_url)

    from archetype.app.container import ServiceContainer
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig
    from archetype.experiments.receipts import GraderContract, Outcome

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

        receipt = await c.ingestion_service.ingest_fact(
            wid, [Probe(value=42.0)], external_id="remote-fact-1", producer="probe"
        )
        assert not receipt.duplicate

        eval_receipt = await c.command_service.evaluate(
            __import__("archetype.app.auth.models", fromlist=["ActorCtx"]).ActorCtx(
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
