# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation receipts (issue #275, C): claim-before-grade.

Exactly one VISIBLE durable receipt per evaluation_id — never exactly-once
grader execution. The claim precedes the grader; replay returns the
persisted receipt without re-grading; the subject is pinned by snapshot
reference, never row-content hashing; receipts are evidence, never
authority.
"""

import json
import subprocess
import sys
import textwrap

import pytest
from uuid_utils import uuid7

from archetype.app._catalog import ClaimConflictError, SqliteControlCatalog, claim_scope_key
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.receipts import (
    EvalReceipt,
    GraderContract,
    Outcome,
    subject_digest,
)

pytestmark = pytest.mark.asyncio


class Telemetry(Component):
    reading: float = 0.0


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


def _ctx(role: str = "operator") -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={role})


def _contract(**overrides) -> GraderContract:
    base = dict(
        grader_id="mean-reading-v1",
        implementation_version="2026.07.15",
        thresholds={"min": 0.5},
    )
    base.update(overrides)
    return GraderContract(**base)


async def _seeded_world(c: ServiceContainer, storage):
    world = await c.world_service.create_world(WorldConfig(name="w"), storage)
    await c.mutation_service.create_entity(world.world_id, [Telemetry(reading=0.8)])
    await c.simulation_service.step(world.world_id, RunConfig())
    return world


def _counting_grader(calls: list[int], outcome: Outcome):
    def grader(df):
        calls.append(1)
        rows = df.to_pylist()
        assert rows, "the pinned subject must have rows"
        return outcome

    return grader


async def test_replay_returns_original_receipt_without_regrading(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        calls: list[int] = []
        grader = _counting_grader(calls, Outcome(status="pass", score=0.8))

        first = await c.command_service.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="trial-1",
        )
        assert not first.duplicate and len(calls) == 1

        replay = await c.command_service.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="trial-1",
        )
        assert replay.duplicate and replay.commit_token == first.commit_token
        assert len(calls) == 1, "replay must not re-run the grader"

        # The persisted receipt row is queryable evidence.
        df = await c.query_service.query_components(
            [EvalReceipt], str(world.world_id), str(world.run_id), storage
        )
        rows = df.to_pylist()
        assert len(rows) == 1
        assert rows[0]["evalreceipt__outcome"] == "pass"
        assert rows[0]["evalreceipt__evaluation_id"] == "trial-1"
    finally:
        await c.shutdown()


async def test_grader_reads_the_captured_snapshot_when_world_advances(tmp_path, monkeypatch):
    """A step between snapshot capture and grading cannot change the subject rows."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        original_snapshot_ref = c.ingestion_service.snapshot_ref

        async def _capture_then_advance(*args, **kwargs):
            snapshot = await original_snapshot_ref(*args, **kwargs)
            await c.simulation_service.step(world.world_id, RunConfig())
            return snapshot

        monkeypatch.setattr(c.ingestion_service, "snapshot_ref", _capture_then_advance)
        graded_ticks: list[int] = []

        def grader(df):
            rows = df.to_pylist()
            graded_ticks.extend(int(row["tick"]) for row in rows)
            return Outcome(status="pass", score=1.0)

        await c.command_service.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="pinned-while-advancing",
        )

        assert graded_ticks == [0], "the later tick must not leak into the pinned evaluation"
    finally:
        await c.shutdown()


async def test_grader_snapshot_includes_completed_fact_claims(tmp_path):
    """Pinned reads retain durable facts visible when the snapshot is captured."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        await c.ingestion_service.ingest_fact(
            str(world.world_id),
            [Telemetry(reading=1.2)],
            external_id="sensor-reading-1",
            producer="sensor",
        )
        graded_readings: list[float] = []

        def grader(df):
            graded_readings.extend(float(row["telemetry__reading"]) for row in df.to_pylist())
            return Outcome(status="pass", score=1.0)

        await c.command_service.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="fact-aware-snapshot",
        )

        assert sorted(graded_readings) == [0.8, 1.2]
    finally:
        await c.shutdown()


async def test_new_trials_of_nondeterministic_graders_are_new_receipts(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        calls: list[int] = []

        for i, status in enumerate(("pass", "fail")):
            receipt = await c.command_service.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=_counting_grader(calls, Outcome(status=status)),
                evaluation_id=f"trial-{i}",
            )
            assert not receipt.duplicate
        assert len(calls) == 2, "each trial grades once — trials are a feature"

        df = await c.query_service.query_components(
            [EvalReceipt], str(world.world_id), str(world.run_id), storage
        )
        outcomes = sorted(r["evalreceipt__outcome"] for r in df.to_pylist())
        assert outcomes == ["fail", "pass"], "both trials visible, distinct ids"
    finally:
        await c.shutdown()


async def test_same_id_different_contract_conflicts_loudly(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        grader = _counting_grader([], Outcome(status="pass"))

        await c.command_service.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="trial-x",
        )
        with pytest.raises(ClaimConflictError):
            await c.command_service.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=_contract(implementation_version="2026.08.01"),
                grader=grader,
                evaluation_id="trial-x",
            )
    finally:
        await c.shutdown()


async def test_recovery_registers_orphaned_receipt_before_completion(tmp_path, monkeypatch):
    """Takeover restores cold discovery without re-running the grader."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        wid = str(world.world_id)
        calls: list[int] = []
        grader = _counting_grader(calls, Outcome(status="pass", score=1.0))

        real_register = SqliteControlCatalog.register_signature

        async def _crash(self, *args, **kwargs):
            raise RuntimeError("injected crash after append, before signature registration")

        monkeypatch.setattr(SqliteControlCatalog, "register_signature", _crash)
        with pytest.raises(RuntimeError, match="before signature registration"):
            await c.command_service.evaluate(
                _ctx(),
                wid,
                [Telemetry],
                contract=_contract(),
                grader=grader,
                evaluation_id="crashy",
            )
        monkeypatch.setattr(SqliteControlCatalog, "register_signature", real_register)
        assert len(calls) == 1

        # Expire the lease: the owner is presumed dead.
        catalog = c.storage_service.get_control_catalog(storage)
        scope = claim_scope_key(wid, str(world.run_id), "evals", "crashy")
        orphaned_claim = await catalog.get_claim(wid, scope)
        assert orphaned_claim is not None and orphaned_claim.table_id
        assert orphaned_claim.table_id not in {
            record.table_id for record in await catalog.list_signatures()
        }

        def _expire():
            conn = catalog._connect_sync()
            with conn:
                conn.execute("UPDATE claims SET lease_expires_at=0 WHERE scope_key=?", (scope,))

        await catalog._run(_expire)

        receipt = await c.command_service.evaluate(
            _ctx(),
            wid,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="crashy",
        )
        assert not receipt.duplicate
        assert len(calls) == 1, "orphan found by token: the grader must NOT re-run"
        assert receipt.table_id in {record.table_id for record in await catalog.list_signatures()}

        # A fresh service has no live signature registry. It must discover the
        # recovered receipt table from the catalog record published by takeover.
        cold = ServiceContainer()
        try:
            df = await cold.query_service.query_components(
                [EvalReceipt], wid, str(world.run_id), storage
            )
            assert len(df.to_pylist()) == 1, "exactly one cold-visible receipt"
        finally:
            await cold.shutdown()
    finally:
        await c.shutdown()


async def test_fail_closed_inputs(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)

        # Bare callable without a contract descriptor.
        with pytest.raises(ValueError, match="GraderContract"):
            await c.command_service.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=None,
                grader=lambda df: Outcome(status="pass"),
                evaluation_id="t",
            )

        # Untyped grader output.
        with pytest.raises(ValueError, match="typed Outcome"):
            await c.command_service.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=lambda df: 0.9,
                evaluation_id="t2",
            )

        # A world with no published visibility has nothing to pin.
        bare = await c.world_service.create_world(WorldConfig(name="unstepped"), storage)
        with pytest.raises(RuntimeError, match="no published visibility"):
            await c.command_service.evaluate(
                _ctx(),
                bare.world_id,
                [Telemetry],
                contract=_contract(),
                grader=lambda df: Outcome(status="pass"),
                evaluation_id="t3",
            )

        # Invalid outcome statuses and non-finite scores refuse at construction.
        with pytest.raises(ValueError):
            Outcome(status="maybe")
        with pytest.raises(ValueError):
            Outcome(status="pass", score=float("inf"))
    finally:
        await c.shutdown()


async def test_receipt_is_attributable_from_the_pinned_snapshot(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(c, storage)
        wid, rid = str(world.world_id), str(world.run_id)
        contract = _contract()

        await c.command_service.evaluate(
            _ctx(),
            wid,
            [Telemetry],
            contract=contract,
            grader=lambda df: Outcome(status="pass", score=0.8),
            evaluation_id="attrib",
        )

        df = await c.query_service.query_components([EvalReceipt], wid, rid, storage)
        row = df.to_pylist()[0]

        # Recompute the subject digest from the catalog's pinned snapshot —
        # manifests only: receipts attach to a snapshot without perturbing it.
        catalog = c.storage_service.get_control_catalog(storage)
        manifests = await catalog.list_manifests(wid, rid)
        head = max(m.tick for m in manifests)
        recomputed = subject_digest(
            wid,
            rid,
            snapshot_tick=head,
            snapshot_tokens=sorted(m.commit_token for m in manifests if m.tick == head),
            component_names=[Telemetry.__name__],
        )
        assert row["evalreceipt__subject_digest"] == recomputed
        assert row["evalreceipt__contract_digest"] == contract.digest()
    finally:
        await c.shutdown()


async def test_flagship_worker_writes_then_cold_process_grades(tmp_path):
    """The physical-AI flow: a worker subprocess writes trajectories; a
    separate process cold-discovers the world and grades it — one visible
    receipt, no live world, no shared memory."""
    script = textwrap.dedent(
        """
        import asyncio, json, sys

        from archetype.app.container import ServiceContainer
        from archetype.core.component import Component
        from archetype.core.config import RunConfig, StorageConfig, WorldConfig

        class Telemetry(Component):
            reading: float = 0.0

        async def main(uri):
            c = ServiceContainer()
            try:
                storage = StorageConfig(uri=uri, namespace="ns")
                world = await c.world_service.create_world(WorldConfig(name="gpu"), storage)
                await c.mutation_service.create_entity(world.world_id, [Telemetry(reading=0.9)])
                for _ in range(3):
                    await c.simulation_service.step(world.world_id, RunConfig())
                print(json.dumps({"world_id": str(world.world_id), "run_id": str(world.run_id)}))
            finally:
                await c.shutdown()

        asyncio.run(main(sys.argv[1]))
        """
    )
    uri = str(tmp_path / "store")
    proc = subprocess.run(
        [sys.executable, "-c", script, uri],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stderr
    info = json.loads(proc.stdout.strip().splitlines()[-1])

    cold = ServiceContainer()
    try:
        storage = StorageConfig(uri=uri, namespace="ns")
        receipt = await cold.command_service.evaluate(
            _ctx(),
            info["world_id"],
            [Telemetry],
            contract=_contract(),
            grader=lambda df: Outcome(
                status="pass", score=float(df.to_pylist()[-1]["telemetry__reading"])
            ),
            evaluation_id="cold-grade-1",
            storage_config=storage,
        )
        assert not receipt.duplicate

        df = await cold.query_service.query_components(
            [EvalReceipt], info["world_id"], info["run_id"], storage
        )
        rows = df.to_pylist()
        assert len(rows) == 1 and rows[0]["evalreceipt__outcome"] == "pass"
    finally:
        await cold.shutdown()
