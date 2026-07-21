# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Snapshot-pinned evaluation evidence over the general ingestion service."""

import json
import subprocess
import sys
import textwrap

import pytest
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.evaluation.contracts import GraderContract, Outcome, subject_digest
from archetype.ingestion import IngestionTable

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("evaluation.result.snapshot_pinned"),
    pytest.mark.integration,
]

_EVALUATION_RESULTS = IngestionTable("evaluation_results", key_columns=("evaluation_id",))


class Telemetry(Component):
    reading: float = 0.0


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


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


async def _seeded_world(container: ServiceContainer, storage):
    world = await container.world_service.create_world(WorldConfig(name="w"), storage)
    await container.mutation_service.create_entity(world.world_id, [Telemetry(reading=0.8)])
    await container.simulation_service.step(world.world_id, RunConfig())
    return world


def _counting_grader(calls: list[int], outcome: Outcome):
    def grader(df):
        calls.append(1)
        assert df.to_pylist(), "the pinned subject must have rows"
        return outcome

    return grader


async def _results(
    container: ServiceContainer,
    world_id: str,
    storage_config: StorageConfig | None = None,
):
    return await container.ingestion_service.read(
        world_id,
        _EVALUATION_RESULTS,
        storage_config=storage_config,
    )


async def test_replay_returns_persisted_result_without_regrading(tmp_path):
    container = ServiceContainer()
    try:
        world = await _seeded_world(container, _storage(tmp_path))
        calls: list[int] = []
        grader = _counting_grader(calls, Outcome(status="pass", score=0.8))

        first = await container.command_gateway.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="trial-1",
        )
        replay = await container.command_gateway.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="trial-1",
        )

        assert replay == first
        assert calls == [1]
        rows = (await _results(container, str(world.world_id))).to_pylist()
        assert len(rows) == 1
        assert rows[0]["outcome"] == "pass"
        assert rows[0]["evaluation_id"] == "trial-1"
    finally:
        await container.shutdown()


async def test_grader_reads_captured_snapshot_when_world_advances(tmp_path, monkeypatch):
    container = ServiceContainer()
    try:
        world = await _seeded_world(container, _storage(tmp_path))
        original_snapshot = container.evaluation_service._snapshot

        async def capture_then_advance(*args, **kwargs):
            snapshot = await original_snapshot(*args, **kwargs)
            await container.simulation_service.step(world.world_id, RunConfig())
            return snapshot

        monkeypatch.setattr(container.evaluation_service, "_snapshot", capture_then_advance)
        graded_ticks: list[int] = []

        def grader(df):
            graded_ticks.extend(int(row["tick"]) for row in df.to_pylist())
            return Outcome(status="pass", score=1.0)

        await container.command_gateway.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="pinned-while-advancing",
        )

        assert graded_ticks == [0]
    finally:
        await container.shutdown()


async def test_distinct_trials_record_distinct_results(tmp_path):
    container = ServiceContainer()
    try:
        world = await _seeded_world(container, _storage(tmp_path))
        calls: list[int] = []

        for index, status in enumerate(("pass", "fail")):
            await container.command_gateway.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=_counting_grader(calls, Outcome(status=status)),
                evaluation_id=f"trial-{index}",
            )

        assert len(calls) == 2
        outcomes = sorted(
            row["outcome"] for row in (await _results(container, str(world.world_id))).to_pylist()
        )
        assert outcomes == ["fail", "pass"]
    finally:
        await container.shutdown()


async def test_same_id_with_different_contract_conflicts(tmp_path):
    container = ServiceContainer()
    try:
        world = await _seeded_world(container, _storage(tmp_path))
        grader = _counting_grader([], Outcome(status="pass"))

        await container.command_gateway.evaluate(
            _ctx(),
            world.world_id,
            [Telemetry],
            contract=_contract(),
            grader=grader,
            evaluation_id="trial-x",
        )
        with pytest.raises(ValueError, match="different subject or grader contract"):
            await container.command_gateway.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=_contract(implementation_version="2026.08.01"),
                grader=grader,
                evaluation_id="trial-x",
            )
    finally:
        await container.shutdown()


async def test_fail_closed_inputs(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(container, storage)

        with pytest.raises(ValueError, match="GraderContract"):
            await container.command_gateway.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=None,
                grader=lambda df: Outcome(status="pass"),
                evaluation_id="t",
            )
        with pytest.raises(ValueError, match="typed Outcome"):
            await container.command_gateway.evaluate(
                _ctx(),
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=lambda df: 0.9,
                evaluation_id="t2",
            )

        bare = await container.world_service.create_world(WorldConfig(name="unstepped"), storage)
        with pytest.raises(RuntimeError, match="no published visibility"):
            await container.command_gateway.evaluate(
                _ctx(),
                bare.world_id,
                [Telemetry],
                contract=_contract(),
                grader=lambda df: Outcome(status="pass"),
                evaluation_id="t3",
            )
        with pytest.raises(ValueError):
            Outcome(status="maybe")
        with pytest.raises(ValueError):
            Outcome(status="pass", score=float("inf"))
    finally:
        await container.shutdown()


async def test_result_is_attributable_to_pinned_snapshot(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(container, storage)
        wid, rid = str(world.world_id), str(world.run_id)
        contract = _contract()

        await container.command_gateway.evaluate(
            _ctx(),
            wid,
            [Telemetry],
            contract=contract,
            grader=lambda df: Outcome(status="pass", score=0.8),
            evaluation_id="attrib",
        )
        row = (await _results(container, wid)).to_pylist()[0]

        catalog = container.storage_service.get_control_catalog(storage)
        manifests = await catalog.list_manifests(wid, rid)
        head = max(manifest.tick for manifest in manifests)
        recomputed = subject_digest(
            wid,
            rid,
            snapshot_tick=head,
            snapshot_tokens=sorted(
                manifest.commit_token for manifest in manifests if manifest.tick == head
            ),
            component_names=[Telemetry.__name__],
        )
        assert row["subject_digest"] == recomputed
        assert row["contract_digest"] == contract.digest()
    finally:
        await container.shutdown()


async def test_cold_process_can_grade_persisted_world(tmp_path):
    script = textwrap.dedent(
        """
        import asyncio, json, sys

        from archetype.app.container import ServiceContainer
        from archetype.core.component import Component
        from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig

        class Telemetry(Component):
            reading: float = 0.0

        async def main(uri):
            c = ServiceContainer()
            try:
                storage = StorageConfig(
                    uri=uri,
                    namespace="ns",
                    backend=StorageBackend.ICEBERG,
                )
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
    process = subprocess.run(
        [sys.executable, "-c", script, uri],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert process.returncode == 0, process.stderr
    info = json.loads(process.stdout.strip().splitlines()[-1])

    cold = ServiceContainer()
    try:
        storage = StorageConfig(
            uri=uri,
            namespace="ns",
            backend=StorageBackend.ICEBERG,
        )
        result = await cold.command_gateway.evaluate(
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
        assert result.outcome == "pass"
        rows = (await _results(cold, info["world_id"], storage)).to_pylist()
        assert len(rows) == 1
        assert rows[0]["outcome"] == "pass"
    finally:
        await cold.shutdown()
