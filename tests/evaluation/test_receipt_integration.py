# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Snapshot-pinned evaluation evidence through the family-owned handlers."""

import asyncio
import json
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import daft
import pyarrow as pa
import pytest
from uuid_utils import uuid7

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.evaluation import handlers as evaluation_handlers
from archetype.evaluation import views as evaluation_views
from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import GraderContract, Outcome, subject_digest
from archetype.evaluation.models import Evaluate
from archetype.runtime_resources import RuntimeResources
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world.models import CreateWorld, ForkWorld, Spawn, Step
from tests._runtime import build_test_runtime

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("evaluation.result.snapshot_pinned"),
    pytest.mark.integration,
]

_EVALUATION_RESULTS = evaluation_views.EVALUATION_RESULTS_TABLE


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


def _storage_service(tmp_path: Path) -> StorageService:
    return StorageService(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / "control-catalogs",
        )
    )


def _runtime(
    tmp_path: Path,
    *,
    storage_service: StorageService | None = None,
) -> tuple[RuntimeResources, StorageService]:
    storage = storage_service or _storage_service(tmp_path)
    return build_test_runtime(tmp_path, storage_service=storage), storage


async def _shutdown(resources: RuntimeResources, storage: StorageService) -> None:
    await resources.aclose()
    await storage.shutdown()


def _evaluate(
    world_id: object,
    components: list[type[Component]],
    *,
    contract: Any,
    grader: Any,
    evaluation_id: str,
    storage_config: StorageConfig,
) -> Evaluate:
    return Evaluate(
        world_id=world_id,
        components=tuple(components),
        contract=contract,
        grader=grader,
        evaluation_id=evaluation_id,
        storage_config=storage_config,
    )


async def _seeded_world(dispatcher: CommandDispatcher, storage: StorageConfig):
    world = await dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name="w"),
            storage_config=storage,
        )
    )
    await dispatcher.apply(
        Spawn.from_components(
            world_id=world.world_id,
            components=[Telemetry(reading=0.8)],
        )
    )
    await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))
    return world


def _counting_grader(calls: list[int], outcome: Outcome):
    def grader(df):
        calls.append(1)
        assert df.to_pylist(), "the pinned subject must have rows"
        return outcome

    return grader


async def _results(
    storage_service: StorageService,
    world_id: str,
    storage_config: StorageConfig,
):
    return await storage_service.read_world_rows(
        storage_config,
        str(world_id),
        _EVALUATION_RESULTS,
    )


async def test_replay_returns_persisted_result_without_regrading(tmp_path):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        calls: list[int] = []
        grader = _counting_grader(calls, Outcome(status="pass", score=0.8))

        first = await dispatcher.apply_as(
            _ctx(),
            _evaluate(
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=grader,
                evaluation_id="trial-1",
                storage_config=storage,
            ),
        )
        replay = await dispatcher.apply_as(
            _ctx(),
            _evaluate(
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=grader,
                evaluation_id="trial-1",
                storage_config=storage,
            ),
        )

        assert replay == first
        assert calls == [1]
        rows = (await _results(storage_service, str(world.world_id), storage)).to_pylist()
        assert len(rows) == 1
        assert rows[0]["outcome"] == "pass"
        assert rows[0]["evaluation_id"] == "trial-1"
    finally:
        await _shutdown(resources, storage_service)


@pytest.mark.parametrize(
    ("child_steps", "fork_depth", "expected_ticks"),
    [(0, 1, [0]), (1, 1, [0, 1]), (0, 2, [0])],
    ids=["fresh-fork", "stepped-fork", "nested-fresh-fork"],
)
async def test_fork_evaluation_pins_durable_lineage_without_live_registry(
    tmp_path,
    child_steps,
    fork_depth,
    expected_ticks,
):
    """A fork receipt grades its immutable inherited prefix plus child rows."""

    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    storage = _storage(tmp_path)
    try:
        source = await _seeded_world(dispatcher, storage)
        first_fork = await dispatcher.apply(
            ForkWorld(
                source_world_id=source.world_id,
                name=f"branch-{child_steps}-{fork_depth}-1",
            )
        )
        fork = first_fork
        if fork_depth == 2:
            fork = await dispatcher.apply(
                ForkWorld(
                    source_world_id=first_fork.world_id,
                    name=f"branch-{child_steps}-{fork_depth}-2",
                )
            )
        for _ in range(child_steps):
            await dispatcher.apply(Step(world_id=fork.world_id, run_config=RunConfig()))

        # Publish another source tick after the fork boundary. The child must
        # retain tick 0 without leaking this later source state.
        await dispatcher.apply(
            Spawn.from_components(
                world_id=source.world_id,
                components=[Telemetry(reading=99.0)],
            )
        )
        await dispatcher.apply(Step(world_id=source.world_id, run_config=RunConfig()))
    finally:
        await _shutdown(resources, storage_service)

    pin_events: list[tuple[str, str | None, int | None]] = []
    grader_started = False

    class RecordingStorage(StorageService):
        async def pin_visibility(
            self,
            storage_config,
            world_id,
            *,
            run_id=None,
            max_tick=None,
        ):
            assert not grader_started, "all lineage visibility must be pinned before grading"
            pin_events.append(
                (str(world_id), str(run_id) if run_id is not None else None, max_tick)
            )
            return await super().pin_visibility(
                storage_config,
                world_id,
                run_id=run_id,
                max_tick=max_tick,
            )

    cold_storage = RecordingStorage(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / "control-catalogs",
        )
    )
    observed_rows: list[dict[str, Any]] = []

    def grader(frame):
        nonlocal grader_started
        grader_started = True
        observed_rows.extend(frame.to_pylist())
        return Outcome(status="pass", score=1.0)

    try:
        result = await evaluation_handlers.evaluate(
            cold_storage,
            _evaluate(
                fork.world_id,
                [Telemetry],
                contract=_contract(),
                grader=grader,
                evaluation_id=f"fork-lineage-{child_steps}-{fork_depth}",
                storage_config=storage,
            ),
        )
    finally:
        await cold_storage.shutdown()

    assert result.outcome == "pass"
    assert sorted(row["tick"] for row in observed_rows) == expected_ticks
    assert {row["telemetry__reading"] for row in observed_rows} == {0.8}
    expected_pins = [
        (str(fork.world_id), str(fork.run_id), None),
        (str(source.world_id), str(source.run_id), 0),
    ]
    if fork_depth == 2:
        expected_pins.append((str(first_fork.world_id), str(first_fork.run_id), 0))
    assert pin_events == expected_pins


async def test_parent_without_lineage_preserves_empty_prefix_and_cross_store_forks(
    tmp_path,
):
    """No-lineage forks remain gradeable only for their child-owned history."""

    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    storage = _storage(tmp_path)
    cross_storage = StorageConfig(
        uri=str(tmp_path / "cross-store"),
        namespace="cross-ns",
        backend=StorageBackend.ICEBERG,
    )
    try:
        empty_source = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="empty-prefix-source"),
                storage_config=storage,
            )
        )
        empty_entity = await dispatcher.apply(
            Spawn.from_components(
                world_id=empty_source.world_id,
                components=[Telemetry(reading=0.4)],
            )
        )
        empty_fork = await dispatcher.apply(
            ForkWorld(source_world_id=empty_source.world_id, name="empty-prefix-child")
        )
        with pytest.raises(RuntimeError, match="no published visibility"):
            await dispatcher.apply(
                _evaluate(
                    empty_fork.world_id,
                    [Telemetry],
                    contract=_contract(),
                    grader=lambda _frame: Outcome(status="pass"),
                    evaluation_id="fresh-empty-prefix",
                    storage_config=storage,
                )
            )
        await dispatcher.apply(Step(world_id=empty_fork.world_id, run_config=RunConfig()))

        cross_source = await _seeded_world(dispatcher, storage)
        cross_entity = await dispatcher.apply(
            Spawn.from_components(
                world_id=cross_source.world_id,
                components=[Telemetry(reading=0.6)],
            )
        )
        cross_fork = await dispatcher.apply(
            ForkWorld(
                source_world_id=cross_source.world_id,
                name="cross-store-child",
                storage_config=cross_storage,
            )
        )
        await dispatcher.apply(Step(world_id=cross_fork.world_id, run_config=RunConfig()))
    finally:
        await _shutdown(resources, storage_service)

    cold_storage = _storage_service(tmp_path)
    observed: dict[str, list[dict[str, Any]]] = {}

    def capture(label):
        def grader(frame):
            observed[label] = frame.to_pylist()
            return Outcome(status="pass", score=1.0)

        return grader

    try:
        await evaluation_handlers.evaluate(
            cold_storage,
            _evaluate(
                empty_fork.world_id,
                [Telemetry],
                contract=_contract(),
                grader=capture("empty"),
                evaluation_id="empty-prefix-evaluation",
                storage_config=storage,
            ),
        )
        await evaluation_handlers.evaluate(
            cold_storage,
            _evaluate(
                cross_fork.world_id,
                [Telemetry],
                contract=_contract(),
                grader=capture("cross"),
                evaluation_id="cross-store-evaluation",
                storage_config=cross_storage,
            ),
        )
    finally:
        await cold_storage.shutdown()

    assert [
        (row["entity_id"], row["tick"], row["telemetry__reading"]) for row in observed["empty"]
    ] == [(empty_entity, 0, 0.4)]
    assert [
        (row["entity_id"], row["tick"], row["telemetry__reading"]) for row in observed["cross"]
    ] == [(cross_entity, 1, 0.6)]


async def test_concurrent_service_graphs_run_paid_grader_once(tmp_path):
    """Two independent process graphs converge through the durable catalog lease."""
    storage = _storage(tmp_path)
    first_storage = _storage_service(tmp_path)
    second_storage = _storage_service(tmp_path)
    first_resources, _ = _runtime(tmp_path, storage_service=first_storage)
    second_resources, _ = _runtime(tmp_path, storage_service=second_storage)
    first_dispatcher = first_resources.dispatcher
    second_dispatcher = second_resources.dispatcher
    first_task = None
    second_task = None
    release_grader = asyncio.Event()
    try:
        world = await _seeded_world(first_dispatcher, storage)
        grader_started = asyncio.Event()
        calls: list[int] = []

        async def grader(df):
            calls.append(1)
            assert df.to_pylist()
            grader_started.set()
            await release_grader.wait()
            return Outcome(status="pass", score=0.8)

        first_task = asyncio.create_task(
            first_dispatcher.apply_as(
                _ctx(),
                _evaluate(
                    world.world_id,
                    [Telemetry],
                    contract=_contract(),
                    grader=grader,
                    evaluation_id="concurrent-paid-grade",
                    storage_config=storage,
                ),
            )
        )
        await asyncio.wait_for(grader_started.wait(), timeout=30)
        second_task = asyncio.create_task(
            second_dispatcher.apply(
                _evaluate(
                    world.world_id,
                    [Telemetry],
                    contract=_contract(),
                    grader=grader,
                    evaluation_id="concurrent-paid-grade",
                    storage_config=storage,
                )
            )
        )

        await asyncio.sleep(0.2)
        assert calls == [1]
        assert not second_task.done()

        release_grader.set()
        first, second = await asyncio.gather(first_task, second_task)
        assert first == second
        assert calls == [1]
        assert len((await _results(first_storage, str(world.world_id), storage)).to_pylist()) == 1
    finally:
        release_grader.set()
        pending = [
            task for task in (first_task, second_task) if task is not None and not task.done()
        ]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await _shutdown(second_resources, second_storage)
        await _shutdown(first_resources, first_storage)


async def test_expired_owner_with_persisted_result_recovers_without_regrading(tmp_path):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        wid = str(world.world_id)
        snapshot = await evaluation_views.pin_snapshot(
            storage_service,
            world_id=wid,
            storage_config=storage,
        )
        contract = _contract()
        subject = subject_digest(
            wid,
            snapshot.run_id,
            snapshot_tick=snapshot.tick,
            snapshot_tokens=list(snapshot.head_tokens),
            component_names=[Telemetry.__name__],
        )
        catalog = storage_service.get_control_catalog(storage)
        await catalog.lease_evaluation(
            wid,
            snapshot.run_id,
            "append-before-crash",
            subject,
            contract.digest(),
            "crashed-owner",
            lease_seconds=0.01,
        )
        persisted = EvalReceipt(
            evaluation_id="append-before-crash",
            subject_digest=subject,
            contract_digest=contract.digest(),
            grader_id=contract.grader_id,
            outcome="pass",
            score=0.8,
            graded_at_ms=int(time.time() * 1000),
            evidence_json="{}",
        )
        await storage_service.append_world_rows(
            storage,
            wid,
            evaluation_views.EVALUATION_RESULTS_TABLE,
            daft.from_arrow(
                pa.Table.from_pylist(
                    [persisted.model_dump()],
                    schema=evaluation_handlers.EVALUATION_SCHEMA,
                )
            ),
            key_columns=("evaluation_id",),
        )
        await asyncio.sleep(0.02)
        calls: list[int] = []

        def must_not_grade(_df):
            calls.append(1)
            return Outcome(status="fail")

        recovered = await dispatcher.apply_as(
            _ctx(),
            _evaluate(
                wid,
                [Telemetry],
                contract=contract,
                grader=must_not_grade,
                evaluation_id="append-before-crash",
                storage_config=storage,
            ),
        )

        assert recovered == persisted
        assert calls == []
        complete = await catalog.lease_evaluation(
            wid,
            snapshot.run_id,
            "append-before-crash",
            subject,
            contract.digest(),
            "observer",
        )
        assert complete.status == "COMPLETE" and not complete.acquired
    finally:
        await _shutdown(resources, storage_service)


async def test_evaluation_heartbeat_renews_and_detects_lost_owner(monkeypatch):
    monkeypatch.setattr(evaluation_handlers, "EVALUATION_LEASE_SECONDS", 0.03)
    monkeypatch.setattr(evaluation_handlers, "EVALUATION_POLL_SECONDS", 0.001)
    lease = SimpleNamespace(
        world_id="world",
        run_id="run",
        evaluation_id="evaluation",
        subject_digest="subject",
        contract_digest="contract",
    )

    class Catalog:
        def __init__(self, *, acquired=True, owner="owner"):
            self.acquired = acquired
            self.owner = owner
            self.calls = 0
            self.renewed = asyncio.Event()

        async def lease_evaluation(self, *_args, **_kwargs):
            self.calls += 1
            self.renewed.set()
            return SimpleNamespace(acquired=self.acquired, owner=self.owner)

    renewing = Catalog()
    stop = asyncio.Event()
    lost = asyncio.Event()
    task = asyncio.create_task(containerless_heartbeat(renewing, lease, stop=stop, lost=lost))
    try:
        await asyncio.wait_for(renewing.renewed.wait(), timeout=1)
    finally:
        stop.set()
        await task
    assert renewing.calls >= 1
    assert not lost.is_set()

    displaced = Catalog(acquired=False, owner="other")
    lost = asyncio.Event()
    await asyncio.wait_for(
        containerless_heartbeat(displaced, lease, stop=asyncio.Event(), lost=lost),
        timeout=1,
    )
    assert lost.is_set()


async def test_failed_lease_release_does_not_mask_grader_failure(tmp_path, monkeypatch, caplog):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        catalog = storage_service.get_control_catalog(storage)

        async def fail_release(*_args, **_kwargs):
            raise RuntimeError("release transport failed")

        def fail_grader(_df):
            raise ValueError("grader failed")

        monkeypatch.setattr(catalog, "release_evaluation", fail_release)
        with caplog.at_level("WARNING", logger=evaluation_handlers.__name__):
            with pytest.raises(ValueError, match="grader failed"):
                await dispatcher.apply_as(
                    _ctx(),
                    _evaluate(
                        world.world_id,
                        [Telemetry],
                        contract=_contract(),
                        grader=fail_grader,
                        evaluation_id="failed-release-preserves-grader-error",
                        storage_config=storage,
                    ),
                )

        assert "failed to release durable evaluation lease" in caplog.text
        assert "release transport failed" not in caplog.text
    finally:
        await _shutdown(resources, storage_service)


async def containerless_heartbeat(catalog, lease, *, stop, lost):
    return await evaluation_handlers._heartbeat_evaluation(
        catalog,
        lease,
        owner="owner",
        stop=stop,
        lost=lost,
    )


async def test_grader_reads_captured_snapshot_when_world_advances(tmp_path, monkeypatch):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        original_snapshot = evaluation_views.pin_snapshot

        async def capture_then_advance(*args, **kwargs):
            snapshot = await original_snapshot(*args, **kwargs)
            await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))
            return snapshot

        monkeypatch.setattr(evaluation_views, "pin_snapshot", capture_then_advance)
        graded_ticks: list[int] = []

        def grader(df):
            graded_ticks.extend(int(row["tick"]) for row in df.to_pylist())
            return Outcome(status="pass", score=1.0)

        await dispatcher.apply_as(
            _ctx(),
            _evaluate(
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=grader,
                evaluation_id="pinned-while-advancing",
                storage_config=storage,
            ),
        )

        assert graded_ticks == [0]
    finally:
        await _shutdown(resources, storage_service)


async def test_distinct_trials_record_distinct_results(tmp_path):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        calls: list[int] = []

        for index, status in enumerate(("pass", "fail")):
            await dispatcher.apply_as(
                _ctx(),
                _evaluate(
                    world.world_id,
                    [Telemetry],
                    contract=_contract(),
                    grader=_counting_grader(calls, Outcome(status=status)),
                    evaluation_id=f"trial-{index}",
                    storage_config=storage,
                ),
            )

        assert len(calls) == 2
        outcomes = sorted(
            row["outcome"]
            for row in (await _results(storage_service, str(world.world_id), storage)).to_pylist()
        )
        assert outcomes == ["fail", "pass"]
    finally:
        await _shutdown(resources, storage_service)


async def test_same_id_with_different_contract_conflicts(tmp_path):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        grader = _counting_grader([], Outcome(status="pass"))

        await dispatcher.apply_as(
            _ctx(),
            _evaluate(
                world.world_id,
                [Telemetry],
                contract=_contract(),
                grader=grader,
                evaluation_id="trial-x",
                storage_config=storage,
            ),
        )
        with pytest.raises(ValueError, match="different subject or grader contract"):
            await dispatcher.apply_as(
                _ctx(),
                _evaluate(
                    world.world_id,
                    [Telemetry],
                    contract=_contract(implementation_version="2026.08.01"),
                    grader=grader,
                    evaluation_id="trial-x",
                    storage_config=storage,
                ),
            )
    finally:
        await _shutdown(resources, storage_service)


async def test_fail_closed_inputs(tmp_path):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)

        with pytest.raises(ValueError, match="GraderContract"):
            _evaluate(
                world.world_id,
                [Telemetry],
                contract=None,
                grader=lambda df: Outcome(status="pass"),
                evaluation_id="t",
                storage_config=storage,
            )
        with pytest.raises(ValueError, match="typed Outcome"):
            await dispatcher.apply_as(
                _ctx(),
                _evaluate(
                    world.world_id,
                    [Telemetry],
                    contract=_contract(),
                    grader=lambda df: 0.9,
                    evaluation_id="t2",
                    storage_config=storage,
                ),
            )

        bare = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="unstepped"),
                storage_config=storage,
            )
        )
        with pytest.raises(RuntimeError, match="no published visibility"):
            await dispatcher.apply_as(
                _ctx(),
                _evaluate(
                    bare.world_id,
                    [Telemetry],
                    contract=_contract(),
                    grader=lambda df: Outcome(status="pass"),
                    evaluation_id="t3",
                    storage_config=storage,
                ),
            )
        with pytest.raises(ValueError):
            Outcome(status="maybe")
        with pytest.raises(ValueError):
            Outcome(status="pass", score=float("inf"))
    finally:
        await _shutdown(resources, storage_service)


async def test_result_is_attributable_to_pinned_snapshot(tmp_path):
    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = _storage(tmp_path)
        world = await _seeded_world(dispatcher, storage)
        wid, rid = str(world.world_id), str(world.run_id)
        contract = _contract()

        await dispatcher.apply_as(
            _ctx(),
            _evaluate(
                wid,
                [Telemetry],
                contract=contract,
                grader=lambda df: Outcome(status="pass", score=0.8),
                evaluation_id="attrib",
                storage_config=storage,
            ),
        )
        row = (await _results(storage_service, wid, storage)).to_pylist()[0]

        catalog = storage_service.get_control_catalog(storage)
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
        await _shutdown(resources, storage_service)


async def test_cold_process_can_grade_persisted_world(tmp_path):
    script = textwrap.dedent(
        """
        import asyncio, json, sys
        from pathlib import Path

        from archetype.core.component import Component
        from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
        from archetype.storage.config import ControlCatalogConfig
        from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
        from archetype.world.models import CreateWorld, Spawn, Step

        class Telemetry(Component):
            reading: float = 0.0

        async def main(uri, control_dir):
            resources = build_runtime_resources(
                RuntimeBootstrapConfig(
                    control_catalog_config=ControlCatalogConfig(
                        catalog_dir=Path(control_dir),
                    )
                )
            )
            dispatcher = resources.dispatcher
            try:
                storage = StorageConfig(
                    uri=uri,
                    namespace="ns",
                    backend=StorageBackend.ICEBERG,
                )
                world = await dispatcher.apply(
                    CreateWorld(
                        config=WorldConfig(name="gpu"),
                        storage_config=storage,
                    )
                )
                await dispatcher.apply(
                    Spawn.from_components(
                        world_id=world.world_id,
                        components=[Telemetry(reading=0.9)],
                    )
                )
                for _ in range(3):
                    await dispatcher.apply(
                        Step(world_id=world.world_id, run_config=RunConfig())
                    )
                print(json.dumps({"world_id": str(world.world_id), "run_id": str(world.run_id)}))
            finally:
                await resources.aclose()

        asyncio.run(main(sys.argv[1], sys.argv[2]))
        """
    )
    uri = str(tmp_path / "store")
    control_dir = str(tmp_path / "control-catalogs")
    process = subprocess.run(
        [sys.executable, "-c", script, uri, control_dir],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert process.returncode == 0, process.stderr
    info = json.loads(process.stdout.strip().splitlines()[-1])

    resources, storage_service = _runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        storage = StorageConfig(
            uri=uri,
            namespace="ns",
            backend=StorageBackend.ICEBERG,
        )
        result = await dispatcher.apply(
            _evaluate(
                info["world_id"],
                [Telemetry],
                contract=_contract(),
                grader=lambda df: Outcome(
                    status="pass",
                    score=float(df.to_pylist()[-1]["telemetry__reading"]),
                ),
                evaluation_id="cold-grade-1",
                storage_config=storage,
            ),
        )
        assert result.outcome == "pass"
        rows = (await _results(storage_service, info["world_id"], storage)).to_pylist()
        assert len(rows) == 1
        assert rows[0]["outcome"] == "pass"
    finally:
        await _shutdown(resources, storage_service)
