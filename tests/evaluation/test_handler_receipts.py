# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable receipt contracts against the family-owned free handlers."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import daft
import pyarrow as pa
import pytest

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.evaluation import handlers, views
from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import subject_digest
from archetype.evaluation.models import Evaluate, GraderContract, Outcome
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world.models import CreateWorld, Spawn, Step
from archetype.world.registry import WorldRegistry
from tests._runtime import build_test_runtime

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("evaluation.result.snapshot_pinned"),
    pytest.mark.integration,
]


class Telemetry(Component):
    reading: float = 0.0


def _storage_config(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="family_eval",
        backend=StorageBackend.ICEBERG,
    )


def _storage_service(tmp_path: Path) -> StorageService:
    return StorageService(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / "control-catalogs",
        )
    )


def _contract(**overrides: object) -> GraderContract:
    values: dict[str, object] = {
        "grader_id": "mean-reading-v1",
        "implementation_version": "2026.07.24",
        "thresholds": {"min": 0.5},
    }
    values.update(overrides)
    return GraderContract(**values)  # type: ignore[arg-type]


def _evaluate(
    world_id: object,
    storage_config: StorageConfig,
    grader: object,
    *,
    evaluation_id: str,
    contract: GraderContract | None = None,
) -> Evaluate:
    return Evaluate(
        world_id=world_id,
        components=(Telemetry,),
        contract=contract or _contract(),
        grader=grader,
        evaluation_id=evaluation_id,
        storage_config=storage_config,
    )


async def _seed_world(dispatcher: object, storage_config: StorageConfig) -> object:
    world = await dispatcher.apply(  # type: ignore[attr-defined]
        CreateWorld(
            config=WorldConfig(name="family-eval"),
            storage_config=storage_config,
        )
    )
    await dispatcher.apply(  # type: ignore[attr-defined]
        Spawn.from_components(
            world_id=world.world_id,
            components=[Telemetry(reading=0.8)],
        )
    )
    await dispatcher.apply(  # type: ignore[attr-defined]
        Step(world_id=world.world_id, run_config=RunConfig())
    )
    return world


async def _close_runtime(resources: object, storage: StorageService) -> None:
    await resources.aclose()  # type: ignore[attr-defined]
    await storage.shutdown()


async def test_cold_explicit_storage_replays_without_registry_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_config = _storage_config(tmp_path)
    warm_storage = _storage_service(tmp_path)
    resources = build_test_runtime(tmp_path, storage_service=warm_storage)
    world = await _seed_world(resources.dispatcher, storage_config)
    world_id = str(world.world_id)  # type: ignore[attr-defined]
    await _close_runtime(resources, warm_storage)

    async def registry_trap(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("cold evaluation consulted the live world registry")

    monkeypatch.setattr(WorldRegistry, "storage_record", registry_trap)
    cold_storage = _storage_service(tmp_path)
    calls: list[int] = []

    def grader(frame: object) -> Outcome:
        calls.append(1)
        rows = frame.to_pylist()  # type: ignore[attr-defined]
        return Outcome(status="pass", score=float(rows[-1]["telemetry__reading"]))

    try:
        operation = _evaluate(
            world_id,
            storage_config,
            grader,
            evaluation_id="cold-explicit",
        )
        first = await handlers.evaluate(cold_storage, operation)
        replay = await handlers.evaluate(cold_storage, operation)

        assert first == replay
        assert first.outcome == "pass"
        assert calls == [1]
        rows = (
            await cold_storage.read_world_rows(
                storage_config,
                world_id,
                views.EVALUATION_RESULTS_TABLE,
            )
        ).to_pylist()
        assert [row["evaluation_id"] for row in rows] == ["cold-explicit"]
    finally:
        await cold_storage.shutdown()


async def test_independent_handlers_pay_exactly_one_racing_grader(
    tmp_path: Path,
) -> None:
    storage_config = _storage_config(tmp_path)
    first_storage = _storage_service(tmp_path)
    second_storage = _storage_service(tmp_path)
    resources = build_test_runtime(tmp_path, storage_service=first_storage)
    release = asyncio.Event()
    first_task: asyncio.Task[EvalReceipt] | None = None
    second_task: asyncio.Task[EvalReceipt] | None = None
    try:
        world = await _seed_world(resources.dispatcher, storage_config)
        started = asyncio.Event()
        calls: list[int] = []

        async def grader(frame: object) -> Outcome:
            calls.append(1)
            assert frame.to_pylist()  # type: ignore[attr-defined]
            started.set()
            await release.wait()
            return Outcome(status="pass", score=0.8)

        operation = _evaluate(
            world.world_id,  # type: ignore[attr-defined]
            storage_config,
            grader,
            evaluation_id="racing-paid-grade",
        )
        first_task = asyncio.create_task(handlers.evaluate(first_storage, operation))
        await asyncio.wait_for(started.wait(), timeout=30)
        second_task = asyncio.create_task(handlers.evaluate(second_storage, operation))

        await asyncio.sleep(0.2)
        assert calls == [1]
        assert not second_task.done()

        release.set()
        first, second = await asyncio.gather(first_task, second_task)
        assert first == second
        assert calls == [1]
    finally:
        release.set()
        pending = [
            task for task in (first_task, second_task) if task is not None and not task.done()
        ]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await resources.aclose()
        await second_storage.shutdown()
        await first_storage.shutdown()


async def test_append_before_control_completion_recovers_without_regrading(
    tmp_path: Path,
) -> None:
    storage_config = _storage_config(tmp_path)
    storage = _storage_service(tmp_path)
    resources = build_test_runtime(tmp_path, storage_service=storage)
    try:
        world = await _seed_world(resources.dispatcher, storage_config)
        world_id = str(world.world_id)  # type: ignore[attr-defined]
        snapshot = await views.pin_snapshot(
            storage,
            world_id=world_id,
            storage_config=storage_config,
        )
        contract = _contract()
        subject = subject_digest(
            world_id,
            snapshot.run_id,
            snapshot_tick=snapshot.tick,
            snapshot_tokens=list(snapshot.head_tokens),
            component_names=[Telemetry.__name__],
        )
        catalog = storage.get_control_catalog(storage_config)
        await catalog.lease_evaluation(
            world_id,
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
        await storage.append_world_rows(
            storage_config,
            world_id,
            views.EVALUATION_RESULTS_TABLE,
            daft.from_arrow(
                pa.Table.from_pylist(
                    [persisted.model_dump()],
                    schema=handlers.EVALUATION_SCHEMA,
                )
            ),
            key_columns=("evaluation_id",),
        )
        await asyncio.sleep(0.02)
        calls: list[int] = []

        def must_not_grade(_frame: object) -> Outcome:
            calls.append(1)
            return Outcome(status="fail")

        recovered = await handlers.evaluate(
            storage,
            _evaluate(
                world_id,
                storage_config,
                must_not_grade,
                contract=contract,
                evaluation_id="append-before-crash",
            ),
        )

        assert recovered == persisted
        assert calls == []
        complete = await catalog.lease_evaluation(
            world_id,
            snapshot.run_id,
            "append-before-crash",
            subject,
            contract.digest(),
            "observer",
        )
        assert complete.status == "COMPLETE"
        assert not complete.acquired
    finally:
        await resources.aclose()
        await storage.shutdown()


async def test_pinned_subject_does_not_follow_a_later_tick(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_config = _storage_config(tmp_path)
    storage = _storage_service(tmp_path)
    resources = build_test_runtime(tmp_path, storage_service=storage)
    try:
        world = await _seed_world(resources.dispatcher, storage_config)
        original = views.pin_snapshot

        async def capture_then_advance(*args: object, **kwargs: object) -> object:
            snapshot = await original(*args, **kwargs)  # type: ignore[arg-type]
            await resources.dispatcher.apply(
                Step(world_id=world.world_id, run_config=RunConfig())  # type: ignore[attr-defined]
            )
            return snapshot

        monkeypatch.setattr(views, "pin_snapshot", capture_then_advance)
        graded_ticks: list[int] = []

        def grader(frame: object) -> Outcome:
            graded_ticks.extend(
                int(row["tick"])
                for row in frame.to_pylist()  # type: ignore[attr-defined]
            )
            return Outcome(status="pass")

        await handlers.evaluate(
            storage,
            _evaluate(
                world.world_id,  # type: ignore[attr-defined]
                storage_config,
                grader,
                evaluation_id="pinned-before-advance",
            ),
        )
        assert graded_ticks == [0]
    finally:
        await resources.aclose()
        await storage.shutdown()


async def test_heartbeat_renews_and_detects_displacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(handlers, "EVALUATION_LEASE_SECONDS", 0.03)
    monkeypatch.setattr(handlers, "EVALUATION_POLL_SECONDS", 0.001)
    lease = SimpleNamespace(
        world_id="world",
        run_id="run",
        evaluation_id="evaluation",
        subject_digest="subject",
        contract_digest="contract",
    )

    class Catalog:
        def __init__(self, *, acquired: bool = True, owner: str = "owner") -> None:
            self.acquired = acquired
            self.owner = owner
            self.calls = 0
            self.renewed = asyncio.Event()

        async def lease_evaluation(self, *_args: object, **_kwargs: object) -> object:
            self.calls += 1
            self.renewed.set()
            return SimpleNamespace(acquired=self.acquired, owner=self.owner)

    renewing = Catalog()
    stop = asyncio.Event()
    lost = asyncio.Event()
    task = asyncio.create_task(
        handlers._heartbeat_evaluation(
            renewing,
            lease,
            owner="owner",
            stop=stop,
            lost=lost,
        )
    )
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
        handlers._heartbeat_evaluation(
            displaced,
            lease,
            owner="owner",
            stop=asyncio.Event(),
            lost=lost,
        ),
        timeout=1,
    )
    assert lost.is_set()


async def test_identity_conflict_and_release_failure_remain_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    storage_config = _storage_config(tmp_path)
    storage = _storage_service(tmp_path)
    resources = build_test_runtime(tmp_path, storage_service=storage)
    try:
        world = await _seed_world(resources.dispatcher, storage_config)
        passing = _evaluate(
            world.world_id,  # type: ignore[attr-defined]
            storage_config,
            lambda _frame: Outcome(status="pass"),
            evaluation_id="identity",
        )
        await handlers.evaluate(storage, passing)
        with pytest.raises(ValueError, match="different subject or grader contract"):
            await handlers.evaluate(
                storage,
                _evaluate(
                    world.world_id,  # type: ignore[attr-defined]
                    storage_config,
                    lambda _frame: Outcome(status="pass"),
                    contract=_contract(implementation_version="v2"),
                    evaluation_id="identity",
                ),
            )

        catalog = storage.get_control_catalog(storage_config)

        async def fail_release(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("release transport failed")

        monkeypatch.setattr(catalog, "release_evaluation", fail_release)

        def fail_grader(_frame: object) -> Outcome:
            raise ValueError("grader failed")

        with caplog.at_level("WARNING", logger=handlers.__name__):
            with pytest.raises(ValueError, match="grader failed"):
                await handlers.evaluate(
                    storage,
                    _evaluate(
                        world.world_id,  # type: ignore[attr-defined]
                        storage_config,
                        fail_grader,
                        evaluation_id="failed-release",
                    ),
                )
        assert "failed to release durable evaluation lease" in caplog.text
        assert "release transport failed" not in caplog.text
    finally:
        await resources.aclose()
        await storage.shutdown()
