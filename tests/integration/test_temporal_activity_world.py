# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small end-to-end proof of Temporal-owned work between ECS ticks."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment

from archetype.activities import (
    ActivityAdmission,
    ActivityCoordinator,
    ActivityResultRef,
    ActivitySettlement,
)
from archetype.activities.temporal import create_temporal_worker, durable_workflow_id
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import CommittedTickReceipt, StaleWriterError
from archetype.runtime_resources import RuntimeResources
from archetype.storage.activity_catalog import SqliteActivityCatalog
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.models import (
    ComponentTypeRef,
    ComponentValue,
    CreateWorld,
    DestroyWorld,
    ForkWorld,
    QueryComponents,
    ResumeWorld,
    Spawn,
    Step,
    Update,
    WorldInfo,
)
from archetype.world.registry import WorldRegistry
from tests._runtime import build_test_runtime

_ACTIVITY_KIND = "proof.counter.double"
_ACTIVITY_NAME = "proof.counter.double.execute"
_TASK_QUEUE = "archetype-temporal-counter-proof"
_WORKFLOW_NAME = "proof.counter.double.workflow"
_FORK_START_ACTIVITY = "proof.counter.fork.start"
_FORK_RESUME_ACTIVITY = "proof.counter.fork.resume"
_FORK_TASK_QUEUE = "archetype-temporal-fork-proof"
_FORK_WORKFLOW_NAME = "proof.counter.fork.workflow"


class TemporalCounter(Component):
    value: int = 0


@dataclass(frozen=True, slots=True)
class _DoubleInput:
    workflow_id: str
    world_id: str
    activity_id: str
    value: int


@dataclass(frozen=True, slots=True)
class _DoubleOutput:
    value: int
    result_ref: str
    result_digest: str


@dataclass(frozen=True, slots=True)
class _ForkInput:
    workflow_id: str
    start_paused: bool = True


@dataclass(frozen=True, slots=True)
class _ForkCheckpoint:
    workflow_id: str
    parent_world_id: str
    parent_run_id: str
    fork_world_id: str
    fork_run_id: str
    entity_id: int
    parent_tick: int
    fork_tick: int


@dataclass(frozen=True, slots=True)
class _ForkRecovery:
    fork_world_id: str
    run_id: str
    resumed_tick: int
    value: int


@workflow.defn(name=_WORKFLOW_NAME)
class _DoubleWorkflow:
    @workflow.run
    async def run(self, command: _DoubleInput) -> _DoubleOutput:
        return await workflow.execute_activity(
            _ACTIVITY_NAME,
            command,
            result_type=_DoubleOutput,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=RetryPolicy(maximum_attempts=3),
            activity_id=f"{command.workflow_id}:double",
        )


@workflow.defn(name=_FORK_WORKFLOW_NAME)
class _ForkRecoveryWorkflow:
    def __init__(self) -> None:
        self._checkpoint: _ForkCheckpoint | None = None
        self._release = False

    @workflow.run
    async def run(self, command: _ForkInput) -> _ForkRecovery:
        self._checkpoint = await workflow.execute_activity(
            _FORK_START_ACTIVITY,
            command,
            result_type=_ForkCheckpoint,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
            activity_id=f"{command.workflow_id}:fork",
        )
        if command.start_paused:
            await workflow.wait_condition(lambda: self._release)
        return await workflow.execute_activity(
            _FORK_RESUME_ACTIVITY,
            self._checkpoint,
            result_type=_ForkRecovery,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=2),
            activity_id=f"{command.workflow_id}:resume",
        )

    @workflow.signal
    def release_recovery(self) -> None:
        self._release = True

    @workflow.query
    def checkpoint(self) -> _ForkCheckpoint | None:
        return self._checkpoint


class _DoubleActivities:
    def __init__(self, coordinator: ActivityCoordinator) -> None:
        self._coordinator = coordinator

    @activity.defn(name=_ACTIVITY_NAME)
    async def execute(self, command: _DoubleInput) -> _DoubleOutput:
        existing = await self._coordinator.get(
            command.world_id,
            _ACTIVITY_KIND,
            command.activity_id,
        )
        if existing is None:
            raise RuntimeError("Temporal Activity has no committed ECS admission")
        if existing.result is not None:
            value = int(existing.result.ref.rsplit("/", maxsplit=1)[-1])
            return _DoubleOutput(value, existing.result.ref, existing.result.digest)

        claim = await self._coordinator.claim(
            command.world_id,
            _ACTIVITY_KIND,
            command.activity_id,
            owner=f"temporal:{command.workflow_id}",
        )
        if not claim.acquired:
            raise RuntimeError("Temporal Activity result publication is already claimed")
        bound = await self._coordinator.bind_provider_operation(
            claim,
            "temporal",
            command.workflow_id,
        )
        value = command.value * 2
        encoded = json.dumps({"value": value}, separators=(",", ":"), sort_keys=True).encode()
        result = ActivityResultRef(
            ref=f"inline://counter/{value}",
            digest=hashlib.sha256(encoded).hexdigest(),
            media_type="application/json",
            size_bytes=len(encoded),
        )
        await self._coordinator.record_result(bound, result)
        return _DoubleOutput(value, result.ref, result.digest)


class _ForkActivities:
    """Own the incumbent and replacement runtime hosts for the crash proof."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._control = ControlCatalogConfig(catalog_dir=root / "fork-control")
        self._storage = StorageConfig(uri=str(root / "fork-worlds"), namespace="fork-proof")
        self.incumbent_resources: RuntimeResources | None = None
        self.incumbent_storage: StorageService | None = None
        self.replacement_resources: RuntimeResources | None = None
        self.replacement_storage: StorageService | None = None

    def _build_host(self) -> tuple[RuntimeResources, StorageService, WorldRegistry]:
        storage = StorageService(control_catalog_config=self._control)
        worlds = WorldRegistry()
        resources = build_runtime_resources(
            RuntimeBootstrapConfig(
                control_catalog_config=self._control,
                storage_service=storage,
                world_registry=worlds,
            )
        )
        return resources, storage, worlds

    @activity.defn(name=_FORK_START_ACTIVITY)
    async def start_fork(self, command: _ForkInput) -> _ForkCheckpoint:
        resources, storage_service, worlds = self._build_host()
        self.incumbent_resources = resources
        self.incumbent_storage = storage_service
        parent = await resources.dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="temporal-fork-parent"),
                storage_config=self._storage,
            )
        )
        entity_id = await resources.dispatcher.apply(
            Spawn.from_components(
                world_id=parent.world_id,
                components=[TemporalCounter(value=21)],
            )
        )
        await resources.dispatcher.apply(Step(world_id=parent.world_id))
        fork = await resources.dispatcher.apply(
            ForkWorld(source_world_id=parent.world_id, name="temporal-fork-child")
        )

        await resources.dispatcher.apply(
            Update(
                world_id=parent.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(TemporalCounter(value=22)),),
            )
        )
        await resources.dispatcher.apply(Step(world_id=parent.world_id))
        await resources.dispatcher.apply(
            Update(
                world_id=fork.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(TemporalCounter(value=100)),),
            )
        )
        await resources.dispatcher.apply(Step(world_id=fork.world_id))

        async with worlds.operation(parent.world_id) as parent_world:
            parent_tick = parent_world.tick
        async with worlds.operation(fork.world_id) as fork_world:
            fork_tick = fork_world.tick
        return _ForkCheckpoint(
            workflow_id=command.workflow_id,
            parent_world_id=str(parent.world_id),
            parent_run_id=str(parent.run_id),
            fork_world_id=str(fork.world_id),
            fork_run_id=str(fork.run_id),
            entity_id=entity_id,
            parent_tick=parent_tick,
            fork_tick=fork_tick,
        )

    @activity.defn(name=_FORK_RESUME_ACTIVITY)
    async def resume_fork(self, checkpoint: _ForkCheckpoint) -> _ForkRecovery:
        resources, storage_service, worlds = self._build_host()
        self.replacement_resources = resources
        self.replacement_storage = storage_service
        await resources.dispatcher.apply(
            ResumeWorld(
                storage_config=self._storage,
                world_id=checkpoint.fork_world_id,
            )
        )
        await resources.dispatcher.apply(
            Update(
                world_id=checkpoint.fork_world_id,
                entity_id=checkpoint.entity_id,
                components=(ComponentValue.from_component(TemporalCounter(value=101)),),
            )
        )
        await resources.dispatcher.apply(Step(world_id=checkpoint.fork_world_id))
        async with worlds.operation(checkpoint.fork_world_id) as resumed:
            resumed_tick = resumed.tick
            run_id = str(resumed.run_id)
        rows = await _counter_rows(
            resources,
            WorldInfo(
                world_id=checkpoint.fork_world_id,
                run_id=checkpoint.fork_run_id,
                tick=resumed_tick,
            ),
            self._storage,
            ticks=(resumed_tick - 1,),
        )
        return _ForkRecovery(
            fork_world_id=checkpoint.fork_world_id,
            run_id=run_id,
            resumed_tick=resumed_tick,
            value=int(rows[0]["temporalcounter__value"]),
        )

    async def close(self) -> None:
        for resources, storage in (
            (self.replacement_resources, self.replacement_storage),
            (self.incumbent_resources, self.incumbent_storage),
        ):
            if resources is not None:
                await resources.aclose()
            if storage is not None:
                await storage.shutdown()


async def _receipt(worlds: WorldRegistry, world_id: object) -> CommittedTickReceipt:
    async with worlds.operation(world_id) as world:
        receipt = world.last_committed_receipt
    assert isinstance(receipt, CommittedTickReceipt)
    assert receipt.visibility_token is not None
    return receipt


async def _counter_rows(
    resources: RuntimeResources,
    info: WorldInfo,
    storage: StorageConfig,
    *,
    ticks: tuple[int, ...],
) -> list[dict[str, Any]]:
    frame = await resources.dispatcher.apply(
        QueryComponents(
            components=(ComponentTypeRef.from_type(TemporalCounter),),
            world_id=info.world_id,
            run_id=info.run_id,
            storage_config=storage,
            ticks=ticks,
        )
    )
    return frame.to_pylist()


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.process
async def test_trivial_temporal_activity_crosses_two_real_world_receipts(
    tmp_path: Path,
) -> None:
    """A committed world admits work; a later commit observes its result."""

    worlds = WorldRegistry()
    resources = build_test_runtime(tmp_path, world_registry=worlds)
    catalog = SqliteActivityCatalog(tmp_path / "counter-activities.db")
    coordinator = ActivityCoordinator(catalog)
    environment = await WorkflowEnvironment.start_local()
    storage = StorageConfig(uri=str(tmp_path / "counter-world"), namespace="counter-proof")
    info: WorldInfo | None = None
    try:
        info = await resources.dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="temporal-counter-proof"),
                storage_config=storage,
            )
        )
        entity_id = await resources.dispatcher.apply(
            Spawn.from_components(
                world_id=info.world_id,
                components=[TemporalCounter(value=21)],
            )
        )
        await resources.dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        source = await _receipt(worlds, info.world_id)

        input_digest = hashlib.sha256(b'{"value":21}').hexdigest()
        activity_id = "double-counter-once"
        workflow_id = durable_workflow_id(
            _WORKFLOW_NAME,
            "counter-proof",
            activity_id,
            prefix="activity",
        )
        await coordinator.admit(
            ActivityAdmission(
                activity_id=activity_id,
                kind=_ACTIVITY_KIND,
                source=source,
                input_ref="inline://counter/21",
                input_digest=input_digest,
            )
        )

        activities = _DoubleActivities(coordinator)
        worker = create_temporal_worker(
            environment.client,
            task_queue=_TASK_QUEUE,
            workflows=[_DoubleWorkflow],
            activities=[activities.execute],
            passthrough_modules=("archetype", __name__),
        )
        async with worker:
            result = await environment.client.execute_workflow(
                _WORKFLOW_NAME,
                _DoubleInput(
                    workflow_id=workflow_id,
                    world_id=str(info.world_id),
                    activity_id=activity_id,
                    value=21,
                ),
                id=workflow_id,
                task_queue=_TASK_QUEUE,
                result_type=_DoubleOutput,
            )

        assert result.value == 42
        pending = await coordinator.pending_results(
            kind=_ACTIVITY_KIND,
            world_id=str(info.world_id),
        )
        assert len(pending) == 1
        assert pending[0].result is not None
        assert pending[0].result.ref == "inline://counter/42"

        await resources.dispatcher.apply(
            Update(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(TemporalCounter(value=result.value)),),
            )
        )
        await resources.dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        observed = await _receipt(worlds, info.world_id)
        settled = await coordinator.settle_observation(
            str(info.world_id),
            _ACTIVITY_KIND,
            activity_id,
            ActivitySettlement(receipt=observed, result_digest=result.result_digest),
        )

        assert source.committed_tick == 0
        assert observed.committed_tick == 1
        assert settled.settlement is not None
        assert settled.settlement.receipt.identity == observed.identity
        rows = await _counter_rows(resources, info, storage, ticks=(1,))
        assert rows[0]["temporalcounter__value"] == 42
        assert not await coordinator.has_unsettled(str(info.world_id))
        await resources.dispatcher.apply(DestroyWorld(world_id=info.world_id))
        info = None
    finally:
        await environment.shutdown()
        if info is not None and not await coordinator.has_unsettled(str(info.world_id)):
            await resources.dispatcher.apply(DestroyWorld(world_id=info.world_id))
        await resources.aclose()
        await catalog.close()


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.process
async def test_replacement_worker_fences_and_recovers_a_mid_simulation_fork(
    tmp_path: Path,
) -> None:
    """Temporal history resumes one fork while fencing its stale writer."""

    environment = await WorkflowEnvironment.start_local()
    activities = _ForkActivities(tmp_path)
    workflow_id = durable_workflow_id(
        _FORK_WORKFLOW_NAME,
        "counter-proof",
        "recover-one-fork",
        prefix="world",
    )
    try:
        first_worker = create_temporal_worker(
            environment.client,
            task_queue=_FORK_TASK_QUEUE,
            workflows=[_ForkRecoveryWorkflow],
            activities=[activities.start_fork, activities.resume_fork],
            passthrough_modules=("archetype", __name__),
        )
        async with first_worker:
            handle = await environment.client.start_workflow(
                _FORK_WORKFLOW_NAME,
                _ForkInput(workflow_id=workflow_id),
                id=workflow_id,
                task_queue=_FORK_TASK_QUEUE,
                result_type=_ForkRecovery,
            )

            async def wait_for_checkpoint() -> _ForkCheckpoint:
                while True:
                    checkpoint = await handle.query(_ForkRecoveryWorkflow.checkpoint)
                    if checkpoint is not None:
                        return checkpoint
                    await asyncio.sleep(0.01)

            checkpoint = await asyncio.wait_for(wait_for_checkpoint(), timeout=20)
            assert checkpoint.parent_tick == 2
            assert checkpoint.fork_tick == 2
            assert checkpoint.parent_world_id != checkpoint.fork_world_id

        # The first Temporal Worker is now gone. A replacement consumes the
        # durable history, resumes only the fork, and advances its next tick.
        replacement_worker = create_temporal_worker(
            environment.client,
            task_queue=_FORK_TASK_QUEUE,
            workflows=[_ForkRecoveryWorkflow],
            activities=[activities.start_fork, activities.resume_fork],
            passthrough_modules=("archetype", __name__),
        )
        async with replacement_worker:
            await handle.signal(_ForkRecoveryWorkflow.release_recovery)
            recovered = await handle.result()

        assert recovered.fork_world_id == checkpoint.fork_world_id
        assert recovered.run_id == checkpoint.fork_run_id
        assert recovered.resumed_tick == 3
        assert recovered.value == 101

        incumbent = activities.incumbent_resources
        assert incumbent is not None
        # A stale epoch may surface directly or through prepared-commit
        # reconciliation after the replacement has published that same tick.
        with pytest.raises((StaleWriterError, RuntimeError)):
            await incumbent.dispatcher.apply(
                Step(world_id=checkpoint.fork_world_id, run_config=RunConfig())
            )
    finally:
        await activities.close()
        await environment.shutdown()
