# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small end-to-end proof of Temporal-owned work between ECS ticks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from uuid_utils import UUID

from archetype.activities import (
    ActivityAdmission,
    ActivityCoordinator,
    ActivityResultRef,
    ActivitySettlement,
)
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import OnDestroy
from archetype.core.interfaces import CommittedTickReceipt
from archetype.orchestration.temporal import create_temporal_worker, durable_workflow_id
from archetype.runtime_resources import RuntimeResources
from archetype.storage.activity_catalog import SqliteActivityCatalog
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.errors import WorldHasUnsettledWorkError
from archetype.world.models import (
    AddHook,
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
from archetype.world.simulation import RequiredProjector
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
    fork_status: str
    fork_writer_epoch: int
    lineage_json: str
    parent_manifest_tokens_json: str
    manifest_tokens_json: str


@dataclass(frozen=True, slots=True)
class _ForkRecovery:
    fork_world_id: str
    run_id: str
    resumed_tick: int
    value: int
    status_before_resume: str
    status_after_resume: str
    writer_epoch: int
    lineage_json: str
    parent_manifest_tokens_json: str
    manifest_tokens_json: str
    visible_ticks_json: str
    destroy_hook_fired: bool


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
            retry_policy=RetryPolicy(maximum_attempts=3),
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
    """Hard-kill one host, then own the replacement used by Temporal."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._control = ControlCatalogConfig(catalog_dir=root / "fork-control")
        self._storage = StorageConfig(uri=str(root / "fork-worlds"), namespace="fork-proof")
        self._checkpoint_path = root / "fork-crash-checkpoint.json"
        self.destroyed_marker = root / "fork-destroyed.marker"
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
        # The checkpoint is a stable provider receipt. If Temporal loses the
        # Activity response after the subprocess committed its fork, a retry
        # reconciles this file instead of creating another parent/fork pair.
        if not self._checkpoint_path.exists():
            fixture = Path(__file__).resolve().parents[1] / "fixtures" / "temporal_fork_owner.py"
            process = await asyncio.to_thread(
                subprocess.run,
                [
                    sys.executable,
                    str(fixture),
                    str(self._control.catalog_dir),
                    self._storage.uri,
                    str(self._checkpoint_path),
                    str(self.destroyed_marker),
                    command.workflow_id,
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if process.returncode != 17:
                raise RuntimeError(
                    "fork-owner crash fixture did not reach its hard-exit checkpoint: "
                    f"returncode={process.returncode}, stderr={process.stderr[-2000:]}"
                )
        payload = json.loads(self._checkpoint_path.read_text(encoding="utf-8"))
        return _ForkCheckpoint(**payload)

    @activity.defn(name=_FORK_RESUME_ACTIVITY)
    async def resume_fork(self, checkpoint: _ForkCheckpoint) -> _ForkRecovery:
        resources, storage_service, worlds = self._build_host()
        self.replacement_resources = resources
        self.replacement_storage = storage_service
        catalog = storage_service.get_control_catalog(self._storage)
        before = await catalog.get_world(checkpoint.fork_world_id)
        if before is None:
            raise RuntimeError("hard-killed fork is absent from the durable catalog")
        if before.status != "active":
            raise RuntimeError("process death must not destroy its durable fork")
        expected_tokens = json.loads(checkpoint.manifest_tokens_json)
        absolute_target_tick = checkpoint.fork_tick + 1
        before_manifests = await catalog.list_manifests(
            checkpoint.fork_world_id,
            checkpoint.fork_run_id,
        )
        before_tokens = {str(item.tick): item.commit_token for item in before_manifests}
        already_advanced = (
            set(before_tokens)
            == {
                *expected_tokens,
                str(checkpoint.fork_tick),
            }
            and {key: before_tokens.get(key) for key in expected_tokens} == expected_tokens
        )
        if before_tokens != expected_tokens and not already_advanced:
            raise RuntimeError("fork manifests changed between hard kill and recovery")
        await resources.dispatcher.apply(
            ResumeWorld(
                storage_config=self._storage,
                world_id=checkpoint.fork_world_id,
            )
        )
        async with worlds.operation(checkpoint.fork_world_id) as resumed:
            expected_resume_tick = (
                absolute_target_tick if already_advanced else checkpoint.fork_tick
            )
            if resumed.tick != expected_resume_tick:
                raise RuntimeError("fork did not resume at its absolute committed target tick")
            coordinator = resumed.commit_coordinator
            if coordinator is None or coordinator.writer_epoch <= checkpoint.fork_writer_epoch:
                raise RuntimeError("replacement writer did not acquire a higher fence epoch")
            normalized_lineage = [list(segment) for segment in resumed.lineage]
            if normalized_lineage != json.loads(checkpoint.lineage_json):
                raise RuntimeError("fork lineage changed during cold resume")
            resumed_writer_epoch = coordinator.writer_epoch
            resumed_lineage = normalized_lineage
        if not already_advanced:
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
        if resumed_tick != absolute_target_tick:
            raise RuntimeError("fork recovery overshot its absolute target tick")
        after = await catalog.get_world(checkpoint.fork_world_id)
        if after is None:
            raise RuntimeError("resumed fork disappeared from the durable catalog")
        after_manifests = await catalog.list_manifests(
            checkpoint.fork_world_id,
            checkpoint.fork_run_id,
        )
        after_tokens = {str(item.tick): item.commit_token for item in after_manifests}
        if {key: after_tokens.get(key) for key in before_tokens} != before_tokens:
            raise RuntimeError("fork recovery replayed or replaced a committed tick token")
        if set(after_tokens) != {*before_tokens, str(checkpoint.fork_tick)}:
            raise RuntimeError("fork recovery did not publish exactly one next tick")
        if len(set(after_tokens.values())) != len(after_tokens):
            raise RuntimeError("fork recovery reused a prior commit token")
        parent_manifests = await catalog.list_manifests(
            checkpoint.parent_world_id,
            checkpoint.parent_run_id,
        )
        parent_tokens = {str(item.tick): item.commit_token for item in parent_manifests}
        if parent_tokens != json.loads(checkpoint.parent_manifest_tokens_json):
            raise RuntimeError("recovering the fork mutated its parent manifest history")
        parent_rows = await _counter_rows(
            resources,
            WorldInfo(
                world_id=checkpoint.parent_world_id,
                run_id=checkpoint.parent_run_id,
                tick=checkpoint.parent_tick,
            ),
            self._storage,
            ticks=(checkpoint.parent_tick - 1,),
        )
        if len(parent_rows) != 1 or int(parent_rows[0]["temporalcounter__value"]) != 22:
            raise RuntimeError("recovering the fork changed its parent's visible state")
        rows = await _counter_rows(
            resources,
            WorldInfo(
                world_id=checkpoint.fork_world_id,
                run_id=checkpoint.fork_run_id,
                tick=resumed_tick,
            ),
            self._storage,
            ticks=tuple(range(resumed_tick)),
        )
        visible_ticks = sorted(int(row["tick"]) for row in rows)
        if visible_ticks != list(range(resumed_tick)):
            raise RuntimeError(
                "fork recovery did not expose exactly one row set per committed tick: "
                f"{visible_ticks!r}"
            )
        target_rows = [row for row in rows if int(row["tick"]) == resumed_tick - 1]
        if len(target_rows) != 1:
            raise RuntimeError("fork recovery exposed duplicate rows at the absolute target tick")
        return _ForkRecovery(
            fork_world_id=checkpoint.fork_world_id,
            run_id=run_id,
            resumed_tick=resumed_tick,
            value=int(target_rows[0]["temporalcounter__value"]),
            status_before_resume=before.status,
            status_after_resume=after.status,
            writer_epoch=resumed_writer_epoch,
            lineage_json=json.dumps(resumed_lineage, separators=(",", ":")),
            parent_manifest_tokens_json=json.dumps(
                parent_tokens,
                sort_keys=True,
                separators=(",", ":"),
            ),
            manifest_tokens_json=json.dumps(
                after_tokens,
                sort_keys=True,
                separators=(",", ":"),
            ),
            visible_ticks_json=json.dumps(visible_ticks, separators=(",", ":")),
            destroy_hook_fired=self.destroyed_marker.exists(),
        )

    async def destroy_and_refuse_resume(self, checkpoint: _ForkCheckpoint) -> tuple[str, int]:
        resources = self.replacement_resources
        storage_service = self.replacement_storage
        if resources is None or storage_service is None:
            raise RuntimeError("replacement host is not active")

        async def on_destroy(_event: OnDestroy) -> None:
            count = (
                int(self.destroyed_marker.read_text(encoding="utf-8"))
                if self.destroyed_marker.exists()
                else 0
            )
            self.destroyed_marker.write_text(str(count + 1), encoding="utf-8")

        await resources.dispatcher.apply(
            AddHook(
                world_id=checkpoint.fork_world_id,
                event_type=OnDestroy,
                handler=on_destroy,
            )
        )
        await resources.dispatcher.apply(DestroyWorld(world_id=checkpoint.fork_world_id))
        record = await storage_service.get_control_catalog(self._storage).get_world(
            checkpoint.fork_world_id
        )
        if record is None:
            raise RuntimeError("destroyed fork lost its catalog tombstone")
        destroy_count = int(self.destroyed_marker.read_text(encoding="utf-8"))
        if destroy_count != 1:
            raise RuntimeError("explicit destruction must run OnDestroy exactly once")

        verifier_resources, verifier_storage, _worlds = self._build_host()
        try:
            with pytest.raises(RuntimeError, match="destroyed"):
                await verifier_resources.dispatcher.apply(
                    ResumeWorld(
                        storage_config=self._storage,
                        world_id=checkpoint.fork_world_id,
                    )
                )
        finally:
            await verifier_resources.aclose()
            await verifier_storage.shutdown()
        return record.status, destroy_count

    async def close(self) -> None:
        if self.replacement_resources is not None:
            await self.replacement_resources.aclose()
        if self.replacement_storage is not None:
            await self.replacement_storage.shutdown()


async def _receipt(worlds: WorldRegistry, world_id: str | UUID) -> CommittedTickReceipt:
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

    catalog = SqliteActivityCatalog(tmp_path / "counter-activities.db")
    coordinator = ActivityCoordinator(catalog)
    worlds = WorldRegistry()
    projection: dict[str, str | None] = {"activity_id": None, "result_digest": None}

    async def settle_projected_result(receipt: CommittedTickReceipt) -> None:
        activity_id = projection["activity_id"]
        result_digest = projection["result_digest"]
        if activity_id is None or result_digest is None:
            return
        await coordinator.settle_observation(
            str(receipt.world_id),
            _ACTIVITY_KIND,
            activity_id,
            ActivitySettlement(receipt=receipt, result_digest=result_digest),
        )

    projector = RequiredProjector(
        consumer_name="proof.counter.result-settlement",
        project=settle_projected_result,
    )
    resources = build_test_runtime(
        tmp_path,
        world_registry=worlds,
        required_projector_factory=lambda _world_id: projector,
        unsettled_world_oracle=coordinator.has_unsettled,
    )
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
        assert await coordinator.has_unsettled(str(info.world_id))
        with pytest.raises(WorldHasUnsettledWorkError):
            await resources.dispatcher.apply(
                ForkWorld(source_world_id=info.world_id, name="must-not-fork-unsettled")
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
        projection["activity_id"] = activity_id
        projection["result_digest"] = result.result_digest

        await resources.dispatcher.apply(
            Update(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(TemporalCounter(value=result.value)),),
            )
        )
        await resources.dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))
        observed = await _receipt(worlds, info.world_id)
        settled = await coordinator.get(str(info.world_id), _ACTIVITY_KIND, activity_id)

        assert source.committed_tick == 0
        assert observed.committed_tick == 1
        assert settled is not None
        assert settled.settlement is not None
        assert settled.settlement.receipt.identity == observed.identity
        rows = await _counter_rows(resources, info, storage, ticks=(1,))
        assert rows[0]["temporalcounter__value"] == 42
        assert not await coordinator.has_unsettled(str(info.world_id))
        settled_fork = await resources.dispatcher.apply(
            ForkWorld(source_world_id=info.world_id, name="settled-fork")
        )
        await resources.dispatcher.apply(DestroyWorld(world_id=settled_fork.world_id))
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
        assert checkpoint.fork_status == "active"
        assert recovered.status_before_resume == "active"
        assert recovered.status_after_resume == "active"
        assert recovered.writer_epoch > checkpoint.fork_writer_epoch
        assert json.loads(recovered.lineage_json) == json.loads(checkpoint.lineage_json)
        assert not recovered.destroy_hook_fired
        before_tokens = json.loads(checkpoint.manifest_tokens_json)
        after_tokens = json.loads(recovered.manifest_tokens_json)
        assert {key: after_tokens[key] for key in before_tokens} == before_tokens
        assert set(after_tokens) == {*before_tokens, "2"}
        assert json.loads(recovered.parent_manifest_tokens_json) == json.loads(
            checkpoint.parent_manifest_tokens_json
        )
        assert json.loads(recovered.visible_ticks_json) == [0, 1, 2]

        # Crash recovery and intentional deletion are different lifecycle
        # transitions: only explicit destruction creates a terminal tombstone.
        assert await activities.destroy_and_refuse_resume(checkpoint) == ("destroyed", 1)
    finally:
        await activities.close()
        await environment.shutdown()
