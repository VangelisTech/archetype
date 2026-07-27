# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Restart and completeness contracts for hosted Physical-AI Activities."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import daft
import pytest

from archetype.activities import ActivityCoordinator
from archetype.core.component import Component
from archetype.core.interfaces import CommittedTickReceipt
from archetype.physical_ai.hosted_activities import (
    CommittedPhysicalSnapshot,
    HostedEpisodeReconciliationRequired,
    PhysicalHostedActivityCoordinator,
    PhysicalHostedActivityProjector,
    PhysicalHostedActivityWorker,
    prepare_hosted_episode_intent,
)
from archetype.physical_ai.hosted_activity_contracts import (
    HOSTED_EPISODE_ACTIVITY_KIND,
    HOSTED_EPISODE_REQUEST_REF_PREFIX,
    HostedEpisodeActivityResultRef,
    HostedEpisodeIntent,
    HostedEpisodeObservation,
    HostedEpisodePayloadRef,
    HostedEpisodeRecoveryUnknown,
    HostedEpisodeRequestRef,
    hosted_episode_provider_operation_id,
)
from archetype.physical_ai.hosted_activity_values import (
    LocalDurableHostedEpisodeProvider,
    LocalHostedEpisodeValueStore,
    SeededHostedEpisodeRunner,
)
from archetype.physical_ai.hosted_activity_world import (
    StoragePhysicalCommittedIntentReader,
    WorldHostedEpisodeObservationStager,
)
from archetype.physical_ai.hosted_episode import (
    decode_hosted_episode_manifest,
    decode_hosted_episode_requests,
    decode_hosted_episode_trajectory,
    encode_hosted_episode_requests,
    encode_hosted_episode_trajectory,
    hosted_episode_request_digest,
    validate_hosted_episode_result,
)
from archetype.storage.activity_catalog import SqliteActivityCatalog
from archetype.storage.catalog import SignatureRecord
from archetype.world.query import PinnedQuerySegment, PinnedWorldQuerySnapshot


class _Reader:
    def __init__(self, snapshot: CommittedPhysicalSnapshot) -> None:
        self.snapshot = snapshot

    async def read(self, receipt: CommittedTickReceipt) -> CommittedPhysicalSnapshot:
        return self.snapshot


class _ObservationStager:
    def __init__(self, *, crash_once: bool = False) -> None:
        self.crash_once = crash_once
        self.observations: dict[tuple[str, str], HostedEpisodeObservation] = {}

    async def stage_hosted_episode_observation(
        self,
        *,
        world_id: str,
        observation: HostedEpisodeObservation,
    ) -> None:
        if self.crash_once:
            self.crash_once = False
            raise RuntimeError("worker died before observation staging")
        key = (world_id, observation.activity_id)
        existing = self.observations.get(key)
        if existing is not None and existing != observation:
            raise ValueError("conflicting hosted observation")
        self.observations.setdefault(key, observation)


class _CrashDuringRunner:
    def __init__(self, seeded: SeededHostedEpisodeRunner) -> None:
        self.seeded = seeded

    async def run(self, request_ipc: bytes) -> bytes:
        await self.seeded.run(request_ipc)
        raise RuntimeError("GPU process disappeared before result publication")


class _PartialRunner:
    async def run(self, request_ipc: bytes) -> bytes:
        full = await SeededHostedEpisodeRunner().run(request_ipc)
        rows = decode_hosted_episode_trajectory(full)
        first_episode = rows[0]["episode_id"]
        return encode_hosted_episode_trajectory(
            [row for row in rows if row["episode_id"] == first_episode]
        )


class _PendingWorld:
    def __init__(self) -> None:
        self.run_id = "run-a"
        self.tick = 2
        self.next_entity_id = 1
        self.spawn_cache: dict[tuple[type[Component], ...], list[dict[str, Any]]] = {}
        self.entity2sig: dict[int, tuple[type[Component], ...]] = {}

    async def create_entity(self, components: list[Component]) -> int:
        entity_id = self.next_entity_id
        self.next_entity_id += 1
        signature = tuple(type(component) for component in components)
        row: dict[str, Any] = {
            "entity_id": entity_id,
            "tick": self.tick,
            "is_active": True,
        }
        for component in components:
            row.update(component.to_row_dict())
        self.spawn_cache.setdefault(signature, []).append(row)
        self.entity2sig[entity_id] = signature
        return entity_id


class _EmptyControlCatalog:
    async def list_signatures(self):
        return []


class _EmptyStorage:
    def get_control_catalog(self, storage_config):
        return _EmptyControlCatalog()


class _ReaderControlCatalog:
    async def list_signatures(self):
        return [
            SignatureRecord(
                table_id="hosted-intent",
                component_names=("HostedEpisodeIntent",),
                schema_json="{}",
                fingerprint="hosted-intent",
            )
        ]


class _ReaderStorage:
    def get_control_catalog(self, storage_config):
        return _ReaderControlCatalog()

    async def materialize(self, frame):
        return frame


class _PendingRegistry:
    def __init__(self, world: _PendingWorld) -> None:
        self.world = world

    @asynccontextmanager
    async def operation(self, world_id: str):
        assert world_id == "physical-world"
        yield self.world

    async def storage_record(self, world_id: str):
        assert world_id == "physical-world"
        return None


def _frame(entity_id: int, tick: int, *components: Component):
    values: dict[str, list[Any]] = {"entity_id": [entity_id], "tick": [tick]}
    for component in components:
        prefix = type(component).get_prefix()
        for field, value in component.model_dump().items():
            values[f"{prefix}{field}"] = [value]
    return daft.from_pydict(values)


def _request(world_id: str, activity_id: str) -> bytes:
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    return encode_hosted_episode_requests(
        [
            {
                "operation_id": operation_id,
                "trial_id": 1,
                "suite": "seeded-reach",
                "task_id": 7,
                "seed": 101,
                "instruction": "reach the target",
                "max_transitions": 3,
                "environment_id": "seeded-reach@v1",
                "policy_id": "scripted-reach@v1",
                "config_json": {
                    "reward_per_transition": 0.25,
                    "success_after_transitions": 2,
                },
            },
            {
                "operation_id": operation_id,
                "trial_id": 0,
                "suite": "seeded-reach",
                "task_id": 7,
                "seed": 100,
                "instruction": "reach the target",
                "max_transitions": 1,
                "environment_id": "seeded-reach@v1",
                "policy_id": "scripted-reach@v1",
                "config_json": {"reward_per_transition": 0.25},
            },
        ]
    )


async def _intent(
    values: LocalHostedEpisodeValueStore,
    *,
    world_id: str,
    activity_id: str,
) -> tuple[HostedEpisodeIntent, bytes]:
    request_ipc = _request(world_id, activity_id)
    return (
        await prepare_hosted_episode_intent(
            values,
            world_id=world_id,
            activity_id=activity_id,
            request_ipc=request_ipc,
        ),
        request_ipc,
    )


def _snapshot(
    *,
    world_id: str,
    run_id: str,
    tick: int,
    intent: HostedEpisodeIntent,
    observation: HostedEpisodeObservation | None = None,
    visibility_token: str | None = None,
) -> CommittedPhysicalSnapshot:
    results = {
        (HostedEpisodeIntent,): _frame(1, tick, intent),
    }
    if observation is not None:
        results[(HostedEpisodeObservation,)] = _frame(2, tick, observation)
    return CommittedPhysicalSnapshot(
        world_id=world_id,
        run_id=run_id,
        committed_tick=tick,
        visibility_token=visibility_token or f"token-{tick}",
        results=results,
    )


def _open_catalog(
    path: Path,
    *,
    lease_seconds: float = 0.01,
    now_seconds: Callable[[], float] | None = None,
) -> tuple[
    SqliteActivityCatalog,
    ActivityCoordinator,
    PhysicalHostedActivityCoordinator,
]:
    physical = (
        SqliteActivityCatalog(path)
        if now_seconds is None
        else SqliteActivityCatalog(path, now_seconds=now_seconds)
    )
    generic = ActivityCoordinator(physical)
    return (
        physical,
        generic,
        PhysicalHostedActivityCoordinator(
            generic,
            lease_seconds=lease_seconds,
        ),
    )


def test_local_provider_identity_binds_its_exact_durable_root(tmp_path: Path) -> None:
    runner = SeededHostedEpisodeRunner(tmp_path / "counter.json")
    first = LocalDurableHostedEpisodeProvider(tmp_path / "provider-a", runner=runner)
    same = LocalDurableHostedEpisodeProvider(tmp_path / "provider-a", runner=runner)
    other = LocalDurableHostedEpisodeProvider(tmp_path / "provider-b", runner=runner)

    assert first.provider == same.provider
    assert first.provider != other.provider
    assert first.provider.startswith("local-seeded-hosted-episode:")


@pytest.mark.asyncio
async def test_hosted_claim_searches_beyond_one_hundred_live_claims(tmp_path: Path) -> None:
    world_id = "physical-world"
    values = LocalHostedEpisodeValueStore(tmp_path / "values")
    physical, generic, catalog = _open_catalog(
        tmp_path / "activities.db",
        lease_seconds=300,
    )
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)

    activity_ids = [f"episode-{position:03d}" for position in range(101)]
    for activity_id in activity_ids:
        request = await values.put_request(_request(world_id, activity_id))
        await catalog.admit_episode(
            world_id=world_id,
            receipt=receipt,
            activity_id=activity_id,
            request=request,
        )
    for activity_id in activity_ids[:100]:
        claim = await generic.claim(
            world_id,
            HOSTED_EPISODE_ACTIVITY_KIND,
            activity_id,
            f"busy-{activity_id}",
            lease_seconds=300,
        )
        assert claim.acquired

    claim = await catalog.claim_episode(world_id=world_id, owner="available-worker")

    assert claim is not None
    assert claim.activity_id == activity_ids[-1]
    await physical.close()


@pytest.mark.asyncio
async def test_exact_hosted_worker_does_not_claim_older_pending_activity(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    values = LocalHostedEpisodeValueStore(tmp_path / "values")
    physical, generic, catalog = _open_catalog(
        tmp_path / "activities.db",
        lease_seconds=300,
    )
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)
    for activity_id in ("older-pending", "requested"):
        request = await values.put_request(_request(world_id, activity_id))
        await catalog.admit_episode(
            world_id=world_id,
            receipt=receipt,
            activity_id=activity_id,
            request=request,
        )

    stager = _ObservationStager()
    worker = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="operation-worker",
        catalog=catalog,
        values=values,
        provider=LocalDurableHostedEpisodeProvider(
            tmp_path / "provider",
            runner=SeededHostedEpisodeRunner(),
        ),
        stager=stager,
    )

    assert await worker.run_once(activity_id="requested")

    older = await generic.get(
        world_id,
        HOSTED_EPISODE_ACTIVITY_KIND,
        "older-pending",
    )
    requested = await generic.get(
        world_id,
        HOSTED_EPISODE_ACTIVITY_KIND,
        "requested",
    )
    assert older is not None and older.result is None
    assert requested is not None and requested.result is not None
    assert set(stager.observations) == {(world_id, "requested")}
    await physical.close()


@pytest.mark.asyncio
async def test_cold_restart_recovers_first_provider_result_without_second_episode(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    run_id = "run-a"
    activity_id = "eval-batch-7"
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    provider_path = tmp_path / "provider"
    counter_path = tmp_path / "provider-executions.json"
    values = LocalHostedEpisodeValueStore(values_path)
    intent, request_ipc = await _intent(
        values,
        world_id=world_id,
        activity_id=activity_id,
    )
    receipt = CommittedTickReceipt(world_id, run_id, 1, "token-1", 0)
    reader = _Reader(
        _snapshot(
            world_id=world_id,
            run_id=run_id,
            tick=1,
            intent=intent,
        )
    )
    now = [100.0]

    def clock() -> float:
        return now[0]

    physical, generic, catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    projector = PhysicalHostedActivityProjector(
        reader=reader,
        catalog=catalog,
        values=values,
    )
    await projector.project(receipt)
    await projector.project(receipt)
    assert (
        len(
            await generic.pending(
                kind=HOSTED_EPISODE_ACTIVITY_KIND,
                world_id=world_id,
            )
        )
        == 1
    )

    first_runner = SeededHostedEpisodeRunner(counter_path)
    first_worker = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="worker-before-crash",
        catalog=catalog,
        values=values,
        provider=LocalDurableHostedEpisodeProvider(
            provider_path,
            runner=first_runner,
            crash_after_publish=True,
        ),
        stager=_ObservationStager(),
    )
    with pytest.raises(RuntimeError, match="after provider result publication"):
        await first_worker.run_once()
    assert first_runner.execution_count == 1
    await physical.close()
    now[0] += 31

    # Reconstruct every Archetype-side object. The provider index is the first
    # durable result, so generic catalog recording cannot trigger another run.
    recovered_values = LocalHostedEpisodeValueStore(values_path)
    recovered_physical, recovered_generic, recovered_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    recovered_runner = SeededHostedEpisodeRunner(counter_path)
    crash_before_stage = _ObservationStager(crash_once=True)
    recovered_worker = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="worker-after-provider-crash",
        catalog=recovered_catalog,
        values=recovered_values,
        provider=LocalDurableHostedEpisodeProvider(
            provider_path,
            runner=recovered_runner,
        ),
        stager=crash_before_stage,
    )
    with pytest.raises(RuntimeError, match="before observation staging"):
        await recovered_worker.run_once()
    assert recovered_runner.execution_count == 1
    pending_results = await recovered_catalog.pending_episode_results(world_id=world_id)
    assert len(pending_results) == 1
    await recovered_physical.close()

    # A third process redelivers the bounded result and stages the same marker.
    final_physical, _final_generic, final_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    final_stager = _ObservationStager()
    final_runner = SeededHostedEpisodeRunner(counter_path)
    final_worker = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="worker-before-observation-commit",
        catalog=final_catalog,
        values=LocalHostedEpisodeValueStore(values_path),
        provider=LocalDurableHostedEpisodeProvider(
            provider_path,
            runner=final_runner,
        ),
        stager=final_stager,
    )
    assert await final_worker.run_once()
    assert final_runner.execution_count == 1
    observation = final_stager.observations[(world_id, activity_id)]
    assert observation.episode_count == 2
    assert observation.trajectory_row_count == 5
    assert observation.transition_count == 3
    assert observation.success_count == 1
    await final_physical.close()

    # The worker dies after staging but before a tick commits. A new process has
    # no staged mutation, so it redelivers the same immutable marker and still
    # does not touch the provider.
    restage_physical, restage_generic, restage_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    restage_stager = _ObservationStager()
    restage_worker = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="worker-after-staging-crash",
        catalog=restage_catalog,
        values=LocalHostedEpisodeValueStore(values_path),
        provider=LocalDurableHostedEpisodeProvider(
            provider_path,
            runner=final_runner,
        ),
        stager=restage_stager,
    )
    assert await restage_worker.run_once()
    assert restage_stager.observations[(world_id, activity_id)] == observation
    assert final_runner.execution_count == 1

    provider_result = await LocalDurableHostedEpisodeProvider(
        provider_path,
        runner=final_runner,
    ).result_for(
        hosted_episode_provider_operation_id(world_id, activity_id),
        request_ipc,
    )
    assert provider_result is not None
    manifest = validate_hosted_episode_result(
        provider_result.request_ipc,
        provider_result.trajectory_ipc,
        provider_result.episode_results_ipc,
        provider_result.manifest_ipc,
    )
    assert manifest == decode_hosted_episode_manifest(provider_result.manifest_ipc)

    # Only the exact later committed marker settles the Activity.
    reader.snapshot = _snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=2,
        intent=intent,
        observation=observation.model_copy(update={"success_count": 0}),
    )
    await PhysicalHostedActivityProjector(
        reader=reader,
        catalog=restage_catalog,
        values=LocalHostedEpisodeValueStore(values_path),
    ).project(CommittedTickReceipt(world_id, run_id, 2, "token-2", 0))
    assert len(await restage_catalog.pending_episode_results(world_id=world_id)) == 1

    reader.snapshot = _snapshot(
        world_id=world_id,
        run_id=run_id,
        tick=3,
        intent=intent,
        observation=observation,
    )
    await PhysicalHostedActivityProjector(
        reader=reader,
        catalog=restage_catalog,
        values=LocalHostedEpisodeValueStore(values_path),
    ).project(CommittedTickReceipt(world_id, run_id, 3, "token-3", 0))
    settled = await restage_generic.get(
        world_id,
        HOSTED_EPISODE_ACTIVITY_KIND,
        activity_id,
    )
    assert settled is not None and settled.settlement is not None
    assert settled.settlement.receipt.committed_tick == 3
    assert not await restage_catalog.has_unsettled_work(world_id)
    await restage_physical.close()


@pytest.mark.asyncio
async def test_partial_provider_trajectory_never_publishes_a_result(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "partial-batch"
    request_ipc = _request(world_id, activity_id)
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    provider = LocalDurableHostedEpisodeProvider(
        tmp_path / "provider",
        runner=_PartialRunner(),
    )

    with pytest.raises(ValueError, match="exactly every admitted episode"):
        await provider.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    assert await provider.result_for(operation_id, request_ipc) is None
    recovery = await provider.reconcile(
        operation_id=operation_id,
        request_ipc=request_ipc,
    )
    assert isinstance(recovery, HostedEpisodeRecoveryUnknown)


@pytest.mark.asyncio
async def test_lease_expiry_does_not_replay_an_ambiguous_provider_start(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    run_id = "run-a"
    activity_id = "ambiguous-batch"
    values_path = tmp_path / "values"
    catalog_path = tmp_path / "activities.db"
    provider_path = tmp_path / "provider"
    counter_path = tmp_path / "counter.json"
    values = LocalHostedEpisodeValueStore(values_path)
    intent, _ = await _intent(values, world_id=world_id, activity_id=activity_id)
    receipt = CommittedTickReceipt(world_id, run_id, 1, "token-1", 0)
    reader = _Reader(
        _snapshot(
            world_id=world_id,
            run_id=run_id,
            tick=1,
            intent=intent,
        )
    )
    now = [100.0]

    def clock() -> float:
        return now[0]

    physical, _generic, catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    await PhysicalHostedActivityProjector(
        reader=reader,
        catalog=catalog,
        values=values,
    ).project(receipt)
    seeded = SeededHostedEpisodeRunner(counter_path)
    first = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="first",
        catalog=catalog,
        values=values,
        provider=LocalDurableHostedEpisodeProvider(
            provider_path,
            runner=_CrashDuringRunner(seeded),
        ),
        stager=_ObservationStager(),
    )
    with pytest.raises(RuntimeError, match="GPU process disappeared"):
        await first.run_once()
    assert seeded.execution_count == 1
    await physical.close()
    now[0] += 31

    recovered_physical, _generic, recovered_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    healthy_runner = SeededHostedEpisodeRunner(counter_path)
    recovered = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="after-expiry",
        catalog=recovered_catalog,
        values=LocalHostedEpisodeValueStore(values_path),
        provider=LocalDurableHostedEpisodeProvider(
            provider_path,
            runner=healthy_runner,
        ),
        stager=_ObservationStager(),
    )
    with pytest.raises(HostedEpisodeReconciliationRequired) as exc_info:
        await recovered.run_once()
    assert exc_info.value.reason == "provider start exists without a complete result index"
    assert exc_info.value.reason in str(exc_info.value)
    assert healthy_runner.execution_count == 1
    await recovered_physical.close()


@pytest.mark.asyncio
async def test_projector_rejects_another_visibility_token_before_admission(
    tmp_path: Path,
) -> None:
    values = LocalHostedEpisodeValueStore(tmp_path / "values")
    intent, _ = await _intent(
        values,
        world_id="physical-world",
        activity_id="visibility-batch",
    )
    physical, generic, catalog = _open_catalog(tmp_path / "activities.db")
    projector = PhysicalHostedActivityProjector(
        reader=_Reader(
            _snapshot(
                world_id="physical-world",
                run_id="run-a",
                tick=1,
                intent=intent,
                visibility_token="other-token",
            )
        ),
        catalog=catalog,
        values=values,
    )

    with pytest.raises(ValueError, match="exact committed receipt"):
        await projector.project(
            CommittedTickReceipt(
                "physical-world",
                "run-a",
                1,
                "authoritative-token",
                0,
            )
        )

    assert (
        await generic.pending(
            kind=HOSTED_EPISODE_ACTIVITY_KIND,
            world_id="physical-world",
        )
        == ()
    )
    await physical.close()


@pytest.mark.asyncio
async def test_committed_reader_excludes_inherited_intent_from_child_projection(
    monkeypatch,
    tmp_path: Path,
) -> None:
    world_id = "child-world"
    run_id = "child-run"
    activity_id = "child-batch"
    values = LocalHostedEpisodeValueStore(tmp_path / "values")
    intent, _ = await _intent(values, world_id=world_id, activity_id=activity_id)
    snapshot = PinnedWorldQuerySnapshot(
        world_id=world_id,
        run_id=run_id,
        head_tick=7,
        head_tokens=("child-token",),
        current=PinnedQuerySegment(
            world_id=world_id,
            run_id=run_id,
            up_to_tick=None,
            head_tick=7,
            head_tokens=("child-token",),
            visibility_tokens=("child-token",),
        ),
        lineage=(
            PinnedQuerySegment(
                world_id="parent-world",
                run_id="parent-run",
                up_to_tick=6,
                head_tick=6,
                head_tokens=("parent-token",),
                visibility_tokens=("parent-token",),
            ),
        ),
    )
    query_calls: list[tuple[str, str, tuple[str, ...]]] = []

    async def pinned(*_args, **_kwargs):
        return snapshot

    async def queried(
        _storage,
        _components,
        queried_world_id,
        queried_run_id,
        _storage_config,
        *,
        visibility_tokens,
    ):
        query_calls.append((queried_world_id, queried_run_id, tuple(visibility_tokens)))
        return _frame(1, 7, intent)

    monkeypatch.setattr(
        "archetype.physical_ai.hosted_activity_world.pin_query_snapshot",
        pinned,
    )
    monkeypatch.setattr(
        "archetype.physical_ai.hosted_activity_world.query_components",
        queried,
    )

    result = await StoragePhysicalCommittedIntentReader(
        _ReaderStorage(),  # type: ignore[arg-type]
    ).read(CommittedTickReceipt(world_id, run_id, 7, "child-token", 0))

    assert query_calls == [(world_id, run_id, ("child-token",))]
    assert result.visibility_token == "child-token"
    assert set(result.results) == {(HostedEpisodeIntent,)}


@pytest.mark.asyncio
async def test_world_stager_scopes_idempotency_to_child_activity_control(
    monkeypatch,
) -> None:
    digest = "0" * 64
    observation = HostedEpisodeObservation(
        activity_id="reused-family-id",
        operation_id="physical-episode:" + digest,
        request_ref=f"{HOSTED_EPISODE_REQUEST_REF_PREFIX}{digest}",
        request_digest=digest,
        result_ref=f"physical-episode-result+json:sha256:{digest}",
        result_digest=digest,
        trajectory_ref=f"physical-episode-trajectory+arrow:sha256:{digest}",
        trajectory_digest=digest,
        episode_results_ref=f"physical-episode-results+arrow:sha256:{digest}",
        episode_results_digest=digest,
        manifest_ref=f"physical-episode-manifest+arrow:sha256:{digest}",
        manifest_digest=digest,
        episode_count=1,
        trajectory_row_count=2,
        transition_count=1,
        success_count=1,
    )
    snapshot = PinnedWorldQuerySnapshot(
        world_id="physical-world",
        run_id="run-a",
        head_tick=7,
        head_tokens=("child-token",),
        current=PinnedQuerySegment(
            world_id="physical-world",
            run_id="run-a",
            up_to_tick=None,
            head_tick=7,
            head_tokens=("child-token",),
            visibility_tokens=("child-token",),
        ),
        lineage=(
            PinnedQuerySegment(
                world_id="parent-world",
                run_id="parent-run",
                up_to_tick=6,
                head_tick=6,
                head_tokens=("parent-token",),
                visibility_tokens=("parent-token",),
            ),
        ),
    )
    query_calls: list[tuple[str, str, tuple[str, ...]]] = []

    class ObservationControlCatalog:
        async def list_signatures(self):
            return [
                SignatureRecord(
                    table_id="hosted-observation",
                    component_names=("HostedEpisodeObservation",),
                    schema_json="{}",
                    fingerprint="hosted-observation",
                )
            ]

    class ObservationStorage:
        def get_control_catalog(self, storage_config):
            return ObservationControlCatalog()

        async def materialize(self, frame):
            return frame

    async def pinned(*_args, **_kwargs):
        return snapshot

    async def queried(
        _storage,
        _components,
        queried_world_id,
        queried_run_id,
        _storage_config,
        *,
        visibility_tokens,
    ):
        tokens = tuple(visibility_tokens)
        query_calls.append((queried_world_id, queried_run_id, tokens))
        if tokens == ("parent-token",):
            return _frame(
                1,
                6,
                observation.model_copy(update={"success_count": 0}),
            )
        return _frame(
            2,
            7,
            observation.model_copy(update={"activity_id": "unrelated"}),
        )

    monkeypatch.setattr(
        "archetype.physical_ai.hosted_activity_world.pin_query_snapshot",
        pinned,
    )
    monkeypatch.setattr(
        "archetype.physical_ai.hosted_activity_world.query_components",
        queried,
    )
    world = _PendingWorld()
    stager = WorldHostedEpisodeObservationStager(
        storage=ObservationStorage(),  # type: ignore[arg-type]
        registry=_PendingRegistry(world),  # type: ignore[arg-type]
    )

    await stager.stage_hosted_episode_observation(
        world_id="physical-world",
        observation=observation,
    )

    assert query_calls == [("physical-world", "run-a", ("child-token",))]
    assert sum(len(rows) for rows in world.spawn_cache.values()) == 1


def test_provider_operation_identity_is_world_scoped_and_matches_request() -> None:
    first = hosted_episode_provider_operation_id("world-a", "batch")
    second = hosted_episode_provider_operation_id("world-b", "batch")

    assert first != second
    assert first == decode_hosted_episode_requests(_request("world-a", "batch"))[0]["operation_id"]
    assert hosted_episode_request_digest(_request("world-a", "batch"))


@pytest.mark.parametrize(
    "factory",
    [
        lambda digest: HostedEpisodeRequestRef(
            ref=f"{HOSTED_EPISODE_REQUEST_REF_PREFIX}{'0' * 64}",
            digest=digest,
            size_bytes=10,
        ),
        lambda digest: HostedEpisodePayloadRef(
            kind="trajectory",
            ref=f"physical-episode-trajectory+arrow:sha256:{'0' * 64}",
            digest=digest,
            size_bytes=10,
        ),
        lambda digest: HostedEpisodeActivityResultRef(
            ref=f"physical-episode-result+json:sha256:{'0' * 64}",
            digest=digest,
            size_bytes=10,
        ),
    ],
)
def test_content_references_reject_digest_mismatch_at_construction(factory) -> None:
    with pytest.raises(ValueError, match="embed its exact"):
        factory("1" * 64)


@pytest.mark.parametrize("size", [-1, 0])
def test_sized_content_references_reject_nonpositive_bytes(size: int) -> None:
    digest = "0" * 64
    with pytest.raises(ValueError, match="positive"):
        HostedEpisodeRequestRef(
            ref=f"{HOSTED_EPISODE_REQUEST_REF_PREFIX}{digest}",
            digest=digest,
            size_bytes=size,
        )


def test_payload_reference_kind_requires_its_exact_prefix() -> None:
    digest = "0" * 64
    with pytest.raises(ValueError, match="embed its exact"):
        HostedEpisodePayloadRef(
            kind="trajectory",
            ref=f"physical-episode-results+arrow:sha256:{digest}",
            digest=digest,
            size_bytes=10,
        )


@pytest.mark.asyncio
async def test_world_stager_is_idempotent_and_rejects_pending_conflict() -> None:
    digest = "0" * 64
    observation = HostedEpisodeObservation(
        activity_id="batch",
        operation_id="physical-episode:" + digest,
        request_ref=f"{HOSTED_EPISODE_REQUEST_REF_PREFIX}{digest}",
        request_digest=digest,
        result_ref=f"physical-episode-result+json:sha256:{digest}",
        result_digest=digest,
        trajectory_ref=f"physical-episode-trajectory+arrow:sha256:{digest}",
        trajectory_digest=digest,
        episode_results_ref=f"physical-episode-results+arrow:sha256:{digest}",
        episode_results_digest=digest,
        manifest_ref=f"physical-episode-manifest+arrow:sha256:{digest}",
        manifest_digest=digest,
        episode_count=1,
        trajectory_row_count=2,
        transition_count=1,
        success_count=1,
    )
    world = _PendingWorld()
    stager = WorldHostedEpisodeObservationStager(
        storage=_EmptyStorage(),  # type: ignore[arg-type]
        registry=_PendingRegistry(world),  # type: ignore[arg-type]
    )

    await stager.stage_hosted_episode_observation(
        world_id="physical-world",
        observation=observation,
    )
    await stager.stage_hosted_episode_observation(
        world_id="physical-world",
        observation=observation,
    )

    assert world.next_entity_id == 2
    assert sum(len(rows) for rows in world.spawn_cache.values()) == 1
    with pytest.raises(ValueError, match="conflicting"):
        await stager.stage_hosted_episode_observation(
            world_id="physical-world",
            observation=observation.model_copy(update={"success_count": 0}),
        )


@pytest.mark.asyncio
async def test_hosted_claim_pages_past_a_full_batch_of_live_claims(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claimable Activity beyond the scan page must not be stranded.

    With more admitted Activities than one pending page, a page-sized prefix
    of foreign leases previously made claim_episode report no work, leaving
    later claimable Activities pending until an unrelated prefix change.
    """
    import archetype.physical_ai.hosted_activities as hosted_module

    monkeypatch.setattr(hosted_module, "_CLAIM_SCAN_PAGE", 3)
    world_id = "physical-world"
    values = LocalHostedEpisodeValueStore(tmp_path / "values")
    physical, generic, catalog = _open_catalog(
        tmp_path / "activities.db",
        lease_seconds=300,
    )
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)

    activity_ids = [f"episode-{position:03d}" for position in range(7)]
    for activity_id in activity_ids:
        request = await values.put_request(_request(world_id, activity_id))
        await catalog.admit_episode(
            world_id=world_id,
            receipt=receipt,
            activity_id=activity_id,
            request=request,
        )
    # Lease two full pages' worth so the claimable Activity sits on page 3.
    for activity_id in activity_ids[:6]:
        claim = await generic.claim(
            world_id,
            HOSTED_EPISODE_ACTIVITY_KIND,
            activity_id,
            f"busy-{activity_id}",
            lease_seconds=300,
        )
        assert claim.acquired

    claim = await catalog.claim_episode(world_id=world_id, owner="available-worker")

    assert claim is not None
    assert claim.activity_id == activity_ids[-1]

    # Every Activity now leased: an honest None, reached only by paging to
    # the catalog's end rather than stopping at page one.
    assert await catalog.claim_episode(world_id=world_id, owner="late-worker") is None
    await physical.close()


@pytest.mark.asyncio
async def test_pending_results_page_past_a_full_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable result beyond the first page must still be delivered."""
    import archetype.physical_ai.hosted_activities as hosted_module

    monkeypatch.setattr(hosted_module, "_CLAIM_SCAN_PAGE", 2)
    world_id = "physical-world"
    values = LocalHostedEpisodeValueStore(tmp_path / "values")
    physical, generic, catalog = _open_catalog(
        tmp_path / "activities.db",
        lease_seconds=300,
    )
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)

    activity_ids = [f"episode-{position:03d}" for position in range(5)]
    for activity_id in activity_ids:
        request = await values.put_request(_request(world_id, activity_id))
        await catalog.admit_episode(
            world_id=world_id,
            receipt=receipt,
            activity_id=activity_id,
            request=request,
        )
        claim = await catalog.claim_episode(world_id=world_id, owner="worker")
        assert claim is not None
        claim = await catalog.bind_provider_operation(
            claim,
            provider="local",
            operation_id=hosted_episode_provider_operation_id(world_id, claim.activity_id),
        )
        digest = "a" * 64
        await catalog.record_episode_result(
            claim,
            HostedEpisodeActivityResultRef(
                ref=f"physical-episode-result+json:sha256:{digest}",
                digest=digest,
                size_bytes=10,
            ),
        )

    deliveries = await catalog.pending_episode_results(world_id=world_id)

    assert sorted(delivery.activity_id for delivery in deliveries) == activity_ids
