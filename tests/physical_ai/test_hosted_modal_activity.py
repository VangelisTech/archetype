# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal admission and restart contracts for hosted Physical-AI Activities."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

from archetype import __version__
from archetype.activities import ActivityCoordinator
from archetype.core.interfaces import CommittedTickReceipt
from archetype.physical_ai import hosted_modal
from archetype.physical_ai.hosted_activities import (
    PhysicalHostedActivityCatalog,
    PhysicalHostedActivityCoordinator,
    PhysicalHostedActivityWorker,
)
from archetype.physical_ai.hosted_activity_contracts import (
    HostedEpisodeConfirmedAbsent,
    HostedEpisodeObservation,
    HostedEpisodeProviderResult,
    HostedEpisodeRecovered,
    HostedEpisodeRecoveryUnknown,
    HostedEpisodeRetryGuard,
    hosted_episode_provider_operation_id,
)
from archetype.physical_ai.hosted_activity_values import (
    LocalHostedEpisodeValueStore,
    SeededHostedEpisodeRunner,
)
from archetype.physical_ai.hosted_episode import (
    build_hosted_episode_manifest,
    build_hosted_episode_results,
    decode_hosted_episode_manifest,
    encode_hosted_episode_requests,
    validate_hosted_episode_result,
)
from archetype.physical_ai.hosted_modal import (
    ModalHostedEpisodeConfig,
    ModalHostedEpisodeProvider,
    ModalHostedEpisodeProviderUnknown,
)
from archetype.storage.activity_catalog import SqliteActivityCatalog


def test_seeded_modal_image_pin_matches_release_version() -> None:
    project = tomllib.loads(
        (Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert project["project"]["version"] == __version__
    assert f'.uv_pip_install("archetype-ecs=={__version__}")' in inspect.getsource(
        hosted_modal.build_seeded_modal_hosted_episode_app
    )


def _config(**changes: object) -> ModalHostedEpisodeConfig:
    values: dict[str, Any] = {
        "workspace_name": "test-workspace",
        "environment_name": "test-environment",
        "app_name": "physical-ai-proof",
        "function_name": "seeded-hosted-episode",
        "result_dict_name": "physical-ai-results",
        "result_volume_name": "physical-ai-values",
        "call_timeout_seconds": 60,
    }
    values.update(changes)
    return ModalHostedEpisodeConfig(**values)


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


def _provider_operation_key(operation_id: str, prefix: str) -> str:
    digest = hashlib.sha256(operation_id.encode()).hexdigest()
    return f"{prefix}:{digest}"


@dataclass
class _FakeModalState:
    root: Path
    values: dict[str, object] = field(default_factory=dict)
    blobs: dict[str, bytes] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    wait_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_wait: asyncio.Event = field(default_factory=asyncio.Event)
    spawn_attempts: int = 0
    execution_count: int = 0
    calls: dict[str, tuple[str, bytes, str]] = field(default_factory=dict)
    started_calls: set[str] = field(default_factory=set)
    cancelled_calls: set[str] = field(default_factory=set)


class _FakeModalRuntime:
    def __init__(
        self,
        state: _FakeModalState,
        *,
        block_before_result: bool = False,
        fail_spawn: bool = False,
        lose_response_after_result: bool = False,
    ) -> None:
        self.state = state
        self.block_before_result = block_before_result
        self.fail_spawn = fail_spawn
        self.lose_response_after_result = lose_response_after_result

    async def get(self, key: str) -> object:
        async with self.state.lock:
            return self.state.values.get(key)

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool:
        async with self.state.lock:
            if key in self.state.values:
                return False
            self.state.values[key] = dict(value)
            return True

    async def spawn(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        namespace_digest: str,
    ) -> object:
        self.state.spawn_attempts += 1
        if self.fail_spawn:
            raise RuntimeError("spawn response disappeared")
        call = (operation_id, request_ipc, namespace_digest)
        self.state.calls[self.call_id(call)] = call
        return call

    def call_id(self, call: object) -> str:
        operation_id, _, _ = cast(tuple[str, bytes, str], call)
        return f"fc-{hashlib.sha256(operation_id.encode()).hexdigest()}"

    async def reattach(self, call_id: str) -> object:
        return self.state.calls[call_id]

    async def cancel(self, call: object) -> None:
        self.state.cancelled_calls.add(self.call_id(call))

    async def wait(self, call: object) -> object:
        operation_id, request_ipc, namespace_digest = cast(
            tuple[str, bytes, str],
            call,
        )
        call_id = self.call_id(call)
        if call_id not in self.state.started_calls:
            self.state.started_calls.add(call_id)
            self.state.execution_count += 1
        self.state.wait_started.set()
        if self.block_before_result:
            await self.state.release_wait.wait()
        trajectory_ipc = await SeededHostedEpisodeRunner().run(request_ipc)
        episode_results_ipc = build_hosted_episode_results(
            request_ipc,
            trajectory_ipc,
        )
        manifest_ipc = build_hosted_episode_manifest(
            request_ipc,
            trajectory_ipc,
            episode_results_ipc,
        )
        result = HostedEpisodeProviderResult(
            request_ipc=request_ipc,
            trajectory_ipc=trajectory_ipc,
            episode_results_ipc=episode_results_ipc,
            manifest_ipc=manifest_ipc,
        )
        index = hosted_modal._publish_remote_result(
            mount=self.state.root,
            operation_id=operation_id,
            namespace_digest=namespace_digest,
            protocol_epoch=hosted_modal.MODAL_HOSTED_EPISODE_PROTOCOL_EPOCH,
            result=result,
        )
        payloads = cast(dict[str, dict[str, object]], index["payloads"])
        for record in payloads.values():
            path = str(record["path"])
            self.state.blobs[path] = (self.state.root / path).read_bytes()
        key = _provider_operation_key(operation_id, "result")
        async with self.state.lock:
            existing = self.state.values.get(key)
            if existing is not None and existing != index:
                raise RuntimeError("first result index conflicts")
            self.state.values.setdefault(key, index)
        if self.lose_response_after_result:
            self.lose_response_after_result = False
            raise RuntimeError("completion response disappeared")
        return {"gpu_count": 1, "schema_version": 1}

    async def read_blob(self, path: str) -> bytes:
        return self.state.blobs[path]


class _ObservationStager:
    def __init__(self) -> None:
        self.observations: dict[tuple[str, str], HostedEpisodeObservation] = {}

    async def stage_hosted_episode_observation(
        self,
        *,
        world_id: str,
        observation: HostedEpisodeObservation,
    ) -> None:
        key = world_id, observation.activity_id
        existing = self.observations.get(key)
        if existing is not None and existing != observation:
            raise ValueError("conflicting hosted observation")
        self.observations.setdefault(key, observation)


class _CrashBeforeGenericRecord:
    def __init__(self, catalog: PhysicalHostedActivityCoordinator) -> None:
        self.catalog = catalog

    def __getattr__(self, name: str) -> object:
        return getattr(self.catalog, name)

    async def record_episode_result(self, claim: object, result: object) -> None:
        raise RuntimeError("worker died before generic Activity result recording")


def _open_catalog(
    path: Path,
    *,
    lease_seconds: float = 0.5,
) -> tuple[
    SqliteActivityCatalog,
    ActivityCoordinator,
    PhysicalHostedActivityCoordinator,
]:
    physical = SqliteActivityCatalog(path)
    generic = ActivityCoordinator(physical)
    return (
        physical,
        generic,
        PhysicalHostedActivityCoordinator(
            generic,
            lease_seconds=lease_seconds,
        ),
    )


def test_modal_namespace_identity_binds_every_durable_and_execution_coordinate() -> None:
    base = _config()
    fields = (
        "workspace_name",
        "environment_name",
        "app_name",
        "function_name",
        "result_dict_name",
        "result_volume_name",
    )
    for field_name in fields:
        changed = replace(base, **{field_name: f"{getattr(base, field_name)}-changed"})
        assert changed.provider_identity != base.provider_identity
        assert changed.namespace_digest != base.namespace_digest

    with pytest.raises(ValueError, match="valid Modal name"):
        _config(workspace_name=1)
    with pytest.raises(ValueError, match="protocol epoch"):
        _config(protocol_epoch=True)
    with pytest.raises(ValueError, match="positive integer"):
        _config(call_timeout_seconds=True)
    with pytest.raises(ValueError, match="must be a boolean"):
        _config(create_if_missing=1)


@pytest.mark.asyncio
async def test_lost_modal_completion_response_recovers_provider_first_result(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "response-loss"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    state = _FakeModalState(tmp_path / "remote-volume")
    provider = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state, lose_response_after_result=True),
    )

    result = await provider.execute(
        operation_id=operation_id,
        request_ipc=request_ipc,
        attempt=1,
        fence=1,
        retry_guard=None,
    )

    assert state.spawn_attempts == 1
    assert state.execution_count == 1
    manifest = validate_hosted_episode_result(
        result.request_ipc,
        result.trajectory_ipc,
        result.episode_results_ipc,
        result.manifest_ipc,
    )
    assert decode_hosted_episode_manifest(result.manifest_ipc) == manifest

    reconstructed = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state),
    )
    recovery = await reconstructed.reconcile(
        operation_id=operation_id,
        request_ipc=request_ipc,
    )
    assert isinstance(recovery, HostedEpisodeRecovered)
    assert recovery.result == result


@pytest.mark.asyncio
async def test_replacement_runtime_reattaches_exact_call_without_respawn(tmp_path) -> None:
    state = _FakeModalState(tmp_path)
    world_id = "physical-world"
    activity_id = "reattach-call"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    first = ModalHostedEpisodeProvider(
        _config(), runtime=_FakeModalRuntime(state, block_before_result=True)
    )
    running = asyncio.create_task(
        first.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=None,
        )
    )
    await state.wait_started.wait()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    replacement = ModalHostedEpisodeProvider(
        _config(), runtime=_FakeModalRuntime(state, block_before_result=True)
    )
    resumed = asyncio.create_task(
        replacement.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=2,
            fence=2,
            retry_guard=None,
        )
    )
    state.release_wait.set()
    result = await resumed

    assert result.request_ipc == request_ipc
    assert state.spawn_attempts == 1
    assert state.execution_count == 1


@pytest.mark.asyncio
async def test_cancellation_reattaches_only_the_exact_durable_call(tmp_path) -> None:
    state = _FakeModalState(tmp_path)
    world_id = "physical-world"
    activity_id = "cancel-call"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    provider = ModalHostedEpisodeProvider(
        _config(), runtime=_FakeModalRuntime(state, block_before_result=True)
    )
    running = asyncio.create_task(
        provider.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=None,
        )
    )
    await state.wait_started.wait()

    await provider.cancel(operation_id=operation_id, request_ipc=request_ipc)
    assert state.cancelled_calls == {next(iter(state.calls))}
    assert state.spawn_attempts == 1

    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running


@pytest.mark.asyncio
async def test_cancellation_without_call_identity_fails_closed(tmp_path) -> None:
    state = _FakeModalState(tmp_path)
    world_id = "physical-world"
    activity_id = "cancel-ambiguous-start"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    provider = ModalHostedEpisodeProvider(
        _config(), runtime=_FakeModalRuntime(state, fail_spawn=True)
    )
    with pytest.raises(ModalHostedEpisodeProviderUnknown):
        await provider.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    with pytest.raises(ModalHostedEpisodeProviderUnknown, match="without an exact durable"):
        await provider.cancel(operation_id=operation_id, request_ipc=request_ipc)
    assert not state.cancelled_calls
    assert state.spawn_attempts == 1
    assert state.execution_count == 0


@pytest.mark.asyncio
async def test_permanent_modal_start_without_result_never_replays(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "ambiguous-start"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    state = _FakeModalState(tmp_path / "remote-volume")
    first = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state, fail_spawn=True),
    )

    with pytest.raises(ModalHostedEpisodeProviderUnknown, match="permanent start"):
        await first.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=None,
        )

    reconstructed = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state),
    )
    recovery = await reconstructed.reconcile(
        operation_id=operation_id,
        request_ipc=request_ipc,
    )
    assert isinstance(recovery, HostedEpisodeRecoveryUnknown)
    with pytest.raises(ModalHostedEpisodeProviderUnknown, match="permanent start"):
        await reconstructed.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=2,
            fence=2,
            retry_guard=None,
        )
    assert state.spawn_attempts == 1
    assert state.execution_count == 0


@pytest.mark.asyncio
async def test_modal_confirmed_absence_guard_cannot_bypass_atomic_start(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "guarded-start"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    state = _FakeModalState(tmp_path / "remote-volume")
    provider = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state),
    )
    recovery = await provider.reconcile(
        operation_id=operation_id,
        request_ipc=request_ipc,
    )
    assert isinstance(recovery, HostedEpisodeConfirmedAbsent)

    wrong_guard = HostedEpisodeRetryGuard(
        ref=recovery.guard.ref,
        digest="0" * 64,
    )
    with pytest.raises(ModalHostedEpisodeProviderUnknown, match="retry guard"):
        await provider.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=wrong_guard,
        )
    assert state.values == {}

    await provider.execute(
        operation_id=operation_id,
        request_ipc=request_ipc,
        attempt=1,
        fence=1,
        retry_guard=recovery.guard,
    )
    assert state.spawn_attempts == 1
    assert state.execution_count == 1


@pytest.mark.asyncio
async def test_concurrent_modal_claimants_admit_only_one_remote_execution(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "concurrent-start"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    state = _FakeModalState(tmp_path / "remote-volume")
    first = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state, block_before_result=True),
    )
    second = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state),
    )

    first_task = asyncio.create_task(
        first.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=1,
            fence=1,
            retry_guard=None,
        )
    )
    await state.wait_started.wait()
    second_result = await second.execute(
        operation_id=operation_id,
        request_ipc=request_ipc,
        attempt=2,
        fence=2,
        retry_guard=None,
    )
    state.release_wait.set()
    first_result = await first_task

    assert second_result == first_result
    assert state.spawn_attempts == 1
    assert state.execution_count == 1
    recovered = await second.reconcile(
        operation_id=operation_id,
        request_ipc=request_ipc,
    )
    assert isinstance(recovered, HostedEpisodeRecovered)


@pytest.mark.asyncio
async def test_modal_result_identity_corruption_fails_closed(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "corrupt-index"
    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    request_ipc = _request(world_id, activity_id)
    state = _FakeModalState(tmp_path / "remote-volume")
    provider = ModalHostedEpisodeProvider(
        _config(),
        runtime=_FakeModalRuntime(state),
    )
    await provider.execute(
        operation_id=operation_id,
        request_ipc=request_ipc,
        attempt=1,
        fence=1,
        retry_guard=None,
    )
    result_key = _provider_operation_key(operation_id, "result")
    index = state.values[result_key]
    assert isinstance(index, dict)
    state.values[result_key] = {
        **index,
        "namespace_digest": "0" * 64,
    }

    with pytest.raises(ValueError, match="identity is incompatible"):
        await provider.reconcile(
            operation_id=operation_id,
            request_ipc=request_ipc,
        )

    state.values[result_key] = index
    del state.values[_provider_operation_key(operation_id, "start")]
    recovery = await provider.reconcile(
        operation_id=operation_id,
        request_ipc=request_ipc,
    )
    assert isinstance(recovery, HostedEpisodeRecoveryUnknown)
    with pytest.raises(
        ModalHostedEpisodeProviderUnknown,
        match="without its exact permanent start",
    ):
        await provider.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=2,
            fence=2,
            retry_guard=None,
        )
    assert state.spawn_attempts == 1


@pytest.mark.asyncio
async def test_activity_restart_recovers_modal_result_before_generic_record(
    tmp_path: Path,
) -> None:
    world_id = "physical-world"
    activity_id = "activity-restart"
    request_ipc = _request(world_id, activity_id)
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    values = LocalHostedEpisodeValueStore(values_path)
    request_ref = await values.put_request(request_ipc)
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)
    physical, _generic, catalog = _open_catalog(catalog_path)
    await catalog.admit_episode(
        world_id=world_id,
        receipt=receipt,
        activity_id=activity_id,
        request=request_ref,
    )
    state = _FakeModalState(tmp_path / "remote-volume")
    first = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="before-crash",
        catalog=cast(
            PhysicalHostedActivityCatalog,
            _CrashBeforeGenericRecord(catalog),
        ),
        values=values,
        provider=ModalHostedEpisodeProvider(
            _config(),
            runtime=_FakeModalRuntime(state),
        ),
        stager=_ObservationStager(),
    )

    with pytest.raises(RuntimeError, match="before generic Activity result"):
        await first.run_once()
    assert state.execution_count == 1
    await physical.close()
    await asyncio.sleep(0.55)

    recovered_physical, _recovered_generic, recovered_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
    )
    stager = _ObservationStager()
    reconstructed = PhysicalHostedActivityWorker(
        world_id=world_id,
        owner="after-restart",
        catalog=recovered_catalog,
        values=LocalHostedEpisodeValueStore(values_path),
        provider=ModalHostedEpisodeProvider(
            _config(),
            runtime=_FakeModalRuntime(state),
        ),
        stager=stager,
    )
    assert await reconstructed.run_once()

    assert state.spawn_attempts == 1
    assert state.execution_count == 1
    observation = stager.observations[(world_id, activity_id)]
    assert observation.episode_count == 2
    assert observation.trajectory_row_count == 5
    assert observation.transition_count == 3
    assert observation.success_count == 1
    assert len(await recovered_catalog.pending_episode_results(world_id=world_id)) == 1
    await recovered_physical.close()
