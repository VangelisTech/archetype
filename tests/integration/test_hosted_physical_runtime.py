# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Public hosted Physical-AI path across restart and fork boundaries."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest

from archetype import (
    ArchetypeRuntime,
    HostedEpisodeRequest,
    ModalHostedEpisodeConfig,
    StorageConfig,
)
from archetype.activities import ActivityAdmission, ActivityCoordinator
from archetype.core.config import StorageBackend
from archetype.core.interfaces import CommittedTickReceipt
from archetype.physical_ai import hosted_modal
from archetype.physical_ai.hosted_activity_contracts import HostedEpisodeProviderResult
from archetype.physical_ai.hosted_activity_values import SeededHostedEpisodeRunner
from archetype.physical_ai.hosted_episode import (
    build_hosted_episode_manifest,
    build_hosted_episode_results,
)
from archetype.physical_ai.hosted_modal import ModalHostedEpisodeProvider
from archetype.storage.activity_catalog import (
    SqliteActivityCatalog,
    activity_catalog_path_for,
)
from archetype.storage.config import ControlCatalogConfig
from archetype.wiring import RuntimeBootstrapConfig
from archetype.world.registry import WorldRegistry


@dataclass
class _ModalState:
    root: Path
    registry: WorldRegistry
    world_id: str = ""
    values: dict[str, object] = field(default_factory=dict)
    blobs: dict[str, bytes] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    spawn_count: int = 0
    execution_count: int = 0
    crash_once: bool = True


class _ModalRuntime:
    def __init__(self, state: _ModalState) -> None:
        self._state = state

    async def get(self, key: str) -> object:
        return self._state.values.get(key)

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool:
        async with self._state.lock:
            if key in self._state.values:
                return False
            self._state.values[key] = dict(value)
            return True

    async def spawn(
        self,
        *,
        operation_id: str,
        request_ipc: bytes,
        namespace_digest: str,
    ) -> object:
        self._state.spawn_count += 1
        return operation_id, request_ipc, namespace_digest

    async def wait(self, call: object) -> object:
        operation_id, request_ipc, namespace_digest = cast(
            tuple[str, bytes, str],
            call,
        )
        # Provider execution must not retain exact-world tick authority.
        async with self._state.registry.operation(self._state.world_id):
            pass
        self._state.execution_count += 1
        trajectory = await SeededHostedEpisodeRunner().run(request_ipc)
        results = build_hosted_episode_results(request_ipc, trajectory)
        manifest = build_hosted_episode_manifest(request_ipc, trajectory, results)
        published = HostedEpisodeProviderResult(
            request_ipc=request_ipc,
            trajectory_ipc=trajectory,
            episode_results_ipc=results,
            manifest_ipc=manifest,
        )
        index = hosted_modal._publish_remote_result(
            mount=self._state.root,
            operation_id=operation_id,
            namespace_digest=namespace_digest,
            protocol_epoch=hosted_modal.MODAL_HOSTED_EPISODE_PROTOCOL_EPOCH,
            result=published,
        )
        for record in cast(dict[str, dict[str, object]], index["payloads"]).values():
            path = str(record["path"])
            self._state.blobs[path] = (self._state.root / path).read_bytes()
        key = f"result:{hashlib.sha256(operation_id.encode()).hexdigest()}"
        self._state.values[key] = index
        return {"gpu_count": 1, "schema_version": 1}

    async def read_blob(self, path: str) -> bytes:
        return self._state.blobs[path]


class _CrashAfterProviderPublication:
    def __init__(self, delegate: ModalHostedEpisodeProvider, state: _ModalState) -> None:
        self._delegate = delegate
        self._state = state

    @property
    def provider(self) -> str:
        return self._delegate.provider

    async def execute(self, **kwargs: Any):
        result = await self._delegate.execute(**kwargs)
        if self._state.crash_once:
            self._state.crash_once = False
            raise RuntimeError("worker stopped before generic result recording")
        return result

    async def reconcile(self, **kwargs: Any):
        return await self._delegate.reconcile(**kwargs)


def _provider_config() -> ModalHostedEpisodeConfig:
    return ModalHostedEpisodeConfig(
        workspace_name="test-workspace",
        environment_name="test-environment",
        app_name="hosted-physical-proof",
        function_name="run-hosted-episode",
        result_dict_name="hosted-physical-results",
        result_volume_name="hosted-physical-values",
    )


def _request() -> HostedEpisodeRequest:
    return HostedEpisodeRequest(
        trial_id=0,
        suite="seeded-reach",
        task_id=7,
        seed=100,
        instruction="reach the target",
        max_transitions=2,
        environment_id="seeded-reach@v1",
        policy_id="scripted-reach@v1",
        config_json='{"reward_per_transition":0.25}',
    )


@pytest.mark.asyncio
async def test_public_hosted_episode_ignores_unrelated_unsettled_activity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "worlds"),
        namespace="hosted-runtime",
        backend=StorageBackend.ICEBERG,
    )
    catalog_config = ControlCatalogConfig(catalog_dir=tmp_path / "catalogs")
    registry = WorldRegistry()
    state = _ModalState(tmp_path / "modal", registry, crash_once=False)

    def provider_factory(config: ModalHostedEpisodeConfig):
        return ModalHostedEpisodeProvider(config, runtime=_ModalRuntime(state))

    monkeypatch.setattr(
        "archetype.runtime.runtime._bootstrap_config",
        lambda **_kwargs: RuntimeBootstrapConfig(
            control_catalog_config=catalog_config,
            world_registry=registry,
            audit_storage_config=storage,
            hosted_episode_provider_factory=provider_factory,
        ),
    )

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("hosted-with-mission-work", storage=storage)
        info = await world.info()
        state.world_id = str(info.world_id)
        physical = SqliteActivityCatalog(activity_catalog_path_for(storage, catalog_config))
        coordinator = ActivityCoordinator(physical)
        await coordinator.admit(
            ActivityAdmission(
                activity_id="unrelated-mission",
                kind="missions.author",
                source=CommittedTickReceipt(
                    state.world_id,
                    str(info.run_id),
                    info.tick,
                    "unrelated-visible-receipt",
                    0,
                ),
                input_ref="mission-request:unrelated",
                input_digest="unrelated-mission-request",
            )
        )
        await physical.close()

        observation = await world.run_hosted_episode(
            [_request()],
            provider=_provider_config(),
            activity_id="requested-hosted-episode",
        )

        assert observation.activity_id == "requested-hosted-episode"
        check = SqliteActivityCatalog(activity_catalog_path_for(storage, catalog_config))
        assert await ActivityCoordinator(check).has_unsettled(state.world_id)
        await check.close()


@pytest.mark.asyncio
async def test_public_hosted_episode_recovers_without_replay_and_isolates_fork(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "worlds"),
        namespace="hosted-runtime",
        backend=StorageBackend.ICEBERG,
    )
    catalog = ControlCatalogConfig(catalog_dir=tmp_path / "catalogs")
    first_registry = WorldRegistry()
    second_registry = WorldRegistry()
    state = _ModalState(tmp_path / "modal", first_registry)

    def provider_factory(config: ModalHostedEpisodeConfig):
        provider = ModalHostedEpisodeProvider(config, runtime=_ModalRuntime(state))
        return _CrashAfterProviderPublication(provider, state)

    configs = [
        RuntimeBootstrapConfig(
            control_catalog_config=catalog,
            world_registry=first_registry,
            audit_storage_config=storage,
            hosted_episode_provider_factory=provider_factory,
            hosted_activity_lease_seconds=300,
        ),
        RuntimeBootstrapConfig(
            control_catalog_config=catalog,
            world_registry=second_registry,
            audit_storage_config=storage,
            hosted_episode_provider_factory=provider_factory,
            hosted_activity_lease_seconds=300,
        ),
    ]
    monkeypatch.setattr(
        "archetype.runtime.runtime._bootstrap_config",
        lambda **_kwargs: configs.pop(0),
    )
    # Lease expiry between the two runtime generations is driven by this
    # manual clock instead of racing a sub-second wall-clock lease against
    # runner load (issue #720): within a phase the clock is frozen, so the
    # crashed generation's claim cannot expire while its episode is still
    # running.
    clock = [time.time()]
    monkeypatch.setattr(
        "archetype.wiring.SqliteActivityCatalog",
        lambda path: SqliteActivityCatalog(path, now_seconds=lambda: clock[0]),
    )

    activity_id = "restart-proof"
    provider_config = _provider_config()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("hosted-parent", storage=storage)
        parent_id = str((await world.info()).world_id)
        state.world_id = parent_id
        with pytest.raises(RuntimeError, match="before generic result"):
            await world.run_hosted_episode(
                [_request()],
                provider=provider_config,
                activity_id=activity_id,
            )

    assert state.execution_count == 1
    clock[0] += 301.0

    state.registry = second_registry
    async with ArchetypeRuntime() as runtime:
        parent = await runtime.resume(parent_id, storage=storage)
        state.world_id = parent_id
        observation = await parent.run_hosted_episode(
            [_request()],
            provider=provider_config,
            activity_id=activity_id,
        )
        assert observation.activity_id == activity_id
        assert state.execution_count == 1

        child = await parent.fork("hosted-child")
        state.world_id = str(child.world_id)
        child_observation = await child.run_hosted_episode(
            [_request()],
            provider=provider_config,
            activity_id=activity_id,
        )
        assert child_observation.activity_id == activity_id
        assert child_observation.operation_id != observation.operation_id
        assert state.execution_count == 2


def _single_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    namespace: str,
    crash_once: bool,
) -> tuple[StorageConfig, _ModalState]:
    storage = StorageConfig(
        uri=str(tmp_path / "worlds"),
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
    )
    catalog = ControlCatalogConfig(catalog_dir=tmp_path / "catalogs")
    registry = WorldRegistry()
    state = _ModalState(tmp_path / "modal", registry, crash_once=crash_once)

    def provider_factory(config: ModalHostedEpisodeConfig):
        provider = ModalHostedEpisodeProvider(config, runtime=_ModalRuntime(state))
        if crash_once:
            return _CrashAfterProviderPublication(provider, state)
        return provider

    bootstrap = RuntimeBootstrapConfig(
        control_catalog_config=catalog,
        world_registry=registry,
        audit_storage_config=storage,
        hosted_episode_provider_factory=provider_factory,
        hosted_activity_lease_seconds=30,
    )
    monkeypatch.setattr(
        "archetype.runtime.runtime._bootstrap_config",
        lambda **_kwargs: bootstrap,
    )
    return storage, state


@pytest.mark.asyncio
async def test_concurrent_hosted_episodes_each_complete_their_own_activity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two in-flight episodes must not claim or settle across each other."""

    storage, state = _single_bootstrap(
        tmp_path,
        monkeypatch,
        namespace="hosted-concurrent",
        crash_once=False,
    )
    provider_config = _provider_config()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("hosted-concurrent", storage=storage)
        state.world_id = str((await world.info()).world_id)
        first, second = await asyncio.gather(
            world.run_hosted_episode(
                [_request()],
                provider=provider_config,
                activity_id="episode-one",
            ),
            world.run_hosted_episode(
                [_request()],
                provider=provider_config,
                activity_id="episode-two",
            ),
        )

    assert first.activity_id == "episode-one"
    assert second.activity_id == "episode-two"
    assert first.operation_id != second.operation_id
    assert state.execution_count == 2


@pytest.mark.asyncio
async def test_stale_unsettled_episode_does_not_fail_a_completed_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A leftover unsettled Activity is not a later operation's failure.

    The stale episode's worker dies before generic result recording, so its
    Activity stays unsettled.  A subsequent episode must complete on its own
    activity's settlement rather than requiring a globally quiet world.
    """

    storage, state = _single_bootstrap(
        tmp_path,
        monkeypatch,
        namespace="hosted-stale",
        crash_once=True,
    )
    provider_config = _provider_config()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("hosted-stale", storage=storage)
        state.world_id = str((await world.info()).world_id)
        with pytest.raises(RuntimeError, match="before generic result"):
            await world.run_hosted_episode(
                [_request()],
                provider=provider_config,
                activity_id="episode-stale",
            )

        observation = await world.run_hosted_episode(
            [_request()],
            provider=provider_config,
            activity_id="episode-fresh",
        )

    assert observation.activity_id == "episode-fresh"
    assert state.execution_count == 2
