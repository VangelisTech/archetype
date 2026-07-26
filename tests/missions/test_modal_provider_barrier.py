# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Adversarial contracts for the persistent two-tier Modal start barrier."""

from __future__ import annotations

import asyncio
import inspect
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from archetype.missions.sandboxes import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalPersistentDictMarker,
    ModalProviderBarrierUnknown,
    ModalProviderMarkerExists,
    ModalProviderOperationGuard,
    ModalProviderRunPermit,
    ModalProviderStartBarrier,
    ModalSandboxOperationIdentity,
)


class _NotFoundError(Exception):
    pass


class _AlreadyExistsError(Exception):
    pass


class _ConnectionError(Exception):
    pass


class _AsyncCall:
    def __init__(self, callback) -> None:
        self._callback = callback

    async def aio(self, *args, **kwargs):
        result = self._callback(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result


class _DictObject:
    def __init__(self, object_id: str) -> None:
        self.object_id = object_id


class _WorkspaceHandle:
    def __init__(self, registry: _DictRegistry) -> None:
        self._registry = registry
        self.hydrate = _AsyncCall(lambda: self)

    @property
    def name(self) -> str:
        return self._registry.workspace_name

    @property
    def client(self) -> object:
        return self._registry.client


class _DictHandle:
    def __init__(
        self,
        registry: _DictRegistry,
        *,
        environment_name: str,
        name: str,
    ) -> None:
        self._registry = registry
        self._key = (environment_name, name)
        self.object_id = ""
        self.hydrate = _AsyncCall(self._hydrate)

    def _hydrate(self) -> _DictHandle:
        error = self._registry.lookup_errors.get(self._key)
        if error is not None:
            raise error
        try:
            value = self._registry.objects[self._key]
        except KeyError as exc:
            raise _NotFoundError(self._key) from exc
        self.object_id = value.object_id
        return self


class _DictRegistry:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], _DictObject] = {}
        self.create_calls: list[dict[str, Any]] = []
        self.lookup_calls: list[dict[str, Any]] = []
        self.raise_after_create: set[tuple[str, str]] = set()
        self.create_errors: dict[tuple[str, str], Exception] = {}
        self.lookup_errors: dict[tuple[str, str], Exception] = {}
        self.workspace_name = "vangelis"
        self.client = object()
        self._sequence = 0
        self._create_lock = asyncio.Lock()
        self._create_barriers: dict[tuple[str, str], asyncio.Barrier] = {}

    def race_creates(self, *, environment_name: str, name: str) -> None:
        self._create_barriers[(environment_name, name)] = asyncio.Barrier(2)

    async def create(
        self,
        name: str,
        *,
        allow_existing: bool,
        environment_name: str,
        client: object,
    ) -> None:
        assert allow_existing is False
        assert client is self.client
        key = (environment_name, name)
        self.create_calls.append(
            {
                "name": name,
                "allow_existing": allow_existing,
                "environment_name": environment_name,
                "client": client,
            }
        )
        barrier = self._create_barriers.get(key)
        if barrier is not None:
            await barrier.wait()
        async with self._create_lock:
            self._create(key)

    def _create(self, key: tuple[str, str]) -> None:
        error = self.create_errors.get(key)
        if error is not None:
            raise error
        if key in self.objects:
            raise _AlreadyExistsError(key)
        self._sequence += 1
        self.objects[key] = _DictObject(f"di-{self._sequence}")
        if key in self.raise_after_create:
            raise _ConnectionError("provider response lost after persistent create")

    def from_name(
        self,
        name: str,
        *,
        create_if_missing: bool,
        environment_name: str,
        client: object,
    ) -> _DictHandle:
        assert create_if_missing is False
        assert client is self.client
        self.lookup_calls.append(
            {
                "name": name,
                "create_if_missing": create_if_missing,
                "environment_name": environment_name,
                "client": client,
            }
        )
        return _DictHandle(
            self,
            environment_name=environment_name,
            name=name,
        )


def _fake_modal(registry: _DictRegistry) -> object:
    return SimpleNamespace(
        exception=SimpleNamespace(
            AlreadyExistsError=_AlreadyExistsError,
            NotFoundError=_NotFoundError,
        ),
        Workspace=SimpleNamespace(
            from_context=lambda: _WorkspaceHandle(registry),
        ),
        Dict=SimpleNamespace(
            objects=SimpleNamespace(create=_AsyncCall(registry.create)),
            from_name=registry.from_name,
        ),
    )


@pytest.fixture
def provider_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_DictRegistry, ModalProviderStartBarrier]:
    registry = _DictRegistry()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(registry))
    return registry, ModalProviderStartBarrier(
        workspace_name="vangelis",
        environment_name="main",
        app_name="archetype-agent-missions-test",
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )


def _identity(
    operation_id: str,
    *,
    workspace_name: str = "vangelis",
    environment_name: str = "main",
    app_name: str = "archetype-agent-missions-test",
    protocol_epoch: int = MODAL_ACTIVITY_PROTOCOL_EPOCH,
) -> ModalSandboxOperationIdentity:
    return ModalSandboxOperationIdentity(
        workspace_name=workspace_name,
        environment_name=environment_name,
        app_name=app_name,
        operation_id=operation_id,
        protocol_epoch=protocol_epoch,
    )


def test_provider_marker_names_bind_full_workspace_environment_and_app_namespace() -> None:
    operation_id = "missions.author:world-a:dispatch-full-namespace"
    namespaces = (
        ("vangelis", "main", "archetype-agent-missions-test"),
        ("another-workspace", "main", "archetype-agent-missions-test"),
        ("vangelis", "staging", "archetype-agent-missions-test"),
        ("vangelis", "main", "another-app"),
    )
    names = {
        ModalProviderStartBarrier(
            workspace_name=workspace_name,
            environment_name=environment_name,
            app_name=app_name,
            protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        ).operation_marker_name(
            _identity(
                operation_id,
                workspace_name=workspace_name,
                environment_name=environment_name,
                app_name=app_name,
            )
        )
        for workspace_name, environment_name, app_name in namespaces
    }

    assert len(names) == len(namespaces)


@pytest.mark.asyncio
async def test_two_tier_persistent_barrier_selects_one_owner_at_each_race(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-race"
    identity = _identity(operation_id)
    operation_marker_name = barrier.operation_marker_name(identity)
    registry.race_creates(
        environment_name="main",
        name=operation_marker_name,
    )

    first, second = await asyncio.wait_for(
        asyncio.gather(
            barrier._acquire_initial(identity=identity),
            barrier._acquire_initial(identity=identity),
        ),
        timeout=1,
    )
    guards = [
        outcome for outcome in (first, second) if isinstance(outcome, ModalProviderOperationGuard)
    ]
    conflicts = [
        outcome for outcome in (first, second) if isinstance(outcome, ModalProviderMarkerExists)
    ]
    assert len(guards) == len(conflicts) == 1
    guard = guards[0]
    assert guard.reference.startswith("modal-dict://vangelis/main/op-v1-")
    assert guard.digest.startswith("sha256:")
    assert len(registry.objects) == 1

    run_marker_name = barrier.run_marker_name(guard)
    registry.race_creates(
        environment_name="main",
        name=run_marker_name,
    )
    run_first, run_second = await asyncio.wait_for(
        asyncio.gather(
            barrier._acquire_run(guard=guard),
            barrier._acquire_run(guard=guard),
        ),
        timeout=1,
    )
    permits = [
        outcome
        for outcome in (run_first, run_second)
        if isinstance(outcome, ModalProviderRunPermit)
    ]
    run_conflicts = [
        outcome
        for outcome in (run_first, run_second)
        if isinstance(outcome, ModalProviderMarkerExists)
    ]
    assert len(permits) == len(run_conflicts) == 1
    assert permits[0].guard == guard
    assert permits[0].reference.startswith("modal-dict://vangelis/main/run-v1-")
    assert permits[0].digest.startswith("sha256:")
    assert len(registry.objects) == 2
    assert all(call["allow_existing"] is False for call in registry.create_calls)
    assert all(call["client"] is registry.client for call in registry.create_calls)
    assert all(call["client"] is registry.client for call in registry.lookup_calls)
    assert not hasattr(barrier, "delete")


@pytest.mark.asyncio
async def test_retry_guard_winner_permanently_blocks_a_delayed_initial_worker(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-retry-wins"
    identity = _identity(operation_id)

    retry_guard = await barrier._acquire_retry_guard(identity=identity)
    assert isinstance(retry_guard, ModalProviderOperationGuard)

    stale_initial = await barrier._acquire_initial(identity=identity)
    assert isinstance(stale_initial, ModalProviderMarkerExists)
    assert stale_initial.phase == "operation"

    run = await barrier._acquire_run(guard=retry_guard)
    assert isinstance(run, ModalProviderRunPermit)
    assert len(registry.objects) == 2


@pytest.mark.asyncio
async def test_stale_initial_winner_blocks_retry_without_handoff_or_replay(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-stale-wins"
    identity = _identity(operation_id)

    stale_guard = await barrier._acquire_initial(identity=identity)
    assert isinstance(stale_guard, ModalProviderOperationGuard)

    retry = await barrier._acquire_retry_guard(identity=identity)
    repeated_retry = await barrier._acquire_retry_guard(identity=identity)
    assert isinstance(retry, ModalProviderMarkerExists)
    assert isinstance(repeated_retry, ModalProviderMarkerExists)
    assert len(registry.objects) == 1
    assert all(not name.startswith("run-v1-") for _environment, name in registry.objects)


@pytest.mark.asyncio
async def test_ambiguous_operation_marker_create_is_unknown_forever(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-ambiguous-operation"
    identity = _identity(operation_id)
    marker_name = barrier.operation_marker_name(identity)
    registry.raise_after_create.add(("main", marker_name))

    ambiguous = await barrier._acquire_initial(identity=identity)
    assert isinstance(ambiguous, ModalProviderBarrierUnknown)
    assert ambiguous.phase == "operation"
    assert "ConnectionError" in ambiguous.reason
    assert "provider response lost" not in ambiguous.reason

    retry = await barrier._acquire_retry_guard(identity=identity)
    assert isinstance(retry, ModalProviderMarkerExists)
    assert len(registry.objects) == 1


@pytest.mark.asyncio
async def test_ambiguous_run_marker_never_reconstructs_a_permit(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-ambiguous-run"
    identity = _identity(operation_id)
    guard = await barrier._acquire_initial(identity=identity)
    assert isinstance(guard, ModalProviderOperationGuard)
    run_name = barrier.run_marker_name(guard)
    registry.raise_after_create.add(("main", run_name))

    ambiguous = await barrier._acquire_run(guard=guard)
    assert isinstance(ambiguous, ModalProviderBarrierUnknown)
    assert ambiguous.phase == "run"

    repeated = await barrier._acquire_run(guard=guard)
    assert isinstance(repeated, ModalProviderMarkerExists)
    assert repeated.phase == "run"
    assert len(registry.objects) == 2


@pytest.mark.asyncio
async def test_acknowledged_run_winner_loss_is_safe_but_permanently_stuck(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-run-winner-lost"
    identity = _identity(operation_id)
    guard = await barrier._acquire_initial(identity=identity)
    assert isinstance(guard, ModalProviderOperationGuard)
    permit = await barrier._acquire_run(guard=guard)
    assert isinstance(permit, ModalProviderRunPermit)

    # Simulate loss before the effect starts. Provider lookup can establish
    # that a winner existed, but no later claimant reconstructs its permit.
    repeated = await barrier._acquire_run(guard=guard)
    assert isinstance(repeated, ModalProviderMarkerExists)
    assert repeated.phase == "run"
    assert len(registry.objects) == 2


@pytest.mark.asyncio
async def test_run_requires_exact_guard_object_and_provider_environment(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-exact-guard"
    identity = _identity(operation_id)
    guard = await barrier._acquire_initial(identity=identity)
    assert isinstance(guard, ModalProviderOperationGuard)

    wrong_object = ModalProviderOperationGuard(
        identity=guard.identity,
        marker=ModalPersistentDictMarker(
            workspace_name="vangelis",
            environment_name="main",
            app_name="archetype-agent-missions-test",
            protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
            name=guard.marker.name,
            object_id="di-another-object",
        ),
    )
    rejected_object = await barrier._acquire_run(guard=wrong_object)
    assert isinstance(rejected_object, ModalProviderBarrierUnknown)
    assert "identity changed" in rejected_object.reason

    other_environment = ModalProviderStartBarrier(
        workspace_name="vangelis",
        environment_name="staging",
        app_name="archetype-agent-missions-test",
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )
    rejected_environment = await other_environment._acquire_run(guard=guard)
    assert isinstance(rejected_environment, ModalProviderBarrierUnknown)
    assert "another Modal environment" in rejected_environment.reason
    assert len(registry.objects) == 1

    other_workspace = ModalProviderStartBarrier(
        workspace_name="another-workspace",
        environment_name="main",
        app_name="archetype-agent-missions-test",
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )
    rejected_workspace = await other_workspace._acquire_run(guard=guard)
    assert isinstance(rejected_workspace, ModalProviderBarrierUnknown)
    assert "another Modal workspace" in rejected_workspace.reason
    assert len(registry.objects) == 1

    other_app = ModalProviderStartBarrier(
        workspace_name="vangelis",
        environment_name="main",
        app_name="another-app",
        protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
    )
    rejected_app = await other_app._acquire_run(guard=guard)
    assert isinstance(rejected_app, ModalProviderBarrierUnknown)
    assert "another Modal app" in rejected_app.reason
    assert len(registry.objects) == 1


@pytest.mark.asyncio
async def test_provider_failures_are_bounded_unknowns_without_secret_text(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    operation_id = "missions.author:world-a:dispatch-provider-error"
    identity = _identity(operation_id)
    marker_name = barrier.operation_marker_name(identity)
    registry.create_errors[("main", marker_name)] = _ConnectionError("credential-canary")

    outcome = await barrier._acquire_initial(identity=identity)
    assert isinstance(outcome, ModalProviderBarrierUnknown)
    assert "ConnectionError" in outcome.reason
    assert "credential-canary" not in outcome.reason
    assert registry.objects == {}


@pytest.mark.asyncio
async def test_authenticated_workspace_mismatch_fails_before_provider_create(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    registry.workspace_name = "unexpected-workspace"

    outcome = await barrier._acquire_initial(
        identity=_identity("missions.author:world-a:dispatch-wrong-workspace")
    )

    assert isinstance(outcome, ModalProviderBarrierUnknown)
    assert "workspace identity" in outcome.reason
    assert registry.create_calls == []
    assert registry.objects == {}


@pytest.mark.asyncio
async def test_pre_barrier_and_in_flight_legacy_operations_never_gain_retry_authority(
    provider_barrier,
) -> None:
    registry, barrier = provider_barrier
    legacy = _identity(
        "missions.author:world-a:dispatch-pre-cutover",
        protocol_epoch=0,
    )

    outcome = await barrier._acquire_retry_guard(identity=legacy)

    assert isinstance(outcome, ModalProviderBarrierUnknown)
    assert "predates the barrier-aware protocol epoch" in outcome.reason
    assert "legacy or in-flight absence remains Unknown" in outcome.reason
    assert registry.lookup_calls == []
    assert registry.create_calls == []
    assert registry.objects == {}
    with pytest.raises(ValueError, match="predates"):
        barrier.operation_marker_name(legacy)
    with pytest.raises(ValueError, match="barrier-aware protocol epoch"):
        ModalProviderStartBarrier(
            workspace_name="vangelis",
            environment_name="main",
            app_name="archetype-agent-missions-test",
            protocol_epoch=0,
        )
