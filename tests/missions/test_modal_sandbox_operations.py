# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Crash contracts for stable named Modal mission sandbox operations."""

from __future__ import annotations

import asyncio
import re
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from archetype.missions.sandboxes import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalSandboxBackend,
    ModalSandboxConfig,
    ModalSandboxOperationCapability,
    ModalSandboxOperationIdentity,
    ModalSandboxOperationRunning,
    ModalSandboxOperationUnknown,
    ModalSandboxSession,
    SandboxSpec,
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
        return self._callback(*args, **kwargs)


class _WorkspaceHandle:
    def __init__(self, registry: _ModalRegistry) -> None:
        self._registry = registry
        self.hydrate = _AsyncCall(lambda: self)

    @property
    def name(self) -> str:
        return self._registry.workspace_name

    @property
    def client(self) -> object:
        return self._registry.client


class _RemoteApp:
    def __init__(
        self,
        *,
        name: str,
        environment_name: str,
        client: object,
    ) -> None:
        self.name = name
        self.environment_name = environment_name
        self.client = client


class _RemoteSandbox:
    def __init__(
        self,
        registry: _ModalRegistry,
        name: str,
        object_id: str,
        *,
        app_name: str,
        environment_name: str,
        tags: dict[str, str],
    ) -> None:
        self._registry = registry
        self.name = name
        self.object_id = object_id
        self.app_name = app_name
        self.environment_name = environment_name
        self.tags = dict(tags)
        self.returncode: int | None = None
        self.poll_error: Exception | None = None
        self.tag_error: Exception | None = None
        self.terminated = 0
        self.detached = 0
        self.poll = _AsyncCall(self._poll)
        self.get_tags = _AsyncCall(self._get_tags)
        self.terminate = _AsyncCall(self._terminate)
        self.detach = _AsyncCall(self._detach)

    def _poll(self) -> int | None:
        if self.poll_error is not None:
            raise self.poll_error
        return self.returncode

    def _get_tags(self) -> dict[str, str]:
        if self.tag_error is not None:
            raise self.tag_error
        return dict(self.tags)

    def _terminate(self, *, wait: bool) -> None:
        assert wait is True
        self.terminated += 1
        self.returncode = 0
        if self._registry.resources.get(self.name) is self:
            self._registry.resources.pop(self.name)

    def _detach(self) -> None:
        self.detached += 1


class _ModalRegistry:
    def __init__(self) -> None:
        self.resources: dict[str, _RemoteSandbox] = {}
        self.create_calls: list[dict[str, Any]] = []
        self.lookup_calls: list[dict[str, Any]] = []
        self.app_calls: list[dict[str, Any]] = []
        self.volume_calls: list[dict[str, Any]] = []
        self.secret_calls: list[dict[str, Any]] = []
        self.lookup_errors: dict[str, Exception] = {}
        self.raise_after_create: set[str] = set()
        self.workspace_name = "vangelis"
        self.client = object()
        self._sequence = 0

    def app_lookup(
        self,
        app_name: str,
        *,
        create_if_missing: bool,
        environment_name: str,
        client: object,
    ) -> _RemoteApp:
        assert create_if_missing is True
        assert client is self.client
        self.app_calls.append(
            {
                "app_name": app_name,
                "environment_name": environment_name,
                "client": client,
            }
        )
        return _RemoteApp(
            name=app_name,
            environment_name=environment_name,
            client=client,
        )

    def create(self, *command: str, **kwargs: Any) -> _RemoteSandbox:
        name = str(kwargs["name"])
        app = kwargs["app"]
        environment_name = str(kwargs["environment_name"])
        assert isinstance(app, _RemoteApp)
        assert app.environment_name == environment_name
        assert kwargs["client"] is self.client
        self.create_calls.append({"command": command, **kwargs})
        if name in self.resources:
            raise _AlreadyExistsError(name)
        self._sequence += 1
        sandbox = _RemoteSandbox(
            self,
            name,
            f"sb-{self._sequence}",
            app_name=app.name,
            environment_name=environment_name,
            tags=kwargs["tags"],
        )
        self.resources[name] = sandbox
        if name in self.raise_after_create:
            raise _ConnectionError("provider response lost after create")
        return sandbox

    def lookup(
        self,
        app_name: str,
        name: str,
        *,
        environment_name: str,
        client: object,
    ) -> _RemoteSandbox:
        assert client is self.client
        self.lookup_calls.append(
            {
                "app_name": app_name,
                "name": name,
                "environment_name": environment_name,
                "client": client,
            }
        )
        error = self.lookup_errors.get(name)
        if error is not None:
            raise error
        try:
            sandbox = self.resources[name]
        except KeyError as exc:
            raise _NotFoundError(name) from exc
        if sandbox.app_name != app_name or sandbox.environment_name != environment_name:
            raise _NotFoundError(name)
        return sandbox

    def volume_from_name(self, name: str, **kwargs: Any) -> object:
        assert kwargs["client"] is self.client
        self.volume_calls.append({"name": name, **kwargs})
        return SimpleNamespace(hydrate=_AsyncCall(lambda: None))

    def secret_from_name(self, name: str, **kwargs: Any) -> object:
        assert kwargs["client"] is self.client
        self.secret_calls.append({"name": name, **kwargs})
        return (name, kwargs)


def _fake_modal(registry: _ModalRegistry) -> object:
    return SimpleNamespace(
        exception=SimpleNamespace(
            NotFoundError=_NotFoundError,
            AlreadyExistsError=_AlreadyExistsError,
        ),
        Workspace=SimpleNamespace(
            from_context=lambda: _WorkspaceHandle(registry),
        ),
        App=SimpleNamespace(
            lookup=_AsyncCall(registry.app_lookup),
        ),
        Volume=SimpleNamespace(
            from_name=registry.volume_from_name,
        ),
        Secret=SimpleNamespace(
            from_name=registry.secret_from_name,
        ),
        Image=SimpleNamespace(
            from_id=lambda image_id, *, client: ("image", image_id, client),
        ),
        Sandbox=SimpleNamespace(
            create=_AsyncCall(registry.create),
            from_name=_AsyncCall(registry.lookup),
        ),
    )


@pytest.fixture
def modal_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    _ModalRegistry,
    ModalSandboxBackend,
    ModalSandboxOperationCapability,
    SandboxSpec,
]:
    registry = _ModalRegistry()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(registry))

    async def verified(*args, **kwargs) -> None:
        del args, kwargs

    monkeypatch.setattr(
        "archetype.missions.sandboxes.modal.verify_coding_agent_environment",
        verified,
    )
    backend = ModalSandboxBackend(
        ModalSandboxConfig(
            app_name="archetype-agent-missions-test",
            image_id="im-reviewed",
            workspace_name="vangelis",
            environment_name="main",
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        )
    )
    capability = ModalSandboxOperationCapability(backend)
    spec = SandboxSpec(
        "modal",
        backend.environment,
        "/workspace/repo",
        metadata=(
            ("mission", "mission-1"),
            ("kind", "caller-cannot-replace-kind"),
            ("operation_digest", "caller-cannot-replace-operation"),
        ),
    )
    return registry, backend, capability, spec


def _operation_identity(
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


def _remote_sandbox(
    registry: _ModalRegistry,
    identity: ModalSandboxOperationIdentity,
    *,
    role: str,
    object_id: str,
    cohort_id: str = "cohort-v1:00000000000000000000000000000001",
) -> _RemoteSandbox:
    return _RemoteSandbox(
        registry,
        (identity.mission_sandbox_name if role == "mission" else identity.auth_sandbox_name),
        object_id,
        app_name=identity.app_name,
        environment_name=identity.environment_name,
        tags={
            "kind": f"archetype-agent-{role}",
            "operation_digest": identity.digest,
            "operation_cohort": cohort_id,
            "operation_protocol_epoch": str(identity.protocol_epoch),
        },
    )


def test_modal_operation_names_are_stable_safe_and_world_isolated() -> None:
    operation_id = "missions.author:world-a:dispatch-1"
    first = _operation_identity(operation_id)
    repeated = _operation_identity(operation_id)
    other_world = _operation_identity("missions.author:world-b:dispatch-1")
    unusual = _operation_identity("missions.author:wørld:dispatch/with spaces")
    other_workspace = _operation_identity(operation_id, workspace_name="another-workspace")
    other_environment = _operation_identity(operation_id, environment_name="staging")
    other_app = _operation_identity(operation_id, app_name="another-app")
    legacy_epoch = _operation_identity(operation_id, protocol_epoch=0)

    assert first == repeated
    assert first.digest == repeated.digest
    assert first.mission_sandbox_name == repeated.mission_sandbox_name
    assert first.auth_sandbox_name == repeated.auth_sandbox_name
    assert first.digest != other_world.digest
    assert (
        len(
            {
                first.digest,
                other_workspace.digest,
                other_environment.digest,
                other_app.digest,
                legacy_epoch.digest,
            }
        )
        == 5
    )
    assert (
        len(
            {
                first.mission_sandbox_name,
                other_workspace.mission_sandbox_name,
                other_environment.mission_sandbox_name,
                other_app.mission_sandbox_name,
                legacy_epoch.mission_sandbox_name,
            }
        )
        == 5
    )
    assert (
        len(
            {
                first.mission_sandbox_name,
                first.auth_sandbox_name,
                other_world.mission_sandbox_name,
                other_world.auth_sandbox_name,
            }
        )
        == 4
    )
    for name in (
        first.mission_sandbox_name,
        first.auth_sandbox_name,
        other_world.mission_sandbox_name,
        other_world.auth_sandbox_name,
        unusual.mission_sandbox_name,
        unusual.auth_sandbox_name,
    ):
        assert len(name) <= 63
        assert re.fullmatch(r"[a-z0-9][a-z0-9-]*[a-z0-9]", name)

    for invalid in ("", " ", " leading", "trailing ", "nul\x00value", "x" * 1025):
        with pytest.raises(ValueError):
            _operation_identity(invalid)

    for field in ("workspace_name", "environment_name", "app_name"):
        with pytest.raises(ValueError):
            _operation_identity(operation_id, **{field: "invalid/name"})
    with pytest.raises(ValueError, match="epoch"):
        _operation_identity(operation_id, protocol_epoch=-1)


@pytest.mark.asyncio
async def test_named_modal_operation_reports_running_without_sharing_execution_handle(
    modal_operation,
) -> None:
    registry, backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-1"
    identity = capability.identity(operation_id)

    created = await capability.start(operation_id=operation_id, spec=spec)
    mission = registry.resources[identity.mission_sandbox_name]
    auth = registry.resources[identity.auth_sandbox_name]

    assert created.operation_identity == identity
    assert created.identity.sandbox_id == mission.object_id
    assert len(registry.create_calls) == 2
    assert {call["name"] for call in registry.create_calls} == {
        identity.mission_sandbox_name,
        identity.auth_sandbox_name,
    }
    for call in registry.create_calls:
        assert call["command"] == ("sleep", "infinity")
        assert call["tags"]["operation_digest"] == identity.digest
        assert call["tags"]["operation_cohort"] == created.operation_cohort_id
        assert call["tags"]["operation_protocol_epoch"] == str(identity.protocol_epoch)
        assert call["environment_name"] == identity.environment_name
        assert call["client"] is registry.client
        assert call["tags"]["kind"] in {
            "archetype-agent-auth",
            "archetype-agent-mission",
        }
    assert all(call["app_name"] == identity.app_name for call in registry.app_calls)
    assert all(
        call["environment_name"] == identity.environment_name and call["client"] is registry.client
        for call in (*registry.app_calls, *registry.volume_calls, *registry.secret_calls)
    )

    # Simulate a process crash by discarding the local handle and rebuilding
    # both the backend and capability over the same provider namespace.
    restarted = ModalSandboxOperationCapability(ModalSandboxBackend(backend.config))
    running = await restarted.reconcile(operation_id=operation_id, spec=spec)

    assert isinstance(running, ModalSandboxOperationRunning)
    assert running.identity == identity
    assert running.mission_sandbox_id == mission.object_id
    assert running.auth_sandbox_id == auth.object_id
    assert running.cohort_id == created.operation_cohort_id
    assert not hasattr(running, "session")
    assert not hasattr(running, "retry_guard")
    assert len(registry.create_calls) == 2

    # A newer claimant cannot acquire the live provider name and reconcile
    # does not hand it a session through which to invoke a second inner exec.
    with pytest.raises(_AlreadyExistsError):
        await restarted.start(operation_id=operation_id, spec=spec)
    assert len(registry.resources) == 2
    assert not hasattr(restarted, "cleanup")

    await created.close()
    assert registry.resources == {}
    assert mission.terminated == auth.terminated == 1
    assert mission.detached == auth.detached == 1


@pytest.mark.asyncio
async def test_modal_reconciliation_never_turns_missing_names_into_retry_permission(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-absent"
    identity = capability.identity(operation_id)

    missing = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(missing, ModalSandboxOperationUnknown)
    assert missing.identity == identity
    assert "not retry authorization" in missing.reason
    assert not hasattr(missing, "retry_guard")
    assert registry.create_calls == []

    registry.resources[identity.auth_sandbox_name] = _remote_sandbox(
        registry,
        identity,
        role="auth",
        object_id="sb-partial-auth",
    )
    partial = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(partial, ModalSandboxOperationUnknown)
    assert "partial" in partial.reason
    assert "auth" not in partial.reason.partition("absent=")[2]
    assert registry.create_calls == []


@pytest.mark.asyncio
async def test_modal_reconciliation_keeps_provider_and_poll_errors_unknown(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-unknown"
    identity = capability.identity(operation_id)
    registry.lookup_errors[identity.mission_sandbox_name] = _ConnectionError("credential-canary")

    lookup_unknown = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(lookup_unknown, ModalSandboxOperationUnknown)
    assert "ConnectionError" in lookup_unknown.reason
    assert "credential-canary" not in lookup_unknown.reason
    assert registry.create_calls == []

    registry.lookup_errors.clear()
    mission = _remote_sandbox(registry, identity, role="mission", object_id="sb-mission")
    auth = _remote_sandbox(registry, identity, role="auth", object_id="sb-auth")
    registry.resources[mission.name] = mission
    registry.resources[auth.name] = auth
    mission.poll_error = _ConnectionError("poll-credential-canary")

    poll_unknown = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(poll_unknown, ModalSandboxOperationUnknown)
    assert "poll failed" in poll_unknown.reason
    assert "poll-credential-canary" not in poll_unknown.reason

    mission.poll_error = None
    mission.returncode = 0
    stopped_unknown = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(stopped_unknown, ModalSandboxOperationUnknown)
    assert "not running" in stopped_unknown.reason


@pytest.mark.asyncio
async def test_modal_reconciliation_rejects_mixed_or_unverifiable_cohorts(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-mixed-generation"
    identity = capability.identity(operation_id)
    mission = _remote_sandbox(
        registry,
        identity,
        role="mission",
        object_id="sb-old-mission",
        cohort_id="cohort-v1:00000000000000000000000000000001",
    )
    auth = _remote_sandbox(
        registry,
        identity,
        role="auth",
        object_id="sb-new-auth",
        cohort_id="cohort-v1:00000000000000000000000000000002",
    )
    registry.resources[mission.name] = mission
    registry.resources[auth.name] = auth

    mixed = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(mixed, ModalSandboxOperationUnknown)
    assert "mixed-generation" in mixed.reason

    auth.tags["operation_cohort"] = mission.tags["operation_cohort"]
    auth.tags["operation_digest"] = "sha256:" + "0" * 64
    wrong_identity = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(wrong_identity, ModalSandboxOperationUnknown)
    assert "full provider operation identity" in wrong_identity.reason

    auth.tags["operation_digest"] = identity.digest
    auth.tag_error = _ConnectionError("tag-credential-canary")
    unreadable = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(unreadable, ModalSandboxOperationUnknown)
    assert "cohort lookup failed" in unreadable.reason
    assert "tag-credential-canary" not in unreadable.reason


@pytest.mark.asyncio
async def test_ambiguous_create_is_reconciled_as_partial_without_unsafe_name_cleanup(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-crash"
    identity = capability.identity(operation_id)
    registry.raise_after_create.add(identity.mission_sandbox_name)

    with pytest.raises(_ConnectionError, match="response lost"):
        await capability.start(operation_id=operation_id, spec=spec)

    # The auth handle was known and compensated. The mission create response
    # was lost, so its surviving name cannot be treated as safe absence.
    assert set(registry.resources) == {identity.mission_sandbox_name}
    calls_after_crash = len(registry.create_calls)
    unknown = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(unknown, ModalSandboxOperationUnknown)
    assert "partial" in unknown.reason
    assert len(registry.create_calls) == calls_after_crash

    mission = registry.resources[identity.mission_sandbox_name]
    assert mission.terminated == mission.detached == 0
    assert not hasattr(capability, "cleanup")


@pytest.mark.asyncio
async def test_atomic_live_name_allows_only_one_concurrent_pair_owner(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-race"

    first, second = await asyncio.gather(
        capability.start(operation_id=operation_id, spec=spec),
        capability.start(operation_id=operation_id, spec=spec),
        return_exceptions=True,
    )

    outcomes = (first, second)
    winners = [outcome for outcome in outcomes if not isinstance(outcome, BaseException)]
    losers = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
    assert len(winners) == 1
    assert len(losers) == 1
    assert isinstance(losers[0], _AlreadyExistsError)
    assert len(registry.resources) == 2

    running = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(running, ModalSandboxOperationRunning)
    assert not hasattr(running, "session")
    assert isinstance(winners[0], ModalSandboxSession)
    await winners[0].close()


@pytest.mark.asyncio
async def test_ambiguous_first_name_acquisition_never_authorizes_another_start(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-auth-crash"
    identity = capability.identity(operation_id)
    registry.raise_after_create.add(identity.auth_sandbox_name)

    with pytest.raises(_ConnectionError, match="response lost"):
        await capability.start(operation_id=operation_id, spec=spec)

    assert set(registry.resources) == {identity.auth_sandbox_name}
    unknown = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(unknown, ModalSandboxOperationUnknown)
    assert "partial" in unknown.reason
    calls_after_reconcile = len(registry.create_calls)

    # The provider response was ambiguous, but its live name still fences a
    # delayed claimant. Neither reconciliation nor the conflict yields a
    # session or retry guard.
    with pytest.raises(_AlreadyExistsError):
        await capability.start(operation_id=operation_id, spec=spec)
    assert len(registry.create_calls) == calls_after_reconcile + 1
    assert not hasattr(unknown, "session")
    assert not hasattr(unknown, "retry_guard")

    assert not hasattr(capability, "cleanup")


@pytest.mark.asyncio
async def test_exact_session_close_cannot_terminate_a_reused_name_generation(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-late-close"
    identity = capability.identity(operation_id)
    session = await capability.start(operation_id=operation_id, spec=spec)
    old_mission = registry.resources[identity.mission_sandbox_name]
    old_auth = registry.resources[identity.auth_sandbox_name]

    newer_mission = _remote_sandbox(
        registry,
        identity,
        role="mission",
        object_id="sb-newer-mission",
        cohort_id="cohort-v1:00000000000000000000000000000002",
    )
    newer_auth = _remote_sandbox(
        registry,
        identity,
        role="auth",
        object_id="sb-newer-auth",
        cohort_id="cohort-v1:00000000000000000000000000000002",
    )
    registry.resources[identity.mission_sandbox_name] = newer_mission
    registry.resources[identity.auth_sandbox_name] = newer_auth

    await session.close()

    assert registry.resources[identity.mission_sandbox_name] is newer_mission
    assert registry.resources[identity.auth_sandbox_name] is newer_auth
    assert old_mission.terminated == old_auth.terminated == 1
    assert newer_mission.terminated == newer_auth.terminated == 0
    assert not hasattr(capability, "cleanup")


@pytest.mark.asyncio
async def test_named_modal_operations_isolate_same_dispatch_across_worlds(
    modal_operation,
) -> None:
    registry, _backend, capability, spec = modal_operation
    world_a = "missions.author:world-a:same-dispatch"
    world_b = "missions.author:world-b:same-dispatch"

    first = await capability.start(operation_id=world_a, spec=spec)
    second = await capability.start(operation_id=world_b, spec=spec)
    first_id = first.identity.sandbox_id
    second_id = second.identity.sandbox_id

    assert first_id != second_id
    assert len(registry.resources) == 4
    running_a = await capability.reconcile(operation_id=world_a, spec=spec)
    running_b = await capability.reconcile(operation_id=world_b, spec=spec)
    assert isinstance(running_a, ModalSandboxOperationRunning)
    assert isinstance(running_b, ModalSandboxOperationRunning)
    assert running_a.mission_sandbox_id == first_id
    assert running_b.mission_sandbox_id == second_id

    await first.close()
    assert len(registry.resources) == 2
    still_running = await capability.reconcile(operation_id=world_b, spec=spec)
    assert isinstance(still_running, ModalSandboxOperationRunning)
    await second.close()
    assert registry.resources == {}


@pytest.mark.asyncio
async def test_modal_operation_namespace_is_explicit_and_ambient_mismatch_fails_closed(
    modal_operation,
) -> None:
    registry, backend, capability, spec = modal_operation
    operation_id = "missions.author:world-a:dispatch-namespace"
    identity = capability.identity(operation_id)

    for config in (
        ModalSandboxConfig(
            app_name=identity.app_name,
            image_id="im-reviewed",
            environment_name=identity.environment_name,
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        ),
        ModalSandboxConfig(
            app_name=identity.app_name,
            image_id="im-reviewed",
            workspace_name=identity.workspace_name,
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        ),
        ModalSandboxConfig(
            app_name=identity.app_name,
            image_id="im-reviewed",
            workspace_name=identity.workspace_name,
            environment_name=identity.environment_name,
            operation_protocol_epoch=0,
        ),
    ):
        with pytest.raises(ValueError):
            ModalSandboxOperationCapability(ModalSandboxBackend(config))

    other_environment = ModalSandboxOperationCapability(
        ModalSandboxBackend(
            ModalSandboxConfig(
                app_name=identity.app_name,
                image_id="im-reviewed",
                workspace_name=identity.workspace_name,
                environment_name="staging",
                operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
            )
        )
    )
    other_app = ModalSandboxOperationCapability(
        ModalSandboxBackend(
            ModalSandboxConfig(
                app_name="another-app",
                image_id="im-reviewed",
                workspace_name=identity.workspace_name,
                environment_name=identity.environment_name,
                operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
            )
        )
    )
    assert other_environment.identity(operation_id).digest != identity.digest
    assert other_app.identity(operation_id).digest != identity.digest

    environment_absent = await other_environment.reconcile(
        operation_id=operation_id,
        spec=spec,
    )
    app_absent = await other_app.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(environment_absent, ModalSandboxOperationUnknown)
    assert isinstance(app_absent, ModalSandboxOperationUnknown)
    assert {call["environment_name"] for call in registry.lookup_calls[-4:]} == {
        "main",
        "staging",
    }
    assert {call["app_name"] for call in registry.lookup_calls[-4:]} == {
        identity.app_name,
        "another-app",
    }

    lookups_before_mismatch = len(registry.lookup_calls)
    creates_before_mismatch = len(registry.create_calls)
    registry.workspace_name = "unexpected-workspace"
    mismatch = await capability.reconcile(operation_id=operation_id, spec=spec)
    assert isinstance(mismatch, ModalSandboxOperationUnknown)
    assert "workspace identity" in mismatch.reason
    with pytest.raises(RuntimeError, match="workspace identity"):
        await capability.start(operation_id=operation_id, spec=spec)
    assert len(registry.lookup_calls) == lookups_before_mismatch
    assert len(registry.create_calls) == creates_before_mismatch
    assert registry.app_calls == []
