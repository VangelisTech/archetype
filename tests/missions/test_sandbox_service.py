# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the mission-family sandbox resource layer."""

from __future__ import annotations

import asyncio

import pytest

from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxBackend,
    SandboxCapabilities,
    SandboxEvent,
    SandboxEventType,
    SandboxIdentity,
    SandboxKey,
    SandboxService,
    SandboxServiceProtocol,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
    live_observation_paths,
    validate_checkpoint_for_spec,
)


class _Session:
    def __init__(self, identity: SandboxIdentity) -> None:
        self._identity = identity
        self._status = SandboxStatus.READY
        self.closed = 0
        self.close_attempts = 0
        self.close_error = False

    @property
    def identity(self) -> SandboxIdentity:
        return self._identity

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=True)

    async def status(self) -> SandboxStatus:
        return self._status

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        return ProcessResult(request.argv, 0, stdout="ok")

    async def checkpoint(self) -> CheckpointRef:
        return CheckpointRef(
            "fake",
            "checkpoint",
            "fake://checkpoint",
            1,
            environment=self.identity.environment,
            source_sandbox_id=self.identity.sandbox_id,
        )

    async def close(self) -> None:
        self.close_attempts += 1
        if self.close_error:
            self._status = SandboxStatus.ERRORED
            raise RuntimeError("provider close unavailable")
        self.closed += 1
        self._status = SandboxStatus.CLOSED


class _Backend:
    name = "fake"

    def __init__(self) -> None:
        self.creates = 0
        self.restores = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def create(self, spec: SandboxSpec) -> _Session:
        self.creates += 1
        self.started.set()
        await self.release.wait()
        return _Session(SandboxIdentity(self.name, f"sandbox-{self.creates}", spec.environment))

    async def restore(self, spec: SandboxSpec, checkpoint: CheckpointRef) -> _Session:
        self.restores += 1
        return _Session(SandboxIdentity(self.name, checkpoint.checkpoint_id, spec.environment))


def _spec(*, environment: str = "python-3.12@sha256:test") -> SandboxSpec:
    return SandboxSpec(
        provider="fake",
        environment=environment,
        workdir="/workspace/repo",
    )


def test_sandbox_contracts_describe_resources_not_task_outcomes() -> None:
    statuses = {status.value for status in SandboxStatus}
    assert statuses == {"provisioning", "ready", "errored", "interrupted", "closed"}
    assert statuses.isdisjoint({"accepted", "rejected", "completed"})
    assert isinstance(_Backend(), SandboxBackend)
    assert isinstance(_Session(SandboxIdentity("fake", "sandbox", "environment")), SandboxSession)
    assert isinstance(SandboxService(), SandboxServiceProtocol)


def test_process_request_requires_explicit_portable_inputs() -> None:
    request = ProcessRequest(
        ("python", "-V"),
        workdir="/workspace/repo",
        env=(("NO_COLOR", "1"),),
        secret_names=("github",),
        close_stdin=True,
    )
    assert request.environment_dict() == {"NO_COLOR": "1"}
    with pytest.raises(ValueError, match="absolute"):
        ProcessRequest(("python",), workdir="relative")
    with pytest.raises(ValueError, match="unique"):
        ProcessRequest(("python",), env=(("A", "1"), ("A", "2")))
    with pytest.raises(ValueError, match="home_directory"):
        SandboxCapabilities(home_directory="/")


def test_sandbox_value_contracts_reject_ambiguous_or_unrecoverable_inputs() -> None:
    with pytest.raises(ValueError, match="key"):
        SandboxKey(" ")
    for args, kwargs, error in (
        (("", "env", "/workspace"), {}, "provider"),
        (("fake", "", "/workspace"), {}, "environment"),
        (("fake", "env", "/"), {}, "workdir"),
        (("fake", "env", "/workspace"), {"timeout_seconds": 0}, "timeouts"),
        (
            ("fake", "env", "/workspace"),
            {"metadata": (("duplicate", "1"), ("duplicate", "2"))},
            "metadata",
        ),
    ):
        with pytest.raises(ValueError, match=error):
            SandboxSpec(*args, **kwargs)
    with pytest.raises(ValueError, match="provider"):
        SandboxIdentity("", "sandbox", "environment")
    with pytest.raises(ValueError, match="environment"):
        SandboxIdentity("fake", "sandbox", "")
    with pytest.raises(ValueError, match="negative"):
        SandboxEvent(
            SandboxEventType.READY,
            SandboxIdentity("fake", "sandbox", "environment"),
            -1,
        )
    with pytest.raises(ValueError, match="unique"):
        SandboxCapabilities(secret_names=("oauth", "oauth"))
    with pytest.raises(ValueError, match="observation_directory"):
        SandboxCapabilities(observation_directory="relative")
    for kwargs, error in (
        ({"argv": ()}, "argv"),
        ({"argv": ("true",), "timeout_seconds": 0}, "timeout"),
        ({"argv": ("true",), "secret_names": ("",)}, "secret"),
        ({"argv": ("true",), "secret_names": ("oauth", "oauth")}, "unique"),
    ):
        with pytest.raises(ValueError, match=error):
            ProcessRequest(**kwargs)
    for args, kwargs, error in (
        (("", "id", "fake://id", 1), {}, "requires"),
        (("fake", "id", "fake://id", -1), {}, "negative"),
        (("fake", "id", "fake://id", 2), {"expires_at_ms": 2}, "expiry"),
        (("fake", "id", "fake://id", 1), {"integrity": "sha256:bad"}, "sha256"),
    ):
        with pytest.raises(ValueError, match=error):
            CheckpointRef(*args, **kwargs)
    with pytest.raises(ValueError, match="artifact root"):
        live_observation_paths("relative")
    with pytest.raises(ValueError, match="trace identity"):
        live_observation_paths(trace_id="../another-process")


def test_checkpoint_validation_requires_complete_same_provider_lineage() -> None:
    spec = _spec()
    cases = (
        (CheckpointRef("other", "id", "other://id", 1), "provider"),
        (
            CheckpointRef(
                "fake",
                "id",
                "fake://id",
                1,
                environment=spec.environment,
                source_sandbox_id="source",
                restorable=False,
            ),
            "non-restorable",
        ),
        (
            CheckpointRef("fake", "id", "fake://id", 1, source_sandbox_id="source"),
            "lineage",
        ),
        (
            CheckpointRef("fake", "id", "fake://id", 1, environment=spec.environment),
            "source",
        ),
    )
    for checkpoint, error in cases:
        with pytest.raises(ValueError, match=error):
            validate_checkpoint_for_spec(checkpoint, spec)

    with pytest.raises(ValueError, match="owner"):
        validate_checkpoint_for_spec(
            CheckpointRef(
                "fake",
                "id",
                "fake://id",
                1,
                environment=spec.environment,
                source_sandbox_id="source",
                owner_id="another-mission",
            ),
            spec,
        )


@pytest.mark.asyncio
async def test_acquire_is_single_flight_and_keyed_by_an_exact_spec() -> None:
    backend = _Backend()
    service = SandboxService((backend,))
    key = SandboxKey("mission:7")

    first = asyncio.create_task(service.acquire(key, _spec()))
    second = asyncio.create_task(service.acquire(key, _spec()))
    await backend.started.wait()
    assert backend.creates == 1
    backend.release.set()

    one, two = await asyncio.gather(first, second)
    assert one is two
    assert service.session(key) is one
    with pytest.raises(ValueError, match="another spec"):
        await service.acquire(key, _spec(environment="different"))

    await service.close(key)
    assert one.closed == 1
    assert service.session(key) is None


@pytest.mark.asyncio
async def test_acquire_replaces_a_non_ready_session_after_teardown() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:broken")
    spec = _spec()
    broken = await service.acquire(key, spec)
    broken._status = SandboxStatus.ERRORED

    replacement = await service.acquire(key, spec)

    assert replacement is not broken
    assert broken.closed == 1
    assert backend.creates == 2
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_acquire_forgets_an_already_closed_session_before_replacement() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:closed")
    spec = _spec()
    closed = await service.acquire(key, spec)
    closed._status = SandboxStatus.CLOSED

    replacement = await service.acquire(key, spec)

    assert replacement is not closed
    assert closed.close_attempts == 0
    assert backend.creates == 2
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_restore_and_shutdown_own_only_live_session_lifetime() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    checkpoint = CheckpointRef(
        "fake",
        "restored",
        "fake://restored",
        1,
        environment=_spec().environment,
        source_sandbox_id="source",
    )
    session = await service.restore(SandboxKey("mission:8"), _spec(), checkpoint)

    assert backend.restores == 1
    assert session.identity.sandbox_id == "restored"
    await service.shutdown()
    assert session.closed == 1
    await service.shutdown()
    with pytest.raises(RuntimeError, match="shutting down"):
        await service.acquire(SandboxKey("mission:9"), _spec())


@pytest.mark.asyncio
async def test_restore_replaces_a_retained_live_session_instead_of_ignoring_checkpoint() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:10")
    spec = _spec()
    original = await service.acquire(key, spec)
    checkpoint = CheckpointRef(
        "fake",
        "replacement",
        "fake://replacement",
        1,
        environment=spec.environment,
        source_sandbox_id=original.identity.sandbox_id,
    )

    restored = await service.restore(key, spec, checkpoint)

    assert original.closed == 1
    assert backend.restores == 1
    assert restored is not original
    assert restored.identity.sandbox_id == "replacement"
    assert service.session(key) is restored
    await service.close(key)
    await service.close(key)
    assert restored.closed == 1


@pytest.mark.asyncio
async def test_failed_replacement_close_retains_the_session_for_cleanup_retry() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:close-retry")
    spec = _spec()
    original = await service.acquire(key, spec)
    original.close_error = True
    checkpoint = CheckpointRef(
        "fake",
        "replacement",
        "fake://replacement",
        1,
        environment=spec.environment,
        source_sandbox_id=original.identity.sandbox_id,
    )

    with pytest.raises(RuntimeError, match="provider close unavailable"):
        await service.restore(key, spec, checkpoint)

    assert service.session(key) is original
    assert original.closed == 0
    assert backend.restores == 0

    original.close_error = False
    restored = await service.restore(key, spec, checkpoint)

    assert original.close_attempts == 2
    assert original.closed == 1
    assert backend.restores == 1
    assert service.session(key) is restored
    await service.shutdown()


@pytest.mark.asyncio
async def test_restore_rejects_wrong_environment_owner_expiry_and_fidelity() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    spec = SandboxSpec(
        "fake",
        "environment-a",
        "/workspace/repo",
        metadata=(("mission", "11"),),
    )

    for checkpoint, error in (
        (
            CheckpointRef(
                "fake",
                "wrong-environment",
                "fake://wrong-environment",
                1,
                environment="environment-b",
                source_sandbox_id="source",
                owner_id="11",
            ),
            "environment",
        ),
        (
            CheckpointRef(
                "fake",
                "wrong-owner",
                "fake://wrong-owner",
                1,
                environment="environment-a",
                source_sandbox_id="source",
                owner_id="12",
            ),
            "owner",
        ),
        (
            CheckpointRef(
                "fake",
                "expired",
                "fake://expired",
                1,
                environment="environment-a",
                source_sandbox_id="source",
                owner_id="11",
                expires_at_ms=2,
            ),
            "expired",
        ),
        (
            CheckpointRef(
                "fake",
                "not-restorable",
                "fake://not-restorable",
                1,
                environment="environment-a",
                source_sandbox_id="source",
                owner_id="11",
                restorable=False,
            ),
            "non-restorable",
        ),
    ):
        with pytest.raises(ValueError, match=error):
            await service.restore(SandboxKey("mission:11"), spec, checkpoint)

    assert backend.restores == 0
