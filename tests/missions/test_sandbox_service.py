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
        self.close_failures = 0
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_release.set()
        self.status_started = asyncio.Event()
        self.status_release = asyncio.Event()
        self.status_release.set()

    @property
    def identity(self) -> SandboxIdentity:
        return self._identity

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=True)

    async def status(self) -> SandboxStatus:
        observed = self._status
        self.status_started.set()
        await self.status_release.wait()
        return observed

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
        self.close_started.set()
        await self.close_release.wait()
        if self.close_error or self.close_failures:
            self.close_failures = max(0, self.close_failures - 1)
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
        self.sessions: list[_Session] = []
        self.create_error: Exception | None = None

    async def create(self, spec: SandboxSpec) -> _Session:
        self.creates += 1
        session = _Session(SandboxIdentity(self.name, f"sandbox-{self.creates}", spec.environment))
        self.sessions.append(session)
        self.started.set()
        await self.release.wait()
        if self.create_error is not None:
            raise self.create_error
        return session

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


@pytest.mark.asyncio
async def test_new_non_ready_session_fails_once_without_creation_churn() -> None:
    backend = _Backend()
    service = SandboxService([backend])
    key = SandboxKey("mission:non-ready")

    acquiring = asyncio.create_task(service.acquire(key, _spec()))
    await backend.started.wait()
    backend.sessions[0]._status = SandboxStatus.ERRORED
    backend.release.set()

    with pytest.raises(RuntimeError, match="became non-ready: errored"):
        await asyncio.wait_for(acquiring, timeout=1)

    assert backend.creates == 1
    assert service.session(key) is backend.sessions[0]
    assert await backend.sessions[0].status() is SandboxStatus.ERRORED
    await service.shutdown()


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
async def test_failed_direct_close_retains_the_session_for_cleanup_retry() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:direct-close-retry")
    session = await service.acquire(key, _spec())
    session.close_error = True

    with pytest.raises(RuntimeError, match="provider close unavailable"):
        await service.close(key)

    assert service.session(key) is session
    assert await session.status() is SandboxStatus.ERRORED

    session.close_error = False
    await service.close(key)

    assert session.close_attempts == 2
    assert await session.status() is SandboxStatus.CLOSED
    assert service.session(key) is None


@pytest.mark.asyncio
async def test_acquire_waits_for_direct_close_then_returns_a_replacement() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:close-acquire-race")
    spec = _spec()
    original = await service.acquire(key, spec)
    original.close_release.clear()

    closing = asyncio.create_task(service.close(key))
    await original.close_started.wait()
    acquiring = asyncio.create_task(service.acquire(key, spec))
    await asyncio.sleep(0)

    assert not acquiring.done()
    assert backend.creates == 1

    original.close_release.set()
    replacement = await acquiring
    await closing

    assert replacement is not original
    assert original.close_attempts == 1
    assert await original.status() is SandboxStatus.CLOSED
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_close_linearizes_before_an_inflight_successful_acquire_returns() -> None:
    backend = _Backend()
    service = SandboxService((backend,))
    key = SandboxKey("mission:pending-acquire-close-race")
    spec = _spec()

    acquiring = asyncio.create_task(service.acquire(key, spec))
    await backend.started.wait()
    created = backend.sessions[0]
    created.close_release.clear()
    closing = asyncio.create_task(service.close(key))
    await asyncio.sleep(0)
    backend.release.set()
    await created.close_started.wait()
    await asyncio.sleep(0)

    assert not acquiring.done()
    assert not closing.done()

    created.close_release.set()
    await closing
    replacement = await acquiring

    assert replacement is not created
    assert created.close_attempts == 1
    assert await created.status() is SandboxStatus.CLOSED
    assert backend.creates == 2
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_acquire_retries_after_a_concurrent_direct_close_failure() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:failed-close-acquire-race")
    spec = _spec()
    original = await service.acquire(key, spec)
    original.close_failures = 1
    original.close_release.clear()

    closing = asyncio.create_task(service.close(key))
    await original.close_started.wait()
    acquiring = asyncio.create_task(service.acquire(key, spec))
    await asyncio.sleep(0)
    assert not acquiring.done()

    original.close_release.set()
    with pytest.raises(RuntimeError, match="provider close unavailable"):
        await closing
    replacement = await acquiring

    assert replacement is not original
    assert original.close_attempts == 2
    assert await original.status() is SandboxStatus.CLOSED
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_cancelled_close_waiter_does_not_cancel_provider_teardown() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:cancelled-close-waiter")
    spec = _spec()
    original = await service.acquire(key, spec)
    original.close_release.clear()

    closing = asyncio.create_task(service.close(key))
    await original.close_started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing

    acquiring = asyncio.create_task(service.acquire(key, spec))
    await asyncio.sleep(0)
    assert not acquiring.done()

    original.close_release.set()
    replacement = await acquiring

    assert replacement is not original
    assert original.close_attempts == 1
    assert await original.status() is SandboxStatus.CLOSED
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_close_waits_for_a_shielded_creator_after_its_waiter_is_cancelled() -> None:
    backend = _Backend()
    service = SandboxService((backend,))
    key = SandboxKey("mission:cancelled-create-waiter")
    spec = _spec()

    acquiring = asyncio.create_task(service.acquire(key, spec))
    await backend.started.wait()
    created = backend.sessions[0]
    acquiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquiring

    closing = asyncio.create_task(service.close(key))
    await asyncio.sleep(0)
    assert not closing.done()
    assert created.close_attempts == 0

    backend.release.set()
    await closing

    assert created.close_attempts == 1
    assert await created.status() is SandboxStatus.CLOSED
    assert service.session(key) is None


@pytest.mark.asyncio
async def test_cancelled_acquire_consumes_a_later_creator_failure() -> None:
    backend = _Backend()
    backend.create_error = RuntimeError("creator exploded")
    service = SandboxService((backend,))
    completed = asyncio.Event()

    acquiring = asyncio.create_task(
        service.acquire(SandboxKey("mission:abandoned-create"), _spec())
    )
    await backend.started.wait()
    pending = next(iter(service._pending.values()))[2]  # noqa: SLF001
    pending.add_done_callback(lambda _task: completed.set())

    acquiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquiring
    backend.release.set()
    await completed.wait()
    await asyncio.sleep(0)

    assert pending.done()
    assert pending._log_traceback is False  # noqa: SLF001 - exception was retrieved
    await service.shutdown()


@pytest.mark.asyncio
async def test_failed_shutdown_retains_sessions_for_cleanup_retry() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:shutdown-retry")
    session = await service.acquire(key, _spec())
    session.close_error = True

    with pytest.raises(BaseExceptionGroup, match="failed to close 1"):
        await service.shutdown()

    assert service.session(key) is session
    assert await session.status() is SandboxStatus.ERRORED

    session.close_error = False
    await service.shutdown()

    assert session.close_attempts == 2
    assert await session.status() is SandboxStatus.CLOSED
    assert service.session(key) is None


@pytest.mark.asyncio
async def test_shutdown_retains_failed_cleanup_from_an_inflight_create() -> None:
    backend = _Backend()
    service = SandboxService((backend,))
    key = SandboxKey("mission:pending-shutdown-cleanup")

    acquiring = asyncio.create_task(service.acquire(key, _spec()))
    await backend.started.wait()
    created = backend.sessions[0]
    created.close_failures = 1
    shutting_down = asyncio.create_task(service.shutdown())
    while service._accepting:  # noqa: SLF001 - deterministic concurrency contract
        await asyncio.sleep(0)
    backend.release.set()

    with pytest.raises(RuntimeError, match="shutting down"):
        await acquiring
    with pytest.raises(BaseExceptionGroup, match="failed to close 1"):
        await shutting_down

    assert created.close_attempts == 1
    assert await created.status() is SandboxStatus.ERRORED
    assert service.session(key) is None
    assert len(service._cleanup_sessions) == 1  # noqa: SLF001 - retained cleanup ownership

    await service.shutdown()

    assert created.close_attempts == 2
    assert await created.status() is SandboxStatus.CLOSED
    assert not service._cleanup_sessions  # noqa: SLF001 - successful retry releases ownership


@pytest.mark.asyncio
async def test_concurrent_direct_close_and_shutdown_close_the_session_once() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:close-shutdown-race")
    session = await service.acquire(key, _spec())
    session.close_release.clear()

    closing = asyncio.create_task(service.close(key))
    await session.close_started.wait()
    shutting_down = asyncio.create_task(service.shutdown())
    while service._accepting:  # noqa: SLF001 - deterministic concurrency contract
        await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert session.close_attempts == 1

    session.close_release.set()
    await asyncio.gather(closing, shutting_down)

    assert session.close_attempts == 1
    assert await session.status() is SandboxStatus.CLOSED
    assert service.session(key) is None


@pytest.mark.asyncio
async def test_closing_one_key_does_not_block_acquisition_for_another() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    first_key = SandboxKey("mission:independent-close")
    second_key = SandboxKey("mission:independent-acquire")
    spec = _spec()
    first = await service.acquire(first_key, spec)
    second = await service.acquire(second_key, spec)
    first.close_release.clear()

    closing = asyncio.create_task(service.close(first_key))
    await first.close_started.wait()
    acquiring = asyncio.create_task(service.acquire(second_key, spec))
    await asyncio.sleep(0)

    assert acquiring.done()
    assert await acquiring is second

    first.close_release.set()
    await closing
    await service.shutdown()


@pytest.mark.asyncio
async def test_observing_one_key_status_does_not_block_another_key() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    first_key = SandboxKey("mission:slow-status")
    second_key = SandboxKey("mission:status-independent")
    spec = _spec()
    first = await service.acquire(first_key, spec)
    second = await service.acquire(second_key, spec)
    first.status_started.clear()
    first.status_release.clear()

    blocked = asyncio.create_task(service.acquire(first_key, spec))
    await first.status_started.wait()
    second.status_started.clear()
    unrelated = asyncio.create_task(service.acquire(second_key, spec))

    try:
        await asyncio.wait_for(second.status_started.wait(), timeout=1)
        assert await unrelated is second
    finally:
        first.status_release.set()
        await asyncio.gather(blocked, unrelated, return_exceptions=True)
        await service.shutdown()


@pytest.mark.asyncio
async def test_acquire_revalidates_a_status_observation_after_concurrent_close() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:stale-status")
    spec = _spec()
    original = await service.acquire(key, spec)
    original.status_started.clear()
    original.status_release.clear()

    acquiring = asyncio.create_task(service.acquire(key, spec))
    await original.status_started.wait()
    await asyncio.wait_for(service.close(key), timeout=1)
    replacement = await service.acquire(key, spec)
    original.status_release.set()

    assert await acquiring is replacement
    assert replacement is not original
    assert await original.status() is SandboxStatus.CLOSED
    assert service.session(key) is replacement
    await service.shutdown()


@pytest.mark.asyncio
async def test_acquire_discards_stale_ready_after_concurrent_close_failure() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    key = SandboxKey("mission:stale-status-close-failure")
    spec = _spec()
    original = await service.acquire(key, spec)
    original.close_failures = 1
    original.status_started.clear()
    original.status_release.clear()

    acquiring = asyncio.create_task(service.acquire(key, spec))
    await original.status_started.wait()
    with pytest.raises(RuntimeError, match="provider close unavailable"):
        await service.close(key)
    original.status_release.set()

    replacement = await acquiring

    assert replacement is not original
    assert original.close_attempts == 2
    assert await original.status() is SandboxStatus.CLOSED
    assert service.session(key) is replacement
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
