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
    SandboxIdentity,
    SandboxKey,
    SandboxService,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
)


class _Session:
    def __init__(self, identity: SandboxIdentity) -> None:
        self._identity = identity
        self.closed = 0

    @property
    def identity(self) -> SandboxIdentity:
        return self._identity

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=True)

    async def status(self) -> SandboxStatus:
        return SandboxStatus.CLOSED if self.closed else SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        return ProcessResult(request.argv, 0, stdout="ok")

    async def checkpoint(self) -> CheckpointRef:
        return CheckpointRef("fake", "checkpoint", "fake://checkpoint", 1)

    async def close(self) -> None:
        self.closed += 1


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
async def test_restore_and_shutdown_own_only_live_session_lifetime() -> None:
    backend = _Backend()
    backend.release.set()
    service = SandboxService((backend,))
    checkpoint = CheckpointRef("fake", "restored", "fake://restored", 1)
    session = await service.restore(SandboxKey("mission:8"), _spec(), checkpoint)

    assert backend.restores == 1
    assert session.identity.sandbox_id == "restored"
    await service.shutdown()
    assert session.closed == 1
    await service.shutdown()
    with pytest.raises(RuntimeError, match="shutting down"):
        await service.acquire(SandboxKey("mission:9"), _spec())
