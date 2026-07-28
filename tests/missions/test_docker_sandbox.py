# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free contracts for the Docker Sandbox reference Backend."""

from __future__ import annotations

import asyncio

import pytest

from archetype.missions.sandboxes import (
    CheckpointLocality,
    CheckpointRef,
    DockerSandboxBackend,
    DockerSandboxConfig,
    DockerSandboxSession,
    ProcessRequest,
    ProcessResult,
    SandboxBackend,
    SandboxSpec,
    SandboxStatus,
)
from archetype.missions.sandboxes._image import (
    coding_agent_containerfile,
    coding_agent_environment,
)


def _spec(config: DockerSandboxConfig) -> SandboxSpec:
    return SandboxSpec(
        provider="docker",
        environment=coding_agent_environment(),
        workdir="/workspace/repo",
    )


def _session() -> DockerSandboxSession:
    config = DockerSandboxConfig()
    return DockerSandboxSession(
        spec=_spec(config),
        config=config,
        sandbox_id="mission-container",
    )


def test_docker_backend_is_a_credential_free_protocol_reference() -> None:
    backend = DockerSandboxBackend()

    assert isinstance(backend, SandboxBackend)
    assert backend.environment == coding_agent_environment()
    assert _session().capabilities.checkpoints is True
    assert _session().capabilities.secret_names == ()
    assert "util-linux" not in coding_agent_containerfile()


@pytest.mark.asyncio
@pytest.mark.parametrize("secret_name", ("codex_oauth", "github"))
async def test_exec_rejects_provider_secrets(
    monkeypatch: pytest.MonkeyPatch,
    secret_name: str,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        calls.append(tuple(argv))
        return ProcessResult(tuple(argv), 0)

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)

    with pytest.raises(ValueError, match="unsupported Docker secret"):
        await _session().exec(
            ProcessRequest(
                ("true",),
                workdir="/workspace/repo",
                secret_names=(secret_name,),
            )
        )

    assert calls == []


@pytest.mark.asyncio
async def test_checkpoint_uses_docker_commit_and_returns_immutable_image_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    digest = "a" * 64

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        stdout = f"sha256:{digest}\n" if command[:3] == ("docker", "image", "inspect") else ""
        return ProcessResult(command, 0, stdout=stdout)

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)

    checkpoint = await _session().checkpoint()

    assert checkpoint.provider == "docker"
    assert checkpoint.checkpoint_id == digest
    assert checkpoint.uri == f"docker-image://sha256:{digest}"
    assert any(call[:2] == ("docker", "commit") for call in calls)
    assert any(call[:3] == ("docker", "image", "inspect") for call in calls)
    assert calls[0][-4:] == ("test", "!", "-e", "/home/agent/.codex/auth.json")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("inspect_result", "error"),
    (
        (ProcessResult(("inspect",), 7, stderr="inspect failed"), "checkpoint inspect"),
        (ProcessResult(("inspect",), 0, stdout="floating-tag"), "invalid image ID"),
    ),
)
async def test_checkpoint_removes_committed_tag_when_inspection_fails(
    monkeypatch: pytest.MonkeyPatch,
    inspect_result: ProcessResult,
    error: str,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return ProcessResult(
                command,
                inspect_result.returncode,
                stdout=inspect_result.stdout,
                stderr=inspect_result.stderr,
            )
        return ProcessResult(command, 0)

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)

    with pytest.raises(RuntimeError, match=error):
        await _session().checkpoint()

    tag = next(call[-1] for call in calls if call[:2] == ("docker", "commit"))
    assert calls[-1] == ("docker", "rmi", "--force", tag)


@pytest.mark.asyncio
async def test_checkpoint_refuses_to_commit_a_staged_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        return ProcessResult(command, 1, stderr="credential canary present")

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)

    with pytest.raises(RuntimeError, match="credential-free checkpoint"):
        await _session().checkpoint()

    assert not any(call[:2] == ("docker", "commit") for call in calls)


def test_restore_accepts_only_an_immutable_same_provider_image() -> None:
    digest = "b" * 64
    checkpoint = CheckpointRef(
        "docker",
        digest,
        f"docker-image://sha256:{digest}",
        1,
        environment="docker-test",
        source_sandbox_id="docker-source",
        locality=CheckpointLocality.HOST,
        integrity=f"sha256:{digest}",
    )

    assert DockerSandboxBackend._checkpoint_image(checkpoint) == f"sha256:{digest}"
    with pytest.raises(ValueError, match="provider"):
        DockerSandboxBackend._checkpoint_image(
            CheckpointRef(
                "modal",
                "id",
                "modal-image://im-id",
                1,
                environment="modal-test",
                source_sandbox_id="modal-source",
            )
        )
    with pytest.raises(ValueError, match="image"):
        DockerSandboxBackend._checkpoint_image(
            CheckpointRef(
                "docker",
                "tag",
                "docker-image://archetype:latest",
                1,
                environment="docker-test",
                source_sandbox_id="docker-source",
                locality=CheckpointLocality.HOST,
            )
        )
    with pytest.raises(ValueError, match="image"):
        DockerSandboxBackend._checkpoint_image(
            CheckpointRef(
                "docker",
                digest,
                f"docker-image://sha256:{digest}#/workspace/file",
                1,
                environment="docker-test",
                source_sandbox_id="docker-source",
                locality=CheckpointLocality.HOST,
            )
        )


def test_docker_config_and_spec_validation_fail_closed() -> None:
    for kwargs, error in (
        ({"cpus": 0}, "positive"),
        ({"image_name": "bad image"}, "image_name"),
    ):
        with pytest.raises(ValueError, match=error):
            DockerSandboxConfig(**kwargs)

    backend = DockerSandboxBackend()
    with pytest.raises(ValueError, match="different provider"):
        backend._validate_spec(
            SandboxSpec("apple-container", backend.environment, "/workspace/repo")
        )
    with pytest.raises(ValueError, match="environment"):
        backend._validate_spec(SandboxSpec("docker", "wrong", "/workspace/repo"))


@pytest.mark.asyncio
async def test_create_restore_and_close_launch_only_mission_containers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    resolved_image_inspections = 0

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        nonlocal resolved_image_inspections
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("docker", "image", "inspect") and command[-1].startswith(
            "archetype-agent:codex-"
        ):
            resolved_image_inspections += 1
            return ProcessResult(command, 1 if resolved_image_inspections == 1 else 0)
        return ProcessResult(command, 0)

    async def verified(*args, **kwargs) -> None:
        del args, kwargs

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.verify_coding_agent_environment",
        verified,
    )
    config = DockerSandboxConfig()
    backend = DockerSandboxBackend(config)
    spec = _spec(config)

    created = await backend.create(spec)
    assert created.identity.provider == "docker"
    await created.close()
    await created.close()

    digest = "c" * 64
    restored = await backend.restore(
        spec,
        CheckpointRef(
            "docker",
            digest,
            f"docker-image://sha256:{digest}",
            1,
            environment=spec.environment,
            source_sandbox_id="source",
            locality=CheckpointLocality.HOST,
            integrity=f"sha256:{digest}",
        ),
    )
    await restored.close()

    launches = [
        command for command in calls if command[:2] == ("docker", "run") and "--detach" in command
    ]
    assert len(launches) == 2
    assert all("--volume" not in command for command in launches)
    assert all("archetype.kind=agent-mission" in command for command in launches)
    assert any(command[:2] == ("docker", "build") for command in calls)
    assert not any(command[:2] == ("docker", "volume") for command in calls)
    assert not any("--device-auth" in command for command in calls)


@pytest.mark.asyncio
async def test_exec_cancellation_interrupts_the_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session()

    async def cancelled(_request: ProcessRequest) -> ProcessResult:
        raise asyncio.CancelledError

    monkeypatch.setattr(session, "_exec_request", cancelled)

    with pytest.raises(asyncio.CancelledError):
        await session.exec(ProcessRequest(("true",)))
    assert await session.status() is SandboxStatus.INTERRUPTED


@pytest.mark.asyncio
async def test_close_waits_for_active_exec(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _session()
    started = asyncio.Event()
    release = asyncio.Event()
    removed: list[str] = []

    async def blocked_exec(request: ProcessRequest) -> ProcessResult:
        started.set()
        await release.wait()
        return ProcessResult(request.argv, 0)

    async def host(*arguments: str, timeout: int) -> ProcessResult:
        del timeout
        removed.append(arguments[-1])
        return ProcessResult(arguments, 0)

    monkeypatch.setattr(session, "_exec_request", blocked_exec)
    monkeypatch.setattr(session, "_host", host)

    exec_task = asyncio.create_task(session.exec(ProcessRequest(("true",))))
    await started.wait()
    close_task = asyncio.create_task(session.close())
    await asyncio.sleep(0)
    assert not close_task.done()
    assert removed == []

    release.set()
    await exec_task
    await close_task
    assert removed == ["mission-container"]
    assert await session.status() is SandboxStatus.CLOSED


@pytest.mark.asyncio
async def test_runtime_and_close_failures_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.shutil.which",
        lambda _name: None,
    )
    with pytest.raises(RuntimeError, match="Docker is required"):
        await DockerSandboxBackend._require_runtime()

    calls = 0

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        nonlocal calls
        del timeout_seconds, stdin
        calls += 1
        command = tuple(argv)
        return ProcessResult(command, 7 if calls == 1 else 0, stderr="delete failed")

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)
    session = _session()
    with pytest.raises(BaseExceptionGroup, match="failed to close"):
        await session.close()
    assert await session.status() is SandboxStatus.ERRORED

    await session.close()
    assert await session.status() is SandboxStatus.CLOSED
    assert calls == 2
