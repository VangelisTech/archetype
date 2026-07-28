# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free contracts for the Apple Container Sandbox Backend."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest

from archetype.missions.sandboxes import (
    AppleContainerSandboxBackend,
    AppleContainerSandboxConfig,
    AppleContainerSandboxSession,
    CheckpointLocality,
    CheckpointRef,
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


def _spec(config: AppleContainerSandboxConfig) -> SandboxSpec:
    return SandboxSpec(
        provider="apple-container",
        environment=coding_agent_environment(),
        workdir="/workspace/repo",
    )


def _session(tmp_path: Path) -> AppleContainerSandboxSession:
    config = AppleContainerSandboxConfig(state_dir=str(tmp_path))
    return AppleContainerSandboxSession(
        spec=_spec(config),
        config=config,
        sandbox_id="mission-vm",
    )


def test_apple_backend_uses_the_pinned_credential_free_image_recipe() -> None:
    backend = AppleContainerSandboxBackend()
    image = coding_agent_containerfile()

    assert isinstance(backend, SandboxBackend)
    assert backend.environment == coding_agent_environment()
    assert "@openai/codex@0.144.6" in image
    assert "sha512sum --check --strict" in image
    assert "ttyd.x86_64" in image
    assert "ttyd.aarch64" in image
    assert "sha256sum --check --strict" in image
    assert "tmux" in image
    assert "util-linux" not in image
    assert f"ARCHETYPE_SANDBOX_ENVIRONMENT={backend.environment}" in image
    assert "USER agent" in image


@pytest.mark.asyncio
@pytest.mark.parametrize("secret_name", ("codex_oauth", "github"))
async def test_exec_rejects_provider_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    secret_name: str,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        calls.append(tuple(argv))
        return ProcessResult(tuple(argv), 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)

    assert session.capabilities.secret_names == ()
    with pytest.raises(ValueError, match="unsupported Apple Container secret"):
        await session.exec(
            ProcessRequest(
                ("true",),
                workdir="/workspace/repo",
                secret_names=(secret_name,),
            )
        )

    assert calls == []


@pytest.mark.asyncio
async def test_checkpoint_exports_session_rootfs_atomically_and_restarts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if command[:2] == ("container", "export"):
            output = Path(command[command.index("--output") + 1])
            output.write_bytes(b"rootfs including repository and .context")
        return ProcessResult(command, 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )

    checkpoint = await _session(tmp_path).checkpoint()

    assert checkpoint.provider == "apple-container"
    assert checkpoint.restorable is True
    archive = Path(checkpoint.uri.removeprefix("apple-container-rootfs://"))
    assert archive.read_bytes() == b"rootfs including repository and .context"
    assert not list(tmp_path.glob("*.partial"))
    assert [call[1] for call in calls] == ["exec", "stop", "export", "start"]
    assert calls[0][-4:] == ("test", "!", "-e", "/home/agent/.codex/auth.json")


@pytest.mark.asyncio
async def test_checkpoint_restarts_after_export_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        return ProcessResult(command, 7 if command[:2] == ("container", "export") else 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )

    with pytest.raises(RuntimeError, match="filesystem export"):
        await _session(tmp_path).checkpoint()

    assert any(call[:2] == ("container", "start") for call in calls)
    assert not list(tmp_path.glob("*.partial"))


@pytest.mark.asyncio
async def test_checkpoint_restart_failure_marks_session_errored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        if command[:2] == ("container", "export"):
            output = Path(command[command.index("--output") + 1])
            output.write_bytes(b"rootfs")
        return ProcessResult(
            command,
            7 if command[:2] == ("container", "start") else 0,
            stderr="restart failed",
        )

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)

    with pytest.raises(RuntimeError, match="completed checkpoint preserved at") as raised:
        await session.checkpoint()

    assert await session.status() is SandboxStatus.ERRORED
    archives = list(tmp_path.glob("*.rootfs.tar"))
    assert len(archives) == 1
    assert archives[0].read_bytes() == b"rootfs"
    assert str(archives[0]) in str(raised.value)
    with pytest.raises(RuntimeError, match="errored"):
        await session.exec(ProcessRequest(("true",)))
    with pytest.raises(RuntimeError, match="errored"):
        await session.checkpoint()


@pytest.mark.asyncio
async def test_checkpoint_discards_incomplete_export_when_restart_also_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        failed = command[:2] in {("container", "export"), ("container", "start")}
        return ProcessResult(command, 7 if failed else 0, stderr="provider failed")

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)

    with pytest.raises(BaseExceptionGroup) as raised:
        await session.checkpoint()

    assert "filesystem export" in str(raised.value.exceptions[0])
    assert "restart after checkpoint" in str(raised.value.exceptions[1])
    assert await session.status() is SandboxStatus.ERRORED
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_checkpoint_refuses_to_capture_a_staged_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        return ProcessResult(command, 1, stderr="credential canary present")

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )

    with pytest.raises(RuntimeError, match="credential-free checkpoint"):
        await _session(tmp_path).checkpoint()

    assert not any(call[:2] == ("container", "stop") for call in calls)


def test_restore_rejects_cross_provider_missing_and_fragment_refs(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="provider"):
        AppleContainerSandboxBackend._checkpoint_archive(
            CheckpointRef(
                "docker",
                "id",
                "docker-image://id",
                1,
                environment="docker-test",
                source_sandbox_id="docker-source",
            )
        )
    with pytest.raises(ValueError, match="rootfs"):
        AppleContainerSandboxBackend._checkpoint_archive(
            CheckpointRef(
                "apple-container",
                "id",
                f"apple-container-rootfs://{tmp_path / 'rootfs.tar'}#/workspace/file",
                1,
                environment="apple-test",
                source_sandbox_id="apple-source",
            )
        )
    with pytest.raises(FileNotFoundError):
        AppleContainerSandboxBackend._checkpoint_archive(
            CheckpointRef(
                "apple-container",
                "id",
                f"apple-container-rootfs://{tmp_path / 'missing.tar'}",
                1,
                environment="apple-test",
                source_sandbox_id="apple-source",
            )
        )


def test_apple_config_and_spec_validation_fail_closed() -> None:
    for kwargs, error in (
        ({"cpus": 0}, "positive"),
        ({"image_name": "bad image"}, "image_name"),
        ({"checkpoint_timeout_seconds": 0}, "checkpoint timeout"),
    ):
        with pytest.raises(ValueError, match=error):
            AppleContainerSandboxConfig(**kwargs)

    backend = AppleContainerSandboxBackend()
    with pytest.raises(ValueError, match="different provider"):
        backend._validate_spec(SandboxSpec("docker", backend.environment, "/workspace/repo"))
    with pytest.raises(ValueError, match="environment"):
        backend._validate_spec(SandboxSpec("apple-container", "wrong", "/workspace/repo"))


@pytest.mark.asyncio
async def test_create_restore_and_close_launch_only_mission_vms(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    image_inspections = 0

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        nonlocal image_inspections
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("container", "image", "inspect"):
            image_inspections += 1
            return ProcessResult(command, 1 if image_inspections in {1, 3} else 0)
        return ProcessResult(command, 0)

    async def verified(*args, **kwargs) -> None:
        del args, kwargs

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.verify_coding_agent_environment",
        verified,
    )
    config = AppleContainerSandboxConfig(state_dir=str(tmp_path))
    backend = AppleContainerSandboxBackend(config)
    spec = _spec(config)

    created = await backend.create(spec)
    assert created.identity.provider == "apple-container"
    await created.close()
    await created.close()

    archive = tmp_path / "restore.tar"
    archive.write_bytes(b"portable rootfs")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    restored = await backend.restore(
        spec,
        CheckpointRef(
            "apple-container",
            digest,
            f"apple-container-rootfs://{archive}",
            1,
            environment=spec.environment,
            source_sandbox_id="source",
            locality=CheckpointLocality.HOST,
            integrity=f"sha256:{digest}",
        ),
    )
    await restored.close()

    launches = [
        command
        for command in calls
        if command[:2] == ("container", "run") and "--detach" in command
    ]
    assert len(launches) == 2
    assert all("--volume" not in command for command in launches)
    assert all("archetype.kind=agent-mission" in command for command in launches)
    assert any(command[:2] == ("container", "build") for command in calls)
    assert not any(command[:2] == ("container", "volume") for command in calls)
    assert not any("--device-auth" in command for command in calls)


@pytest.mark.asyncio
async def test_exec_cancellation_interrupts_the_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(tmp_path)

    async def cancelled(_request: ProcessRequest) -> ProcessResult:
        raise asyncio.CancelledError

    monkeypatch.setattr(session, "_exec_request", cancelled)

    with pytest.raises(asyncio.CancelledError):
        await session.exec(ProcessRequest(("true",)))
    assert await session.status() is SandboxStatus.INTERRUPTED


@pytest.mark.asyncio
async def test_close_waits_for_active_exec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(tmp_path)
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
    assert removed == ["mission-vm"]
    assert await session.status() is SandboxStatus.CLOSED


@pytest.mark.asyncio
async def test_runtime_and_close_failures_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.shutil.which",
        lambda _name: None,
    )
    with pytest.raises(RuntimeError, match="Apple Container is required"):
        await AppleContainerSandboxBackend._require_runtime()

    calls = 0

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        nonlocal calls
        del timeout_seconds, stdin
        calls += 1
        command = tuple(argv)
        return ProcessResult(command, 7 if calls == 1 else 0, stderr="delete failed")

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)
    with pytest.raises(BaseExceptionGroup, match="failed to close"):
        await session.close()
    assert await session.status() is SandboxStatus.ERRORED

    await session.close()
    assert await session.status() is SandboxStatus.CLOSED
    assert calls == 2
