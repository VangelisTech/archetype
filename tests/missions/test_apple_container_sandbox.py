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
        auth_sandbox_id="auth-vm",
    )


def test_apple_backend_uses_the_pinned_shared_image_recipe() -> None:
    config = AppleContainerSandboxConfig()
    backend = AppleContainerSandboxBackend(config)

    assert isinstance(backend, SandboxBackend)
    assert backend.environment == coding_agent_environment()
    assert "@openai/codex@0.144.6" in coding_agent_containerfile()
    assert "sha512sum --check --strict" in coding_agent_containerfile()
    assert f"ARCHETYPE_SANDBOX_ENVIRONMENT={backend.environment}" in coding_agent_containerfile()
    assert "USER agent" in coding_agent_containerfile()


@pytest.mark.asyncio
async def test_exec_maps_symbolic_secrets_without_putting_values_in_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    observed_env_files: list[tuple[Path, str]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        env_file = Path(command[command.index("--env-file") + 1])
        observed_env_files.append((env_file, env_file.read_text(encoding="utf-8")))
        return ProcessResult(command, 0, stdout="ok")

    monkeypatch.setenv("GITHUB_TOKEN", "must-not-enter-argv")
    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)

    result = await session.exec(
        ProcessRequest(
            ("git", "status"),
            workdir="/workspace/repo",
            env=(("NO_COLOR", "1"),),
            secret_names=("github",),
            close_stdin=True,
        )
    )

    assert result.returncode == 0
    assert session.capabilities.home_directory == "/home/agent"
    argv = calls[0]
    assert argv[:4] == ("container", "exec", "--user", "agent")
    assert "--env-file" in argv
    assert "GITHUB_TOKEN" not in argv
    assert not any("must-not-enter-argv" in argument for argument in argv)
    assert "--volume" not in argv
    assert observed_env_files[0][1] == "GITHUB_TOKEN=must-not-enter-argv\n"
    assert not observed_env_files[0][0].exists()


@pytest.mark.asyncio
async def test_oauth_is_present_only_around_the_selected_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(tmp_path)
    events: list[str] = []

    async def stage() -> None:
        events.append("stage")

    async def execute(request: ProcessRequest) -> ProcessResult:
        events.append("exec")
        return ProcessResult(request.argv, 0)

    async def persist() -> None:
        events.append("remove")

    monkeypatch.setattr(session, "_stage_oauth", stage)
    monkeypatch.setattr(session, "_exec_request", execute)
    monkeypatch.setattr(session, "_persist_and_remove_oauth", persist)

    await session.exec(ProcessRequest(("codex", "exec", "fix it"), secret_names=("codex_oauth",)))
    await session.exec(ProcessRequest(("pytest", "-q")))

    assert events == ["stage", "exec", "remove", "exec"]


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
    session = _session(tmp_path)

    checkpoint = await session.checkpoint()

    assert checkpoint.provider == "apple-container"
    assert checkpoint.restorable is True
    archive = Path(checkpoint.uri.removeprefix("apple-container-rootfs://"))
    assert archive.read_bytes() == b"rootfs including repository and .context"
    assert not list(tmp_path.glob("*.partial"))
    assert [call[1] for call in calls] == ["exec", "stop", "export", "start"]


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
    session = _session(tmp_path)

    with pytest.raises(RuntimeError, match="filesystem export"):
        await session.checkpoint()

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
    assert not list(tmp_path.glob("*.partial"))
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
        ({"github_token_env": "lowercase"}, "environment variable"),
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
async def test_apple_backend_create_restore_login_and_close_lifecycle(
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
        if command[:3] == ("container", "volume", "inspect"):
            volume_inspections = len([c for c in calls if c[:3] == command[:3]])
            return ProcessResult(command, 1 if volume_inspections == 3 else 0)
        return ProcessResult(command, 0)

    async def fake_passthrough(argv) -> int:
        calls.append(tuple(argv))
        return 0

    async def verified(*args, **kwargs) -> None:
        del args, kwargs

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host_passthrough",
        fake_passthrough,
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
    assert await created.status() is SandboxStatus.CLOSED

    archive = tmp_path / "restore.tar"
    archive.write_bytes(b"portable rootfs")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    checkpoint = CheckpointRef(
        "apple-container",
        digest,
        f"apple-container-rootfs://{archive}",
        1,
        environment=spec.environment,
        source_sandbox_id="source",
        locality=CheckpointLocality.HOST,
        integrity=f"sha256:{digest}",
    )
    restored = await backend.restore(spec, checkpoint)
    assert restored.identity.provider == "apple-container"
    await restored.close()

    await backend.login_codex()

    assert any(command[:2] == ("container", "build") for command in calls)
    assert any(command[:3] == ("container", "volume", "create") for command in calls)
    assert any("--device-auth" in command for command in calls)


@pytest.mark.asyncio
async def test_apple_oauth_round_trip_and_session_error_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], str | None]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds
        command = tuple(argv)
        calls.append((command, stdin))
        stdout = "oauth-archive" if any("tar -C" in value for value in command) else ""
        return ProcessResult(command, 0, stdout=stdout)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)

    await session._stage_oauth()
    await session._persist_and_remove_oauth()
    assert any(stdin == "oauth-archive" for _command, stdin in calls)

    with pytest.raises(ValueError, match="unsupported"):
        await session.exec(ProcessRequest(("true",), secret_names=("unknown",)))

    async def cancelled(_request: ProcessRequest) -> ProcessResult:
        raise asyncio.CancelledError

    monkeypatch.setattr(session, "_exec_request", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await session.exec(ProcessRequest(("true",)))
    assert await session.status() is SandboxStatus.INTERRUPTED

    executed = False

    async def should_not_execute(_request: ProcessRequest) -> ProcessResult:
        nonlocal executed
        executed = True
        return ProcessResult(("true",), 0)

    monkeypatch.setattr(session, "_exec_request", should_not_execute)
    with pytest.raises(RuntimeError, match="interrupted"):
        await session.exec(ProcessRequest(("true",)))
    with pytest.raises(RuntimeError, match="interrupted"):
        await session.checkpoint()
    assert executed is False

    async def errored(_request: ProcessRequest) -> ProcessResult:
        raise RuntimeError("provider exec failed")

    errored_session = _session(tmp_path)
    monkeypatch.setattr(errored_session, "_exec_request", errored)
    with pytest.raises(RuntimeError, match="provider exec failed"):
        await errored_session.exec(ProcessRequest(("true",)))
    assert await errored_session.status() is SandboxStatus.ERRORED
    with pytest.raises(RuntimeError, match="errored"):
        await errored_session.exec(ProcessRequest(("true",)))
    with pytest.raises(RuntimeError, match="errored"):
        await errored_session.checkpoint()

    monkeypatch.setattr(errored_session, "_exec_request", should_not_execute)
    with pytest.raises(RuntimeError, match="errored"):
        await errored_session.exec(ProcessRequest(("true",)))
    with pytest.raises(RuntimeError, match="errored"):
        await errored_session.checkpoint()
    assert executed is False

    errored_session._status = SandboxStatus.CLOSED
    with pytest.raises(RuntimeError, match="closed"):
        await errored_session.exec(ProcessRequest(("true",)))
    with pytest.raises(RuntimeError, match="closed"):
        await errored_session.checkpoint()


@pytest.mark.asyncio
async def test_apple_oauth_stage_failure_marks_errored_and_cleans_partial_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(tmp_path)
    removed = 0
    executed = False

    async def fail_stage() -> None:
        raise RuntimeError("stage failed")

    async def remove() -> None:
        nonlocal removed
        removed += 1

    async def execute(_request: ProcessRequest) -> ProcessResult:
        nonlocal executed
        executed = True
        return ProcessResult(("true",), 0)

    monkeypatch.setattr(session, "_stage_oauth", fail_stage)
    monkeypatch.setattr(session, "_remove_oauth", remove)
    monkeypatch.setattr(session, "_exec_request", execute)

    with pytest.raises(RuntimeError, match="stage failed"):
        await session.exec(ProcessRequest(("true",), secret_names=("codex_oauth",)))

    assert await session.status() is SandboxStatus.ERRORED
    assert removed == 1
    assert executed is False


@pytest.mark.asyncio
async def test_apple_oauth_reports_persistence_and_removal_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(tmp_path)

    async def exec_request(request: ProcessRequest) -> ProcessResult:
        if request.argv[:2] == ("rm", "-rf"):
            return ProcessResult(request.argv, 9, stderr="remove failed")
        return ProcessResult(request.argv, 0, stdout="oauth-archive")

    async def auth_exec(*arguments: str, timeout: int, stdin: str | None = None):
        del timeout, stdin
        return ProcessResult(tuple(arguments), 8, stderr="persist failed")

    monkeypatch.setattr(session, "_exec_request", exec_request)
    monkeypatch.setattr(session, "_auth_exec", auth_exec)

    with pytest.raises(BaseExceptionGroup) as raised:
        await session._persist_and_remove_oauth()

    assert "persist refreshed" in str(raised.value.exceptions[0])
    assert "remove staged" in str(raised.value.exceptions[1])


@pytest.mark.asyncio
async def test_apple_close_waits_for_active_exec(
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
    assert removed == ["mission-vm", "auth-vm"]
    assert await session.status() is SandboxStatus.CLOSED


@pytest.mark.asyncio
async def test_apple_runtime_and_close_failures_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls: list[str] = []
    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.shutil.which",
        lambda _name: None,
    )
    with pytest.raises(RuntimeError, match="Apple Container is required"):
        await AppleContainerSandboxBackend._require_runtime()

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        close_calls.append(command[-1])
        return ProcessResult(
            command,
            0 if command[-1] == "mission-vm" else 7,
            stderr="delete failed",
        )

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    session = _session(tmp_path)
    with pytest.raises(BaseExceptionGroup, match="failed to close"):
        await session.close()
    assert await session.status() is SandboxStatus.ERRORED

    async def successful_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        close_calls.append(tuple(argv)[-1])
        return ProcessResult(tuple(argv), 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        successful_run_host,
    )
    await session.close()
    assert await session.status() is SandboxStatus.CLOSED
    assert close_calls == ["mission-vm", "auth-vm", "auth-vm"]


@pytest.mark.asyncio
async def test_apple_broker_and_compensating_delete_failures_are_combined(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if "archetype.kind=codex-auth-broker" in command:
            return ProcessResult(command, 7, stderr="broker launch failed")
        if command[:3] == ("container", "delete", "--force"):
            return ProcessResult(command, 8, stderr="cleanup failed")
        return ProcessResult(command, 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.apple_container.run_host",
        fake_run_host,
    )
    config = AppleContainerSandboxConfig(state_dir=str(tmp_path))
    backend = AppleContainerSandboxBackend(config)

    with pytest.raises(ExceptionGroup, match="may remain live") as raised:
        await backend._launch(_spec(config), config.resolved_image_name)

    assert "broker launch failed" in str(raised.value.exceptions[0])
    assert "cleanup failed" in str(raised.value.exceptions[1])
    launched_id = calls[0][calls[0].index("--name") + 1]
    assert calls[-1] == ("container", "delete", "--force", launched_id)
