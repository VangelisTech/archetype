# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free contracts for the Apple Container Sandbox Backend."""

from __future__ import annotations

from pathlib import Path

import pytest

from archetype.missions.sandboxes import (
    AppleContainerSandboxBackend,
    AppleContainerSandboxConfig,
    AppleContainerSandboxSession,
    ProcessRequest,
    ProcessResult,
    SandboxBackend,
    SandboxSpec,
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

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        calls.append(tuple(argv))
        return ProcessResult(tuple(argv), 0, stdout="ok")

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
    assert "GITHUB_TOKEN" in argv
    assert not any("must-not-enter-argv" in argument for argument in argv)
    assert "--volume" not in argv


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
    from archetype.missions.sandboxes import CheckpointRef

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
