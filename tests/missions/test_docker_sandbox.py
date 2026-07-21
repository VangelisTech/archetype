# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free contracts for the Docker Sandbox reference Backend."""

from __future__ import annotations

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
)
from archetype.missions.sandboxes._image import coding_agent_environment


def _spec(config: DockerSandboxConfig) -> SandboxSpec:
    return SandboxSpec(
        provider="docker",
        environment=coding_agent_environment(),
        workdir="/workspace/repo",
    )


def _session(*, oauth: bool = False) -> DockerSandboxSession:
    config = DockerSandboxConfig(auth_volume_name="codex-auth" if oauth else "")
    return DockerSandboxSession(
        spec=_spec(config),
        config=config,
        sandbox_id="mission-container",
        auth_sandbox_id="auth-container" if oauth else "",
    )


def test_docker_backend_is_the_portable_protocol_reference() -> None:
    backend = DockerSandboxBackend()

    assert isinstance(backend, SandboxBackend)
    assert backend.environment == coding_agent_environment()
    assert _session().capabilities.checkpoints is True
    assert _session().capabilities.secret_names == ("github",)
    assert _session(oauth=True).capabilities.secret_names == ("codex_oauth", "github")


@pytest.mark.asyncio
async def test_exec_uses_no_workspace_mount_and_keeps_secret_values_out_of_argv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        calls.append(tuple(argv))
        return ProcessResult(tuple(argv), 0)

    monkeypatch.setenv("GITHUB_TOKEN", "must-not-enter-argv")
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.run_host",
        fake_run_host,
    )
    session = _session()

    await session.exec(
        ProcessRequest(
            ("git", "status"),
            workdir="/workspace/repo",
            env=(("NO_COLOR", "1"),),
            secret_names=("github",),
        )
    )

    argv = calls[0]
    assert argv[:4] == ("docker", "exec", "--user", "agent")
    assert "GITHUB_TOKEN" in argv
    assert not any("must-not-enter-argv" in value for value in argv)
    assert "--volume" not in argv


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

    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.run_host",
        fake_run_host,
    )
    session = _session()

    checkpoint = await session.checkpoint()

    assert checkpoint.provider == "docker"
    assert checkpoint.checkpoint_id == digest
    assert checkpoint.uri == f"docker-image://sha256:{digest}"
    assert any(call[:2] == ("docker", "commit") for call in calls)
    assert any(call[:3] == ("docker", "image", "inspect") for call in calls)


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

    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.run_host",
        fake_run_host,
    )

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
