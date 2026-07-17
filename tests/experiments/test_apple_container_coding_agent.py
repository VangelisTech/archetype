# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the Apple Container coding-agent transport."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from archetype.experiments.apple_container_coding_agent import (
    AppleContainerSandboxClient,
    AppleContainerSandboxSpec,
)
from archetype.experiments.modal_coding_agent import CommandResult


def _spec(**overrides: Any) -> AppleContainerSandboxSpec:
    values: dict[str, Any] = {
        "repo_url": "https://github.com/example/repo.git",
        "branch": "agent/test",
        "snapshot_after_attempt": False,
        "capture_filesystem_manifests": False,
    }
    values.update(overrides)
    return AppleContainerSandboxSpec(**values)


def test_spec_validates_identity_and_has_content_addressed_image() -> None:
    codex = _spec(harness="codex")
    claude = _spec(harness="claude-code")

    assert codex.resolved_image_name.startswith("archetype-coding-agent-codex:local-")
    assert claude.resolved_image_name.startswith("archetype-coding-agent-claudecode:local-")
    assert codex.resolved_image_name != claude.resolved_image_name
    assert codex.agent_secret_env == ""
    assert codex.codex_auth_volume == "archetype-codex-auth"
    assert claude.agent_secret_env == "ANTHROPIC_API_KEY"

    with pytest.raises(ValueError, match="non-root absolute"):
        _spec(workspace="/")
    with pytest.raises(ValueError, match="environment variable"):
        _spec(codex_auth_env="not-valid")
    with pytest.raises(ValueError, match="volume name"):
        _spec(codex_auth_volume="--not-valid")


@pytest.mark.asyncio
async def test_exec_passes_secret_by_name_without_mounting_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], int | None, str | None]] = []

    async def fake_run_host(
        *args: str, timeout: int | None, stdin: str | None = None
    ) -> CommandResult:
        calls.append((args, timeout, stdin))
        return CommandResult(args, 0, "ok", "")

    monkeypatch.setenv("CODEX_API_KEY", "do-not-put-this-in-argv")
    monkeypatch.setattr(AppleContainerSandboxClient, "_run_host", staticmethod(fake_run_host))
    client = AppleContainerSandboxClient(
        _spec(codex_auth_env="CODEX_API_KEY"), "sandbox-id", "CODEX_API_KEY"
    )

    result = await client._exec(
        "codex",
        "--version",
        workdir="/workspace/repo",
        timeout=17,
        secrets=["CODEX_API_KEY"],
        env={"NO_COLOR": "1"},
    )

    assert result.stdout == "ok"
    argv = calls[0][0]
    assert argv[:4] == ("container", "exec", "--user", "agent")
    assert ("--workdir", "/workspace/repo") == argv[4:6]
    assert "CODEX_API_KEY" in argv
    assert "do-not-put-this-in-argv" not in argv
    assert not any("volume" in part or "mount" in part for part in argv)


@pytest.mark.asyncio
async def test_device_login_creates_and_mounts_named_auth_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host_calls: list[tuple[str, ...]] = []
    passthrough_calls: list[tuple[str, ...]] = []

    async def fake_preflight() -> None:
        return None

    async def fake_ensure_image(spec: AppleContainerSandboxSpec) -> None:
        del spec

    async def fake_run_host(
        *args: str, timeout: int | None, stdin: str | None = None
    ) -> CommandResult:
        del timeout, stdin
        host_calls.append(args)
        if args[:3] == ("container", "volume", "inspect"):
            return CommandResult(args, 1, "", "missing")
        return CommandResult(args, 0, "", "")

    async def fake_passthrough(*args: str) -> int:
        passthrough_calls.append(args)
        return 0

    monkeypatch.setattr(
        AppleContainerSandboxClient,
        "_require_container_runtime",
        staticmethod(fake_preflight),
    )
    monkeypatch.setattr(
        AppleContainerSandboxClient, "_ensure_image", staticmethod(fake_ensure_image)
    )
    monkeypatch.setattr(AppleContainerSandboxClient, "_run_host", staticmethod(fake_run_host))
    monkeypatch.setattr(
        AppleContainerSandboxClient,
        "_run_host_passthrough",
        staticmethod(fake_passthrough),
    )

    await AppleContainerSandboxClient.login_codex(_spec())

    assert any(call[:3] == ("container", "volume", "create") for call in host_calls)
    login = passthrough_calls[0]
    assert "archetype-codex-auth:/home/agent/.codex" in login
    assert login[-2:] == ("login", "--device-auth")


@pytest.mark.asyncio
async def test_oauth_volume_is_mounted_only_in_credential_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host_calls: list[tuple[str, ...]] = []

    async def fake_preflight() -> None:
        return None

    async def fake_ensure_image(spec: AppleContainerSandboxSpec) -> None:
        del spec

    async def fake_run_host(
        *args: str, timeout: int | None, stdin: str | None = None
    ) -> CommandResult:
        del timeout, stdin
        host_calls.append(args)
        return CommandResult(args, 0, "", "")

    async def fake_check(self: AppleContainerSandboxClient) -> None:
        return None

    async def fake_prepare(self: AppleContainerSandboxClient) -> None:
        return None

    monkeypatch.setattr(
        AppleContainerSandboxClient,
        "_require_container_runtime",
        staticmethod(fake_preflight),
    )
    monkeypatch.setattr(
        AppleContainerSandboxClient, "_ensure_image", staticmethod(fake_ensure_image)
    )
    monkeypatch.setattr(AppleContainerSandboxClient, "_run_host", staticmethod(fake_run_host))
    monkeypatch.setattr(AppleContainerSandboxClient, "_check_codex_oauth", fake_check)
    monkeypatch.setattr(AppleContainerSandboxClient, "_prepare_repository", fake_prepare)

    client = await AppleContainerSandboxClient.create(_spec())
    await client.close()

    run_calls = [call for call in host_calls if call[:2] == ("container", "run")]
    mission, broker = run_calls
    assert "--volume" not in mission
    assert "archetype-codex-auth:/home/agent/.codex" in broker
    assert client._auth_sandbox is not None
    deletes = [call[-1] for call in host_calls if call[:3] == ("container", "delete", "--force")]
    assert client.sandbox_id in deletes
    assert client._auth_sandbox in deletes


@pytest.mark.asyncio
async def test_oauth_is_staged_for_codex_then_removed_before_validators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    client = AppleContainerSandboxClient(_spec(), "sandbox-id", None)

    async def fake_stage() -> None:
        events.append("stage")

    async def fake_persist() -> None:
        events.append("remove")

    async def fake_exec(*args: str, **kwargs: Any) -> CommandResult:
        del kwargs
        events.append(args[0])
        return CommandResult(args, 0, "{}\n", "")

    monkeypatch.setattr(client, "_stage_codex_oauth", fake_stage)
    monkeypatch.setattr(client, "_persist_and_remove_codex_oauth", fake_persist)
    monkeypatch.setattr(client, "_exec", fake_exec)

    await client._run_codex("fix it", session_id="")

    assert events == ["stage", "codex", "remove"]


@pytest.mark.asyncio
async def test_snapshot_exports_the_complete_container_filesystem(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    exec_calls: list[tuple[str, ...]] = []
    host_calls: list[tuple[str, ...]] = []

    async def fake_exec(*args: str, **kwargs: Any) -> CommandResult:
        exec_calls.append(args)
        stdout = "abcdef1234567890\n" if args[:2] == ("git", "rev-parse") else ""
        return CommandResult(args, 0, stdout, "")

    async def fake_run_host(
        *args: str, timeout: int | None, stdin: str | None = None
    ) -> CommandResult:
        del timeout, stdin
        host_calls.append(args)
        return CommandResult(args, 0, "", "")

    monkeypatch.setattr(AppleContainerSandboxClient, "_run_host", staticmethod(fake_run_host))
    client = AppleContainerSandboxClient(
        _spec(
            snapshot_after_attempt=True,
            state_dir=str(tmp_path),
            codex_auth_env="CODEX_API_KEY",
        ),
        "sandbox-id",
        "CODEX_API_KEY",
    )
    monkeypatch.setattr(client, "_exec", fake_exec)

    reference = await client._snapshot_if_configured()

    assert reference.startswith(f"apple-container-rootfs://{tmp_path}")
    assert not any(call[0] == "tar" for call in exec_calls)
    stop = next(call for call in host_calls if call[:2] == ("container", "stop"))
    export = next(call for call in host_calls if call[:2] == ("container", "export"))
    start = next(call for call in host_calls if call[:2] == ("container", "start"))
    assert stop[-1] == "sandbox-id"
    assert export[2:4] == ("--output", reference.removeprefix("apple-container-rootfs://"))
    assert export[-1] == "sandbox-id"
    assert start[-1] == "sandbox-id"


@pytest.mark.asyncio
async def test_snapshot_restarts_container_when_export_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    host_calls: list[tuple[str, ...]] = []

    async def fake_exec(*args: str, **kwargs: Any) -> CommandResult:
        del kwargs
        return CommandResult(args, 0, "abcdef1234567890\n", "")

    async def fake_run_host(
        *args: str, timeout: int | None, stdin: str | None = None
    ) -> CommandResult:
        del timeout, stdin
        host_calls.append(args)
        if args[:2] == ("container", "export"):
            return CommandResult(args, 1, "", "export failed")
        return CommandResult(args, 0, "", "")

    monkeypatch.setattr(AppleContainerSandboxClient, "_run_host", staticmethod(fake_run_host))
    client = AppleContainerSandboxClient(
        _spec(snapshot_after_attempt=True, state_dir=str(tmp_path)),
        "sandbox-id",
        "CODEX_API_KEY",
    )
    monkeypatch.setattr(client, "_exec", fake_exec)

    with pytest.raises(RuntimeError, match="filesystem export"):
        await client._snapshot_if_configured("attempt-id")

    assert any(call[:2] == ("container", "start") for call in host_calls)


@pytest.mark.asyncio
async def test_restore_rehydrates_rootfs_into_content_addressed_mission_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    archive = tmp_path / "rootfs.tar"
    archive.write_bytes(b"complete exported filesystem")
    host_calls: list[tuple[str, ...]] = []
    restored_containerfile = ""

    async def fake_preflight(spec: AppleContainerSandboxSpec) -> None:
        del spec

    async def fake_ensure_image(spec: AppleContainerSandboxSpec) -> None:
        del spec

    async def fake_run_host(
        *args: str, timeout: int | None, stdin: str | None = None
    ) -> CommandResult:
        del timeout, stdin
        nonlocal restored_containerfile
        host_calls.append(args)
        if args[:3] == ("container", "image", "inspect"):
            return CommandResult(args, 1, "", "missing")
        if args[:2] == ("container", "build"):
            containerfile = Path(args[args.index("--file") + 1])
            restored_containerfile = containerfile.read_text()
            assert (containerfile.parent / "rootfs.tar").read_bytes() == archive.read_bytes()
        if args[:4] == ("container", "exec", "--user", "agent"):
            return CommandResult(args, 0, "true\n", "")
        return CommandResult(args, 0, "", "")

    monkeypatch.setenv("CODEX_API_KEY", "integration-placeholder-not-used")
    monkeypatch.setattr(
        AppleContainerSandboxClient,
        "_preflight",
        staticmethod(fake_preflight),
    )
    monkeypatch.setattr(
        AppleContainerSandboxClient,
        "_ensure_image",
        staticmethod(fake_ensure_image),
    )
    monkeypatch.setattr(AppleContainerSandboxClient, "_run_host", staticmethod(fake_run_host))

    client = await AppleContainerSandboxClient.restore(
        _spec(codex_auth_env="CODEX_API_KEY"),
        f"apple-container-rootfs://{archive}",
    )
    try:
        build = next(call for call in host_calls if call[:2] == ("container", "build"))
        restored_image = build[build.index("--tag") + 1]
        assert restored_image.startswith("archetype-coding-agent-codex:restore-")
        mission_run = next(call for call in host_calls if call[:2] == ("container", "run"))
        assert restored_image in mission_run
        assert "ADD rootfs.tar /" in restored_containerfile
        assert "USER agent" in restored_containerfile
        assert not any("--volume" in call for call in (mission_run,))
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_restore_rejects_missing_or_artifact_fragment_refs(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        await AppleContainerSandboxClient.restore(
            _spec(), f"apple-container-rootfs://{tmp_path / 'missing.tar'}"
        )
    with pytest.raises(ValueError, match="checkpoint"):
        await AppleContainerSandboxClient.restore(
            _spec(), f"apple-container-rootfs://{tmp_path / 'rootfs.tar'}#/workspace/file"
        )
