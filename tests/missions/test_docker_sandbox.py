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
    assert _session().capabilities.secret_names == ()
    assert _session(oauth=True).capabilities.secret_names == ("codex_oauth",)
    assert "util-linux" in coding_agent_containerfile()


@pytest.mark.asyncio
async def test_exec_rejects_same_sandbox_github_secret_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        calls.append(tuple(argv))
        return ProcessResult(tuple(argv), 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.run_host",
        fake_run_host,
    )
    session = _session()

    with pytest.raises(ValueError, match="unsupported Docker secret"):
        await session.exec(
            ProcessRequest(
                ("git", "status"),
                workdir="/workspace/repo",
                secret_names=("github",),
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


def test_docker_config_and_spec_validation_fail_closed() -> None:
    for kwargs, error in (
        ({"cpus": 0}, "positive"),
        ({"image_name": "bad image"}, "image_name"),
        ({"auth_volume_name": "bad volume"}, "auth_volume_name"),
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
async def test_docker_backend_create_restore_login_and_close_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    resolved_image_inspections = 0

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        nonlocal resolved_image_inspections
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            if command[-1].startswith("archetype-agent:codex-"):
                resolved_image_inspections += 1
                return ProcessResult(command, 1 if resolved_image_inspections == 1 else 0)
            return ProcessResult(command, 0)
        if command[:3] == ("docker", "volume", "inspect"):
            volume_inspections = len([c for c in calls if c[:3] == command[:3]])
            return ProcessResult(command, 1 if volume_inspections == 3 else 0)
        return ProcessResult(command, 0)

    async def fake_passthrough(argv) -> int:
        calls.append(tuple(argv))
        return 0

    async def verified(*args, **kwargs) -> None:
        del args, kwargs

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.run_host_passthrough",
        fake_passthrough,
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.verify_coding_agent_environment",
        verified,
    )
    config = DockerSandboxConfig(auth_volume_name="codex-auth")
    backend = DockerSandboxBackend(config)
    spec = _spec(config)

    created = await backend.create(spec)
    assert created.identity.provider == "docker"
    await created.close()
    await created.close()
    assert await created.status() is SandboxStatus.CLOSED

    digest = "c" * 64
    checkpoint = CheckpointRef(
        "docker",
        digest,
        f"docker-image://sha256:{digest}",
        1,
        environment=spec.environment,
        source_sandbox_id="source",
        locality=CheckpointLocality.HOST,
        integrity=f"sha256:{digest}",
    )
    restored = await backend.restore(spec, checkpoint)
    assert restored.identity.provider == "docker"
    await restored.close()

    await backend.login_codex()

    assert any(command[:2] == ("docker", "build") for command in calls)
    assert any(command[:3] == ("docker", "volume", "create") for command in calls)
    login = next(
        command for command in calls if any("--device-auth" in argument for argument in command)
    )
    narrowed = next(
        command for command in calls if any("! -name auth.json" in argument for argument in command)
    )
    for command, label in (
        (login, "archetype-oauth-login"),
        (narrowed, "archetype-oauth-narrow"),
    ):
        assert command[-2:] == (
            label,
            "/home/agent/.codex/.auth-json.persist.lock",
        )
        assert 'exec 9>>"$lock_path"' in command[-3]
        assert "flock -x 9" in command[-3]
    assert "! -name '.auth-json.persist.lock'" in narrowed[-3]


@pytest.mark.asyncio
async def test_docker_oauth_round_trip_and_session_error_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], str | None]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds
        command = tuple(argv)
        calls.append((command, stdin))
        stdout = (
            "oauth-credential" if "base64" in command and "-w" in command and "0" in command else ""
        )
        return ProcessResult(command, 0, stdout=stdout)

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)
    session = _session(oauth=True)

    await session._stage_oauth()
    await session._persist_and_remove_oauth()
    assert any(stdin == "oauth-credential" for _command, stdin in calls)
    assert not any("tar" in argument for command, _stdin in calls for argument in command)
    assert any("/home/agent/.codex/auth.json" in command for command, _stdin in calls)

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

    errored_session = _session(oauth=True)
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
async def test_docker_oauth_stage_failure_marks_errored_and_cleans_partial_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(oauth=True)
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
async def test_docker_oauth_reports_persistence_and_removal_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(oauth=True)

    async def exec_request(request: ProcessRequest) -> ProcessResult:
        if request.argv[:2] == ("rm", "-rf"):
            return ProcessResult(request.argv, 9, stderr="remove failed")
        return ProcessResult(request.argv, 0, stdout="oauth-archive")

    async def auth_exec(*arguments: str, timeout: int, stdin: str | None = None):
        del timeout, stdin
        if len(arguments) > 3 and arguments[3] == "archetype-oauth-cleanup":
            return ProcessResult(tuple(arguments), 7, stderr="staging cleanup failed")
        return ProcessResult(tuple(arguments), 8, stderr="persist failed")

    monkeypatch.setattr(session, "_exec_request", exec_request)
    monkeypatch.setattr(session, "_auth_exec", auth_exec)

    with pytest.raises(BaseExceptionGroup) as raised:
        await session._persist_and_remove_oauth()

    assert "persist refreshed" in str(raised.value.exceptions[0])
    assert "persistence staging file" in str(raised.value.exceptions[1])
    assert "remove staged" in str(raised.value.exceptions[2])


@pytest.mark.asyncio
async def test_docker_oauth_persistence_failure_reclaims_crash_orphans_under_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(oauth=True)
    lock = "/home/agent/.codex/.auth-json.persist.lock"
    owned = "/home/agent/.codex/auth.json.next"
    foreign = "/home/agent/.codex/auth.json.next.foreign-container"
    staging_files = {owned, foreign}
    cleanup_commands: list[tuple[str, ...]] = []

    async def exec_request(request: ProcessRequest) -> ProcessResult:
        return ProcessResult(request.argv, 0, stdout="oauth-credential")

    async def auth_exec(*arguments: str, timeout: int, stdin: str | None = None):
        del timeout, stdin
        command = tuple(arguments)
        if command[3] == "archetype-oauth-persist":
            persistence_script = command[2]
            assert command[3:] == ("archetype-oauth-persist", lock, owned)
            assert persistence_script.count('rm -f -- "$next_path"') == 5
            assert 'exec 9>>"$lock_path"' in persistence_script
            assert "flock -x 9" in persistence_script
            assert 'exit "$status"\' EXIT' in persistence_script
            assert "exit 129' HUP" in persistence_script
            assert "exit 130' INT" in persistence_script
            assert "exit 143' TERM" in persistence_script
            assert "-name 'auth.json.next.*'" in persistence_script
            assert "! -name '.auth-json.persist.lock'" in persistence_script
            return ProcessResult(command, 8, stderr="persist failed")
        assert command[3:] == ("archetype-oauth-cleanup", lock, owned)
        assert 'exec 9>>"$lock_path"' in command[2]
        assert "flock -x 9" in command[2]
        cleanup_commands.append(command[3:])
        staging_files.clear()
        return ProcessResult(command, 0)

    monkeypatch.setattr(session, "_exec_request", exec_request)
    monkeypatch.setattr(session, "_auth_exec", auth_exec)

    with pytest.raises(RuntimeError, match="persist refreshed"):
        await session._persist_and_remove_oauth()

    assert cleanup_commands == [("archetype-oauth-cleanup", lock, owned)]
    assert staging_files == set()


@pytest.mark.asyncio
async def test_docker_oauth_owned_staging_cleanup_finishes_after_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session(oauth=True)
    persist_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    lock = "/home/agent/.codex/.auth-json.persist.lock"
    owned = "/home/agent/.codex/auth.json.next"
    cleanup_commands: list[tuple[str, ...]] = []

    async def exec_request(request: ProcessRequest) -> ProcessResult:
        return ProcessResult(request.argv, 0, stdout="oauth-credential")

    async def auth_exec(*arguments: str, timeout: int, stdin: str | None = None):
        del timeout, stdin
        command = tuple(arguments)
        if command[3] == "archetype-oauth-persist":
            assert command[3:] == ("archetype-oauth-persist", lock, owned)
            assert command[2].count('rm -f -- "$next_path"') == 5
            assert "flock -x 9" in command[2]
            persist_started.set()
            await asyncio.Future()
        assert command[3:] == ("archetype-oauth-cleanup", lock, owned)
        assert "flock -x 9" in command[2]
        cleanup_commands.append(command[3:])
        cleanup_started.set()
        await release_cleanup.wait()
        return ProcessResult(command, 0)

    monkeypatch.setattr(session, "_exec_request", exec_request)
    monkeypatch.setattr(session, "_auth_exec", auth_exec)

    persistence = asyncio.create_task(session._persist_and_remove_oauth())
    await persist_started.wait()
    persistence.cancel()
    await cleanup_started.wait()
    persistence.cancel()
    await asyncio.sleep(0)
    assert not persistence.done()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await persistence

    assert cleanup_commands == [("archetype-oauth-cleanup", lock, owned)]


@pytest.mark.asyncio
async def test_docker_oauth_concurrent_sessions_share_one_persistence_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DockerSandboxConfig(auth_volume_name="codex-auth")
    sessions = tuple(
        DockerSandboxSession(
            spec=_spec(config),
            config=config,
            sandbox_id=f"mission-{label}",
            auth_sandbox_id=f"auth-{label}",
        )
        for label in ("a", "b")
    )
    broker_lock = asyncio.Lock()
    first_persist_started = asyncio.Event()
    second_persist_attempted = asyncio.Event()
    release_first = asyncio.Event()
    order: list[str] = []
    active_writers = 0

    async def exec_request(request: ProcessRequest) -> ProcessResult:
        return ProcessResult(request.argv, 0, stdout="oauth-credential")

    def auth_exec(label: str):
        async def run(*arguments: str, timeout: int, stdin: str | None = None):
            nonlocal active_writers
            del timeout, stdin
            command = tuple(arguments)
            kind = command[3].removeprefix("archetype-oauth-")
            assert command[4:] == (
                "/home/agent/.codex/.auth-json.persist.lock",
                "/home/agent/.codex/auth.json.next",
            )
            assert "flock -x 9" in command[2]
            if label == "b" and kind == "persist":
                second_persist_attempted.set()
            async with broker_lock:
                assert active_writers == 0
                if kind == "persist":
                    active_writers = 1
                    order.append(f"{label}:persist")
                    if label == "a":
                        first_persist_started.set()
                        await release_first.wait()
                    active_writers = 0
                else:
                    order.append(f"{label}:cleanup")
                return ProcessResult(command, 0)

        return run

    for label, session in zip(("a", "b"), sessions, strict=True):
        monkeypatch.setattr(session, "_exec_request", exec_request)
        monkeypatch.setattr(session, "_auth_exec", auth_exec(label))

    first = asyncio.create_task(sessions[0]._persist_and_remove_oauth())
    await first_persist_started.wait()
    second = asyncio.create_task(sessions[1]._persist_and_remove_oauth())
    await second_persist_attempted.wait()
    await asyncio.sleep(0)
    assert order == ["a:persist"]

    release_first.set()
    await asyncio.gather(first, second)

    assert order[:2] == ["a:persist", "b:persist"]
    assert sorted(order[2:]) == ["a:cleanup", "b:cleanup"]


@pytest.mark.asyncio
async def test_docker_close_waits_for_active_exec(monkeypatch: pytest.MonkeyPatch) -> None:
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
async def test_docker_runtime_broker_and_close_failures_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls: list[str] = []
    with pytest.raises(RuntimeError, match="not configured"):
        await _session()._auth_exec("true", timeout=1)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.shutil.which",
        lambda _name: None,
    )
    with pytest.raises(RuntimeError, match="Docker is required"):
        await DockerSandboxBackend._require_runtime()

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        close_calls.append(command[-1])
        return ProcessResult(
            command,
            0 if command[-1] == "mission-container" else 7,
            stderr="delete failed",
        )

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)
    session = _session(oauth=True)
    with pytest.raises(BaseExceptionGroup, match="failed to close"):
        await session.close()
    assert await session.status() is SandboxStatus.ERRORED

    async def successful_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        close_calls.append(tuple(argv)[-1])
        return ProcessResult(tuple(argv), 0)

    monkeypatch.setattr(
        "archetype.missions.sandboxes.docker.run_host",
        successful_run_host,
    )
    await session.close()
    assert await session.status() is SandboxStatus.CLOSED
    assert close_calls == ["mission-container", "auth-container", "auth-container"]


@pytest.mark.asyncio
async def test_docker_broker_and_compensating_remove_failures_are_combined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    async def fake_run_host(argv, *, timeout_seconds: int, stdin: str | None = None):
        del timeout_seconds, stdin
        command = tuple(argv)
        calls.append(command)
        if "archetype.kind=codex-auth-broker" in command:
            return ProcessResult(command, 7, stderr="broker launch failed")
        if command[:3] == ("docker", "rm", "--force"):
            return ProcessResult(command, 8, stderr="cleanup failed")
        return ProcessResult(command, 0)

    monkeypatch.setattr("archetype.missions.sandboxes.docker.run_host", fake_run_host)
    config = DockerSandboxConfig(auth_volume_name="codex-auth")
    backend = DockerSandboxBackend(config)

    with pytest.raises(ExceptionGroup, match="may remain live") as raised:
        await backend._launch(_spec(config), config.resolved_image_name)

    assert "broker launch failed" in str(raised.value.exceptions[0])
    assert "cleanup failed" in str(raised.value.exceptions[1])
    launched_id = calls[0][calls[0].index("--name") + 1]
    assert calls[-1] == ("docker", "rm", "--force", launched_id)
