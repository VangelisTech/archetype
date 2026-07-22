# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused transport contracts for the Modal Sandbox Backend and Session."""

from __future__ import annotations

import asyncio
import io
import json
import sys

import pytest

from archetype.missions.sandboxes import (
    CheckpointLocality,
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxBackend,
    SandboxSpec,
    SandboxStatus,
)
from archetype.missions.sandboxes.modal import (
    ModalSandboxBackend,
    ModalSandboxConfig,
    ModalSandboxSession,
)


class _AsyncMethod:
    def __init__(self, result=None) -> None:
        self.result = result

    async def aio(self, *args, **kwargs):
        return self.result


class _Input:
    def __init__(self) -> None:
        self.eof = False
        self.drain = _AsyncMethod()

    def write_eof(self) -> None:
        self.eof = True


class _Process:
    def __init__(self) -> None:
        self.stdin = _Input()
        self.stdout = type("Output", (), {"read": _AsyncMethod("out")})()
        self.stderr = type("Output", (), {"read": _AsyncMethod("err")})()
        self.wait = _AsyncMethod(0)


class _Sandbox:
    def __init__(self) -> None:
        self.object_id = "sb-test"
        self.process = _Process()
        self.exec = _AsyncMethod(self.process)
        self.snapshot_filesystem = _AsyncMethod(type("Image", (), {"object_id": "im-checkpoint"})())


@pytest.mark.asyncio
async def test_codex_exec_closes_modal_stdin_before_waiting() -> None:
    sandbox = _Sandbox()

    result = await ModalSandboxSession._exec_on(
        sandbox,
        ProcessRequest(
            ("codex", "exec"),
            timeout_seconds=30,
            close_stdin=True,
        ),
    )

    assert sandbox.process.stdin.eof is True
    assert result.returncode == 0
    assert result.stdout == "out"


def test_modal_backend_has_no_task_outcome_or_commit_without_push_mode() -> None:
    backend = ModalSandboxBackend()
    assert isinstance(backend, SandboxBackend)
    assert backend.environment.startswith("modal-agent://sha256:")
    assert ModalSandboxBackend(ModalSandboxConfig(image_id="im-reviewed")).environment == (
        "modal-image://im-reviewed"
    )
    assert "push" not in ModalSandboxConfig.__dataclass_fields__
    with pytest.raises(ValueError, match="GitHub secret"):
        ModalSandboxConfig(github_secret_name="")
    with pytest.raises(ValueError, match="login timeout"):
        ModalSandboxConfig(login_timeout_seconds=0)
    with pytest.raises(ValueError, match="immutable"):
        ModalSandboxConfig(image_id="floating-name")


def test_modal_agent_process_is_wrapped_in_durable_live_output_files() -> None:
    session = ModalSandboxSession(
        spec=SandboxSpec("modal", "modal-codex-test", "/workspace/repo"),
        sandbox=type("Sandbox", (), {"object_id": "sb-live"})(),
        auth_sandbox=object(),
        github_secret=object(),
        auth_volume_name="codex-auth",
        checkpoint_timeout_seconds=120,
        checkpoint_ttl_seconds=3600,
        heartbeat_seconds=15,
    )

    traced = session._trace_request(  # noqa: SLF001 - focused transport contract
        ProcessRequest(
            ("codex", "exec", "fix it"),
            workdir="/workspace/repo",
            env=(("CODEX_HOME", "/root/.codex"),),
            secret_names=("codex_oauth",),
            close_stdin=True,
        )
    )

    assert traced.argv[:5] == ("bash", "-o", "pipefail", "-c", traced.argv[4])
    assert traced.argv[-3:] == ("codex", "exec", "fix it")
    assert any(
        value == "/tmp/archetype-agent-missions/live/agent.stdout.log" for value in traced.argv
    )
    assert any(value.endswith("/live/agent.stderr.log") for value in traced.argv)
    assert not any("/workspace/repo/.context" in value for value in traced.argv)
    assert traced.workdir == "/workspace/repo"
    assert traced.close_stdin is True
    assert traced.env == (("CODEX_HOME", "/root/.codex"),)
    assert traced.secret_names == ()


@pytest.mark.asyncio
async def test_modal_oauth_staging_removes_agent_controlled_path_before_write() -> None:
    payload = '{"access_token":"credential-canary"}'

    class _WriteText:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        async def aio(self, value: str, path: str) -> None:
            self.calls.append((value, path))

    write_text = _WriteText()
    sandbox = type(
        "Sandbox",
        (),
        {
            "object_id": "sb-oauth",
            "filesystem": type("Filesystem", (), {"write_text": write_text})(),
        },
    )()
    auth = type(
        "AuthSandbox",
        (),
        {
            "filesystem": type(
                "Filesystem",
                (),
                {"read_text": _AsyncMethod(payload)},
            )()
        },
    )()
    session = ModalSandboxSession(
        spec=SandboxSpec("modal", "modal-codex-test", "/workspace/repo"),
        sandbox=sandbox,
        auth_sandbox=auth,
        github_secret=object(),
        auth_volume_name="codex-auth",
        checkpoint_timeout_seconds=120,
        checkpoint_ttl_seconds=3600,
        heartbeat_seconds=15,
    )
    commands: list[ProcessRequest] = []

    async def checked(request: ProcessRequest) -> ProcessResult:
        commands.append(request)
        return ProcessResult(request.argv, 0)

    session._checked = checked  # type: ignore[method-assign]

    await session._stage_oauth()  # noqa: SLF001 - credential boundary contract

    assert commands[0].argv == (
        "sh",
        "-c",
        "rm -rf /root/.codex && install -d -m 700 /root/.codex",
    )
    assert not any("credential-canary" in argument for argument in commands[0].argv)
    assert write_text.calls == [(payload, "/root/.codex/auth.json")]


@pytest.mark.asyncio
async def test_modal_checkpoint_is_credential_free_and_restorable() -> None:
    sandbox = _Sandbox()
    session = ModalSandboxSession(
        spec=SandboxSpec("modal", "modal-codex-test", "/workspace/repo"),
        sandbox=sandbox,
        auth_sandbox=object(),
        github_secret=object(),
        auth_volume_name="codex-auth",
        checkpoint_timeout_seconds=120,
        checkpoint_ttl_seconds=3600,
        heartbeat_seconds=15,
    )

    async def ignore_event(*args, **kwargs) -> None:
        del args, kwargs

    session._emit_event = ignore_event  # type: ignore[method-assign]

    checkpoint = await session.checkpoint()

    assert checkpoint.provider == "modal"
    assert checkpoint.checkpoint_id == "im-checkpoint"
    assert checkpoint.uri == "modal-image://im-checkpoint"
    assert session.capabilities.checkpoints is True


def test_modal_restore_accepts_only_exact_same_provider_image_refs() -> None:
    checkpoint = CheckpointRef(
        "modal",
        "im-checkpoint",
        "modal-image://im-checkpoint",
        1,
        environment="modal-codex-test",
        source_sandbox_id="sb-source",
    )

    assert ModalSandboxBackend._checkpoint_image(checkpoint) == "im-checkpoint"
    with pytest.raises(ValueError, match="provider"):
        ModalSandboxBackend._checkpoint_image(
            CheckpointRef(
                "docker",
                "id",
                "docker-image://sha256:id",
                1,
                environment="docker-test",
                source_sandbox_id="docker-source",
            )
        )
    with pytest.raises(ValueError, match="image"):
        ModalSandboxBackend._checkpoint_image(
            CheckpointRef(
                "modal",
                "im-checkpoint",
                "modal-image://im-checkpoint#/workspace/file",
                1,
                environment="modal-codex-test",
                source_sandbox_id="sb-source",
            )
        )


def test_default_modal_image_installs_the_exact_codex_inventory_pin() -> None:
    class _Builder:
        def __init__(self) -> None:
            self.commands: tuple[str, ...] = ()

        def apt_install(self, *packages: str):
            assert "nodejs" in packages and "npm" in packages
            return self

        def run_commands(self, *commands: str):
            self.commands = commands
            return self

        def env(self, values: dict[str, str]):
            assert values["ARCHETYPE_SANDBOX_ENVIRONMENT"].startswith("modal-agent://sha256:")
            return self

    builder = _Builder()
    from archetype.missions.sandboxes._image import BASE_IMAGE_REF

    modal = type(
        "Modal",
        (),
        {
            "Image": type(
                "Image",
                (),
                {
                    "from_registry": lambda reference: (
                        builder if reference == BASE_IMAGE_REF else None
                    )
                },
            )
        },
    )

    image = ModalSandboxBackend._default_image(modal)

    assert image is builder
    assert any("@openai/codex@0.144.6" in command for command in builder.commands)
    assert any("sha512sum --check --strict" in command for command in builder.commands)
    assert not any("chatgpt.com/codex/install.sh" in command for command in builder.commands)


@pytest.mark.asyncio
async def test_modal_monitor_reads_durable_status_and_output_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = ModalSandboxSession.live_observation_paths()
    values = {
        paths["status"]: json.dumps({"type": "process_finished", "returncode": 0}),
        paths["events"]: '{"type":"process_started"}\n{"type":"process_finished"}\n',
        paths["stdout"]: "agent output\n",
        paths["stderr"]: "agent warning\n",
    }

    class _Filesystem:
        class read_text:
            @staticmethod
            async def aio(path: str) -> str:
                return values[path]

    sandbox = type(
        "Sandbox",
        (),
        {"filesystem": _Filesystem(), "poll": _AsyncMethod(None)},
    )()
    from_id = _AsyncMethod(sandbox)
    fake_modal = type(
        "Modal",
        (),
        {"Sandbox": type("Sandbox", (), {"from_id": from_id})},
    )
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    status = await ModalSandboxSession.monitor(
        "sb-live",
        follow=False,
        stdout_target=sys.stdout,
        stderr_target=sys.stderr,
    )

    captured = capsys.readouterr()
    assert status == {"type": "process_finished", "returncode": 0}
    assert "process_started" in captured.out
    assert "agent output" in captured.out
    assert "agent warning" in captured.err


def test_modal_config_validation_and_checkpoint_locality_fail_closed() -> None:
    for kwargs, error in (
        ({"app_name": ""}, "app_name"),
        ({"auth_volume_name": ""}, "auth volume"),
        ({"checkpoint_timeout_seconds": 0}, "checkpoint timeout"),
        ({"checkpoint_ttl_seconds": 0}, "checkpoint TTL"),
        ({"heartbeat_seconds": 0}, "heartbeat"),
    ):
        with pytest.raises(ValueError, match=error):
            ModalSandboxConfig(**kwargs)

    with pytest.raises(ValueError, match="locality"):
        ModalSandboxBackend._checkpoint_image(
            CheckpointRef(
                "modal",
                "im-checkpoint",
                "modal-image://im-checkpoint",
                1,
                environment="modal-codex-test",
                source_sandbox_id="sb-source",
                locality=CheckpointLocality.HOST,
            )
        )


class _MemoryRead:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    async def aio(self, path: str) -> str:
        if path not in self.values:
            raise FileNotFoundError(path)
        return self.values[path]


class _MemoryWrite:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    async def aio(self, value: str, path: str) -> None:
        self.values[path] = value


class _MemoryFilesystem:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values or {}
        self.read_text = _MemoryRead(self.values)
        self.write_text = _MemoryWrite(self.values)


class _LifecycleSandbox:
    def __init__(self, sandbox_id: str, *, snapshot_id: str = "im-checkpoint") -> None:
        self.object_id = sandbox_id
        self.filesystem = _MemoryFilesystem()
        self.snapshot_filesystem = _AsyncMethod(type("Image", (), {"object_id": snapshot_id})())
        self.terminate = _AsyncMethod()
        self.detach = _AsyncMethod()
        self.commands: list[tuple[str, ...]] = []

    @property
    def exec(self):
        owner = self

        class _Exec:
            @staticmethod
            async def aio(*argv, **kwargs):
                del kwargs
                owner.commands.append(tuple(argv))
                if argv[:2] in {("rm", "-f"), ("rm", "-rf")}:
                    for target in (str(value) for value in argv[2:]):
                        for path in tuple(owner.filesystem.values):
                            if path == target or path.startswith(f"{target}/"):
                                owner.filesystem.values.pop(path)
                elif argv[:2] == ("mv", "-f"):
                    value = owner.filesystem.values.pop(str(argv[2]))
                    owner.filesystem.values[str(argv[3])] = value
                return _Process()

        return _Exec()


def _lifecycle_session(
    sandbox: _LifecycleSandbox,
    auth: _LifecycleSandbox | None = None,
) -> ModalSandboxSession:
    return ModalSandboxSession(
        spec=SandboxSpec(
            "modal",
            "modal-codex-test",
            "/workspace/repo",
            metadata=(("mission", "mission-1"),),
        ),
        sandbox=sandbox,
        auth_sandbox=auth or _LifecycleSandbox("sb-auth"),
        github_secret=object(),
        auth_volume_name="codex-auth",
        checkpoint_timeout_seconds=120,
        checkpoint_ttl_seconds=3600,
        heartbeat_seconds=3600,
    )


@pytest.mark.asyncio
async def test_modal_agent_exec_persists_oauth_and_durable_events() -> None:
    payload = '{"access_token":"credential-canary"}'
    sandbox = _LifecycleSandbox("sb-agent")
    auth = _LifecycleSandbox("sb-auth")
    auth.filesystem.values["/auth/auth.json"] = payload
    session = _lifecycle_session(sandbox, auth)

    result = await session.exec(
        ProcessRequest(
            ("codex", "exec", "fix it"),
            workdir="/workspace/repo",
            secret_names=("codex_oauth", "github"),
            close_stdin=True,
        )
    )

    assert result.returncode == 0
    assert result.argv == ("codex", "exec", "fix it")
    assert await session.status() is SandboxStatus.READY
    events = sandbox.filesystem.values["/tmp/archetype-agent-missions/live/events.jsonl"]
    assert "process_started" in events and "process_finished" in events
    assert "/root/.codex/auth.json" not in sandbox.filesystem.values
    assert auth.filesystem.values["/auth/auth.json"] == payload
    assert any(command[0] == "bash" for command in sandbox.commands)


@pytest.mark.asyncio
async def test_modal_agent_outcome_survives_live_observation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _lifecycle_session(_LifecycleSandbox("sb-agent"))
    executed = 0
    executed_argv: tuple[str, ...] = ()

    async def directory_failure() -> None:
        raise RuntimeError("live directory unavailable")

    async def event_failure(*args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("live event unavailable")

    async def execute(_sandbox, request: ProcessRequest, **kwargs) -> ProcessResult:
        nonlocal executed, executed_argv
        del _sandbox, kwargs
        executed += 1
        executed_argv = request.argv
        return ProcessResult(("codex", "exec"), 0, stdout="completed")

    monkeypatch.setattr(session, "_ensure_live_directory", directory_failure)
    monkeypatch.setattr(session, "_emit_event", event_failure)
    monkeypatch.setattr(session, "_exec_on", execute)

    result = await session.exec(ProcessRequest(("codex", "exec"), close_stdin=True))

    assert result.returncode == 0
    assert result.stdout == "completed"
    assert executed == 1
    assert executed_argv == ("codex", "exec")
    assert await session.status() is SandboxStatus.READY


@pytest.mark.asyncio
async def test_modal_checkpoint_outcome_survives_live_observation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _lifecycle_session(_LifecycleSandbox("sb-agent"))

    async def observation_failure(*args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("live observation unavailable")

    monkeypatch.setattr(session, "_ensure_live_directory", observation_failure)
    monkeypatch.setattr(session, "_emit_event", observation_failure)

    checkpoint = await session.checkpoint()

    assert checkpoint.uri == "modal-image://im-checkpoint"
    assert await session.status() is SandboxStatus.READY


@pytest.mark.asyncio
async def test_modal_checkpoint_scrubs_raw_live_output_before_snapshot() -> None:
    sandbox = _LifecycleSandbox("sb-agent")
    paths = ModalSandboxSession.live_observation_paths()
    sandbox.filesystem.values[paths["stdout"]] = "stdout credential canary"
    sandbox.filesystem.values[paths["stderr"]] = "stderr credential canary"
    sandbox.filesystem.values[paths["events"]] = "safe structured event\n"

    class _Snapshot:
        async def aio(self, **kwargs):
            del kwargs
            assert paths["stdout"] not in sandbox.filesystem.values
            assert paths["stderr"] not in sandbox.filesystem.values
            assert paths["events"] in sandbox.filesystem.values
            return type("Image", (), {"object_id": "im-scrubbed"})()

    sandbox.snapshot_filesystem = _Snapshot()
    checkpoint = await _lifecycle_session(sandbox).checkpoint()

    assert checkpoint.uri == "modal-image://im-scrubbed"
    assert (
        "rm",
        "-f",
        paths["stdout"],
        paths["stderr"],
    ) in sandbox.commands


@pytest.mark.asyncio
async def test_modal_checkpoint_fails_closed_when_live_output_scrub_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = _LifecycleSandbox("sb-agent")
    session = _lifecycle_session(sandbox)
    snapshot_called = False

    class _Snapshot:
        async def aio(self, **kwargs):
            nonlocal snapshot_called
            del kwargs
            snapshot_called = True
            return type("Image", (), {"object_id": "im-unsafe"})()

    async def execute(_sandbox, request: ProcessRequest, **kwargs) -> ProcessResult:
        del _sandbox, kwargs
        return ProcessResult(
            request.argv,
            7 if request.argv[:2] == ("rm", "-f") else 0,
            stderr="scrub failed",
        )

    sandbox.snapshot_filesystem = _Snapshot()
    monkeypatch.setattr(session, "_exec_on", execute)

    with pytest.raises(RuntimeError, match="remove raw live output before checkpoint"):
        await session.checkpoint()

    assert snapshot_called is False


@pytest.mark.asyncio
async def test_modal_session_error_checkpoint_and_close_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = _LifecycleSandbox("sb-agent")
    auth = _LifecycleSandbox("sb-auth")
    session = _lifecycle_session(sandbox, auth)

    with pytest.raises(ValueError, match="unsupported"):
        await session.exec(ProcessRequest(("true",), secret_names=("unknown",)))
    with pytest.raises(RuntimeError, match="no Codex credential"):
        await session.exec(ProcessRequest(("true",), secret_names=("codex_oauth",)))

    async def cancelled(*args, **kwargs):
        del args, kwargs
        raise asyncio.CancelledError

    monkeypatch.setattr(session, "_exec_on", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await session.exec(ProcessRequest(("true",)))
    assert await session.status() is SandboxStatus.INTERRUPTED

    async def errored(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("remote failed")

    monkeypatch.setattr(session, "_exec_on", errored)
    with pytest.raises(RuntimeError, match="remote failed"):
        await session.exec(ProcessRequest(("true",)))
    assert await session.status() is SandboxStatus.ERRORED

    session._status = SandboxStatus.CLOSED
    with pytest.raises(RuntimeError, match="closed"):
        await session.exec(ProcessRequest(("true",)))
    with pytest.raises(RuntimeError, match="closed"):
        await session.checkpoint()


@pytest.mark.asyncio
async def test_modal_checkpoint_failure_invalid_identity_and_close_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SnapshotFailure:
        async def aio(self, **kwargs):
            del kwargs
            raise RuntimeError("snapshot unavailable")

    sandbox = _LifecycleSandbox("sb-agent")
    sandbox.snapshot_filesystem = _SnapshotFailure()
    session = _lifecycle_session(sandbox)
    events: list[str] = []

    async def record(kind, **kwargs) -> None:
        del kwargs
        events.append(kind.value)

    monkeypatch.setattr(session, "_emit_event", record)
    with pytest.raises(RuntimeError, match="snapshot unavailable"):
        await session.checkpoint()
    assert events[-1] == "checkpoint_failed"

    invalid = _lifecycle_session(_LifecycleSandbox("sb-agent", snapshot_id="floating"))
    monkeypatch.setattr(invalid, "_emit_event", record)
    with pytest.raises(RuntimeError, match="invalid image ID"):
        await invalid.checkpoint()

    async def event_failure(*args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("event unavailable")

    close_calls: list[str] = []
    closing_sandbox = _LifecycleSandbox("sb-agent")
    closing_auth = _LifecycleSandbox("sb-auth")

    async def terminate_failure(resource) -> None:
        close_calls.append(resource.object_id)
        if resource is closing_auth:
            raise RuntimeError("terminate failed")

    closing = _lifecycle_session(closing_sandbox, closing_auth)
    monkeypatch.setattr(closing, "_emit_event", event_failure)
    monkeypatch.setattr(closing, "_terminate", terminate_failure)
    with pytest.raises(BaseExceptionGroup, match="failed to close 1"):
        await closing.close()
    assert await closing.status() is SandboxStatus.ERRORED

    async def terminate_success(resource) -> None:
        close_calls.append(resource.object_id)

    monkeypatch.setattr(closing, "_terminate", terminate_success)
    await closing.close()
    assert await closing.status() is SandboxStatus.CLOSED
    assert close_calls == ["sb-agent", "sb-auth", "sb-auth"]


@pytest.mark.asyncio
async def test_modal_checkpoint_observation_failure_does_not_mask_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SnapshotFailure:
        async def aio(self, **kwargs):
            del kwargs
            raise RuntimeError("snapshot unavailable")

    sandbox = _LifecycleSandbox("sb-agent")
    sandbox.snapshot_filesystem = _SnapshotFailure()
    session = _lifecycle_session(sandbox)

    async def event_failure(*args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("event unavailable")

    monkeypatch.setattr(session, "_emit_event", event_failure)

    with pytest.raises(RuntimeError, match="snapshot unavailable"):
        await session.checkpoint()


@pytest.mark.asyncio
async def test_modal_oauth_persistence_failure_still_removes_all_codex_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = '{"access_token":"credential-canary"}'
    sandbox = _LifecycleSandbox("sb-agent")
    sandbox.filesystem.values["/root/.codex/auth.json"] = payload
    sandbox.filesystem.values["/root/.codex/sessions/history.jsonl"] = "sensitive transcript"
    session = _lifecycle_session(sandbox)

    async def auth_failure(*arguments: str) -> ProcessResult:
        raise RuntimeError(f"cannot persist {arguments[0]}")

    monkeypatch.setattr(session, "_auth_checked", auth_failure)
    with pytest.raises(RuntimeError, match="cannot persist"):
        await session._persist_and_remove_oauth()
    assert ("rm", "-rf", "/root/.codex") in sandbox.commands
    assert not any(path.startswith("/root/.codex/") for path in sandbox.filesystem.values)

    for value, error in (("not-json", "valid JSON"), ("[]", "non-empty"), ("{}", "non-empty")):
        with pytest.raises(RuntimeError, match=error):
            ModalSandboxSession._validate_oauth(value)


@pytest.mark.asyncio
async def test_modal_oauth_reports_persistence_and_removal_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = '{"access_token":"credential-canary"}'
    sandbox = _LifecycleSandbox("sb-agent")
    sandbox.filesystem.values["/root/.codex/auth.json"] = payload
    session = _lifecycle_session(sandbox)

    async def persistence_failure(*arguments: str) -> ProcessResult:
        raise RuntimeError(f"cannot persist {arguments[0]}")

    async def removal_failure(_request: ProcessRequest) -> ProcessResult:
        raise RuntimeError("cannot remove staged credential")

    monkeypatch.setattr(session, "_auth_checked", persistence_failure)
    monkeypatch.setattr(session, "_checked", removal_failure)

    with pytest.raises(BaseExceptionGroup) as raised:
        await session._persist_and_remove_oauth()

    assert "cannot persist" in str(raised.value.exceptions[0])
    assert "cannot remove" in str(raised.value.exceptions[1])


@pytest.mark.asyncio
async def test_modal_oauth_stage_failure_marks_errored_and_cleans_partial_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _lifecycle_session(_LifecycleSandbox("sb-agent"))
    removed = 0
    executed = False

    async def fail_stage() -> None:
        raise RuntimeError("stage failed")

    async def remove() -> None:
        nonlocal removed
        removed += 1

    async def execute(*args, **kwargs) -> ProcessResult:
        nonlocal executed
        del args, kwargs
        executed = True
        return ProcessResult(("true",), 0)

    monkeypatch.setattr(session, "_stage_oauth", fail_stage)
    monkeypatch.setattr(session, "_remove_oauth", remove)
    monkeypatch.setattr(session, "_exec_on", execute)

    with pytest.raises(RuntimeError, match="stage failed"):
        await session.exec(ProcessRequest(("true",), secret_names=("codex_oauth",)))

    assert await session.status() is SandboxStatus.ERRORED
    assert removed == 1
    assert executed is False


@pytest.mark.asyncio
async def test_modal_close_waits_for_active_exec(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _lifecycle_session(_LifecycleSandbox("sb-agent"))
    started = asyncio.Event()
    release = asyncio.Event()
    terminated: list[str] = []

    async def blocked_exec(*args, **kwargs) -> ProcessResult:
        del args, kwargs
        started.set()
        await release.wait()
        return ProcessResult(("true",), 0)

    async def terminate(resource) -> None:
        terminated.append(resource.object_id)

    monkeypatch.setattr(session, "_exec_on", blocked_exec)
    monkeypatch.setattr(session, "_terminate", terminate)

    exec_task = asyncio.create_task(session.exec(ProcessRequest(("true",))))
    await started.wait()
    close_task = asyncio.create_task(session.close())
    await asyncio.sleep(0)
    assert not close_task.done()
    assert terminated == []

    release.set()
    await exec_task
    await close_task
    assert terminated == ["sb-agent", "sb-auth"]
    assert await session.status() is SandboxStatus.CLOSED


@pytest.mark.asyncio
async def test_modal_monitor_recovers_then_reports_provider_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = ModalSandboxSession.live_observation_paths()
    calls = 0

    class _Filesystem:
        class read_text:
            @staticmethod
            async def aio(path: str) -> str:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise RuntimeError("temporary disconnect")
                if path == paths["status"]:
                    return "not-json"
                if path == paths["stdout"]:
                    return "agent output"
                raise FileNotFoundError(path)

    poll_values = iter((None, 17))

    class _Poll:
        @staticmethod
        async def aio():
            return next(poll_values)

    sandbox = type("Sandbox", (), {"filesystem": _Filesystem(), "poll": _Poll()})()
    fake_modal = type(
        "Modal",
        (),
        {"Sandbox": type("Sandbox", (), {"from_id": _AsyncMethod(sandbox)})},
    )
    monkeypatch.setitem(sys.modules, "modal", fake_modal)
    output = io.StringIO()
    monitor_events: list[dict[str, str]] = []

    status = await ModalSandboxSession.monitor(
        "sb-reconnect",
        poll_seconds=0.001,
        disconnect_grace_seconds=1,
        stdout_target=output,
        on_monitor_event=monitor_events.append,
    )

    assert status == {"provider_returncode": 17}
    assert output.getvalue() == "agent output"
    assert monitor_events == [{"type": "monitor_reconnected", "sandbox_id": "sb-reconnect"}]

    with pytest.raises(ValueError, match="must start"):
        await ModalSandboxSession.monitor("invalid", follow=False)
    with pytest.raises(ValueError, match="positive"):
        await ModalSandboxSession.monitor("sb-valid", poll_seconds=0)


def test_modal_delta_restarts_after_remote_log_truncation() -> None:
    target = io.StringIO()
    offsets = {"log": 100}

    ModalSandboxSession._write_delta("log", "new", offsets, target)

    assert target.getvalue() == "new"
    assert offsets == {"log": 3}


@pytest.mark.asyncio
async def test_modal_passthrough_streams_remote_output_without_a_local_stdin(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _Reader:
        def __init__(self, values):
            self.values = iter(values)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.values)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    process = type(
        "Process",
        (),
        {
            "stdin": object(),
            "stdout": _Reader((b"device code\n",)),
            "stderr": _Reader(("warning\n",)),
            "wait": _AsyncMethod(0),
        },
    )()
    monkeypatch.setattr(sys.stdin, "fileno", lambda: (_ for _ in ()).throw(OSError()))

    assert await ModalSandboxBackend._passthrough_process(process) == 0
    captured = capsys.readouterr()
    assert "device code" in captured.out
    assert "warning" in captured.err


def _fake_modal(sandboxes: list[object]):
    class _Create:
        calls: list[dict[str, object]] = []

        @classmethod
        async def aio(cls, **kwargs):
            cls.calls.append(kwargs)
            candidate = sandboxes.pop(0)
            if isinstance(candidate, BaseException):
                raise candidate
            return candidate

    volume = type("Volume", (), {"hydrate": _AsyncMethod()})()
    app = object()
    image = object()
    return type(
        "Modal",
        (),
        {
            "App": type("App", (), {"lookup": _AsyncMethod(app)}),
            "Volume": type(
                "Volume",
                (),
                {"from_name": staticmethod(lambda *args, **kwargs: volume)},
            ),
            "Secret": type(
                "Secret",
                (),
                {"from_name": staticmethod(lambda *args, **kwargs: "github-secret")},
            ),
            "Image": type(
                "Image",
                (),
                {"from_id": staticmethod(lambda image_id: (image, image_id))},
            ),
            "Sandbox": type("Sandbox", (), {"create": _Create()}),
        },
    )


@pytest.mark.asyncio
async def test_modal_backend_create_restore_and_login_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resources = [_LifecycleSandbox(f"sb-{index}") for index in range(1, 6)]
    fake_modal = _fake_modal(list(resources))
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    async def verified(*args, **kwargs) -> None:
        del args, kwargs

    async def passthrough(_process) -> int:
        return 0

    monkeypatch.setattr(
        "archetype.missions.sandboxes.modal.verify_coding_agent_environment",
        verified,
    )
    monkeypatch.setattr(
        ModalSandboxBackend,
        "_passthrough_process",
        staticmethod(passthrough),
    )
    backend = ModalSandboxBackend(ModalSandboxConfig(image_id="im-reviewed"))
    spec = SandboxSpec(
        "modal",
        backend.environment,
        "/workspace/repo",
        metadata=(("mission", "mission-1"),),
    )

    created = await backend.create(spec)
    assert created.identity.sandbox_id == "sb-2"
    await created.close()

    checkpoint = CheckpointRef(
        "modal",
        "im-checkpoint",
        "modal-image://im-checkpoint",
        1,
        environment=spec.environment,
        source_sandbox_id="sb-source",
        owner_id="mission-1",
    )
    restored = await backend.restore(spec, checkpoint)
    assert restored.identity.sandbox_id == "sb-4"
    assert any(command[:2] == ("test", "-d") for command in resources[3].commands)
    await restored.close()

    await backend.login_codex()
    assert any(
        command[:3] == ("codex", "login", "--device-auth") for command in resources[4].commands
    )
    assert resources[4].terminate.result is None

    with pytest.raises(ValueError, match="non-Modal"):
        await backend.create(SandboxSpec("docker", backend.environment, "/workspace/repo"))
    with pytest.raises(ValueError, match="environment"):
        await backend.restore(
            SandboxSpec("modal", "wrong", "/workspace/repo"),
            checkpoint,
        )


@pytest.mark.asyncio
async def test_modal_start_cleans_resources_after_provider_and_attestation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth = _LifecycleSandbox("sb-auth")
    fake_modal = _fake_modal([auth, RuntimeError("mission create failed")])
    backend = ModalSandboxBackend(ModalSandboxConfig(image_id="im-reviewed"))
    spec = SandboxSpec("modal", backend.environment, "/workspace/repo")

    with pytest.raises(RuntimeError, match="mission create failed"):
        await backend._start(fake_modal, spec, object())
    assert auth.detach.result is None

    auth = _LifecycleSandbox("sb-auth-2")
    mission = _LifecycleSandbox("sb-mission-2")
    fake_modal = _fake_modal([auth, mission])

    async def rejected(*args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("attestation failed")

    monkeypatch.setattr(
        "archetype.missions.sandboxes.modal.verify_coding_agent_environment",
        rejected,
    )
    with pytest.raises(RuntimeError, match="attestation failed"):
        await backend._start(fake_modal, spec, object())
    assert mission.detach.result is None
    assert auth.detach.result is None


@pytest.mark.asyncio
async def test_modal_login_failure_still_sanitizes_and_closes_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _LifecycleSandbox("sb-login")
    fake_modal = _fake_modal([broker])
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    async def failed(_process) -> int:
        return 7

    monkeypatch.setattr(
        ModalSandboxBackend,
        "_passthrough_process",
        staticmethod(failed),
    )
    backend = ModalSandboxBackend(ModalSandboxConfig(image_id="im-reviewed"))

    with pytest.raises(RuntimeError, match="device login failed"):
        await backend.login_codex()
    assert broker.commands[-1][0] == "sh"
    assert broker.detach.result is None
