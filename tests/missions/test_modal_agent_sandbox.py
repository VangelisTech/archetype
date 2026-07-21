# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused transport contracts for the Modal Sandbox Backend and Session."""

from __future__ import annotations

import json
import sys

import pytest

from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxBackend,
    SandboxSpec,
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
