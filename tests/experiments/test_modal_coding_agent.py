# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the provider-neutral gate around a Modal coding agent."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from archetype.app.artifacts import ArtifactCandidate
from archetype.experiments.modal_coding_agent import (
    _AGENT_STREAM_SCRIPT,
    _FILESYSTEM_DIFF_SCRIPT,
    _FILESYSTEM_MANIFEST_SCRIPT,
    ModalArtifactSourceResolver,
    ModalSandboxClient,
    ModalSandboxSpec,
    ValidatorSpec,
)


class _AsyncMethod:
    def __init__(self, function: Callable[..., Any]) -> None:
        self.aio = function


class _Reader:
    def __init__(self, value: str) -> None:
        self._value = value

        async def read() -> str:
            return value

        self.read = _AsyncMethod(read)

    def __aiter__(self) -> Any:
        async def iterate() -> Any:
            for line in self._value.splitlines(keepends=True):
                await asyncio.sleep(0)
                yield line

        return iterate()


class _Writer:
    def __init__(self) -> None:
        self.eof_writes = 0
        self.drains = 0

        async def drain() -> None:
            self.drains += 1

        self.drain = _AsyncMethod(drain)

    def write_eof(self) -> None:
        self.eof_writes += 1


class _Process:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.stdout = _Reader(stdout)
        self.stderr = _Reader(stderr)
        self.stdin = _Writer()

        async def wait() -> int:
            return returncode

        self.wait = _AsyncMethod(wait)


class _FakeFilesystem:
    def __init__(self, *, modal_missing_error: bool = False) -> None:
        self.files: dict[str, str] = {}

        async def write_text(value: str, path: str) -> None:
            self.files[path] = value

        self.write_text = _AsyncMethod(write_text)

        async def read_text(path: str) -> str:
            if path not in self.files:
                if modal_missing_error:
                    error_type = type("SandboxFilesystemNotFoundError", (Exception,), {})
                    raise error_type(path)
                raise FileNotFoundError(path)
            return self.files[path]

        self.read_text = _AsyncMethod(read_text)

        async def copy_to_local(remote_path: str, local_path: str | Path) -> None:
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).write_text(self.files[remote_path])

        self.copy_to_local = _AsyncMethod(copy_to_local)

        async def list_files(remote_path: str) -> list[Any]:
            prefix = remote_path.rstrip("/") + "/"
            children: dict[str, bool] = {}
            for path in self.files:
                if not path.startswith(prefix):
                    continue
                relative = path[len(prefix) :]
                name, separator, _remainder = relative.partition("/")
                children[name] = children.get(name, False) or bool(separator)

            class Entry:
                def __init__(self, name: str, directory: bool) -> None:
                    self.path = prefix + name
                    self._directory = directory

                def is_file(self) -> bool:
                    return not self._directory

                def is_dir(self) -> bool:
                    return self._directory

            return [Entry(name, directory) for name, directory in sorted(children.items())]

        self.list_files = _AsyncMethod(list_files)


class _FakeSandbox:
    object_id = "sb-test"

    def __init__(
        self,
        *,
        validator_codes: list[int] | None = None,
        snapshot_error: Exception | None = None,
        modal_missing_error: bool = False,
    ) -> None:
        self.calls: list[dict[str, Any]] = []
        self.filesystem = _FakeFilesystem(modal_missing_error=modal_missing_error)
        self.validator_codes = list(validator_codes or [0])
        self._head = "base"
        self._dirty = True
        self.terminated = 0
        self.detached = 0
        self.snapshots = 0
        self.snapshot_error = snapshot_error
        self.agent_processes: list[_Process] = []

        async def execute(*args: str, **kwargs: Any) -> _Process:
            raw_args = args
            trace_path = stderr_path = ""
            if args[:5] == ("bash", "-o", "pipefail", "-c", _AGENT_STREAM_SCRIPT):
                trace_path = args[6]
                stderr_path = args[7]
                args = args[8:]
            self.calls.append({"args": args, "raw_args": raw_args, "kwargs": kwargs})
            if args[0] == "cat":
                value = self.filesystem.files.get(args[1])
                return _Process(0, value) if value is not None else _Process(1, stderr="missing")
            if args[:2] == ("git", "rev-parse"):
                return _Process(stdout=f"{self._head}\n")
            if args[:3] == ("git", "status", "--porcelain"):
                return _Process(stdout=" M src/example.py\n" if self._dirty else "")
            if args[:2] == ("test", "-d"):
                return _Process(1)
            if args[:2] == ("git", "commit"):
                self._head = "verified"
                self._dirty = False
                return _Process(stdout="[branch verified] task")
            if args[0] == "mv":
                source, destination = args[-2:]
                self.filesystem.files[destination] = self.filesystem.files.pop(source)
                return _Process()
            if args[0] == "rm":
                self.filesystem.files.pop(args[-1], None)
                return _Process()
            if args[0] in {"git", "mkdir", "chmod", "sh", "sync"}:
                return _Process()
            if args[:3] == ("codex", "login", "status"):
                return _Process(stdout="Logged in using ChatGPT\n")
            if args[:3] == ("claude", "auth", "status"):
                return _Process(stdout='{"loggedIn":true}\n')
            if args[:3] == ("codex", "login", "--device-auth"):
                return _Process(stdout="device login\n")
            if args[:3] == ("claude", "auth", "login"):
                return _Process(stdout="browser login\n")
            if args[:2] == ("codex", "exec"):
                stream = (
                    '{"type":"thread.started","thread_id":"thread-123"}\n'
                    '{"type":"turn.completed"}\n'
                )
                if trace_path:
                    self.filesystem.files[trace_path] = stream
                    self.filesystem.files[stderr_path] = ""
                process = _Process(stdout=stream)
                self.agent_processes.append(process)
                return process
            if args[0] == "claude":
                stream = (
                    '{"type":"system","session_id":"claude-123"}\n'
                    '{"type":"result","session_id":"claude-123"}\n'
                )
                if trace_path:
                    self.filesystem.files[trace_path] = stream
                    self.filesystem.files[stderr_path] = ""
                process = _Process(stdout=stream)
                self.agent_processes.append(process)
                return process
            if args[0] == "verify":
                code = self.validator_codes.pop(0)
                return _Process(code, stdout="ok" if code == 0 else "failed")
            raise AssertionError(f"unexpected command: {args}")

        async def terminate(*, wait: bool) -> int:
            assert wait is True
            self.terminated += 1
            return 137

        async def detach() -> None:
            self.detached += 1

        async def snapshot_filesystem(*, timeout: int, ttl: int | None) -> Any:
            assert timeout == 120
            assert ttl == 30 * 24 * 60 * 60
            self.snapshots += 1
            if self.snapshot_error is not None:
                raise self.snapshot_error
            return type("SnapshotImage", (), {"object_id": "im-snapshot"})()

        self.exec = _AsyncMethod(execute)
        self.terminate = _AsyncMethod(terminate)
        self.detach = _AsyncMethod(detach)
        self.snapshot_filesystem = _AsyncMethod(snapshot_filesystem)


def _spec(**overrides: Any) -> ModalSandboxSpec:
    values: dict[str, Any] = {
        "repo_url": "https://github.com/example/repo.git",
        "branch": "agent/test",
        "snapshot_after_attempt": False,
        "capture_filesystem_manifests": False,
    }
    values.update(overrides)
    return ModalSandboxSpec(**values)


def test_spec_rejects_unsafe_or_incomplete_identity() -> None:
    with pytest.raises(ValueError, match="non-root absolute"):
        _spec(workspace="/")
    with pytest.raises(ValueError, match="branch"):
        _spec(branch="--force")
    with pytest.raises(ValueError, match="github_secret_name"):
        _spec(push=True)
    with pytest.raises(ValueError, match="unsupported"):
        _spec(harness="opencode")
    with pytest.raises(ValueError, match="auth mode"):
        _spec(auth_mode="password")
    with pytest.raises(ValueError, match="volume name"):
        _spec(codex_auth_volume_name="--invalid")


@pytest.mark.parametrize(
    ("harness", "volume_name", "volume_path", "mission_path"),
    [
        (
            "codex",
            "archetype-codex-auth",
            "/auth/auth.json",
            "/root/.codex/auth.json",
        ),
        (
            "claude-code",
            "archetype-claude-code-auth",
            "/auth/.credentials.json",
            "/root/.claude/.credentials.json",
        ),
    ],
)
def test_oauth_spec_selects_harness_specific_volume_contract(
    harness: str,
    volume_name: str,
    volume_path: str,
    mission_path: str,
) -> None:
    spec = _spec(harness=harness, auth_mode="oauth")

    assert spec.auth_volume_name == volume_name
    assert spec.auth_volume_path == volume_path
    assert spec.mission_auth_path == mission_path


def test_validator_round_trip_normalizes_command() -> None:
    validator = ValidatorSpec.from_dict(
        {"name": "tests", "command": ["uv", "run", "pytest"], "timeout_seconds": 42}
    )
    assert validator.command == ("uv", "run", "pytest")
    assert validator.to_dict() == {
        "name": "tests",
        "command": ["uv", "run", "pytest"],
        "expected_returncode": 0,
        "timeout_seconds": 42,
    }


@pytest.mark.parametrize("harness", ["codex", "claude-code"])
@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.asyncio
async def test_modal_login_creates_v2_volume_verifies_and_syncs(
    harness: str,
    cancelled: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    volume_calls: list[dict[str, Any]] = []
    create_calls: list[dict[str, Any]] = []
    volume = object()
    sandbox = _FakeSandbox()

    class _VolumeReference:
        def __init__(self) -> None:
            async def hydrate() -> None:
                return None

            self.hydrate = _AsyncMethod(hydrate)

    reference = _VolumeReference()

    class _Volume:
        @staticmethod
        def from_name(name: str, **kwargs: Any) -> _VolumeReference:
            volume_calls.append({"name": name, **kwargs})
            return reference

    class _Image:
        @staticmethod
        def from_name(name: str) -> object:
            assert name == "agent-image"
            return volume

    modal = type("Modal", (), {"Volume": _Volume, "Image": _Image})()

    async def fake_base(spec: ModalSandboxSpec) -> tuple[Any, Any]:
        del spec
        return modal, object()

    async def fake_create(
        spec: ModalSandboxSpec,
        **kwargs: Any,
    ) -> _FakeSandbox:
        del spec
        create_calls.append(kwargs)
        return sandbox

    async def fake_passthrough(process: Any) -> int:
        del process
        if cancelled:
            raise asyncio.CancelledError
        return 0

    monkeypatch.setattr(ModalSandboxClient, "_modal_base", staticmethod(fake_base))
    monkeypatch.setattr(ModalSandboxClient, "_create_modal_sandbox", staticmethod(fake_create))
    monkeypatch.setattr(ModalSandboxClient, "_passthrough_process", staticmethod(fake_passthrough))

    spec = _spec(
        harness=harness,
        auth_mode="oauth",
        image_name="agent-image",
    )
    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await ModalSandboxClient.login_oauth(spec)
    else:
        await ModalSandboxClient.login_oauth(spec)

    assert volume_calls == [
        {
            "name": spec.auth_volume_name,
            "create_if_missing": True,
            "version": 2,
        }
    ]
    assert create_calls[0]["volumes"] == {"/auth": reference}
    assert create_calls[0]["kind"] == "archetype-agent-oauth-login"
    assert any(call["args"][:2] == ("sync", "/auth") for call in sandbox.calls)
    assert sandbox.terminated == 1
    assert sandbox.detached == 1


@pytest.mark.parametrize(
    ("harness", "executable", "session_id"),
    [("codex", "codex", "thread-123"), ("claude-code", "claude", "claude-123")],
)
@pytest.mark.asyncio
async def test_each_attempt_is_returned_and_retry_policy_stays_outside_transport(
    harness: str, executable: str, session_id: str
) -> None:
    sandbox = _FakeSandbox(validator_codes=[1, 0])
    secret = object()
    client = ModalSandboxClient(_spec(harness=harness), sandbox, secret)
    validator = ValidatorSpec("tests", ("verify",), timeout_seconds=5)

    rejected = await client.run_attempt(
        prompt="Fix the regression",
        validators=[validator],
        step_name="fix",
        attempt_index=1,
        idempotency_key="world:entity:step-1:attempt-1",
    )

    assert rejected["status"] == "rejected"
    assert rejected["accepted"] is False
    assert rejected["attempt_index"] == 1
    assert rejected["sha"] == ""
    assert rejected["results"] == {"tests": False}
    assert len(rejected["friction"]) == 1

    outcome = await client.run_attempt(
        prompt="Fix the regression",
        validators=[validator],
        step_name="fix",
        attempt_index=2,
        idempotency_key="world:entity:step-1:attempt-2",
        previous_session_id=rejected["agent_session_id"],
        previous_validator_details=rejected["validator_details"],
    )

    assert outcome["status"] == "accepted"
    assert outcome["accepted"] is True
    assert outcome["sha"] == "verified"
    assert outcome["attempts"] == 2
    assert outcome["agent_session_id"] == session_id
    assert outcome["harness"] == harness
    assert outcome["results"] == {"tests": True}

    agent_calls = [call for call in sandbox.calls if call["args"][0] == executable]
    assert len(agent_calls) == 2
    assert agent_calls[0]["kwargs"]["secrets"] == [secret]
    if harness == "codex":
        assert "--dangerously-bypass-approvals-and-sandbox" in agent_calls[0]["args"]
        assert (
            'shell_environment_policy.exclude=["*KEY*","*SECRET*","*TOKEN*"]'
            in agent_calls[0]["args"]
        )
        assert "resume" in agent_calls[1]["args"]
    else:
        assert "--dangerously-skip-permissions" in agent_calls[0]["args"]

        assert "--resume" in agent_calls[1]["args"]
    assert session_id in agent_calls[1]["args"]

    validator_calls = [call for call in sandbox.calls if call["args"] == ("verify",)]
    assert len(validator_calls) == 2
    assert all(call["kwargs"]["secrets"] == [] for call in validator_calls)

    receipt_paths = [path for path in sandbox.filesystem.files if "/gates/" in path]
    assert len(receipt_paths) == 2
    before_retry = len(sandbox.calls)
    retried = await client.run_attempt(
        prompt="Fix the regression",
        validators=[validator],
        step_name="fix",
        attempt_index=2,
        idempotency_key="world:entity:step-1:attempt-2",
        previous_session_id=rejected["agent_session_id"],
        previous_validator_details=rejected["validator_details"],
    )
    assert retried == outcome
    assert len(sandbox.calls) == before_retry

    await client.close()
    await client.close()
    assert sandbox.terminated == 1
    assert sandbox.detached == 1


@pytest.mark.parametrize(
    ("harness", "executable", "session_marker"),
    [
        ("codex", "codex", '"thread_id":"thread-123"'),
        ("claude-code", "claude", '"session_id":"claude-123"'),
    ],
)
@pytest.mark.asyncio
async def test_agent_stream_and_live_phase_files_are_observable_during_attempt(
    harness: str,
    executable: str,
    session_marker: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sandbox = _FakeSandbox(modal_missing_error=True)
    client = ModalSandboxClient(_spec(harness=harness, stream_agent_output=True), sandbox, object())

    outcome = await client.run_attempt(
        prompt="Fix it",
        validators=[ValidatorSpec("tests", ("verify",))],
        step_name="observable",
        attempt_index=1,
        idempotency_key=f"observable:{harness}",
    )

    captured = capsys.readouterr()
    assert session_marker in captured.out
    status_path, events_path = client._live_artifact_paths()
    status = json.loads(sandbox.filesystem.files[status_path])
    events = [json.loads(line) for line in sandbox.filesystem.files[events_path].splitlines()]
    event_types = [event["type"] for event in events]
    assert status["type"] == "attempt_completed"
    assert event_types == [
        "attempt_started",
        "agent_started",
        "agent_finished",
        "validator_started",
        "validator_finished",
        "commit_started",
        "commit_finished",
        "evidence_capture_started",
        "evidence_capture_finished",
        "checkpoint_started",
        "checkpoint_finished",
        "attempt_completed",
    ]
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
    assert all(event["sandbox_id"] == "sb-test" for event in events)
    assert sandbox.agent_processes[0].stdin.eof_writes == 1
    assert sandbox.agent_processes[0].stdin.drains == 1
    assert outcome["live_status_ref"] == f"modal-sandbox://sb-test{status_path}"
    assert outcome["live_events_ref"] == f"modal-sandbox://sb-test{events_path}"

    agent_call = next(call for call in sandbox.calls if call["args"][0] == executable)
    assert agent_call["raw_args"][:4] == ("bash", "-o", "pipefail", "-c")
    assert sandbox.filesystem.files[outcome["trace_ref"].removeprefix("modal-sandbox://sb-test")]


@pytest.mark.asyncio
async def test_monitor_attaches_by_sandbox_id_and_reads_live_deltas(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sandbox = _FakeSandbox()
    status_path, events_path = ModalSandboxClient.live_artifact_paths("/workspace/repo")
    trace_path = "/workspace/repo/.archetype-agent/traces/attempt.jsonl"
    stderr_path = f"{trace_path}.stderr"
    sandbox.filesystem.files.update(
        {
            status_path: json.dumps(
                {
                    "type": "heartbeat",
                    "sandbox_id": "sb-test",
                    "trace_path": trace_path,
                    "trace_stderr_path": stderr_path,
                }
            ),
            events_path: '{"type":"agent_started"}\n{"type":"heartbeat"}\n',
            trace_path: '{"type":"thread.started","thread_id":"thread-live"}\n',
            stderr_path: "remote warning\n",
        }
    )

    async def from_id(sandbox_id: str) -> _FakeSandbox:
        assert sandbox_id == "sb-test"
        return sandbox

    sandbox_api = type("Sandbox", (), {"from_id": _AsyncMethod(from_id)})()
    monkeypatch.setitem(sys.modules, "modal", type("Modal", (), {"Sandbox": sandbox_api})())

    status = await ModalSandboxClient.monitor("sb-test", follow=False)

    captured = capsys.readouterr()
    assert status["type"] == "heartbeat"
    assert '"type":"agent_started"' in captured.out
    assert '"thread_id":"thread-live"' in captured.out
    assert captured.err == "remote warning\n"


@pytest.mark.asyncio
async def test_monitor_retries_snapshot_filesystem_interruption_until_teardown(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sandbox = _FakeSandbox()
    status_path, events_path = ModalSandboxClient.live_artifact_paths("/workspace/repo")
    sandbox.filesystem.files.update(
        {
            status_path: json.dumps(
                {"type": "sandbox_closing", "sandbox_id": "sb-test", "phase": "teardown"}
            ),
            events_path: '{"type":"sandbox_closing"}\n',
        }
    )
    original_read = sandbox.filesystem.read_text.aio
    interrupted = False

    async def read_with_snapshot_interruption(path: str) -> str:
        nonlocal interrupted
        if path == status_path and not interrupted:
            interrupted = True
            raise RuntimeError("filesystem unavailable while snapshotting")
        return await original_read(path)

    sandbox.filesystem.read_text = _AsyncMethod(read_with_snapshot_interruption)

    async def from_id(_sandbox_id: str) -> _FakeSandbox:
        return sandbox

    sandbox_api = type("Sandbox", (), {"from_id": _AsyncMethod(from_id)})()
    monkeypatch.setitem(sys.modules, "modal", type("Modal", (), {"Sandbox": sandbox_api})())

    status = await ModalSandboxClient.monitor(
        "sb-test",
        poll_seconds=0.001,
        disconnect_grace_seconds=0.1,
    )

    captured = capsys.readouterr()
    assert status["type"] == "sandbox_closing"
    assert '"type": "monitor_read_interrupted"' in captured.out
    assert '"type": "monitor_reconnected"' in captured.out
    assert '"type":"sandbox_closing"' in captured.out


@pytest.mark.asyncio
async def test_running_agent_heartbeat_updates_durable_status() -> None:
    sandbox = _FakeSandbox()
    client = ModalSandboxClient(
        _spec(heartbeat_seconds=1, stream_agent_output=False), sandbox, object()
    )
    client._live_context = {"attempt_id": "attempt-live"}
    client._live_phase = "agent_running"
    client._live_phase_started_at = time.monotonic()
    client._live_session_started_at = time.monotonic()

    heartbeat = asyncio.create_task(client._heartbeat_session())
    await asyncio.sleep(1.05)
    heartbeat.cancel()
    await asyncio.gather(heartbeat, return_exceptions=True)

    status_path, _events_path = client._live_artifact_paths()
    status = json.loads(sandbox.filesystem.files[status_path])
    assert status["type"] == "heartbeat"
    assert status["phase"] == "agent_running"
    assert status["attempt_id"] == "attempt-live"
    assert status["elapsed_seconds"] >= 1
    assert status["agent_stdout_bytes"] == 0
    assert status["agent_stderr_bytes"] == 0
    assert status["agent_output_bytes"] == 0
    assert status["seconds_since_agent_output"] is None


@pytest.mark.parametrize("harness", ["codex", "claude-code"])
@pytest.mark.asyncio
async def test_oauth_broker_stages_only_for_agent_then_persists_and_removes(
    harness: str,
) -> None:
    spec = _spec(harness=harness, auth_mode="oauth")
    mission = _FakeSandbox()
    broker = _FakeSandbox()
    broker.filesystem.files[spec.auth_volume_path] = '{"oauth":"credential"}'
    client = ModalSandboxClient(spec, mission, None, _auth_sandbox=broker)

    outcome = await client.run_attempt(
        prompt="Fix it",
        validators=[ValidatorSpec("tests", ("verify",), timeout_seconds=5)],
        step_name="oauth",
        attempt_index=1,
        idempotency_key=f"oauth:{harness}:1",
    )

    assert outcome["accepted"] is True
    assert spec.mission_auth_path not in mission.filesystem.files
    assert broker.filesystem.files[spec.auth_volume_path] == '{"oauth":"credential"}'
    commands = [call["args"] for call in mission.calls]
    agent_index = next(
        index
        for index, command in enumerate(commands)
        if command[0] == ("codex" if harness == "codex" else "claude")
    )
    remove_index = next(
        index
        for index, command in enumerate(commands)
        if command[:2] == ("rm", "-f") and command[-1] == spec.mission_auth_path
    )
    validator_index = next(
        index for index, command in enumerate(commands) if command[0] == "verify"
    )
    assert agent_index < remove_index < validator_index
    agent_call = mission.calls[agent_index]
    assert agent_call["kwargs"]["secrets"] == []
    assert any(call["args"][:2] == ("sync", "/auth") for call in broker.calls)

    await client.close()
    assert mission.terminated == 1
    assert mission.detached == 1
    assert broker.terminated == 1
    assert broker.detached == 1


@pytest.mark.asyncio
async def test_rejected_attempt_does_not_commit() -> None:
    sandbox = _FakeSandbox(validator_codes=[1])
    client = ModalSandboxClient(_spec(), sandbox, object())

    outcome = await client.run_attempt(
        prompt="Fix it",
        validators=[ValidatorSpec("tests", ("verify",))],
        step_name="fix",
        attempt_index=1,
        idempotency_key="failure",
    )

    assert outcome["status"] == "rejected"
    assert not any(call["args"][:2] == ("git", "commit") for call in sandbox.calls)


@pytest.mark.asyncio
async def test_snapshot_returns_durable_modal_image_reference() -> None:
    sandbox = _FakeSandbox()
    client = ModalSandboxClient(_spec(snapshot_after_attempt=True), sandbox, object())

    assert await client._snapshot_if_configured() == "modal-image://im-snapshot"
    assert client._latest_checkpoint_ref == "modal-image://im-snapshot"
    assert sandbox.snapshots == 1


@pytest.mark.asyncio
async def test_checkpoint_failure_is_persisted_without_aborting_attempt_tick() -> None:
    sandbox = _FakeSandbox(snapshot_error=RuntimeError("provider unavailable"))
    client = ModalSandboxClient(_spec(snapshot_after_attempt=True), sandbox, object())

    outcome = await client.run_attempt(
        prompt="Fix it",
        validators=[ValidatorSpec("tests", ("verify",))],
        step_name="fix",
        attempt_index=1,
        idempotency_key="checkpoint-failure",
    )

    assert outcome["accepted"] is True
    assert outcome["checkpoint_status"] == "failed"
    assert outcome["checkpoint_restorable"] is False
    assert outcome["finalization_phase"] == "captured"
    assert "provider unavailable" in outcome["finalization_error"]
    assert any(item["finding"] == "Provider checkpoint failed" for item in outcome["friction"])


@pytest.mark.asyncio
async def test_restore_uses_snapshot_image_and_verifies_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image_ids: list[str] = []

    class _Image:
        @staticmethod
        def from_id(image_id: str) -> object:
            image_ids.append(image_id)
            return object()

    modal = type("Modal", (), {"Image": _Image})()
    sandbox = _FakeSandbox()

    async def fake_base(spec: ModalSandboxSpec) -> tuple[Any, Any]:
        del spec
        return modal, object()

    async def fake_start(
        spec: ModalSandboxSpec,
        *,
        image: Any,
        app: Any,
        agent_secret: Any,
        github_secret: Any,
        auth_volume: Any,
    ) -> ModalSandboxClient:
        del image, app, github_secret, auth_volume
        return ModalSandboxClient(spec, sandbox, agent_secret)

    monkeypatch.setattr(
        ModalSandboxClient,
        "_modal_base",
        staticmethod(fake_base),
    )
    monkeypatch.setattr(ModalSandboxClient, "_start", staticmethod(fake_start))

    client = await ModalSandboxClient.restore(_spec(), "modal-image://im-recovery")

    assert image_ids == ["im-recovery"]
    assert client._agent_secret is None
    assert client._latest_checkpoint_ref == "modal-image://im-recovery"
    assert any(
        call["args"][:3] == ("git", "rev-parse", "--is-inside-work-tree") for call in sandbox.calls
    )
    await client.close()

    with pytest.raises(ValueError, match="checkpoint"):
        await ModalSandboxClient.restore(_spec(), "modal-image://im-recovery#/workspace/file")


@pytest.mark.asyncio
async def test_modal_artifact_resolver_reads_live_checkpoint_files(tmp_path: Path) -> None:
    sandbox = _FakeSandbox()
    sandbox.filesystem.files.update(
        {
            "/workspace/repo/result.json": '{"ok":true}',
            "/workspace/repo/.context/findings.md": "finding",
            "/workspace/repo/.context/nested/note.txt": "note",
        }
    )
    client = ModalSandboxClient(_spec(), sandbox, object())
    client._latest_checkpoint_ref = "modal-image://im-snapshot"
    resolver = ModalArtifactSourceResolver(spec=_spec(), sandbox=client)

    values = await resolver.materialize(
        (
            ArtifactCandidate(
                source_ref="modal-image://im-snapshot#/workspace/repo/result.json",
                logical_path="result.json",
            ),
            ArtifactCandidate(
                source_ref="modal-image://im-snapshot#/workspace/repo/.context",
                logical_path="context",
                recursive=True,
            ),
        ),
        tmp_path / "resolved",
    )

    assert {value.logical_path for value in values} == {
        "result.json",
        "context/findings.md",
        "context/nested/note.txt",
    }
    assert {value.path.read_text() for value in values} == {
        '{"ok":true}',
        "finding",
        "note",
    }


@pytest.mark.asyncio
async def test_modal_artifact_resolver_restores_nonmatching_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    live = ModalSandboxClient(_spec(), _FakeSandbox(), object())
    live._latest_checkpoint_ref = "modal-image://im-latest"
    restored_sandbox = _FakeSandbox()
    restored_sandbox.filesystem.files["/workspace/repo/result.json"] = "from-old-snapshot"
    restored_refs: list[str] = []

    async def fake_restore(cls, spec: ModalSandboxSpec, checkpoint_ref: str) -> ModalSandboxClient:
        del cls
        restored_refs.append(checkpoint_ref)
        restored = ModalSandboxClient(spec, restored_sandbox, object())
        restored._latest_checkpoint_ref = checkpoint_ref
        return restored

    monkeypatch.setattr(ModalSandboxClient, "restore", classmethod(fake_restore))
    resolver = ModalArtifactSourceResolver(spec=_spec(), sandbox=live)

    values = await resolver.materialize(
        (
            ArtifactCandidate(
                source_ref="modal-image://im-older#/workspace/repo/result.json",
                logical_path="result.json",
            ),
        ),
        tmp_path / "restored",
    )

    assert restored_refs == ["modal-image://im-older"]
    assert values[0].path.read_text() == "from-old-snapshot"
    assert restored_sandbox.terminated == 1


def test_session_parser_ignores_non_json_progress() -> None:
    codex = ModalSandboxClient(_spec(harness="codex"), _FakeSandbox(), object())
    claude = ModalSandboxClient(_spec(harness="claude-code"), _FakeSandbox(), object())
    assert (
        codex._session_id('progress\n{"type":"thread.started","thread_id":"019abc"}\n') == "019abc"
    )
    assert claude._session_id('{"type":"system","session_id":"claude-abc"}\n') == "claude-abc"


def test_filesystem_diff_includes_ignored_and_non_git_files(tmp_path: Path) -> None:
    root = tmp_path / "rootfs"
    artifacts = root / ".archetype-agent" / "filesystem"
    start = artifacts / "start.jsonl"
    end = artifacts / "end.jsonl"
    diff = artifacts / "diff.jsonl"
    tracked = root / "workspace" / "tracked.py"
    deleted = root / "home" / "agent" / "scratch.txt"
    ignored = root / "workspace" / ".context" / "findings.json"
    tracked.parent.mkdir(parents=True)
    deleted.parent.mkdir(parents=True)
    tracked.write_text("before\n")
    deleted.write_text("remove me\n")

    subprocess.run(
        [
            sys.executable,
            "-c",
            _FILESYSTEM_MANIFEST_SCRIPT,
            str(root),
            str(start),
            str(artifacts),
        ],
        check=True,
    )
    tracked.write_text("after\n")
    deleted.unlink()
    ignored.parent.mkdir(parents=True)
    ignored.write_text('{"finding":"persist me"}\n')
    subprocess.run(
        [
            sys.executable,
            "-c",
            _FILESYSTEM_MANIFEST_SCRIPT,
            str(root),
            str(end),
            str(artifacts),
        ],
        check=True,
    )
    subprocess.run(
        [sys.executable, "-c", _FILESYSTEM_DIFF_SCRIPT, str(start), str(end), str(diff)],
        check=True,
    )

    changes = {
        record["path"]: record["change"]
        for line in diff.read_text().splitlines()
        if (record := json.loads(line))
    }
    assert changes[str(tracked)] == "modified"
    assert changes[str(deleted)] == "deleted"
    assert changes[str(ignored)] == "created"
