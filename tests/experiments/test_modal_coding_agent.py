# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the provider-neutral gate around a Modal coding agent."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from archetype.app.artifacts import ArtifactCandidate
from archetype.experiments.modal_coding_agent import (
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
        async def read() -> str:
            return value

        self.read = _AsyncMethod(read)


class _Process:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.stdout = _Reader(stdout)
        self.stderr = _Reader(stderr)

        async def wait() -> int:
            return returncode

        self.wait = _AsyncMethod(wait)


class _FakeFilesystem:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}

        async def write_text(value: str, path: str) -> None:
            self.files[path] = value

        self.write_text = _AsyncMethod(write_text)

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
    ) -> None:
        self.calls: list[dict[str, Any]] = []
        self.filesystem = _FakeFilesystem()
        self.validator_codes = list(validator_codes or [0])
        self._head = "base"
        self._dirty = True
        self.terminated = 0
        self.detached = 0
        self.snapshots = 0
        self.snapshot_error = snapshot_error

        async def execute(*args: str, **kwargs: Any) -> _Process:
            self.calls.append({"args": args, "kwargs": kwargs})
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
            if args[0] == "git" or args[0] == "mkdir":
                return _Process()
            if args[:2] == ("codex", "exec"):
                stream = (
                    '{"type":"thread.started","thread_id":"thread-123"}\n'
                    '{"type":"turn.completed"}\n'
                )
                return _Process(stdout=stream)
            if args[0] == "claude":
                stream = (
                    '{"type":"system","session_id":"claude-123"}\n'
                    '{"type":"result","session_id":"claude-123"}\n'
                )
                return _Process(stdout=stream)
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
    ) -> ModalSandboxClient:
        del image, app, github_secret
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
