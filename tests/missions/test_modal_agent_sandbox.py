# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused transport contracts for the Modal Sandbox Backend and Session."""

from __future__ import annotations

import pytest

from archetype.missions.sandboxes import ProcessRequest, SandboxBackend
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
        self.process = _Process()
        self.exec = _AsyncMethod(self.process)


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
    assert "push" not in ModalSandboxConfig.__dataclass_fields__
    with pytest.raises(ValueError, match="GitHub secret"):
        ModalSandboxConfig(github_secret_name="")
