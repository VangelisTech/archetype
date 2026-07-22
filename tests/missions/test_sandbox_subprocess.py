# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the CLI-backed sandbox subprocess boundary."""

from __future__ import annotations

import asyncio

import pytest

from archetype.missions.sandboxes._subprocess import run_host, run_host_passthrough


@pytest.mark.asyncio
async def test_run_host_captures_output_and_passthrough_returns_status() -> None:
    result = await run_host(
        ("sh", "-c", "printf output; printf warning >&2"),
        timeout_seconds=10,
        stdin="",
    )
    assert result.returncode == 0
    assert result.stdout == "output"
    assert result.stderr == "warning"
    assert await run_host_passthrough(("sh", "-c", "exit 7")) == 7


@pytest.mark.asyncio
async def test_run_host_kills_a_timed_out_process(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Process:
        returncode = None

        def __init__(self) -> None:
            self.killed = False

        async def communicate(self, _stdin=None):
            if not self.killed:
                await asyncio.sleep(1)
            self.returncode = 9
            return b"partial", b"provider warning"

        def kill(self) -> None:
            self.killed = True

    process = _Process()

    async def create(*args, **kwargs):
        del args, kwargs
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)

    result = await run_host(("slow",), timeout_seconds=0)

    assert process.killed is True
    assert result.returncode == 124
    assert result.stdout == "partial"
    assert "timed out" in result.stderr
