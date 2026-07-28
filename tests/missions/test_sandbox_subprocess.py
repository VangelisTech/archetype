# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the CLI-backed sandbox subprocess boundary."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from archetype.missions.sandboxes._subprocess import run_host, run_host_passthrough


async def _wait_for_pid(path: Path) -> int:
    async def read_started_process() -> int:
        while True:
            try:
                value = path.read_text(encoding="utf-8")
                if value:
                    return int(value)
            except FileNotFoundError:
                pass
            await asyncio.sleep(0.01)

    return await asyncio.wait_for(read_started_process(), timeout=5)


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


@pytest.mark.asyncio
@pytest.mark.parametrize("passthrough", [False, True], ids=["captured", "passthrough"])
async def test_cancellation_kills_and_fully_reaps_owned_process(
    tmp_path: Path,
    passthrough: bool,
) -> None:
    pid_path = tmp_path / "child.pid"
    command = (
        sys.executable,
        "-c",
        (
            "import os, pathlib, sys, time; "
            "path = pathlib.Path(sys.argv[1]); pending = path.with_suffix('.next'); "
            "pending.write_text(str(os.getpid()), encoding='utf-8'); pending.replace(path); "
            "time.sleep(60)"
        ),
        str(pid_path),
    )
    if passthrough:
        execution = asyncio.create_task(run_host_passthrough(command))
    else:
        execution = asyncio.create_task(run_host(command, timeout_seconds=60))
    pid = await _wait_for_pid(pid_path)
    os.kill(pid, 0)

    execution.cancel("cancel owned host process")
    with pytest.raises(asyncio.CancelledError, match="cancel owned host process"):
        await execution

    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("passthrough", [False, True], ids=["captured", "passthrough"])
async def test_cancellation_during_process_creation_still_reaps_created_child(
    monkeypatch: pytest.MonkeyPatch,
    passthrough: bool,
) -> None:
    real_create = asyncio.create_subprocess_exec
    process_created = asyncio.Event()
    release_creation = asyncio.Event()
    created_process: asyncio.subprocess.Process | None = None

    async def delayed_create(*args, **kwargs):
        nonlocal created_process
        created_process = await real_create(*args, **kwargs)
        process_created.set()
        await release_creation.wait()
        return created_process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_create)
    command = (sys.executable, "-c", "import time; time.sleep(60)")
    if passthrough:
        execution = asyncio.create_task(run_host_passthrough(command))
    else:
        execution = asyncio.create_task(run_host(command, timeout_seconds=60))
    await asyncio.wait_for(process_created.wait(), timeout=5)
    assert created_process is not None
    pid = created_process.pid
    os.kill(pid, 0)

    execution.cancel("cancel during host process creation")
    await asyncio.sleep(0)
    assert not execution.done()
    execution.cancel("repeat cancellation during host process creation")
    await asyncio.sleep(0)
    assert not execution.done()
    release_creation.set()
    with pytest.raises(asyncio.CancelledError, match="cancel during host process creation"):
        await execution

    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
