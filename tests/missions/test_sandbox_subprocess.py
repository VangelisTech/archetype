# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the CLI-backed sandbox subprocess boundary."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest

from archetype.missions.sandboxes import _subprocess as subprocess_boundary
from archetype.missions.sandboxes._subprocess import run_host


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


async def _wait_for_processes(path: Path) -> tuple[int, int]:
    async def read_started_processes() -> tuple[int, int]:
        while True:
            try:
                values = path.read_text(encoding="utf-8").split()
                if len(values) == 2:
                    return int(values[0]), int(values[1])
            except FileNotFoundError:
                pass
            await asyncio.sleep(0.01)

    return await asyncio.wait_for(read_started_processes(), timeout=5)


async def _wait_for_pid_exit(pid: int) -> None:
    async def wait_until_gone() -> None:
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_until_gone(), timeout=5)


def _descendant_command(path: Path, *, leader_exits: bool) -> tuple[str, ...]:
    leader_tail = "raise SystemExit(0)" if leader_exits else "time.sleep(60)"
    return (
        sys.executable,
        "-c",
        (
            "import os, pathlib, subprocess, sys, time; "
            "child = subprocess.Popen("
            "[sys.executable, '-c', 'import time; time.sleep(60)']); "
            "path = pathlib.Path(sys.argv[1]); pending = path.with_suffix('.next'); "
            "pending.write_text(f'{os.getpid()} {child.pid}', encoding='utf-8'); "
            f"pending.replace(path); {leader_tail}"
        ),
        str(path),
    )


def _kill_if_alive(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


@pytest.mark.asyncio
async def test_run_host_captures_output() -> None:
    result = await run_host(
        ("sh", "-c", "printf output; printf warning >&2"),
        timeout_seconds=10,
        stdin="",
    )
    assert result.returncode == 0
    assert result.stdout == "output"
    assert result.stderr == "warning"


@pytest.mark.asyncio
async def test_run_host_kills_a_timed_out_process(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Process:
        returncode = None
        pid = 12345

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
    monkeypatch.setattr(
        subprocess_boundary.os,
        "killpg",
        lambda pgid, signum: process.kill(),
    )

    result = await run_host(("slow",), timeout_seconds=0)

    assert process.killed is True
    assert result.returncode == 124
    assert result.stdout == "partial"
    assert "timed out" in result.stderr


@pytest.mark.asyncio
async def test_cancellation_kills_and_fully_reaps_owned_process(
    tmp_path: Path,
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
    execution = asyncio.create_task(run_host(command, timeout_seconds=60))
    pid = await _wait_for_pid(pid_path)
    os.kill(pid, 0)

    execution.cancel("cancel owned host process")
    with pytest.raises(asyncio.CancelledError, match="cancel owned host process"):
        await execution

    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_cancellation_during_process_creation_still_reaps_created_child(
    monkeypatch: pytest.MonkeyPatch,
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


@pytest.mark.asyncio
async def test_cancellation_terminates_the_owned_process_group(
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "processes.pid"
    command = _descendant_command(pid_path, leader_exits=False)
    execution = asyncio.create_task(run_host(command, timeout_seconds=60))
    leader_pid, descendant_pid = await _wait_for_processes(pid_path)

    try:
        execution.cancel("cancel owned process group")
        with pytest.raises(asyncio.CancelledError, match="cancel owned process group"):
            await asyncio.wait_for(execution, timeout=5)

        await asyncio.gather(
            _wait_for_pid_exit(leader_pid),
            _wait_for_pid_exit(descendant_pid),
        )
    finally:
        _kill_if_alive(leader_pid)
        _kill_if_alive(descendant_pid)


@pytest.mark.asyncio
async def test_timeout_terminates_descendant_after_leader_exit(tmp_path: Path) -> None:
    pid_path = tmp_path / "processes.pid"
    command = _descendant_command(pid_path, leader_exits=True)
    execution = asyncio.create_task(run_host(command, timeout_seconds=1))
    leader_pid, descendant_pid = await _wait_for_processes(pid_path)

    try:
        await _wait_for_pid_exit(leader_pid)
        result = await asyncio.wait_for(execution, timeout=5)

        assert result.returncode == 124
        assert "timed out" in result.stderr
        await _wait_for_pid_exit(descendant_pid)
    finally:
        _kill_if_alive(leader_pid)
        _kill_if_alive(descendant_pid)


@pytest.mark.asyncio
async def test_cleanup_deadline_preserves_timeout_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    communicate_started = asyncio.Event()
    communicate_cancelled = asyncio.Event()

    class _Process:
        returncode = None
        pid = 12345

        async def communicate(self, _stdin=None):
            communicate_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                communicate_cancelled.set()

        def kill(self) -> None:
            self.returncode = 9

    process = _Process()

    async def create(*args, **kwargs):
        del args, kwargs
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(
        subprocess_boundary.os,
        "killpg",
        lambda pgid, signum: process.kill(),
    )
    monkeypatch.setattr(subprocess_boundary, "_CLEANUP_JOIN_TIMEOUT_SECONDS", 0.01)

    result = await run_host(("stuck-cleanup",), timeout_seconds=0)
    await communicate_started.wait()
    await asyncio.sleep(0)

    assert result.returncode == 124
    assert "completion did not finish" in result.stderr
    assert communicate_cancelled.is_set()


@pytest.mark.asyncio
async def test_cleanup_deadline_preserves_original_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    communicate_started = asyncio.Event()
    communicate_cancelled = asyncio.Event()

    class _Process:
        returncode = None
        pid = 12345

        async def communicate(self, _stdin=None):
            communicate_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                communicate_cancelled.set()

        def kill(self) -> None:
            self.returncode = 9

    process = _Process()

    async def create(*args, **kwargs):
        del args, kwargs
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(
        subprocess_boundary.os,
        "killpg",
        lambda pgid, signum: process.kill(),
    )
    monkeypatch.setattr(subprocess_boundary, "_CLEANUP_JOIN_TIMEOUT_SECONDS", 0.01)

    execution = asyncio.create_task(run_host(("stuck-cleanup",), timeout_seconds=60))
    await communicate_started.wait()
    execution.cancel("cancel stuck cleanup")
    with pytest.raises(asyncio.CancelledError, match="cancel stuck cleanup") as raised:
        await execution
    await asyncio.sleep(0)

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert communicate_cancelled.is_set()
