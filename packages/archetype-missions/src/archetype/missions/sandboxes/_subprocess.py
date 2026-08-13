# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small async subprocess boundary shared by CLI-backed sandbox adapters."""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Sequence

from archetype.missions.sandboxes.contracts import ProcessResult

_CLEANUP_JOIN_TIMEOUT_SECONDS = 5.0


class _JoinTimeout(TimeoutError):
    def __init__(self, cancellation: asyncio.CancelledError | None) -> None:
        super().__init__("subprocess completion did not finish after group termination")
        self.cancellation = cancellation


class _CleanupTimeout(RuntimeError):
    pass


async def _join_uninterrupted[T](
    completion: asyncio.Future[T],
    *,
    cancellation: asyncio.CancelledError | None,
    timeout_seconds: float | None = None,
) -> tuple[T, asyncio.CancelledError | None]:
    loop = asyncio.get_running_loop()
    deadline = None if timeout_seconds is None else loop.time() + max(timeout_seconds, 0)
    while not completion.done():
        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            raise _JoinTimeout(cancellation)
        try:
            shielded = asyncio.shield(completion)
            if remaining is None:
                await shielded
            else:
                await asyncio.wait_for(shielded, timeout=remaining)
        except asyncio.CancelledError as interrupted:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                cancellation = cancellation or interrupted
        except TimeoutError:
            if not completion.done():
                raise _JoinTimeout(cancellation) from None
            break
        except BaseException:
            break

    try:
        result = completion.result()
    except BaseException as completion_error:
        if cancellation is not None:
            raise cancellation from completion_error
        raise
    return result, cancellation


def _consume_future_result[T](completion: asyncio.Future[T]) -> None:
    try:
        completion.result()
    except BaseException:
        pass


def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
            return
        except ProcessLookupError:
            # The leader may already be reaped and the group may already be empty.
            pass

    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass


async def _kill_and_join[T](
    process: asyncio.subprocess.Process,
    completion: asyncio.Future[T],
    *,
    cancellation: asyncio.CancelledError | None,
) -> T:
    """Kill one owned child and join its completion despite repeated cancellation."""

    try:
        _terminate_process_group(process)
    except BaseException as termination_error:
        completion.cancel("subprocess group termination failed")
        completion.add_done_callback(_consume_future_result)
        if cancellation is not None:
            raise cancellation from termination_error
        raise

    try:
        result, cancellation = await _join_uninterrupted(
            completion,
            cancellation=cancellation,
            timeout_seconds=_CLEANUP_JOIN_TIMEOUT_SECONDS,
        )
    except _JoinTimeout as timeout:
        completion.cancel("subprocess cleanup timed out")
        completion.add_done_callback(_consume_future_result)
        cleanup_error = _CleanupTimeout(str(timeout))
        if timeout.cancellation is not None:
            raise timeout.cancellation from cleanup_error
        raise cleanup_error from timeout
    if cancellation is not None:
        raise cancellation
    return result


async def run_host(
    argv: Sequence[str],
    *,
    timeout_seconds: int,
    stdin: str | None = None,
) -> ProcessResult:
    """Run one host command with bounded output and deterministic stdin closure."""

    command = tuple(argv)
    creation = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
    )
    try:
        process = await asyncio.shield(creation)
    except asyncio.CancelledError as cancellation:
        process, cancellation = await _join_uninterrupted(
            creation,
            cancellation=cancellation,
        )
        completion = asyncio.create_task(process.communicate())
        await _kill_and_join(
            process,
            completion,
            cancellation=cancellation,
        )
        raise
    completion = asyncio.create_task(
        process.communicate(stdin.encode() if stdin is not None else None)
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.shield(completion),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        try:
            stdout, stderr = await _kill_and_join(
                process,
                completion,
                cancellation=None,
            )
        except _CleanupTimeout as cleanup_error:
            stdout = b""
            stderr = f"\n{cleanup_error}".encode()
        stderr += f"\ncommand timed out after {timeout_seconds}s".encode()
        return ProcessResult(
            command,
            124,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
        )
    except asyncio.CancelledError as cancellation:
        await _kill_and_join(
            process,
            completion,
            cancellation=cancellation,
        )
        raise
    return ProcessResult(
        command,
        int(process.returncode or 0),
        stdout.decode(errors="replace"),
        stderr.decode(errors="replace"),
    )


__all__ = ["run_host"]
