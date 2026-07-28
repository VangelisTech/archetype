# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small async subprocess boundary shared by CLI-backed sandbox adapters."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from archetype.missions.sandboxes.contracts import ProcessResult


async def _join_uninterrupted[T](
    completion: asyncio.Future[T],
    *,
    cancellation: asyncio.CancelledError | None,
) -> tuple[T, asyncio.CancelledError | None]:
    while not completion.done():
        try:
            await asyncio.shield(completion)
        except asyncio.CancelledError as interrupted:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                cancellation = cancellation or interrupted
        except BaseException:
            break

    try:
        result = completion.result()
    except BaseException as completion_error:
        if cancellation is not None:
            raise cancellation from completion_error
        raise
    return result, cancellation


async def _kill_and_join[T](
    process: asyncio.subprocess.Process,
    completion: asyncio.Future[T],
    *,
    cancellation: asyncio.CancelledError | None,
) -> T:
    """Kill one owned child and join its completion despite repeated cancellation."""

    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass

    result, cancellation = await _join_uninterrupted(
        completion,
        cancellation=cancellation,
    )
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
        stdout, stderr = await _kill_and_join(
            process,
            completion,
            cancellation=None,
        )
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


async def run_host_passthrough(argv: Sequence[str]) -> int:
    """Run an interactive host command without capturing its terminal."""

    creation = asyncio.create_task(asyncio.create_subprocess_exec(*tuple(argv)))
    try:
        process = await asyncio.shield(creation)
    except asyncio.CancelledError as cancellation:
        process, cancellation = await _join_uninterrupted(
            creation,
            cancellation=cancellation,
        )
        completion = asyncio.create_task(process.wait())
        await _kill_and_join(
            process,
            completion,
            cancellation=cancellation,
        )
        raise
    completion = asyncio.create_task(process.wait())
    try:
        return int(await asyncio.shield(completion))
    except asyncio.CancelledError as cancellation:
        await _kill_and_join(
            process,
            completion,
            cancellation=cancellation,
        )
        raise


__all__ = ["run_host", "run_host_passthrough"]
