# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small async subprocess boundary shared by CLI-backed sandbox adapters."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from archetype.missions.sandboxes.contracts import ProcessResult


async def run_host(
    argv: Sequence[str],
    *,
    timeout_seconds: int,
    stdin: str | None = None,
) -> ProcessResult:
    """Run one host command with bounded output and deterministic stdin closure."""

    command = tuple(argv)
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(stdin.encode() if stdin is not None else None),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        process.kill()
        stdout, stderr = await process.communicate()
        stderr += f"\ncommand timed out after {timeout_seconds}s".encode()
        return ProcessResult(
            command,
            124,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
        )
    return ProcessResult(
        command,
        int(process.returncode or 0),
        stdout.decode(errors="replace"),
        stderr.decode(errors="replace"),
    )


async def run_host_passthrough(argv: Sequence[str]) -> int:
    """Run an interactive host command without capturing its terminal."""

    process = await asyncio.create_subprocess_exec(*tuple(argv))
    return int(await process.wait())


__all__ = ["run_host", "run_host_passthrough"]
