# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real Docker checkpoint/restore parity for the public Sandbox Backend contract."""

from __future__ import annotations

import asyncio
import os

import pytest

from archetype.missions.sandboxes import ProcessRequest, SandboxSpec
from archetype.missions.sandboxes.docker import (
    DockerSandboxBackend,
    DockerSandboxConfig,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("ARCHETYPE_DOCKER_SANDBOX_PARITY") != "1",
        reason="set ARCHETYPE_DOCKER_SANDBOX_PARITY=1 in the dedicated parity lane",
    ),
]


async def _docker(*arguments: str) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        "docker",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return int(process.returncode or 0), stdout.decode(), stderr.decode()


@pytest.mark.asyncio
async def test_docker_checkpoint_restores_the_session_owned_writable_filesystem() -> None:
    backend = DockerSandboxBackend(DockerSandboxConfig(cpus=1, memory="1g"))
    spec = SandboxSpec(
        provider="docker",
        environment=backend.environment,
        workdir="/workspace/repo",
    )
    checkpoint = None
    original = await backend.create(spec)
    restored = None
    try:
        prepared = await original.exec(
            ProcessRequest(
                (
                    "sh",
                    "-lc",
                    "mkdir -p repo/.context && printf tracked > repo/tracked.txt "
                    "&& printf ignored > repo/.context/evidence.json",
                ),
                workdir="/workspace",
            )
        )
        assert prepared.returncode == 0, prepared.stderr
        checkpoint = await original.checkpoint()
        changed_after = await original.exec(
            ProcessRequest(
                ("sh", "-lc", "printf after > /workspace/repo/after-checkpoint.txt"),
            )
        )
        assert changed_after.returncode == 0, changed_after.stderr
        await original.close()

        restored = await backend.restore(spec, checkpoint)
        verified = await restored.exec(
            ProcessRequest(
                (
                    "sh",
                    "-lc",
                    'test "$(cat tracked.txt)" = tracked '
                    '&& test "$(cat .context/evidence.json)" = ignored '
                    "&& test ! -e after-checkpoint.txt "
                    "&& printf resumed > resumed.txt",
                ),
                workdir=spec.workdir,
            )
        )
        assert verified.returncode == 0, verified.stderr
        assert restored.identity.provider == "docker"
        assert checkpoint.restorable is True
    finally:
        if restored is not None:
            await restored.close()
        elif await original.status() != "closed":
            await original.close()
        if checkpoint is not None:
            returncode, _stdout, stderr = await _docker(
                "image", "rm", "--force", f"sha256:{checkpoint.checkpoint_id}"
            )
            assert returncode == 0, stderr
