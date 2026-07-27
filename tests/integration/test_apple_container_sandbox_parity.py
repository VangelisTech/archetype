# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real Apple Container checkpoint/restore parity for the Sandbox contract."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest

from archetype.missions.sandboxes import ProcessRequest, SandboxSpec, SandboxStatus
from archetype.missions.sandboxes.apple_container import (
    AppleContainerSandboxBackend,
    AppleContainerSandboxConfig,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("ARCHETYPE_APPLE_CONTAINER_SANDBOX_PARITY") != "1",
        reason="set ARCHETYPE_APPLE_CONTAINER_SANDBOX_PARITY=1 for the local parity lane",
    ),
]


async def _container(*arguments: str) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        "container",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return int(process.returncode or 0), stdout.decode(), stderr.decode()


@pytest.mark.asyncio
async def test_apple_container_checkpoint_restores_session_owned_rootfs(
    tmp_path: Path,
) -> None:
    auth_volume = f"archetype-test-auth-{uuid4().hex[:12]}"
    returncode, _stdout, stderr = await _container("volume", "create", auth_volume)
    assert returncode == 0, stderr
    config = AppleContainerSandboxConfig(
        state_dir=str(tmp_path / "checkpoints"),
        cpus=1,
        memory="1g",
        auth_volume_name=auth_volume,
    )
    backend = AppleContainerSandboxBackend(config)
    spec = SandboxSpec(
        provider="apple-container",
        environment=backend.environment,
        workdir="/workspace/repo",
    )
    original = None
    restored = None
    checkpoint = None
    try:
        original = await backend.create(spec)
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
        assert restored.identity.provider == "apple-container"
        assert checkpoint.restorable is True
    finally:
        if restored is not None:
            await restored.close()
        elif original is not None and await original.status() is not SandboxStatus.CLOSED:
            await original.close()
        if checkpoint is not None:
            await _container(
                "image",
                "delete",
                "--force",
                f"archetype-agent:restore-{checkpoint.checkpoint_id[:24]}",
            )
        returncode, _stdout, stderr = await _container("volume", "delete", auth_volume)
        assert returncode == 0, stderr
