# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Opt-in live proof for the local Apple Container backend."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from archetype.experiments.apple_container_coding_agent import (
    AppleContainerSandboxClient,
    AppleContainerSandboxSpec,
)
from archetype.experiments.modal_coding_agent import AgentHarness

pytestmark = pytest.mark.apple_container


@pytest.mark.skipif(
    os.environ.get("ARCHETYPE_RUN_APPLE_CONTAINER_INTEGRATION") != "1",
    reason="set ARCHETYPE_RUN_APPLE_CONTAINER_INTEGRATION=1 to run a local VM",
)
@pytest.mark.parametrize("harness", ["codex", "claude-code"])
@pytest.mark.asyncio
async def test_live_apple_container_cli_filesystem_snapshot_and_cleanup(
    harness: AgentHarness, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    secret_env = "CODEX_API_KEY" if harness == "codex" else "ANTHROPIC_API_KEY"
    monkeypatch.setenv(secret_env, "integration-placeholder-not-used")
    token = uuid4().hex[:12]
    spec = AppleContainerSandboxSpec(
        repo_url="https://github.com/octocat/Hello-World.git",
        base_ref="master",
        branch=f"agent/apple-container-{harness}-{token}",
        harness=harness,
        workspace=f"/workspace/{harness}",
        state_dir=str(tmp_path),
        cpus=2,
        memory="4g",
        codex_auth_env=secret_env if harness == "codex" else "",
    )
    client: AppleContainerSandboxClient | None = None
    restored: AppleContainerSandboxClient | None = None
    archive: Path | None = None
    restore_image = ""
    sandbox_id = ""
    restored_id = ""
    try:
        client = await AppleContainerSandboxClient.create(spec)
        sandbox_id = client.sandbox_id
        executable = "codex" if harness == "codex" else "claude"
        version = await client._exec(executable, "--version", timeout=30)
        assert version.returncode == 0, version.stderr

        path = f"{client.spec.workspace}/apple-container-smoke.txt"
        await client._write_text(path, f"{harness}\n")
        readback = await client._exec("cat", path, timeout=30)
        assert readback.stdout == f"{harness}\n"

        snapshot = await client._snapshot_if_configured()
        archive = Path(snapshot.removeprefix("apple-container-rootfs://"))
        assert archive.is_file()
        assert archive.stat().st_size > 0
        await client.close()
        client = None

        restore_image = await AppleContainerSandboxClient._restore_image_name(spec, archive)
        restored = await AppleContainerSandboxClient.restore(spec, snapshot)
        restored_id = restored.sandbox_id
        restored_file = await restored._exec("cat", path, timeout=30)
        assert restored_file.returncode == 0, restored_file.stderr
        assert restored_file.stdout == f"{harness}\n"
    finally:
        if client is not None:
            await client.close()
        if restored is not None:
            await restored.close()
        if restore_image:
            await AppleContainerSandboxClient._run_host(
                "container", "image", "delete", "--force", restore_image, timeout=120
            )
        if archive is not None:
            archive.unlink(missing_ok=True)

    listed = await AppleContainerSandboxClient._run_host(
        "container", "list", "--all", "--quiet", timeout=30
    )
    assert sandbox_id not in listed.stdout
    assert restored_id not in listed.stdout
