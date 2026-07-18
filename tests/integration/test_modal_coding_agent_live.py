# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""User-triggered integration proofs for Modal coding-agent sandboxes.

This deliberately bypasses the Archetype runtime. It proves the external
boundary directly: Modal image build, sandbox creation, repository clone,
non-interactive agent execution, independent validation, git commit, full
filesystem snapshot, restore, and teardown.

Run the Modal-only infrastructure proof with::

    make test-modal-sandbox

Run the paid, API-backed agent edit proof with::

    make test-modal-agent

The default Modal Secret names are ``archetype-codex`` (``CODEX_API_KEY``) and
``archetype-claude-code`` (``ANTHROPIC_API_KEY``). Override the names with
``ARCHETYPE_CODEX_MODAL_SECRET`` and ``ARCHETYPE_CLAUDE_MODAL_SECRET``.
Set ``ARCHETYPE_MODAL_AGENT_AUTH_MODE=oauth`` after bootstrapping the named
subscription Volumes through example 11's ``--modal-login`` command.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, cast
from uuid import uuid4

import pytest

from archetype.experiments.modal_coding_agent import (
    AgentAuthMode,
    AgentHarness,
    ModalSandboxClient,
    ModalSandboxSpec,
    ValidatorSpec,
    _default_agent_image,
)

pytestmark = pytest.mark.modal


@pytest.mark.skipif(
    os.environ.get("ARCHETYPE_RUN_MODAL_SANDBOX_INTEGRATION") != "1",
    reason="set ARCHETYPE_RUN_MODAL_SANDBOX_INTEGRATION=1 to spend Modal compute credits",
)
@pytest.mark.parametrize("harness", ["codex", "claude-code"])
@pytest.mark.asyncio
async def test_live_modal_sandbox_exec_filesystem_snapshot_and_cli(
    harness: AgentHarness,
) -> None:
    modal = pytest.importorskip("modal")
    app = await modal.App.lookup.aio("archetype-coding-agent-integration", create_if_missing=True)
    sandbox = await modal.Sandbox.create.aio(
        app=app,
        image=_default_agent_image(modal, harness),
        timeout=10 * 60,
        idle_timeout=5 * 60,
        tags={"kind": "archetype-integration", "harness": harness},
    )
    try:
        executable = "codex" if harness == "codex" else "claude"
        process = await sandbox.exec.aio(executable, "--version", timeout=30)
        stdout, stderr, returncode = await asyncio.gather(
            process.stdout.read.aio(),
            process.stderr.read.aio(),
            process.wait.aio(),
        )
        assert returncode == 0, stderr
        assert executable in stdout.lower() or "claude code" in stdout.lower()

        help_argv = ("codex", "exec", "--help") if harness == "codex" else ("claude", "--help")
        help_process = await sandbox.exec.aio(*help_argv, timeout=30)
        help_stdout, help_stderr, help_returncode = await asyncio.gather(
            help_process.stdout.read.aio(),
            help_process.stderr.read.aio(),
            help_process.wait.aio(),
        )
        assert help_returncode == 0, help_stderr
        required_flags = (
            ("--json", "--dangerously-bypass-approvals-and-sandbox")
            if harness == "codex"
            else ("--output-format", "--dangerously-skip-permissions", "--resume")
        )
        assert all(flag in help_stdout for flag in required_flags)

        path = f"/tmp/archetype-smoke/{harness}.txt"
        await sandbox.filesystem.write_text.aio(f"{harness}\n", path)
        assert await sandbox.filesystem.read_text.aio(path) == f"{harness}\n"

        monitor_workspace = f"/tmp/archetype-monitor/{harness}"
        status_path, events_path = ModalSandboxClient.live_artifact_paths(monitor_workspace)
        trace_path = f"{monitor_workspace}/agent.jsonl"
        await sandbox.filesystem.write_text.aio('{"type":"agent_started"}\n', events_path)
        await sandbox.filesystem.write_text.aio(
            '{"type":"thread.started","thread_id":"integration"}\n', trace_path
        )
        await sandbox.filesystem.write_text.aio(
            json.dumps(
                {
                    "type": "heartbeat",
                    "sandbox_id": sandbox.object_id,
                    "trace_path": trace_path,
                    "trace_stderr_path": "",
                }
            ),
            status_path,
        )
        monitored = await ModalSandboxClient.monitor(
            sandbox.object_id,
            workspace=monitor_workspace,
            follow=False,
        )
        assert monitored["type"] == "heartbeat"
        assert monitored["sandbox_id"] == sandbox.object_id

        image = await sandbox.snapshot_filesystem.aio(timeout=120, ttl=24 * 60 * 60)
        if not image.object_id:
            await image.hydrate.aio()
        assert image.object_id.startswith("im-")

        restored = await modal.Sandbox.create.aio(
            app=app,
            image=image,
            timeout=10 * 60,
            idle_timeout=5 * 60,
            tags={"kind": "archetype-integration-restore", "harness": harness},
        )
        try:
            restored_file = await restored.exec.aio("cat", path, timeout=30)
            restored_stdout, restored_stderr, restored_returncode = await asyncio.gather(
                restored_file.stdout.read.aio(),
                restored_file.stderr.read.aio(),
                restored_file.wait.aio(),
            )
            assert restored_returncode == 0, restored_stderr
            assert restored_stdout == f"{harness}\n"
        finally:
            await restored.terminate.aio(wait=True)
            await restored.detach.aio()
    finally:
        await sandbox.terminate.aio(wait=True)
        await sandbox.detach.aio()


@pytest.mark.parametrize("harness", ["codex", "claude-code"])
@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("ARCHETYPE_RUN_MODAL_AGENT_INTEGRATION") != "1",
    reason="set ARCHETYPE_RUN_MODAL_AGENT_INTEGRATION=1 to spend Modal/model credits",
)
async def test_live_modal_agent_edits_validates_commits_and_snapshots(
    harness: AgentHarness,
) -> None:
    token = uuid4().hex
    expected = f"{harness} completed Modal integration {token}\n"
    secret_name = (
        os.environ.get("ARCHETYPE_CODEX_MODAL_SECRET", "archetype-codex")
        if harness == "codex"
        else os.environ.get("ARCHETYPE_CLAUDE_MODAL_SECRET", "archetype-claude-code")
    )
    model_env = "ARCHETYPE_CODEX_INTEGRATION_MODEL"
    if harness == "claude-code":
        model_env = "ARCHETYPE_CLAUDE_INTEGRATION_MODEL"
    auth_mode = os.environ.get("ARCHETYPE_MODAL_AGENT_AUTH_MODE", "api-key")

    spec = ModalSandboxSpec(
        repo_url="https://github.com/octocat/Hello-World.git",
        base_ref="master",
        branch=f"agent/modal-{harness}-{token[:12]}",
        harness=harness,
        auth_mode=cast(AgentAuthMode, auth_mode),
        model=os.environ.get(model_env, ""),
        codex_secret_name=(secret_name if harness == "codex" else "archetype-codex"),
        claude_secret_name=(secret_name if harness == "claude-code" else "archetype-claude-code"),
        codex_auth_volume_name=os.environ.get(
            "ARCHETYPE_CODEX_MODAL_AUTH_VOLUME", "archetype-codex-auth"
        ),
        claude_auth_volume_name=os.environ.get(
            "ARCHETYPE_CLAUDE_MODAL_AUTH_VOLUME", "archetype-claude-code-auth"
        ),
        workspace=f"/workspace/{harness}",
        timeout_seconds=30 * 60,
        idle_timeout_seconds=10 * 60,
        agent_timeout_seconds=10 * 60,
        snapshot_ttl_seconds=24 * 60 * 60,
        snapshot_after_attempt=True,
    )

    client = await ModalSandboxClient.create(spec)
    try:
        prompt = (
            "Create a new file named archetype_modal_smoke.txt whose complete contents are "
            f"exactly this single line, including the final newline:\n\n{expected!r}\n\n"
            "Do not modify any other tracked file."
        )
        validators = [
            ValidatorSpec(
                name="exact_file",
                command=(
                    "python3",
                    "-c",
                    (
                        "from pathlib import Path; "
                        f"assert Path('archetype_modal_smoke.txt').read_text() == {expected!r}"
                    ),
                ),
                timeout_seconds=30,
            )
        ]
        previous_session_id = ""
        previous_validator_details: list[dict[str, Any]] = []
        outcome: dict[str, Any] = {}
        for attempt_index in range(1, 3):
            outcome = await client.run_attempt(
                prompt=prompt,
                validators=validators,
                step_name=f"modal-{harness}-smoke",
                attempt_index=attempt_index,
                idempotency_key=f"modal-live:{harness}:{token}:{attempt_index}",
                previous_session_id=previous_session_id,
                previous_validator_details=previous_validator_details,
            )
            if outcome["accepted"]:
                break
            previous_session_id = str(outcome["agent_session_id"])
            previous_validator_details = list(outcome["validator_details"])

        assert outcome["accepted"] is True
        assert outcome["harness"] == harness
        assert outcome["results"] == {"exact_file": True}
        assert outcome["sha"]
        assert outcome["agent_session_id"]
        assert outcome["sandbox_id"].startswith("sb-")
        assert outcome["sandbox_state_ref"].startswith("modal-image://")
        assert outcome["traces_ref"].startswith("modal-image://")
        assert outcome["live_status_ref"].startswith(f"modal-sandbox://{outcome['sandbox_id']}")
        assert outcome["live_events_ref"].startswith(f"modal-sandbox://{outcome['sandbox_id']}")
        status_path, events_path = client._live_artifact_paths()
        live_status = json.loads(await client._sandbox.filesystem.read_text.aio(status_path))
        live_events = [
            json.loads(line)
            for line in (await client._sandbox.filesystem.read_text.aio(events_path)).splitlines()
        ]
        assert live_status["type"] == "attempt_completed"
        assert any(event["type"] == "agent_started" for event in live_events)
        assert any(event["type"] == "checkpoint_finished" for event in live_events)
        assert any(event["type"] == "attempt_completed" for event in live_events)
        if auth_mode == "oauth":
            absent = await client._exec(
                "test", "!", "-e", client.spec.mission_auth_path, timeout=30
            )
            assert absent.returncode == 0

            restored = await ModalSandboxClient.restore(spec, outcome["sandbox_state_ref"])
            try:
                absent_from_snapshot = await restored._exec(
                    "test", "!", "-e", restored.spec.mission_auth_path, timeout=30
                )
                assert absent_from_snapshot.returncode == 0
            finally:
                await restored.close()
    finally:
        await client.close()
