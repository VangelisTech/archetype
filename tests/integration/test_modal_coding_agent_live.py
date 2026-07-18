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

Run the paid cross-sandbox session continuation proof with::

    make test-modal-resume

The default Modal Secret names are ``archetype-codex`` (``CODEX_API_KEY``),
``archetype-claude-code`` (``ANTHROPIC_API_KEY``), and
``archetype-modal-endpoint`` (``MODAL_ENDPOINT_TOKEN_ID`` plus
``MODAL_ENDPOINT_TOKEN_SECRET``). Override the names with the harness-specific
``ARCHETYPE_*_MODAL_SECRET`` variable.
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

from archetype.app.sandboxes.modal import (
    AgentAuthMode,
    AgentHarness,
    ModalSandboxClient,
    ModalSandboxSpec,
    OpenCodeWireAPI,
    ValidatorSpec,
    _default_agent_image,
)

pytestmark = [pytest.mark.modal, pytest.mark.external, pytest.mark.slow]


def _live_agent_spec(harness: AgentHarness, token: str, *, workspace: str) -> ModalSandboxSpec:
    secret_name = {
        "codex": os.environ.get("ARCHETYPE_CODEX_MODAL_SECRET", "archetype-codex"),
        "claude-code": os.environ.get("ARCHETYPE_CLAUDE_MODAL_SECRET", "archetype-claude-code"),
        "opencode": os.environ.get("ARCHETYPE_OPENCODE_MODAL_SECRET", "archetype-modal-endpoint")
        or "archetype-modal-endpoint",
    }[harness]
    model_env = {
        "codex": "ARCHETYPE_CODEX_INTEGRATION_MODEL",
        "claude-code": "ARCHETYPE_CLAUDE_INTEGRATION_MODEL",
        "opencode": "ARCHETYPE_OPENCODE_INTEGRATION_MODEL",
    }[harness]
    auth_mode = (
        "api-key"
        if harness == "opencode"
        else os.environ.get("ARCHETYPE_MODAL_AGENT_AUTH_MODE", "api-key")
    )
    opencode_base_url = os.environ.get("ARCHETYPE_OPENCODE_ENDPOINT_BASE_URL", "")
    if harness == "opencode" and not opencode_base_url:
        pytest.skip("set ARCHETYPE_OPENCODE_ENDPOINT_BASE_URL for the paid OpenCode proof")
    model = os.environ.get(model_env, "") or (
        "Qwen/Qwen3.6-35B-A3B-FP8" if harness == "opencode" else ""
    )
    return ModalSandboxSpec(
        repo_url="https://github.com/octocat/Hello-World.git",
        base_ref="master",
        branch=f"agent/modal-{harness}-{token[:12]}",
        harness=harness,
        auth_mode=cast(AgentAuthMode, auth_mode),
        model=model,
        codex_secret_name=(secret_name if harness == "codex" else "archetype-codex"),
        claude_secret_name=(secret_name if harness == "claude-code" else "archetype-claude-code"),
        opencode_secret_name=(secret_name if harness == "opencode" else "archetype-modal-endpoint"),
        opencode_base_url=opencode_base_url,
        opencode_wire_api=cast(
            OpenCodeWireAPI,
            os.environ.get("ARCHETYPE_OPENCODE_WIRE_API", "") or "chat-completions",
        ),
        codex_auth_volume_name=os.environ.get(
            "ARCHETYPE_CODEX_MODAL_AUTH_VOLUME", "archetype-codex-auth"
        ),
        claude_auth_volume_name=os.environ.get(
            "ARCHETYPE_CLAUDE_MODAL_AUTH_VOLUME", "archetype-claude-code-auth"
        ),
        workspace=workspace,
        timeout_seconds=30 * 60,
        idle_timeout_seconds=10 * 60,
        agent_timeout_seconds=10 * 60,
        snapshot_ttl_seconds=24 * 60 * 60,
        snapshot_after_attempt=True,
    )


@pytest.mark.skipif(
    os.environ.get("ARCHETYPE_RUN_MODAL_SANDBOX_INTEGRATION") != "1",
    reason="set ARCHETYPE_RUN_MODAL_SANDBOX_INTEGRATION=1 to spend Modal compute credits",
)
@pytest.mark.parametrize("harness", ["codex", "claude-code", "opencode"])
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
        executable = {"codex": "codex", "claude-code": "claude", "opencode": "opencode"}[harness]
        process = await sandbox.exec.aio(executable, "--version", timeout=30)
        stdout, stderr, returncode = await asyncio.gather(
            process.stdout.read.aio(),
            process.stderr.read.aio(),
            process.wait.aio(),
        )
        assert returncode == 0, stderr
        if harness == "opencode":
            assert stdout.strip() == "1.18.3"
        else:
            assert executable in stdout.lower() or "claude code" in stdout.lower()

        help_argv = {
            "codex": ("codex", "exec", "--help"),
            "claude-code": ("claude", "--help"),
            "opencode": ("opencode", "run", "--help"),
        }[harness]
        help_process = await sandbox.exec.aio(*help_argv, timeout=30)
        help_stdout, help_stderr, help_returncode = await asyncio.gather(
            help_process.stdout.read.aio(),
            help_process.stderr.read.aio(),
            help_process.wait.aio(),
        )
        assert help_returncode == 0, help_stderr
        required_flags = {
            "codex": ("--json", "--dangerously-bypass-approvals-and-sandbox"),
            "claude-code": ("--output-format", "--dangerously-skip-permissions", "--resume"),
            "opencode": ("--pure", "--format", "--model", "--session", "--auto"),
        }[harness]
        help_output = help_stdout + help_stderr
        missing_flags = [flag for flag in required_flags if flag not in help_output]
        assert not missing_flags, f"missing {missing_flags} from help output:\n{help_output}"

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


@pytest.mark.parametrize("harness", ["codex", "claude-code", "opencode"])
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
    spec = _live_agent_spec(harness, token, workspace=f"/workspace/{harness}")

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
        if spec.auth_mode == "oauth":
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


@pytest.mark.parametrize("harness", ["codex", "claude-code", "opencode"])
@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("ARCHETYPE_RUN_MODAL_RESUME_INTEGRATION") != "1",
    reason="set ARCHETYPE_RUN_MODAL_RESUME_INTEGRATION=1 to spend Modal/model credits",
)
async def test_live_modal_agent_resumes_session_in_new_sandbox(
    harness: AgentHarness,
) -> None:
    token = uuid4().hex
    spec = _live_agent_spec(harness, token, workspace=f"/workspace/resume-{harness}")
    first: ModalSandboxClient | None = await ModalSandboxClient.create(spec)
    resumed: ModalSandboxClient | None = None
    recovery: ModalSandboxClient | None = None
    try:
        phase_a_path = "archetype_modal_resume_a.txt"
        phase_a_value = f"phase-a:{harness}:{token}\n"
        phase_a = await first.run_attempt(
            prompt=(
                f"Create {phase_a_path} with exactly {phase_a_value!r}, including the newline. "
                "Do not modify any other tracked file."
            ),
            validators=(
                ValidatorSpec(
                    name="phase_a_exact",
                    command=(
                        "python3",
                        "-c",
                        (
                            "from pathlib import Path; "
                            f"assert Path({phase_a_path!r}).read_text() == {phase_a_value!r}"
                        ),
                    ),
                    timeout_seconds=30,
                ),
            ),
            step_name=f"modal-{harness}-resume-a",
            attempt_index=1,
            idempotency_key=f"modal-resume:{harness}:{token}:a",
        )
        assert phase_a["accepted"] is True
        assert phase_a["checkpoint_restorable"] is True
        assert phase_a["agent_session_id"]
        source_sandbox_id = first.sandbox_id
        session_id = str(phase_a["agent_session_id"])
        checkpoint_ref = str(phase_a["sandbox_state_ref"])

        await first.close()
        first = None
        resumed = await ModalSandboxClient.resume(spec, checkpoint_ref)
        assert resumed.sandbox_id != source_sandbox_id

        phase_b_path = "archetype_modal_resume_b.txt"
        phase_b_value = f"phase-b:{harness}:{token}\n"
        validator_source = (
            "from pathlib import Path; "
            f"assert Path({phase_a_path!r}).read_text() == {phase_a_value!r}; "
            f"assert Path({phase_b_path!r}).read_text() == {phase_b_value!r}"
        )
        phase_b = await resumed.run_attempt(
            prompt=(
                f"Continue by creating {phase_b_path} with exactly {phase_b_value!r}, "
                "including the newline. Preserve the phase A file and do not modify any "
                "other tracked file."
            ),
            validators=(
                ValidatorSpec(
                    name="phase_a_and_b_exact",
                    command=("python3", "-c", validator_source),
                    timeout_seconds=30,
                ),
            ),
            step_name=f"modal-{harness}-resume-b",
            attempt_index=2,
            idempotency_key=f"modal-resume:{harness}:{token}:b",
            previous_session_id=session_id,
        )
        assert phase_b["accepted"] is True
        assert phase_b["agent_session_id"] == session_id
        assert phase_b["checkpoint_restorable"] is True

        recovery = await ModalSandboxClient.restore(spec, phase_b["sandbox_state_ref"])
        recovered = await recovery._exec(
            "python3",
            "-c",
            validator_source,
            workdir=spec.workspace,
            timeout=30,
        )
        assert recovered.returncode == 0, recovered.stderr
        if spec.auth_mode == "oauth":
            absent = await recovery._exec("test", "!", "-e", spec.mission_auth_path, timeout=30)
            assert absent.returncode == 0
    finally:
        for client in (recovery, resumed, first):
            if client is not None:
                await client.close()
