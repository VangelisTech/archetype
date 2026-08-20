# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Paid exact-wheel evidence for one steerable Codex mission on Modal."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pytest
from websockets.asyncio.client import connect

from archetype.missions import (
    AgentExecutionStatus,
    CommandValidator,
    RepositoryPublicationPolicy,
)
from archetype.missions.coding_agents import (
    AgentExecutionResult,
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.coding_agents.app_server import (
    CodexAppServerDriver,
    run_codex_app_server_turn,
)
from archetype.missions.coding_agents.contracts import (
    DispatchedValidator,
    TaskDispatchRequest,
)
from archetype.missions.sandboxes import (
    ModalCodexAppServerConnector,
    ModalSandboxBackend,
    ModalSandboxConfig,
    ModalSandboxSession,
    ModalViewportGrant,
    ModalViewportMode,
    ProcessRequest,
    ProcessResult,
    SandboxSpec,
    SandboxStatus,
)
from scripts.release_agent_diagnostics import (
    bounded_text_summary,
    summarize_agent_failure,
)

_LIVE = os.environ.get("ARCHETYPE_MODAL_AGENT_MISSION_LIVE") == "1"
_PROOF = "archetype-modal-live-steer-v1\n"
_FINAL_MESSAGE = "ARCHETYPE_MODAL_LIVE_OK"
_PREFLIGHT_MESSAGE = "ARCHETYPE_MODAL_AUTH_OK"
_OBSERVATION_ROOT = "/tmp/archetype-agent-missions/live"
_REMOTE = "/workspace/origin.git"
_BRANCH = "agent/modal-live"
_VIEWPORT_LOGGER = logging.getLogger("archetype.release_evidence.redacted_viewport")
_VIEWPORT_LOGGER.addHandler(logging.NullHandler())
_VIEWPORT_LOGGER.propagate = False

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("missions.environment.pinned"),
    pytest.mark.contract("missions.sandbox.cleanup_ownership"),
    pytest.mark.integration,
    pytest.mark.external,
    pytest.mark.slow,
    pytest.mark.skipif(
        not _LIVE,
        reason="set ARCHETYPE_MODAL_AGENT_MISSION_LIVE=1 for paid Modal evidence",
    ),
]


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise AssertionError(f"live Modal evidence requires {name}")
    return value


async def _remote_exec(
    session: ModalSandboxSession,
    *argv: str,
    workdir: str | None = None,
    timeout_seconds: int = 120,
) -> ProcessResult:
    """Run test-owned inspection against the live provider object.

    ``CodexAppServerDriver`` owns the session lock while the turn is active.
    The release probe therefore uses the same provider exec boundary as the
    ttyd processes to observe and steer the already-running tmux session.
    """

    sandbox: Any = session._sandbox  # noqa: SLF001 - paid provider evidence
    return await ModalSandboxSession._exec_on(  # noqa: SLF001
        sandbox,
        ProcessRequest(
            tuple(argv),
            workdir=workdir,
            timeout_seconds=timeout_seconds,
        ),
    )


def _assert_success(result: ProcessResult, label: str) -> str:
    assert result.returncode == 0, (
        f"{label} failed with {result.returncode}: {result.stderr or result.stdout}"
    )
    return result.stdout


def _dispatch(prompt: str) -> TaskDispatchRequest:
    validator_script = """
set -eu
test "$(cat modal-live-proof.txt)" = "$1"
test ! -e /root/.codex/auth.json
for socket in "$2"/executions/*/tmux.sock; do
    [ -e "$socket" ] || continue
    if tmux -S "$socket" has-session 2>/dev/null; then
        echo "interactive tmux server survived exact-turn completion" >&2
        exit 1
    fi
done
""".strip()
    return TaskDispatchRequest(
        mission_id=1,
        task_id=1,
        task_name="modal_live_steer",
        dispatch_id="modal-live-release-evidence",
        dispatch_sequence=1,
        repository=_REMOTE,
        branch=_BRANCH,
        base_ref="main",
        prompt=prompt,
        validators=(
            DispatchedValidator(
                validator_id=1,
                spec=CommandValidator(
                    name="exact_proof",
                    command=(
                        "sh",
                        "-c",
                        validator_script,
                        "modal-live-validator",
                        _PROOF.rstrip(),
                        _OBSERVATION_ROOT,
                    ),
                    timeout_seconds=60,
                ),
            ),
        ),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
    )


async def _wait_for_takeover(
    session: ModalSandboxSession,
    harness_task: asyncio.Task[AgentExecutionResult],
) -> str:
    script = """
set -eu
events="$1/events.jsonl"
executions="$1/executions"
expected_sandbox_id="$2"
takeover_ready() {
    python3 - "$events" "$expected_sandbox_id" <<'PY'
import json
import sys

events_path, expected_sandbox_id = sys.argv[1:]
try:
    with open(events_path, encoding="utf-8") as source:
        for line in source:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(event, dict)
                and event.get("schema_version") == 1
                and event.get("type") == "session_ready"
                and event.get("operation") == "codex"
                and event.get("sandbox_id") == expected_sandbox_id
            ):
                raise SystemExit(0)
except FileNotFoundError:
    pass
raise SystemExit(1)
PY
}
i=0
while [ "$i" -lt 600 ]; do
    if [ -f "$events" ] && takeover_ready; then
        directory="$(find "$executions" -mindepth 1 -maxdepth 1 -type d -print)"
        test -n "$directory"
        test "$(printf '%s\n' "$directory" | wc -l)" -eq 1
        printf '%s\n' "$directory"
        exit 0
    fi
    i=$((i + 1))
    sleep 0.25
done
echo "timed out waiting for the writable Codex TUI lane" >&2
exit 1
""".strip()
    probe = asyncio.create_task(
        _remote_exec(
            session,
            "sh",
            "-c",
            script,
            "wait-modal-takeover",
            _OBSERVATION_ROOT,
            session.identity.sandbox_id,
            timeout_seconds=165,
        )
    )
    done, _pending = await asyncio.wait(
        (probe, harness_task),
        return_when=asyncio.FIRST_COMPLETED,
    )
    if harness_task in done:
        probe.cancel()
        await asyncio.gather(probe, return_exceptions=True)
        observation = await harness_task
        summary = _safe_agent_failure_summary(observation)
        raise AssertionError(f"Codex turn completed before the writable TUI lane opened: {summary}")
    result = await probe
    return _assert_success(result, "wait for Modal session_ready").strip()


def _authenticated_viewport_status(grant: ModalViewportGrant, label: str) -> int:
    """Reach one bearer URL while keeping its capability out of failures."""

    sensitive_url = grant.browser_url
    request: Request | None = Request(
        sensitive_url,
        headers={"User-Agent": "archetype-release-evidence/1"},
    )
    response: Any = None
    try:
        with urlopen(request, timeout=30) as response:
            status = int(response.status)
            response.read(1)
    except HTTPError as exc:
        status = int(exc.code)
        sensitive_url = ""
        request = None
        raise AssertionError(f"authenticated {label} viewport returned HTTP {status}") from None
    except Exception as exc:
        error_name = type(exc).__name__
        sensitive_url = ""
        request = None
        raise AssertionError(
            f"authenticated {label} viewport was unreachable ({error_name})"
        ) from None
    finally:
        sensitive_url = ""
        request = None
        response = None
    if not 200 <= status < 400:
        raise AssertionError(f"authenticated {label} viewport returned HTTP {status}")
    return status


async def _viewport_websocket_screen(
    grant: ModalViewportGrant,
    label: str,
    *,
    instruction: str = "",
) -> str:
    """Read or steer one ttyd WebSocket without exposing its bearer URI."""

    sensitive_browser_url = grant.browser_url
    parsed = urlsplit(sensitive_browser_url)
    sensitive_websocket_url = urlunsplit(
        (
            "wss",
            parsed.netloc,
            f"{parsed.path.rstrip('/')}/ws",
            parsed.query,
            "",
        )
    )
    websocket: Any = None
    try:
        async with connect(
            sensitive_websocket_url,
            subprotocols=("tty",),
            open_timeout=30,
            close_timeout=5,
            logger=_VIEWPORT_LOGGER,
        ) as websocket:
            await websocket.send(
                json.dumps(
                    {"AuthToken": "", "columns": 220, "rows": 50},
                    separators=(",", ":"),
                )
            )
            if instruction:
                await websocket.send(b"0" + instruction.encode())
            chunks: list[str] = []
            enter_sent = False
            for _index in range(64):
                message = await asyncio.wait_for(websocket.recv(), timeout=15)
                payload = message.encode() if isinstance(message, str) else bytes(message)
                if payload[:1] == b"0":
                    chunks.append(payload[1:].decode(errors="replace"))
                screen = "".join(chunks)
                if not instruction and screen:
                    return screen
                if not enter_sent and "LIVE OPERATOR STEER" in screen:
                    # Codex TUI coalesces paste bursts. Submit Enter in a later
                    # input frame only after the full text has rendered and
                    # the pre-submit terminal stream has gone quiet.
                    await asyncio.sleep(1)
                    while True:
                        try:
                            queued = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=0.05,
                            )
                        except TimeoutError:
                            break
                        queued_payload = (
                            queued.encode() if isinstance(queued, str) else bytes(queued)
                        )
                        if queued_payload[:1] == b"0":
                            chunks.append(queued_payload[1:].decode(errors="replace"))
                    await websocket.send(b"0\r")
                    enter_sent = True
                    continue
                if enter_sent and payload[:1] == b"0" and payload[1:]:
                    # A post-Enter terminal redraw proves the submit event was
                    # processed; pre-submit composer rendering is insufficient.
                    return screen
            raise RuntimeError("ttyd screen marker was not observed")
    except Exception as exc:
        error_name = type(exc).__name__
        sensitive_browser_url = ""
        sensitive_websocket_url = ""
        parsed = None
        websocket = None
        raise AssertionError(
            f"authenticated {label} viewport WebSocket failed ({error_name})"
        ) from None
    finally:
        sensitive_browser_url = ""
        sensitive_websocket_url = ""
        parsed = None
        websocket = None
    raise AssertionError(f"authenticated {label} viewport returned no terminal output")


def _safe_agent_output_summary(value: str) -> str:
    known_markers = {
        _FINAL_MESSAGE,
        "LIVE_MODAL_MISSING_STEER",
        "LIVE_MODAL_CREDENTIAL_EXPOSED",
    }
    return bounded_text_summary(value, allowlisted_markers=known_markers)


def _safe_agent_failure_summary(observation: AgentExecutionResult) -> str:
    return summarize_agent_failure(
        status=observation.status.value,
        returncode=observation.agent_returncode,
        stdout=observation.agent_stdout,
        stderr=observation.agent_stderr,
        error=observation.error,
        friction_messages=(finding.message for finding in observation.friction),
        allowlisted_stdout=(
            _FINAL_MESSAGE,
            "LIVE_MODAL_MISSING_STEER",
            "LIVE_MODAL_CREDENTIAL_EXPOSED",
        ),
    )


async def _create_modal_session(
    backend: ModalSandboxBackend,
    *,
    evidence: str,
) -> ModalSandboxSession:
    session = await backend.create(
        SandboxSpec(
            provider="modal",
            environment=backend.environment,
            workdir="/workspace/repo",
            timeout_seconds=12 * 60,
            idle_timeout_seconds=5 * 60,
            metadata=(("evidence", evidence),),
        )
    )
    assert isinstance(session, ModalSandboxSession)
    return session


async def _preflight_codex_subscription(backend: ModalSandboxBackend) -> None:
    """Prove the exact namespace and subscription before the repository mission."""

    session = await _create_modal_session(backend, evidence="modal-codex-auth-preflight")
    try:
        workspace = await _remote_exec(session, "mkdir", "-p", "/workspace/repo")
        _assert_success(workspace, "create Modal Codex preflight workspace")
        observation = await run_codex_app_server_turn(
            session,
            connector=ModalCodexAppServerConnector(),
            workspace="/workspace/repo",
            prompt=(
                "This is an authentication preflight. Do not call tools. Reply with exactly "
                f"{_PREFLIGHT_MESSAGE} and nothing else."
            ),
            timeout_seconds=90,
        )
        if observation.returncode != 0 or observation.stdout.strip() != _PREFLIGHT_MESSAGE:
            summary = summarize_agent_failure(
                status="exited",
                returncode=observation.returncode,
                stdout=observation.stdout,
                stderr=observation.stderr,
                allowlisted_stdout=(_PREFLIGHT_MESSAGE,),
            )
            raise AssertionError(f"Modal Codex authentication preflight failed: {summary}")
        credential_absent = await _remote_exec(
            session,
            "test",
            "!",
            "-e",
            "/root/.codex/auth.json",
        )
        _assert_success(credential_absent, "verify preflight credential cleanup")
    finally:
        await session.close()

    assert await session.status() is SandboxStatus.CLOSED


async def test_modal_codex_harness_steers_validates_publishes_and_cleans_up() -> None:
    """Prove the full real harness without mutating an external GitHub repository."""

    config = ModalSandboxConfig(
        app_name=os.environ.get("CODING_AGENT_MODAL_APP") or "archetype-agent-missions",
        image_id=os.environ.get("CODING_AGENT_MODAL_IMAGE_ID", ""),
        auth_volume_name=_required_environment("CODEX_AUTH_VOLUME"),
        github_secret_name=_required_environment("CODING_AGENT_GITHUB_SECRET"),
        workspace_name=_required_environment("CODING_AGENT_MODAL_WORKSPACE"),
        environment_name=_required_environment("CODING_AGENT_MODAL_ENVIRONMENT"),
        heartbeat_seconds=5,
    )
    # The explicit live-test opt-in above authorizes use of either the selected
    # authenticated workstation profile or MODAL_TOKEN_ID/MODAL_TOKEN_SECRET.
    # The release scenario registry separately requires the token pair in CI.

    backend = ModalSandboxBackend(config)
    await _preflight_codex_subscription(backend)
    session = await _create_modal_session(backend, evidence="modal-agent-mission-live")
    harness_task: asyncio.Task[AgentExecutionResult] | None = None
    try:
        initialized = await _remote_exec(
            session,
            "sh",
            "-c",
            """
set -eu
rm -rf -- /workspace/repo /workspace/seed /workspace/origin.git
git init -q -b main /workspace/seed
git -C /workspace/seed config user.name 'Archetype Release Evidence'
git -C /workspace/seed config user.email 'release-evidence@example.invalid'
printf '%s\n' seed > /workspace/seed/README.md
git -C /workspace/seed add README.md
git -C /workspace/seed commit -qm seed
git clone -q --bare /workspace/seed /workspace/origin.git
git --git-dir=/workspace/origin.git rev-parse refs/heads/main
rm -rf -- /workspace/seed
""".strip(),
            timeout_seconds=60,
        )
        seed_revision = _assert_success(
            initialized, "initialize provider-local bare remote"
        ).strip()

        initial_prompt = (
            "This is bounded live release evidence. Before any other tool call, run exactly "
            "`test ! -e /root/.codex/auth.json && sleep 75`. If that command returns nonzero, "
            "make no changes and reply LIVE_MODAL_CREDENTIAL_EXPOSED. Do not edit any file "
            "until the command finishes. An operator will steer this active turn through the "
            "attached Codex TUI while it sleeps. After the sleep, follow the latest operator "
            "instruction exactly. If no operator instruction arrived, make no changes and "
            "reply LIVE_MODAL_MISSING_STEER."
        )
        driver = CodexAppServerDriver(
            connector=ModalCodexAppServerConnector(),
            workspace="/workspace/repo",
            timeout_seconds=4 * 60,
        )
        harness = CodingAgentHarness(
            driver,
            CodingAgentHarnessConfig(
                workspace="/workspace/repo",
                agent_timeout_seconds=5 * 60,
                github_secret_name=config.github_secret_name,
            ),
        )
        harness_task = asyncio.create_task(harness.execute(session, _dispatch(initial_prompt)))

        await _wait_for_takeover(session, harness_task)
        monitor_stdout = io.StringIO()
        monitor_stderr = io.StringIO()
        monitor_status = await ModalSandboxSession.monitor(
            session.identity.sandbox_id,
            follow=False,
            stdout_target=monitor_stdout,
            stderr_target=monitor_stderr,
        )
        if (
            monitor_status.get("type") != "session_ready"
            or monitor_status.get("operation") != "codex"
            or monitor_status.get("sandbox_id") != session.identity.sandbox_id
            or '"type": "session_ready"' not in monitor_stdout.getvalue()
        ):
            raise AssertionError("direct Modal monitor missed the active Codex takeover lane")

        spectate, takeover = await asyncio.gather(
            ModalSandboxSession.issue_spectate_grant(session.identity.sandbox_id),
            ModalSandboxSession.issue_takeover_grant(session.identity.sandbox_id),
        )
        assert spectate.mode is ModalViewportMode.SPECTATE
        assert takeover.mode is ModalViewportMode.TAKEOVER
        assert spectate.grant_id != takeover.grant_id
        token_fingerprints = {
            hashlib.sha256(grant.token.encode()).digest() for grant in (spectate, takeover)
        }
        assert len(token_fingerprints) == 2
        viewport_statuses = await asyncio.gather(
            asyncio.to_thread(_authenticated_viewport_status, spectate, "spectate"),
            asyncio.to_thread(_authenticated_viewport_status, takeover, "takeover"),
        )
        assert all(200 <= status < 400 for status in viewport_statuses)
        spectate_screen = await _viewport_websocket_screen(spectate, "spectate")
        if not spectate_screen:
            raise AssertionError("authenticated spectate viewport returned no terminal output")

        instruction = (
            "LIVE OPERATOR STEER: after the current sleep, create modal-live-proof.txt in the "
            f"repository with exactly this one line: {_PROOF.rstrip()!r}. Read the file back, "
            f"then reply with exactly {_FINAL_MESSAGE!r} and nothing else."
        )
        steered_screen = await _viewport_websocket_screen(
            takeover,
            "takeover",
            instruction=instruction,
        )
        if "LIVE OPERATOR STEER" not in steered_screen:
            raise AssertionError(
                "the writable tmux lane did not render the submitted operator input"
            )

        observation = await asyncio.wait_for(harness_task, timeout=5 * 60)
        if observation.status is not AgentExecutionStatus.EXITED:
            summary = _safe_agent_failure_summary(observation)
            raise AssertionError(f"the live repository harness did not exit normally: {summary}")
        if observation.agent_returncode != 0:
            summary = _safe_agent_failure_summary(observation)
            raise AssertionError(f"the live Codex app-server turn returned nonzero: {summary}")
        if len(observation.validation) != 1 or not observation.validation[0].passed:
            summary = _safe_agent_output_summary(observation.agent_stdout)
            raise AssertionError(
                f"the exact post-completion validator did not pass; agent_output={summary}"
            )
        if not observation.commits or not all(commit.pushed for commit in observation.commits):
            raise AssertionError("the harness did not record pushed commit evidence")
        final_commits = [commit for commit in observation.commits if commit.final_revision]
        if len(final_commits) != 1 or final_commits[0].sha != observation.final_revision:
            raise AssertionError("the pushed commit evidence did not bind the final revision")
        if observation.agent_stdout.strip() != _FINAL_MESSAGE:
            summary = _safe_agent_output_summary(observation.agent_stdout)
            raise AssertionError(
                f"the live Codex app-server returned an unexpected marker: {summary}"
            )

        teardown = await _remote_exec(
            session,
            "sh",
            "-c",
            """
set -eu
test "$(cat /workspace/repo/modal-live-proof.txt)" = "archetype-modal-live-steer-v1"
test -z "$(git -C /workspace/repo status --porcelain)"
test ! -e /root/.codex/auth.json
test "$(git --git-dir="$2" rev-parse refs/heads/main)" = "$3"
test "$(git --git-dir="$2" rev-parse "$4")" = "$5"
test "$(git --git-dir="$2" show "$4":modal-live-proof.txt)" = \
    "archetype-modal-live-steer-v1"
for socket in "$1"/executions/*/tmux.sock; do
    [ -e "$socket" ] || continue
    if tmux -S "$socket" has-session 2>/dev/null; then
        echo "interactive tmux server survived exact-turn completion" >&2
        exit 1
    fi
done
cat /workspace/repo/modal-live-proof.txt
""".strip(),
            "verify-modal-cleanup",
            _OBSERVATION_ROOT,
            _REMOTE,
            seed_revision,
            f"refs/heads/{_BRANCH}",
            observation.final_revision,
            timeout_seconds=60,
        )
        assert _assert_success(teardown, "verify exact output and teardown") == _PROOF
        assert await session.status() is SandboxStatus.READY
    finally:
        if harness_task is not None:
            if not harness_task.done():
                harness_task.cancel()
            await asyncio.gather(harness_task, return_exceptions=True)
        await session.close()

    assert await session.status() is SandboxStatus.CLOSED
