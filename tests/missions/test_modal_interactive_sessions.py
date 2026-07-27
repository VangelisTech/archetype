# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic contracts for Modal's steerable Codex terminal substrate."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from archetype.missions.coding_agents.app_server import (
    CodexThread,
    CodexTurn,
    CodexTurnCompletion,
)
from archetype.missions.sandboxes import (
    ModalSandboxSession,
    ModalViewportGrant,
    ModalViewportMode,
    ProcessRequest,
    ProcessResult,
    RepositoryPublicationRequest,
    SandboxEventType,
    SandboxSpec,
    SandboxStatus,
)
from archetype.missions.sandboxes.modal import (
    _PUBLICATION_MEASUREMENT_SCRIPT,
    _UNIX_WEBSOCKET_BRIDGE,
    _ModalAppServerTransport,
)


class _AsyncCall:
    def __init__(self, callback) -> None:
        self._callback = callback

    async def aio(self, *args: Any, **kwargs: Any) -> Any:
        return self._callback(*args, **kwargs)


class _ConnectTokenEndpoint:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def aio(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        port = int(kwargs["port"])
        return SimpleNamespace(
            url=f"https://sandbox-connect.example/{port}",
            token=f"secret/{port}?operator & spectator",
        )


class _GrantSandbox:
    def __init__(self) -> None:
        self.create_connect_token = _ConnectTokenEndpoint()


@pytest.mark.asyncio
async def test_viewport_grants_are_port_scoped_and_redact_bearer_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = _GrantSandbox()
    looked_up: list[str] = []

    async def from_id(sandbox_id: str) -> _GrantSandbox:
        looked_up.append(sandbox_id)
        return sandbox

    monkeypatch.setitem(
        sys.modules,
        "modal",
        SimpleNamespace(
            Sandbox=SimpleNamespace(
                from_id=SimpleNamespace(aio=from_id),
            )
        ),
    )

    spectate = await ModalSandboxSession.issue_spectate_grant("sb-live_1")
    takeover = await ModalSandboxSession.issue_takeover_grant("sb-live_1")

    assert looked_up == ["sb-live_1", "sb-live_1"]
    assert [call["port"] for call in sandbox.create_connect_token.calls] == [7681, 7682]
    assert [call["user_metadata"]["mode"] for call in sandbox.create_connect_token.calls] == [
        "spectate",
        "takeover",
    ]
    assert [call["user_metadata"]["schema"] for call in sandbox.create_connect_token.calls] == [
        1,
        1,
    ]
    assert spectate.mode is ModalViewportMode.SPECTATE
    assert takeover.mode is ModalViewportMode.TAKEOVER
    assert spectate.grant_id == sandbox.create_connect_token.calls[0]["user_metadata"]["grant_id"]
    assert takeover.grant_id == sandbox.create_connect_token.calls[1]["user_metadata"]["grant_id"]
    assert spectate.grant_id != takeover.grant_id

    for grant, port in ((spectate, 7681), (takeover, 7682)):
        assert grant.token not in repr(grant)
        assert "_modal_connect_token" not in repr(grant)
        assert grant.browser_url == (
            f"https://sandbox-connect.example/{port}/"
            f"?_modal_connect_token=secret%2F{port}%3Foperator%20%26%20spectator"
        )

    existing_query = ModalViewportGrant(
        sandbox_id="sb-live_1",
        mode=ModalViewportMode.SPECTATE,
        base_url="https://sandbox-connect.example/7681?region=us-west",
        token="secret",
        grant_id="a" * 32,
    )
    assert existing_query.browser_url == (
        "https://sandbox-connect.example/7681/?region=us-west&_modal_connect_token=secret"
    )


def test_viewport_grant_rejects_invalid_or_insecure_capabilities() -> None:
    with pytest.raises(ValueError, match="sb-"):
        ModalViewportGrant(
            sandbox_id="not-a-sandbox",
            mode=ModalViewportMode.SPECTATE,
            base_url="https://sandbox-connect.example",
            token="secret",
            grant_id="a" * 32,
        )
    with pytest.raises(ValueError, match="HTTPS"):
        ModalViewportGrant(
            sandbox_id="sb-live",
            mode=ModalViewportMode.SPECTATE,
            base_url="http://sandbox-connect.example",
            token="secret",
            grant_id="a" * 32,
        )


class _WriteTextEndpoint:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def aio(self, value: str, path: str) -> None:
        self.calls.append((value, path))


class _RawExecEndpoint:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []
        self.result = object()

    async def aio(self, *argv: str, **kwargs: Any) -> object:
        self.calls.append((argv, kwargs))
        return self.result


def _session() -> tuple[ModalSandboxSession, Any, _WriteTextEndpoint, _RawExecEndpoint]:
    write_text = _WriteTextEndpoint()
    raw_exec = _RawExecEndpoint()
    sandbox = SimpleNamespace(
        object_id="sb-interactive",
        filesystem=SimpleNamespace(write_text=write_text),
        exec=raw_exec,
    )
    session = ModalSandboxSession(
        spec=SandboxSpec(
            "modal",
            "modal-image://im-reviewed",
            "/workspace/repo",
            timeout_seconds=3600,
        ),
        sandbox=sandbox,
        auth_sandbox=SimpleNamespace(object_id="sb-auth"),
        github_secret=object(),
        auth_volume_name="codex-auth",
        checkpoint_timeout_seconds=120,
        checkpoint_ttl_seconds=3600,
        heartbeat_seconds=15,
    )
    return session, sandbox, write_text, raw_exec


def _tmux_session_request(
    requests: list[ProcessRequest],
    *,
    socket: str,
    session_name: str,
) -> ProcessRequest:
    expected = ("tmux", "-S", socket, "new-session", "-d", "-s", session_name)
    return next(request for request in requests if request.argv[:7] == expected)


@pytest.mark.asyncio
async def test_modal_publication_transfers_exact_bundle_to_non_agent_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, sandbox, _writes, _raw_exec = _session()
    revision = "a" * 40
    bundle = b"exact-git-bundle"
    auth_sandbox = session._auth_sandbox  # noqa: SLF001 - provider-boundary oracle
    calls: list[tuple[object, ProcessRequest, list[object]]] = []
    transfers: list[tuple[str, str, int]] = []
    digest = hashlib.sha256(bundle).hexdigest()

    async def transfer(
        *,
        source: str,
        destination: str,
        timeout_seconds: int,
    ) -> tuple[int, str]:
        transfers.append((source, destination, timeout_seconds))
        return len(bundle), digest

    async def execute(
        selected_sandbox: object,
        request: ProcessRequest,
        *,
        secrets: list[object] | None = None,
    ) -> ProcessResult:
        calls.append((selected_sandbox, request, list(secrets or ())))
        if request.argv[:2] == ("sh", "-c") and "sha256sum" in request.argv[2]:
            stdout = f"{digest} {len(bundle)}\n"
        elif "rev-parse" in request.argv:
            stdout = f"{revision}\n"
        else:
            stdout = ""
        return ProcessResult(request.argv, 0, stdout=stdout)

    monkeypatch.setattr(session, "_exec_on", execute)
    monkeypatch.setattr(session, "_stream_publication_bundle", transfer)
    result = await session.publish_repository(
        RepositoryPublicationRequest(
            repository="https://github.com/VangelisTech/archetype.git",
            branch_ref="refs/heads/agent/secure-publication",
            revision=revision,
            worktree="/workspace/repo",
            timeout_seconds=300,
        )
    )

    assert result.returncode == 0
    assert len(transfers) == 1
    mission_path, broker_path, timeout = transfers[0]
    assert mission_path.endswith("/validated.bundle")
    assert broker_path.endswith("/validated.bundle")
    assert timeout == 300
    secret_calls = [call for call in calls if call[2]]
    assert len(secret_calls) == 1
    selected, push, secrets = secret_calls[0]
    assert selected is auth_sandbox
    assert "push" in push.argv
    assert secrets == [session._github_secret]  # noqa: SLF001
    assert push.secret_names == ()
    assert all(not call_secrets for owner, _request, call_secrets in calls if owner is sandbox)
    assert "https://github.com/VangelisTech/archetype.git" in push.argv
    assert not any("exact-git-bundle" in argument for argument in push.argv)


@pytest.mark.asyncio
async def test_modal_publication_cancellation_survives_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, sandbox, _writes, _raw_exec = _session()
    revision = "a" * 40
    bundle = b"exact-git-bundle"
    digest = hashlib.sha256(bundle).hexdigest()
    publication_started = asyncio.Event()
    cleanup_started = asyncio.Event()

    async def transfer(**_kwargs: Any) -> tuple[int, str]:
        publication_started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled publication transfer resumed")

    async def execute(
        selected_sandbox: object,
        request: ProcessRequest,
        **_kwargs: Any,
    ) -> ProcessResult:
        if request.argv[:3] == ("rm", "-rf", "--") and selected_sandbox is sandbox:
            cleanup_started.set()
            return ProcessResult(request.argv, 1, stderr="mission cleanup failed")
        if request.argv[:2] == ("sh", "-c") and "sha256sum" in request.argv[2]:
            return ProcessResult(
                request.argv,
                0,
                stdout=f"{digest} {len(bundle)}\n",
            )
        if "rev-parse" in request.argv:
            return ProcessResult(request.argv, 0, stdout=f"{revision}\n")
        return ProcessResult(request.argv, 0)

    monkeypatch.setattr(session, "_stream_publication_bundle", transfer)
    monkeypatch.setattr(session, "_exec_on", execute)

    publication = asyncio.create_task(
        session.publish_repository(
            RepositoryPublicationRequest(
                repository="https://github.com/VangelisTech/archetype.git",
                branch_ref="refs/heads/agent/cancelled-publication",
                revision=revision,
                worktree="/workspace/repo",
                timeout_seconds=300,
            )
        )
    )
    await asyncio.wait_for(publication_started.wait(), timeout=1)
    publication.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)

    with pytest.raises(asyncio.CancelledError) as caught:
        await publication

    assert isinstance(caught.value.__cause__, BaseExceptionGroup)
    assert "failed to clean repository publication state" in str(caught.value.__cause__)


def test_publication_measurement_shell_preserves_the_bundle_path(tmp_path) -> None:
    bundle = tmp_path / "validated.bundle"
    bundle.write_bytes(b"exact-git-bundle")

    measured = subprocess.run(
        [
            "sh",
            "-c",
            _PUBLICATION_MEASUREMENT_SCRIPT,
            "measure-publication-bundle",
            str(bundle),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert measured.stdout.strip().split() == [
        hashlib.sha256(bundle.read_bytes()).hexdigest(),
        str(bundle.stat().st_size),
    ]


@pytest.mark.asyncio
async def test_interactive_topology_uses_real_codex_tui_and_exact_lane_permissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, sandbox, writes, raw_exec = _session()
    requests: list[ProcessRequest] = []
    events: list[tuple[SandboxEventType, dict[str, Any]]] = []

    async def execute(
        selected_sandbox: object,
        request: ProcessRequest,
        **kwargs: Any,
    ) -> ProcessResult:
        assert selected_sandbox is sandbox
        assert not kwargs
        requests.append(request)
        return ProcessResult(request.argv, 0)

    async def checked(request: ProcessRequest) -> ProcessResult:
        requests.append(request)
        return ProcessResult(request.argv, 0)

    async def emit(kind: SandboxEventType, **kwargs: Any) -> None:
        events.append((kind, kwargs))

    monkeypatch.setattr(session, "_exec_on", execute)
    monkeypatch.setattr(session, "_checked", checked)
    monkeypatch.setattr(session, "_emit_event_best_effort", emit)

    state = session._interactive_state("a" * 32)  # noqa: SLF001
    await session._prepare_interactive_session(state)  # noqa: SLF001
    proxy = await session._start_app_server_proxy(state)  # noqa: SLF001
    await session._start_codex_tui(  # noqa: SLF001
        state,
        CodexThread("thread-exact"),
    )
    assert not any(
        request.argv[:7]
        == (
            "tmux",
            "-S",
            state.tmux_socket,
            "new-session",
            "-d",
            "-s",
            state.takeover_session,
        )
        for request in requests
    )
    assert state.tui_started is True
    assert state.spectate_started is True
    assert state.takeover_started is False
    assert events[-1] == (
        SandboxEventType.SESSION_READY,
        {
            "operation": "codex-spectate",
            "message": "thread_id=thread-exact",
        },
    )
    await session._start_codex_takeover(  # noqa: SLF001
        state,
        CodexTurn("thread-exact", "turn-exact"),
    )

    assert proxy is raw_exec.result
    assert state.directory.endswith("/executions/" + "a" * 32)
    assert state.app_socket == f"{state.directory}/app-server.sock"
    assert state.tmux_socket == f"{state.directory}/tmux.sock"
    assert writes.calls[0][1] == f"{state.directory}/tmux-events.sh"
    assert '"event":"%s"' in writes.calls[0][0]
    assert writes.calls[1] == (
        _UNIX_WEBSOCKET_BRIDGE,
        f"{state.directory}/unix-websocket-bridge.py",
    )
    compile(_UNIX_WEBSOCKET_BRIDGE, "unix-websocket-bridge.py", "exec")

    setup = next(
        request
        for request in requests
        if request.argv[:4] == ("tmux", "-S", state.tmux_socket, "start-server")
    )
    assert ("exit-empty", "off") == (
        setup.argv[setup.argv.index("exit-empty")],
        setup.argv[setup.argv.index("exit-empty") + 1],
    )
    assert ("remain-on-exit", "on") == (
        setup.argv[setup.argv.index("remain-on-exit")],
        setup.argv[setup.argv.index("remain-on-exit") + 1],
    )
    assert ("prefix", "None") == (
        setup.argv[setup.argv.index("prefix")],
        setup.argv[setup.argv.index("prefix") + 1],
    )
    assert ("prefix2", "None") == (
        setup.argv[setup.argv.index("prefix2")],
        setup.argv[setup.argv.index("prefix2") + 1],
    )
    assert ("unbind-key", "-a") == (
        setup.argv[setup.argv.index("unbind-key")],
        setup.argv[setup.argv.index("unbind-key") + 1],
    )
    assert all(
        hook in setup.argv
        for hook in ("client-attached", "client-detached", "pane-died", "session-closed")
    )

    app = _tmux_session_request(
        requests,
        socket=state.tmux_socket,
        session_name=state.app_session,
    )
    assert app.argv[7:13] == ("-x", "220", "-y", "50", "-c", "/workspace/repo")
    assert app.argv[13:19] == (
        "env",
        "CODEX_HOME=/root/.codex",
        "NO_COLOR=1",
        "ARCHETYPE_MODAL_INTERACTIVE_SESSION=" + "a" * 32,
        "codex",
        "app-server",
    )
    assert app.argv[19:].count("-c") == 3
    assert app.argv[-2:] == ("--listen", f"unix://{state.app_socket}")
    assert not any(argument.startswith("tcp://") for argument in app.argv)

    proxy_argv, proxy_kwargs = raw_exec.calls[0]
    assert proxy_argv[-4:] == (
        "python3",
        f"{state.directory}/unix-websocket-bridge.py",
        "--socket",
        state.app_socket,
    )
    assert proxy_kwargs["workdir"] == "/workspace/repo"
    assert proxy_kwargs["timeout"] == 3600
    assert proxy_kwargs["env"] == {"CODEX_HOME": "/root/.codex", "NO_COLOR": "1"}

    tui = _tmux_session_request(
        requests,
        socket=state.tmux_socket,
        session_name=state.tui_session,
    )
    assert tui.argv[13:] == (
        "env",
        "CODEX_HOME=/root/.codex",
        "NO_COLOR=1",
        "ARCHETYPE_MODAL_INTERACTIVE_SESSION=" + "a" * 32,
        "codex",
        "resume",
        "--remote",
        f"unix://{state.app_socket}",
        "--no-alt-screen",
        "--dangerously-bypass-approvals-and-sandbox",
        "-C",
        "/workspace/repo",
        "thread-exact",
    )
    assert any(
        request.argv[:2] == ("sh", "-c")
        and "capture-pane" in request.argv[2]
        and "esctointerrupt" in request.argv[2]
        and '"$stable" -ge 3' in request.argv[2]
        and request.argv[-3] == "wait-codex-tui"
        and request.argv[-2:] == (state.tmux_socket, state.tui_session)
        for request in requests
    )

    spectate = _tmux_session_request(
        requests,
        socket=state.tmux_socket,
        session_name=state.spectate_session,
    )
    takeover = _tmux_session_request(
        requests,
        socket=state.tmux_socket,
        session_name=state.takeover_session,
    )
    assert spectate.argv[7:14] == (
        "env",
        "ARCHETYPE_MODAL_INTERACTIVE_SESSION=" + "a" * 32,
        "ttyd",
        "--port",
        "7681",
        "--interface",
        "0.0.0.0",
    )
    assert "--writable" not in spectate.argv
    assert "--max-clients" not in spectate.argv
    assert spectate.argv[-7:] == (
        "tmux",
        "-S",
        state.tmux_socket,
        "attach-session",
        "-r",
        "-t",
        state.tui_session,
    )
    assert takeover.argv[7:19] == (
        "env",
        "ARCHETYPE_MODAL_INTERACTIVE_SESSION=" + "a" * 32,
        "ttyd",
        "--port",
        "7682",
        "--interface",
        "0.0.0.0",
        "--writable",
        "--max-clients",
        "1",
        "tmux",
        "-S",
    )
    assert "-r" not in takeover.argv
    assert takeover.argv[-4:] == (
        state.tmux_socket,
        "attach-session",
        "-t",
        state.tui_session,
    )
    assert state.tui_started is True
    assert state.spectate_started is True
    assert state.takeover_started is True
    assert events[-1] == (
        SandboxEventType.SESSION_READY,
        {
            "operation": "codex",
            "message": "thread_id=thread-exact turn_id=turn-exact",
        },
    )

    recorded_sessions = {
        request.argv[request.argv.index("-t") + 1]
        for request in requests
        if request.argv[:4] == ("tmux", "-S", state.tmux_socket, "pipe-pane")
    }
    assert recorded_sessions == {state.app_session, state.tui_session}

    with pytest.raises(RuntimeError, match="already attached"):
        await session._start_codex_tui(  # noqa: SLF001
            state,
            CodexThread("thread-other"),
        )

    kill_count = sum(
        request.argv == ("tmux", "-S", state.tmux_socket, "kill-server") for request in requests
    )
    await session._finish_codex_turn(  # noqa: SLF001
        state,
        CodexTurnCompletion("thread-exact", "turn-exact", "completed"),
    )
    assert state.stopped is True
    assert events[-1] == (
        SandboxEventType.PROCESS_FINISHED,
        {"operation": "codex-app-server"},
    )
    await session._stop_interactive_session(state)  # noqa: SLF001
    assert (
        sum(
            request.argv == ("tmux", "-S", state.tmux_socket, "kill-server") for request in requests
        )
        == kill_count + 1
    )
    quiescence = next(
        request
        for request in requests
        if request.argv[:2] == ("sh", "-c") and request.argv[-2] == "quiesce-interactive-processes"
    )
    assert quiescence.argv[-1] == ("ARCHETYPE_MODAL_INTERACTIVE_SESSION=" + "a" * 32)
    assert "kill -TERM" in quiescence.argv[2]
    assert "kill -KILL" in quiescence.argv[2]


@pytest.mark.asyncio
async def test_modal_app_server_wires_exact_post_thread_oauth_scrub_before_turn_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, _sandbox, _writes, _raw_exec = _session()
    order: list[str] = []
    callbacks: dict[str, Any] = {}

    class _Client:
        def __init__(self, _transport: object, **kwargs: Any) -> None:
            callbacks.update(kwargs)

        async def aclose(self) -> None:
            order.append("client-close")

    async def stage() -> None:
        order.append("stage-auth")

    async def prepare(_state: object) -> None:
        order.append("prepare-app-server")

    async def scrub() -> None:
        order.append("scrub-auth-json")

    async def start_tui(_state: object, _thread: CodexThread) -> None:
        order.append("start-tui")

    async def start_takeover(_state: object, _turn: CodexTurn) -> None:
        order.append("start-takeover")

    async def stop(state: Any) -> None:
        state.stopped = True
        order.append("stop-interactive")

    async def remove() -> None:
        order.append("remove-codex-home")

    async def capture(_trace_id: str) -> str:
        return "modal-sandbox://sb-interactive/trace"

    async def heartbeat() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(
        "archetype.missions.coding_agents.app_server.CodexAppServerClient",
        _Client,
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.modal._ModalAppServerTransport",
        lambda _process: object(),
    )
    monkeypatch.setattr(session, "_stage_oauth", stage)
    monkeypatch.setattr(session, "_prepare_interactive_session", prepare)
    monkeypatch.setattr(session, "_start_app_server_proxy", lambda _state: _async_value(object()))
    monkeypatch.setattr(session, "_scrub_mission_oauth", scrub)
    monkeypatch.setattr(session, "_start_codex_tui", start_tui)
    monkeypatch.setattr(session, "_start_codex_takeover", start_takeover)
    monkeypatch.setattr(session, "_stop_interactive_session", stop)
    monkeypatch.setattr(session, "_remove_oauth", remove)
    monkeypatch.setattr(session, "_capture_live_output_best_effort", capture)
    monkeypatch.setattr(session, "_heartbeat", heartbeat)

    async with session.codex_app_server():
        await callbacks["thread_observer"](CodexThread("thread-exact"))
        assert order == ["stage-auth", "prepare-app-server", "scrub-auth-json"]
        await callbacks["turn_observer"](CodexTurn("thread-exact", "turn-exact"))
        assert order[-2:] == ["start-tui", "start-takeover"]

    assert order[-2:] == ["stop-interactive", "remove-codex-home"]


@pytest.mark.asyncio
async def test_modal_app_server_cancellation_survives_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, _sandbox, _writes, _raw_exec = _session()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    class _Client:
        def __init__(self, _transport: object, **_kwargs: Any) -> None:
            pass

        async def aclose(self) -> None:
            cleanup_started.set()
            await release_cleanup.wait()
            raise RuntimeError("client cleanup failed")

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        pass

    async def capture(_trace_id: str) -> str:
        return ""

    async def heartbeat() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(
        "archetype.missions.coding_agents.app_server.CodexAppServerClient",
        _Client,
    )
    monkeypatch.setattr(
        "archetype.missions.sandboxes.modal._ModalAppServerTransport",
        lambda _process: object(),
    )
    monkeypatch.setattr(session, "_stage_oauth", noop)
    monkeypatch.setattr(session, "_prepare_interactive_session", noop)
    monkeypatch.setattr(session, "_start_app_server_proxy", lambda _state: _async_value(object()))
    monkeypatch.setattr(session, "_stop_interactive_session", noop)
    monkeypatch.setattr(session, "_remove_oauth", noop)
    monkeypatch.setattr(session, "_capture_live_output_best_effort", capture)
    monkeypatch.setattr(session, "_heartbeat", heartbeat)

    async def open_and_close() -> None:
        async with session.codex_app_server():
            pass

    driver = asyncio.create_task(open_and_close())
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    driver.cancel()
    await asyncio.sleep(0)
    assert not driver.done()
    driver.cancel()
    await asyncio.sleep(0)
    assert not driver.done()
    release_cleanup.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await driver

    assert isinstance(caught.value.__cause__, BaseExceptionGroup)
    assert "failed to close Modal interactive session" in str(caught.value.__cause__)
    assert await session.status() is SandboxStatus.ERRORED


async def _async_value(value: Any) -> Any:
    return value


@pytest.mark.asyncio
async def test_modal_completion_barrier_fails_closed_when_descendants_do_not_quiesce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, sandbox, _writes, _raw_exec = _session()
    requests: list[ProcessRequest] = []

    async def execute(
        selected_sandbox: object,
        request: ProcessRequest,
        **_kwargs: Any,
    ) -> ProcessResult:
        assert selected_sandbox is sandbox
        requests.append(request)
        if request.argv[-2:-1] == ("quiesce-interactive-processes",):
            return ProcessResult(request.argv, 1, stderr="interactive descendants survived: 42")
        return ProcessResult(request.argv, 0)

    monkeypatch.setattr(session, "_exec_on", execute)
    state = session._interactive_state("b" * 32)  # noqa: SLF001

    with pytest.raises(RuntimeError, match="detached interactive processes"):
        await session._finish_codex_turn(  # noqa: SLF001
            state,
            CodexTurnCompletion("thread-exact", "turn-exact", "completed"),
        )

    assert requests[0].argv == ("tmux", "-S", state.tmux_socket, "kill-server")
    assert requests[1].argv[-2:] == (
        "quiesce-interactive-processes",
        "ARCHETYPE_MODAL_INTERACTIVE_SESSION=" + "b" * 32,
    )
    assert state.stopped is False


class _InputEndpoint:
    def __init__(self) -> None:
        self.values: list[bytes] = []
        self.eof = False
        self.drains = 0
        self.drain = SimpleNamespace(aio=self._drain)

    def write(self, value: bytes) -> None:
        self.values.append(value)

    async def _drain(self) -> None:
        self.drains += 1

    def write_eof(self) -> None:
        self.eof = True


class _ChunkStream:
    def __init__(self, chunks: list[bytes | str]) -> None:
        self._chunks = iter(chunks)
        self.yielded = 0
        self.read = SimpleNamespace(aio=_ReadEndpoint(b"").aio)

    def __aiter__(self) -> _ChunkStream:
        return self

    async def __anext__(self) -> bytes | str:
        try:
            value = next(self._chunks)
            self.yielded += 1
            return value
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _ReadEndpoint:
    def __init__(self, value: bytes | str) -> None:
        self._value = value

    async def aio(self) -> bytes | str:
        return self._value


class _WaitEndpoint:
    def __init__(self, returncode: int = 0) -> None:
        self.calls = 0
        self.returncode = returncode

    async def aio(self) -> int:
        self.calls += 1
        return self.returncode


class _StreamingProcess:
    def __init__(
        self,
        chunks: list[bytes | str],
        *,
        returncode: int = 0,
        stderr: bytes = b"",
    ) -> None:
        self.stdin = _InputEndpoint()
        self.stdout = _ChunkStream(chunks)
        self.stderr = SimpleNamespace(read=SimpleNamespace(aio=_ReadEndpoint(stderr).aio))
        self.wait = _WaitEndpoint(returncode)


@pytest.mark.asyncio
async def test_publication_bundle_stream_forwards_chunks_and_returns_exact_digest() -> None:
    session, sandbox, _writes, _raw_exec = _session()
    source = _StreamingProcess([b"exact", b"-bundle"])
    destination = _StreamingProcess([])
    sandbox.exec = _RawExecEndpoint()
    sandbox.exec.result = source
    auth_sandbox = session._auth_sandbox  # noqa: SLF001 - provider-boundary oracle
    auth_sandbox.exec = _RawExecEndpoint()
    auth_sandbox.exec.result = destination

    size, digest = await session._stream_publication_bundle(  # noqa: SLF001
        source="/tmp/source.bundle",
        destination="/tmp/destination.bundle",
        timeout_seconds=30,
    )

    assert size == len(b"exact-bundle")
    assert digest == hashlib.sha256(b"exact-bundle").hexdigest()
    assert destination.stdin.values == [b"exact", b"-bundle"]
    assert destination.stdin.eof is True
    assert source.wait.calls == 1
    assert destination.wait.calls == 1


@pytest.mark.asyncio
async def test_publication_bundle_reaps_destination_when_source_fails_to_start() -> None:
    session, sandbox, _writes, _raw_exec = _session()
    destination = _StreamingProcess([])
    auth_sandbox = session._auth_sandbox  # noqa: SLF001 - provider-boundary oracle
    auth_sandbox.exec = _RawExecEndpoint()
    auth_sandbox.exec.result = destination

    async def fail_source(*_args: Any, **_kwargs: Any) -> object:
        raise RuntimeError("mission exec unavailable")

    sandbox.exec = SimpleNamespace(aio=fail_source)

    with pytest.raises(RuntimeError, match="mission exec unavailable"):
        await session._stream_publication_bundle(  # noqa: SLF001
            source="/tmp/source.bundle",
            destination="/tmp/destination.bundle",
            timeout_seconds=30,
        )

    assert destination.stdin.eof is True
    assert destination.stdin.drains == 1
    assert destination.wait.calls == 1


@pytest.mark.asyncio
async def test_publication_bundle_cancellation_preserves_source_and_reap_failures() -> None:
    session, sandbox, _writes, _raw_exec = _session()
    destination = _StreamingProcess([])
    drain_started = asyncio.Event()
    release_drain = asyncio.Event()

    async def fail_drain() -> None:
        drain_started.set()
        await release_drain.wait()
        raise RuntimeError("destination reap failed")

    destination.stdin.drain = SimpleNamespace(aio=fail_drain)
    auth_sandbox = session._auth_sandbox  # noqa: SLF001 - provider-boundary oracle
    auth_sandbox.exec = _RawExecEndpoint()
    auth_sandbox.exec.result = destination

    async def fail_source(*_args: Any, **_kwargs: Any) -> object:
        raise RuntimeError("source start failed")

    sandbox.exec = SimpleNamespace(aio=fail_source)
    publication = asyncio.create_task(
        session._stream_publication_bundle(  # noqa: SLF001
            source="/tmp/source.bundle",
            destination="/tmp/destination.bundle",
            timeout_seconds=30,
        )
    )
    await asyncio.wait_for(drain_started.wait(), timeout=1)
    publication.cancel()
    await asyncio.sleep(0)
    assert not publication.done()
    release_drain.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await publication

    cause = caught.value.__cause__
    assert isinstance(cause, BaseExceptionGroup)
    source_error, reap_error = cause.exceptions
    assert str(source_error) == "source start failed"
    assert isinstance(reap_error, BaseExceptionGroup)
    assert "failed to close repository publication destination" in str(reap_error)
    assert [str(error) for error in reap_error.exceptions] == ["destination reap failed"]
    assert destination.stdin.eof is True
    assert destination.wait.calls == 1


@pytest.mark.asyncio
async def test_publication_bundle_stream_is_bounded_without_controller_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, sandbox, _writes, _raw_exec = _session()
    source = _StreamingProcess([b"1234", b"56789", b"must-not-be-read"])
    destination = _StreamingProcess([])
    sandbox.exec = _RawExecEndpoint()
    sandbox.exec.result = source
    auth_sandbox = session._auth_sandbox  # noqa: SLF001 - provider-boundary oracle
    auth_sandbox.exec = _RawExecEndpoint()
    auth_sandbox.exec.result = destination
    monkeypatch.setattr(
        "archetype.missions.sandboxes.modal._MAX_PUBLICATION_BUNDLE_BYTES",
        8,
    )

    with pytest.raises(RuntimeError, match="512 MiB controller-transfer limit"):
        await session._stream_publication_bundle(  # noqa: SLF001
            source="/tmp/source.bundle",
            destination="/tmp/destination.bundle",
            timeout_seconds=30,
        )

    assert source.stdout.yielded == 2
    assert destination.stdin.values == [b"1234"]
    assert destination.stdin.eof is True
    assert source.wait.calls == 0
    assert destination.wait.calls == 1
    source_call = sandbox.exec.calls[0]
    assert source_call[0][:4] == ("head", "-c", "9", "--")
    assert source_call[1]["text"] is False
    assert auth_sandbox.exec.calls[0][1]["text"] is False


@pytest.mark.asyncio
async def test_publication_rejects_bundle_changed_after_bounded_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, _sandbox, _writes, _raw_exec = _session()
    revision = "a" * 40
    transferred = hashlib.sha256(b"first").hexdigest()
    changed = hashlib.sha256(b"changed").hexdigest()
    pushed = False

    async def transfer(**_kwargs: Any) -> tuple[int, str]:
        return 5, transferred

    async def execute(
        _selected_sandbox: object,
        request: ProcessRequest,
        *,
        secrets: list[object] | None = None,
    ) -> ProcessResult:
        nonlocal pushed
        if secrets:
            pushed = True
        if request.argv[:2] == ("sh", "-c") and "sha256sum" in request.argv[2]:
            return ProcessResult(request.argv, 0, stdout=f"{changed} 7\n")
        return ProcessResult(request.argv, 0)

    monkeypatch.setattr(session, "_stream_publication_bundle", transfer)
    monkeypatch.setattr(session, "_exec_on", execute)

    with pytest.raises(RuntimeError, match="changed during bounded"):
        await session.publish_repository(
            RepositoryPublicationRequest(
                repository="https://github.com/VangelisTech/archetype.git",
                branch_ref="refs/heads/agent/secure-publication",
                revision=revision,
                worktree="/workspace/repo",
                timeout_seconds=300,
            )
        )

    assert pushed is False


@pytest.mark.asyncio
async def test_modal_app_server_transport_frames_fragmented_jsonl_and_closes_stdin() -> None:
    stdin = _InputEndpoint()
    wait = _WaitEndpoint()
    process = SimpleNamespace(
        stdin=stdin,
        stdout=_ChunkStream(
            [
                b'{"id":7,"res',
                b'ult":{"ok":true}}\n{"method":"turn/completed",',
                '"params":{"threadId":"thread-1"}}\n',
            ]
        ),
        stderr=SimpleNamespace(read=SimpleNamespace(aio=_ReadEndpoint(b"proxy exited").aio)),
        wait=wait,
    )
    transport = _ModalAppServerTransport(process)

    await transport.send({"params": {"z": 2}, "id": 9})
    assert stdin.values == [b'{"id":9,"params":{"z":2}}\n']
    assert stdin.drains == 1
    assert await transport.receive() == {"id": 7, "result": {"ok": True}}
    assert await transport.receive() == {
        "method": "turn/completed",
        "params": {"threadId": "thread-1"},
    }
    with pytest.raises(RuntimeError, match="proxy closed"):
        await transport.receive()

    await transport.aclose()
    assert stdin.eof is True
    assert wait.calls == 1
    with pytest.raises(RuntimeError, match="closed"):
        await transport.send({"id": 10})
