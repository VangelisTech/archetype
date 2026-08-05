# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact-thread contracts for the steerable Codex app-server seam."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from archetype.missions import CommandValidator, RepositoryPublicationPolicy
from archetype.missions.coding_agents.app_server import (
    CodexAppServerClient,
    CodexAppServerConnection,
    CodexAppServerDriver,
    CodexAppServerError,
    CodexThread,
    CodexTurn,
    CodexTurnCompletion,
    CodexTurnCompletionBarrierError,
)
from archetype.missions.coding_agents.contracts import (
    DispatchedValidator,
    TaskDispatchRequest,
)
from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxSession,
    SandboxStatus,
)


class _Transport:
    def __init__(self) -> None:
        self.sent: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.incoming: asyncio.Queue[dict[str, Any] | BaseException] = asyncio.Queue()
        self.closed = False

    async def send(self, message: dict[str, Any]) -> None:
        await self.sent.put(message)

    async def receive(self) -> dict[str, Any]:
        incoming = await self.incoming.get()
        if isinstance(incoming, BaseException):
            raise incoming
        return incoming

    async def aclose(self) -> None:
        self.closed = True


async def _initialize(
    client: CodexAppServerClient,
    transport: _Transport,
) -> None:
    task = asyncio.create_task(client.initialize())
    request = await transport.sent.get()
    assert request == {
        "method": "initialize",
        "id": 1,
        "params": {
            "clientInfo": {
                "name": "archetype_agent_missions",
                "title": "Archetype Agent Missions",
                "version": "1",
            },
            "capabilities": {"experimentalApi": True},
        },
    }
    await transport.incoming.put(
        {
            "id": 1,
            "result": {
                "userAgent": "codex",
                "codexHome": "/root/.codex",
                "platformFamily": "unix",
                "platformOs": "linux",
            },
        }
    )
    await task
    assert await transport.sent.get() == {"method": "initialized", "params": {}}


async def _respond(
    transport: _Transport,
    task: asyncio.Task[Any],
    *,
    method: str,
    result: dict[str, Any],
) -> tuple[dict[str, Any], Any]:
    request = await transport.sent.get()
    assert request["method"] == method
    await transport.incoming.put({"id": request["id"], "result": result})
    return request, await task


@pytest.mark.asyncio
async def test_handshake_thread_and_exact_turn_completion() -> None:
    transport = _Transport()
    notifications: list[str] = []
    client = CodexAppServerClient(
        transport,
        observer=lambda method, _params: notifications.append(method),
    )
    await _initialize(client, transport)

    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    request, thread = await _respond(
        transport,
        starting,
        method="thread/start",
        result={"thread": {"id": "thr-1"}},
    )
    assert request["params"] == {
        "cwd": "/workspace/repo",
        "approvalPolicy": "never",
        "sandbox": "danger-full-access",
    }
    assert thread == CodexThread("thr-1")

    starting_turn = asyncio.create_task(client.start_turn(thread, "Fix the failing test."))
    request, turn = await _respond(
        transport,
        starting_turn,
        method="turn/start",
        result={"turn": {"id": "turn-1", "status": "inProgress"}},
    )
    assert request["params"] == {
        "threadId": "thr-1",
        "input": [{"type": "text", "text": "Fix the failing test."}],
    }
    assert turn == CodexTurn("thr-1", "turn-1")

    unrelated = CodexTurn("thr-1", "turn-other")
    unrelated_wait = asyncio.create_task(client.wait_for_turn(unrelated))
    completion_wait = asyncio.create_task(client.wait_for_turn(turn))
    await transport.incoming.put(
        {
            "method": "item/completed",
            "params": {
                "threadId": "thr-1",
                "turnId": "turn-1",
                "completedAtMs": 1,
                "item": {"type": "agentMessage", "id": "item-1", "text": "Done."},
            },
        }
    )
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": "thr-1",
                "turn": {
                    "id": "turn-1",
                    "status": "completed",
                    "items": [],
                },
            },
        }
    )

    completion = await asyncio.wait_for(completion_wait, timeout=1)
    assert completion.thread_id == "thr-1"
    assert completion.turn_id == "turn-1"
    assert completion.status == "completed"
    assert completion.final_message == "Done."
    assert unrelated_wait.done() is False
    unrelated_wait.cancel()
    await asyncio.gather(unrelated_wait, return_exceptions=True)
    assert notifications == ["item/completed", "turn/completed"]
    await client.aclose()


@pytest.mark.asyncio
async def test_steer_is_bound_to_expected_active_turn() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    await _initialize(client, transport)
    turn = CodexTurn("thr-1", "turn-1")

    steering = asyncio.create_task(
        client.steer(
            turn,
            "Focus on the cancellation race.",
            client_message_id="operator-7",
        )
    )
    request, _ = await _respond(
        transport,
        steering,
        method="turn/steer",
        result={"turnId": "turn-1"},
    )

    assert request["params"] == {
        "threadId": "thr-1",
        "expectedTurnId": "turn-1",
        "input": [{"type": "text", "text": "Focus on the cancellation race."}],
        "clientUserMessageId": "operator-7",
    }
    await client.aclose()


@pytest.mark.asyncio
async def test_cancelled_waiter_can_rejoin_same_turn() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    await _initialize(client, transport)
    turn = CodexTurn("thr-1", "turn-1")

    first = asyncio.create_task(client.wait_for_turn(turn))
    await asyncio.sleep(0)
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    second = asyncio.create_task(client.wait_for_turn(turn))
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": "thr-1",
                "turn": {"id": "turn-1", "status": "interrupted", "items": []},
            },
        }
    )

    assert (await asyncio.wait_for(second, timeout=1)).status == "interrupted"
    await client.aclose()


@pytest.mark.asyncio
async def test_cancelled_rpc_caller_retains_and_consumes_late_response() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    await _initialize(client, transport)

    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    request = await transport.sent.get()
    starting.cancel()
    await asyncio.gather(starting, return_exceptions=True)
    await transport.incoming.put(
        {
            "id": request["id"],
            "error": {"code": -32000, "message": "late provider response"},
        }
    )
    await asyncio.sleep(0)

    assert client._reader is not None  # noqa: SLF001 - transport-liveness oracle
    assert client._reader.done() is False  # noqa: SLF001
    await client.aclose()


@pytest.mark.asyncio
async def test_terminal_reader_failure_fails_active_and_future_requests() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    await _initialize(client, transport)

    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    request = await transport.sent.get()
    assert request["method"] == "thread/start"
    await transport.incoming.put(OSError("app-server socket closed"))

    with pytest.raises(CodexAppServerError, match=r"transport failed \(OSError\)"):
        await starting
    with pytest.raises(CodexAppServerError, match=r"transport failed \(OSError\)"):
        await client.start_thread(cwd="/workspace/repo")

    assert transport.sent.empty()
    await client.aclose()
    assert transport.closed is True


@pytest.mark.asyncio
async def test_observer_failure_cannot_hide_turn_completion() -> None:
    transport = _Transport()

    def broken_observer(_method: str, _params: object) -> None:
        raise RuntimeError("viewport unavailable")

    client = CodexAppServerClient(transport, observer=broken_observer)
    await _initialize(client, transport)
    turn = CodexTurn("thr-1", "turn-1")
    waiting = asyncio.create_task(client.wait_for_turn(turn))
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": "thr-1",
                "turn": {"id": "turn-1", "status": "completed", "items": []},
            },
        }
    )

    assert (await asyncio.wait_for(waiting, timeout=1)).status == "completed"
    await client.aclose()


@pytest.mark.asyncio
async def test_thread_observer_is_a_pre_turn_readiness_barrier() -> None:
    transport = _Transport()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def prepare_viewport(thread: CodexThread) -> None:
        assert thread == CodexThread("thr-1")
        entered.set()
        await release.wait()

    client = CodexAppServerClient(transport, thread_observer=prepare_viewport)
    await _initialize(client, transport)
    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    request = await transport.sent.get()
    assert request["method"] == "thread/start"
    await transport.incoming.put({"id": request["id"], "result": {"thread": {"id": "thr-1"}}})
    await asyncio.wait_for(entered.wait(), timeout=1)

    assert starting.done() is False
    assert transport.sent.empty()

    release.set()
    thread = await asyncio.wait_for(starting, timeout=1)
    assert thread == CodexThread("thr-1")
    await client.aclose()


@pytest.mark.asyncio
async def test_thread_viewport_failure_prevents_turn_admission() -> None:
    transport = _Transport()

    async def broken_viewport(_thread: CodexThread) -> None:
        raise RuntimeError("Codex TUI never became ready")

    client = CodexAppServerClient(transport, thread_observer=broken_viewport)
    await _initialize(client, transport)
    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    request = await transport.sent.get()
    await transport.incoming.put({"id": request["id"], "result": {"thread": {"id": "thr-1"}}})

    with pytest.raises(RuntimeError, match="never became ready"):
        await starting
    assert transport.sent.empty()
    await client.aclose()


@pytest.mark.asyncio
async def test_completion_barrier_closes_input_before_exact_waiter_is_released() -> None:
    transport = _Transport()
    barrier_entered = asyncio.Event()
    barrier_release = asyncio.Event()
    ordering: list[str] = []

    async def close_interactive(completion: CodexTurnCompletion) -> None:
        assert completion.thread_id == "thr-1"
        assert completion.turn_id == "turn-1"
        ordering.append("close-started")
        barrier_entered.set()
        await barrier_release.wait()
        ordering.append("closed")

    client = CodexAppServerClient(transport, completion_barrier=close_interactive)
    await _initialize(client, transport)
    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    _, thread = await _respond(
        transport,
        starting,
        method="thread/start",
        result={"thread": {"id": "thr-1"}},
    )
    starting_turn = asyncio.create_task(client.start_turn(thread, "Do the work."))
    _, turn = await _respond(
        transport,
        starting_turn,
        method="turn/start",
        result={"turn": {"id": "turn-1", "status": "inProgress"}},
    )
    waiting = asyncio.create_task(client.wait_for_turn(turn))
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": "thr-1",
                "turn": {"id": "turn-1", "status": "completed", "items": []},
            },
        }
    )
    await asyncio.wait_for(barrier_entered.wait(), timeout=1)

    assert waiting.done() is False
    barrier_release.set()
    assert (await asyncio.wait_for(waiting, timeout=1)).status == "completed"
    ordering.append("validators-may-start")
    assert ordering == ["close-started", "closed", "validators-may-start"]
    await client.aclose()


@pytest.mark.asyncio
async def test_completion_barrier_failure_preserves_turn_and_reader_liveness() -> None:
    transport = _Transport()
    barrier_entered = asyncio.Event()
    barrier_release = asyncio.Event()

    async def fail_to_quiesce(_completion: CodexTurnCompletion) -> None:
        barrier_entered.set()
        await barrier_release.wait()
        raise RuntimeError("interactive descendants survived")

    client = CodexAppServerClient(transport, completion_barrier=fail_to_quiesce)
    await _initialize(client, transport)
    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    _, thread = await _respond(
        transport,
        starting,
        method="thread/start",
        result={"thread": {"id": "thr-1"}},
    )
    starting_turn = asyncio.create_task(client.start_turn(thread, "Do the work."))
    _, turn = await _respond(
        transport,
        starting_turn,
        method="turn/start",
        result={"turn": {"id": "turn-1", "status": "inProgress"}},
    )
    first_waiter = asyncio.create_task(client.wait_for_turn(turn))
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": turn.thread_id,
                "turn": {
                    "id": turn.turn_id,
                    "status": "completed",
                    "items": [
                        {
                            "type": "agentMessage",
                            "id": "message-1",
                            "text": "Committed the fix.",
                        }
                    ],
                },
            },
        }
    )
    await asyncio.wait_for(barrier_entered.wait(), timeout=1)

    rejoined_waiter = asyncio.create_task(client.wait_for_turn(turn))
    await asyncio.sleep(0)
    assert first_waiter.done() is False
    assert rejoined_waiter.done() is False
    expected = CodexTurnCompletion(
        "thr-1",
        "turn-1",
        "completed",
        final_message="Committed the fix.",
    )
    assert client._completed_turns[(turn.thread_id, turn.turn_id)] == expected  # noqa: SLF001

    barrier_release.set()
    failures = await asyncio.wait_for(
        asyncio.gather(first_waiter, rejoined_waiter, return_exceptions=True),
        timeout=1,
    )
    assert len(failures) == 2
    for failure in failures:
        assert isinstance(failure, CodexTurnCompletionBarrierError)
        assert failure.completion == expected
        assert isinstance(failure.barrier_error, RuntimeError)

    with pytest.raises(CodexTurnCompletionBarrierError) as rejoined_after_failure:
        await client.wait_for_turn(turn)
    assert rejoined_after_failure.value.completion == expected

    later = CodexTurn("thr-1", "turn-2")
    later_waiter = asyncio.create_task(client.wait_for_turn(later))
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": later.thread_id,
                "turn": {"id": later.turn_id, "status": "completed", "items": []},
            },
        }
    )
    assert (await asyncio.wait_for(later_waiter, timeout=1)).status == "completed"
    assert client._terminal_failure is None  # noqa: SLF001 - reader-liveness oracle
    await client.aclose()


@pytest.mark.asyncio
async def test_completion_barrier_ignores_turns_not_started_by_this_controller() -> None:
    transport = _Transport()
    closed: list[CodexTurnCompletion] = []

    async def close_interactive(completion: CodexTurnCompletion) -> None:
        closed.append(completion)

    client = CodexAppServerClient(transport, completion_barrier=close_interactive)
    await _initialize(client, transport)
    other = CodexTurn("thr-other", "turn-other")
    waiting = asyncio.create_task(client.wait_for_turn(other))
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": other.thread_id,
                "turn": {"id": other.turn_id, "status": "completed", "items": []},
            },
        }
    )

    assert (await asyncio.wait_for(waiting, timeout=1)).status == "completed"
    assert closed == []
    await client.aclose()


@pytest.mark.asyncio
async def test_fast_completion_never_opens_takeover_after_terminal_fact() -> None:
    transport = _Transport()
    observed: list[CodexTurn] = []

    async def open_takeover(turn: CodexTurn) -> None:
        observed.append(turn)

    client = CodexAppServerClient(transport, turn_observer=open_takeover)
    await _initialize(client, transport)
    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    _, thread = await _respond(
        transport,
        starting,
        method="thread/start",
        result={"thread": {"id": "thr-1"}},
    )
    starting_turn = asyncio.create_task(client.start_turn(thread, "Do the work."))
    request = await transport.sent.get()
    transport.incoming.put_nowait(
        {
            "id": request["id"],
            "result": {"turn": {"id": "turn-1", "status": "inProgress"}},
        }
    )
    transport.incoming.put_nowait(
        {
            "method": "turn/completed",
            "params": {
                "threadId": "thr-1",
                "turn": {"id": "turn-1", "status": "completed", "items": []},
            },
        }
    )

    turn = await asyncio.wait_for(starting_turn, timeout=1)
    assert observed == []
    assert (await client.wait_for_turn(turn)).status == "completed"
    await client.aclose()


@pytest.mark.asyncio
async def test_viewport_failure_cannot_block_an_already_started_turn() -> None:
    transport = _Transport()
    observed: list[CodexTurn] = []

    async def broken_viewport(turn: CodexTurn) -> None:
        observed.append(turn)
        raise RuntimeError("ttyd unavailable")

    client = CodexAppServerClient(transport, turn_observer=broken_viewport)
    await _initialize(client, transport)
    starting = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    _, thread = await _respond(
        transport,
        starting,
        method="thread/start",
        result={"thread": {"id": "thr-1"}},
    )
    assert observed == []

    starting_turn = asyncio.create_task(client.start_turn(thread, "Do the work."))
    _, turn = await _respond(
        transport,
        starting_turn,
        method="turn/start",
        result={"turn": {"id": "turn-1", "status": "inProgress"}},
    )

    assert turn == CodexTurn("thr-1", "turn-1")
    assert observed == [turn]
    await client.aclose()


@pytest.mark.asyncio
async def test_rpc_error_fails_closed() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    await _initialize(client, transport)

    task = asyncio.create_task(client.start_thread(cwd="/workspace/repo"))
    request = await transport.sent.get()
    await transport.incoming.put(
        {
            "id": request["id"],
            "error": {"code": -32602, "message": "invalid sandbox policy"},
        }
    )

    with pytest.raises(CodexAppServerError, match="invalid sandbox policy"):
        await task
    await client.aclose()


class _Session:
    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("test", "sandbox-1", "environment-1")

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities()

    async def status(self) -> SandboxStatus:
        return SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        raise AssertionError(f"app-server driver must not fall back to batch exec: {request}")

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError

    async def close(self) -> None:
        return None


class _Connector:
    def __init__(self, client: CodexAppServerClient) -> None:
        self.client = client

    @asynccontextmanager
    async def connect(self, session: SandboxSession):
        del session
        try:
            yield CodexAppServerConnection(
                self.client,
                trace_uri="modal-sandbox://sb-1/executions/trace/stdout.log",
            )
        finally:
            await self.client.aclose()


class _CleanupRetryConnector:
    def __init__(
        self,
        client: CodexAppServerClient,
        cleanup_retry: Callable[[], Awaitable[None]],
    ) -> None:
        self.client = client
        self.cleanup_retry = cleanup_retry

    @asynccontextmanager
    async def connect(self, session: SandboxSession):
        del session
        try:
            yield CodexAppServerConnection(
                self.client,
                trace_uri="modal-sandbox://sb-1/executions/trace/stdout.log",
            )
        finally:
            try:
                await self.cleanup_retry()
            finally:
                await self.client.aclose()


class _StalledConnector:
    def __init__(self) -> None:
        self.closed = False

    @asynccontextmanager
    async def connect(self, session: SandboxSession):
        del session
        try:
            await asyncio.Event().wait()
            raise AssertionError("stalled connector unexpectedly acquired")
            yield  # pragma: no cover
        finally:
            self.closed = True


def _request(*, previous_session_id: str = "") -> TaskDispatchRequest:
    return TaskDispatchRequest(
        mission_id=1,
        task_id=2,
        task_name="implementation",
        dispatch_id="dispatch-1",
        dispatch_sequence=1,
        repository="VangelisTech/archetype",
        branch="agent/app-server",
        base_ref="main",
        prompt="Fix it.",
        validators=(
            DispatchedValidator(
                validator_id=3,
                spec=CommandValidator("tests", ("pytest", "-q")),
            ),
        ),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
        previous_agent_session_id=previous_session_id,
    )


async def _serve_completed_driver_turn(
    transport: _Transport,
    *,
    final_message: str,
) -> None:
    initialize = await transport.sent.get()
    await transport.incoming.put({"id": initialize["id"], "result": {}})
    assert await transport.sent.get() == {"method": "initialized", "params": {}}
    thread = await transport.sent.get()
    await transport.incoming.put({"id": thread["id"], "result": {"thread": {"id": "thr-cleanup"}}})
    turn = await transport.sent.get()
    await transport.incoming.put(
        {
            "id": turn["id"],
            "result": {"turn": {"id": "turn-cleanup", "status": "inProgress"}},
        }
    )
    await transport.incoming.put(
        {
            "method": "turn/completed",
            "params": {
                "threadId": "thr-cleanup",
                "turn": {
                    "id": "turn-cleanup",
                    "status": "completed",
                    "items": [
                        {
                            "type": "agentMessage",
                            "id": "message-cleanup",
                            "text": final_message,
                        }
                    ],
                },
            },
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_session_id", ["", "thr-existing"])
async def test_driver_uses_app_server_completion_not_pty_output(
    previous_session_id: str,
) -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    driver = CodexAppServerDriver(_Connector(client))

    async def server() -> None:
        initialize = await transport.sent.get()
        await transport.incoming.put({"id": initialize["id"], "result": {}})
        assert await transport.sent.get() == {"method": "initialized", "params": {}}
        thread = await transport.sent.get()
        assert thread["method"] == "thread/start"
        thread_id = "thr-new"
        await transport.incoming.put({"id": thread["id"], "result": {"thread": {"id": thread_id}}})
        turn = await transport.sent.get()
        assert turn["method"] == "turn/start"
        await transport.incoming.put(
            {
                "id": turn["id"],
                "result": {"turn": {"id": "turn-1", "status": "inProgress"}},
            }
        )
        await transport.incoming.put(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": thread_id,
                    "turn": {
                        "id": "turn-1",
                        "status": "completed",
                        "items": [
                            {"type": "agentMessage", "id": "message-1", "text": "Implemented."}
                        ],
                    },
                },
            }
        )

    server_task = asyncio.create_task(server())
    observed = await driver.run(
        _Session(), _request(previous_session_id=previous_session_id), "Go."
    )
    await server_task

    assert observed.returncode == 0
    assert observed.session_id == "thr-new"
    assert observed.stdout == "Implemented."
    assert observed.stderr == ""
    assert observed.trace_uri == "modal-sandbox://sb-1/executions/trace/stdout.log"


@pytest.mark.asyncio
async def test_driver_releases_completion_only_after_cleanup_retry_succeeds() -> None:
    transport = _Transport()
    cleanup_calls = 0
    cleanup_retry_entered = asyncio.Event()
    cleanup_retry_release = asyncio.Event()

    async def finish_interactive() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        if cleanup_calls == 1:
            raise RuntimeError("interactive descendants survived")
        cleanup_retry_entered.set()
        await cleanup_retry_release.wait()

    async def completion_barrier(_completion: CodexTurnCompletion) -> None:
        await finish_interactive()

    client = CodexAppServerClient(transport, completion_barrier=completion_barrier)
    driver = CodexAppServerDriver(_CleanupRetryConnector(client, finish_interactive))
    server_task = asyncio.create_task(
        _serve_completed_driver_turn(transport, final_message="Committed the fix.")
    )
    running = asyncio.create_task(driver.run(_Session(), _request(), "Go."))
    await asyncio.wait_for(cleanup_retry_entered.wait(), timeout=1)

    assert running.done() is False
    assert cleanup_calls == 2
    cleanup_retry_release.set()
    observed = await asyncio.wait_for(running, timeout=1)
    await server_task

    assert observed.returncode == 0
    assert observed.stdout == "Committed the fix."
    assert observed.session_id == "thr-cleanup"
    assert transport.closed is True


@pytest.mark.asyncio
async def test_driver_propagates_persistent_cleanup_failure() -> None:
    transport = _Transport()
    cleanup_calls = 0

    async def finish_interactive() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        raise RuntimeError(f"interactive descendants survived attempt {cleanup_calls}")

    async def completion_barrier(_completion: CodexTurnCompletion) -> None:
        await finish_interactive()

    client = CodexAppServerClient(transport, completion_barrier=completion_barrier)
    driver = CodexAppServerDriver(_CleanupRetryConnector(client, finish_interactive))
    server_task = asyncio.create_task(
        _serve_completed_driver_turn(transport, final_message="Committed but unsafe.")
    )

    with pytest.raises(RuntimeError, match="survived attempt 2"):
        await asyncio.wait_for(
            driver.run(_Session(), _request(), "Go."),
            timeout=1,
        )
    await server_task

    assert cleanup_calls == 2
    assert transport.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stalled_phase", "expected_phase"),
    [
        ("initialize", "initialize"),
        ("thread", "thread/start"),
        ("turn", "turn/start"),
    ],
)
async def test_driver_bounds_silent_app_server_admission_phases(
    stalled_phase: str,
    expected_phase: str,
) -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    driver = CodexAppServerDriver(_Connector(client), timeout_seconds=1)

    async def server() -> None:
        initialize = await transport.sent.get()
        assert initialize["method"] == "initialize"
        if stalled_phase == "initialize":
            return
        await transport.incoming.put({"id": initialize["id"], "result": {}})
        assert await transport.sent.get() == {"method": "initialized", "params": {}}

        thread = await transport.sent.get()
        assert thread["method"] == "thread/start"
        if stalled_phase == "thread":
            return
        await transport.incoming.put(
            {"id": thread["id"], "result": {"thread": {"id": "thr-timeout"}}}
        )

        turn = await transport.sent.get()
        assert turn["method"] == "turn/start"

    server_task = asyncio.create_task(server())
    with pytest.raises(
        CodexAppServerError,
        match=rf"timed out during {expected_phase}",
    ):
        await asyncio.wait_for(
            driver.run(_Session(), _request(), "Go."),
            timeout=2,
        )
    await server_task

    assert transport.closed is True


@pytest.mark.asyncio
async def test_driver_bounds_connector_acquisition_and_runs_its_cleanup() -> None:
    connector = _StalledConnector()
    driver = CodexAppServerDriver(connector, timeout_seconds=1)

    with pytest.raises(CodexAppServerError, match=r"timed out during connect"):
        await asyncio.wait_for(
            driver.run(_Session(), _request(), "Go."),
            timeout=2,
        )

    assert connector.closed is True


@pytest.mark.asyncio
async def test_driver_bounds_timeout_triggered_interrupt_by_same_exchange_deadline() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    driver = CodexAppServerDriver(_Connector(client), timeout_seconds=1)

    async def server() -> None:
        initialize = await transport.sent.get()
        await transport.incoming.put({"id": initialize["id"], "result": {}})
        assert await transport.sent.get() == {"method": "initialized", "params": {}}
        thread = await transport.sent.get()
        await transport.incoming.put(
            {"id": thread["id"], "result": {"thread": {"id": "thr-timeout"}}}
        )
        turn = await transport.sent.get()
        await transport.incoming.put(
            {
                "id": turn["id"],
                "result": {"turn": {"id": "turn-timeout", "status": "inProgress"}},
            }
        )
        interrupt = await asyncio.wait_for(transport.sent.get(), timeout=1)
        assert interrupt == {
            "method": "turn/interrupt",
            "id": 4,
            "params": {
                "threadId": "thr-timeout",
                "turnId": "turn-timeout",
            },
        }

    server_task = asyncio.create_task(server())
    with pytest.raises(CodexAppServerError, match=r"timed out during turn/interrupt"):
        await asyncio.wait_for(
            driver.run(_Session(), _request(), "Go."),
            timeout=2,
        )
    await server_task

    assert transport.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt_outcome", ["hang", "reject"])
async def test_exact_terminal_completion_wins_interrupt_race(
    interrupt_outcome: str,
) -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)
    driver = CodexAppServerDriver(_Connector(client), timeout_seconds=1)

    async def server() -> None:
        initialize = await transport.sent.get()
        await transport.incoming.put({"id": initialize["id"], "result": {}})
        assert await transport.sent.get() == {"method": "initialized", "params": {}}
        thread = await transport.sent.get()
        await transport.incoming.put({"id": thread["id"], "result": {"thread": {"id": "thr-race"}}})
        turn = await transport.sent.get()
        await transport.incoming.put(
            {
                "id": turn["id"],
                "result": {"turn": {"id": "turn-race", "status": "inProgress"}},
            }
        )
        interrupt = await asyncio.wait_for(transport.sent.get(), timeout=1)
        assert interrupt["method"] == "turn/interrupt"
        if interrupt_outcome == "reject":
            await transport.incoming.put(
                {
                    "id": interrupt["id"],
                    "error": {
                        "code": -32000,
                        "message": "turn already completed",
                    },
                }
            )
        await transport.incoming.put(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thr-race",
                    "turn": {
                        "id": "turn-race",
                        "status": "completed",
                        "items": [
                            {
                                "type": "agentMessage",
                                "id": "message-race",
                                "text": "Finished at the deadline.",
                            }
                        ],
                    },
                },
            }
        )

    server_task = asyncio.create_task(server())
    observed = await asyncio.wait_for(
        driver.run(_Session(), _request(), "Go."),
        timeout=2,
    )
    await server_task

    assert observed.returncode == 0
    assert observed.stdout == "Finished at the deadline."
    assert observed.session_id == "thr-race"
    assert transport.closed is True


def test_thread_policy_fields_cannot_be_overridden() -> None:
    transport = _Transport()
    client = CodexAppServerClient(transport)

    with pytest.raises(ValueError, match="controlled fields"):
        client._thread_params(  # noqa: SLF001 - focused policy construction contract
            cwd=str(Path("/workspace/repo")),
            model="",
            options={"approvalPolicy": "untrusted"},
        )
