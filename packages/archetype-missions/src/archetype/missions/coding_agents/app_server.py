# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Codex app-server control plane for steerable mission executions.

The app-server remains the process and conversation authority. A terminal UI
may attach to the same server through tmux/ttyd, but PTY bytes are never parsed
to decide whether a turn completed. Completion, interruption, and steering are
bound to exact app-server thread and turn identities.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol, runtime_checkable

from archetype.missions.coding_agents.contracts import (
    AgentProcessObservation,
    TaskDispatchRequest,
)
from archetype.missions.sandboxes.contracts import SandboxSession

type JsonObject = dict[str, Any]
type AppServerNotificationObserver = Callable[[str, Mapping[str, Any]], None]
type AppServerThreadObserver = Callable[["CodexThread"], Awaitable[None]]
type AppServerTurnObserver = Callable[["CodexTurn"], Awaitable[None]]
type AppServerCompletionBarrier = Callable[["CodexTurnCompletion"], Awaitable[None]]


class CodexAppServerError(RuntimeError):
    """The app-server rejected a request or its transport failed."""


async def _await_app_server_phase[T](
    operation: Awaitable[T],
    *,
    deadline: float,
    phase: str,
    propagate_timeout: bool = False,
) -> T:
    """Await one phase against the caller's absolute monotonic deadline."""

    try:
        async with asyncio.timeout_at(deadline):
            return await operation
    except TimeoutError as exc:
        message = f"Codex app-server exchange timed out during {phase}"
        if propagate_timeout:
            raise TimeoutError(message) from exc
        raise CodexAppServerError(message) from exc


async def _interrupt_for_terminal_completion(
    client: CodexAppServerClient,
    turn: CodexTurn,
    *,
    deadline: float,
) -> CodexTurnCompletion:
    """Race best-effort interruption against exact terminal turn authority."""

    completion_task = asyncio.create_task(client.wait_for_turn(turn))
    interrupt_task = asyncio.create_task(client.interrupt(turn))
    interrupt_error: Exception | None = None
    try:
        try:
            async with asyncio.timeout_at(deadline):
                pending: set[asyncio.Task[Any]] = {
                    completion_task,
                    interrupt_task,
                }
                while True:
                    done, _ = await asyncio.wait(
                        pending,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if completion_task in done:
                        return completion_task.result()
                    if interrupt_task in done:
                        try:
                            interrupt_task.result()
                        except Exception as exc:
                            interrupt_error = exc
                        pending = {completion_task}
        except TimeoutError as exc:
            # The terminal notification can race the deadline cancellation.
            # Its exact factual result remains stronger than interrupt RPC
            # acknowledgement or rejection.
            if completion_task.done() and not completion_task.cancelled():
                return completion_task.result()
            if interrupt_error is not None:
                raise interrupt_error from exc
            phase = "turn/interrupt" if not interrupt_task.done() else "interrupted turn completion"
            raise CodexAppServerError(
                f"Codex app-server exchange timed out during {phase}"
            ) from exc
    finally:
        for task in (completion_task, interrupt_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(completion_task, interrupt_task, return_exceptions=True)


@runtime_checkable
class CodexAppServerTransport(Protocol):
    """One already-connected JSON message transport."""

    async def send(self, message: JsonObject) -> None: ...

    async def receive(self) -> JsonObject: ...

    async def aclose(self) -> None: ...


class CodexAppServerSocketTransport:
    """JSON adapter for an injected websocket-like connection.

    This adapter intentionally does not open a network connection. Provider
    code owns endpoint selection and authentication, then supplies a socket
    exposing ``send()``, ``recv()``, and ``close()``.
    """

    def __init__(self, socket: Any) -> None:
        self._socket = socket

    async def send(self, message: JsonObject) -> None:
        await self._socket.send(
            json.dumps(message, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        )

    async def receive(self) -> JsonObject:
        raw = await self._socket.recv()
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            value = json.loads(raw)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CodexAppServerError("Codex app-server returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise CodexAppServerError("Codex app-server message must be an object")
        return value

    async def aclose(self) -> None:
        await self._socket.close()


@dataclass(frozen=True, slots=True)
class CodexThread:
    """Exact app-server thread selected for one mission dispatch."""

    thread_id: str

    def __post_init__(self) -> None:
        if not self.thread_id.strip():
            raise ValueError("Codex app-server thread identity cannot be empty")


@dataclass(frozen=True, slots=True)
class CodexTurn:
    """Exact in-flight app-server turn that may accept steering."""

    thread_id: str
    turn_id: str

    def __post_init__(self) -> None:
        if not self.thread_id.strip() or not self.turn_id.strip():
            raise ValueError("Codex app-server turn requires thread and turn identities")


@dataclass(frozen=True, slots=True)
class CodexTurnCompletion:
    """Terminal factual result for one exact app-server turn."""

    thread_id: str
    turn_id: str
    status: str
    final_message: str = ""
    error: str = ""

    def __post_init__(self) -> None:
        if not self.thread_id.strip() or not self.turn_id.strip():
            raise ValueError("Codex turn completion requires thread and turn identities")
        if self.status not in {"completed", "interrupted", "failed"}:
            raise ValueError(f"unsupported Codex turn completion status {self.status!r}")


class CodexTurnCompletionBarrierError(CodexAppServerError):
    """Provider cleanup failed after one exact authoritative completion."""

    def __init__(
        self,
        completion: CodexTurnCompletion,
        barrier_error: Exception,
    ) -> None:
        self.completion = completion
        self.barrier_error = barrier_error
        super().__init__(
            "Codex completion barrier failed for "
            f"{completion.thread_id}/{completion.turn_id} "
            f"({type(barrier_error).__name__})"
        )


class CodexAppServerClient:
    """Concurrent JSON-RPC client with exact-turn completion routing."""

    def __init__(
        self,
        transport: CodexAppServerTransport,
        *,
        client_name: str = "archetype_agent_missions",
        client_title: str = "Archetype Agent Missions",
        client_version: str = "1",
        observer: AppServerNotificationObserver | None = None,
        thread_observer: AppServerThreadObserver | None = None,
        turn_observer: AppServerTurnObserver | None = None,
        completion_barrier: AppServerCompletionBarrier | None = None,
    ) -> None:
        for label, value in (
            ("client_name", client_name),
            ("client_title", client_title),
            ("client_version", client_version),
        ):
            if not value.strip():
                raise ValueError(f"Codex app-server {label} cannot be empty")
        self._transport = transport
        self._client_info = {
            "name": client_name,
            "title": client_title,
            "version": client_version,
        }
        self._observer = observer
        self._thread_observer = thread_observer
        self._turn_observer = turn_observer
        self._completion_barrier = completion_barrier
        self._next_id = 1
        self._send_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[JsonObject]] = {}
        self._pending_calls: dict[int, tuple[str, JsonObject]] = {}
        self._turn_waiters: dict[tuple[str, str], asyncio.Future[CodexTurnCompletion]] = {}
        self._completed_turns: dict[tuple[str, str], CodexTurnCompletion] = {}
        self._completion_barriers_pending: set[tuple[str, str]] = set()
        self._completion_barrier_failures: dict[
            tuple[str, str], CodexTurnCompletionBarrierError
        ] = {}
        self._completed_agent_items: dict[tuple[str, str, str], str] = {}
        self._last_agent_messages: dict[tuple[str, str], str] = {}
        self._controlled_turns: set[tuple[str, str]] = set()
        self._terminal_turns_seen: set[tuple[str, str]] = set()
        self._reader: asyncio.Task[None] | None = None
        self._initialized = False
        self._closed = False
        self._terminal_failure: CodexAppServerError | None = None

    async def initialize(self) -> Mapping[str, Any]:
        """Perform the mandatory one-time app-server handshake."""

        if self._initialized:
            raise RuntimeError("Codex app-server client is already initialized")
        self._ensure_reader()
        result = await self._request(
            "initialize",
            {
                "clientInfo": self._client_info,
                "capabilities": {"experimentalApi": True},
            },
        )
        await self._transport.send({"method": "initialized", "params": {}})
        self._initialized = True
        return result

    async def start_thread(
        self,
        *,
        cwd: str,
        model: str = "",
        options: Mapping[str, Any] | None = None,
    ) -> CodexThread:
        """Start one thread under the caller-supplied execution policy."""

        params = self._thread_params(cwd=cwd, model=model, options=options)
        thread = self._thread_from_result(await self._request_ready("thread/start", params))
        await self._observe_thread(thread)
        return thread

    async def resume_thread(
        self,
        thread_id: str,
        *,
        cwd: str,
        model: str = "",
        options: Mapping[str, Any] | None = None,
    ) -> CodexThread:
        """Resume one exact durable thread rather than selecting ``--last``."""

        thread = CodexThread(thread_id)
        params = {
            "threadId": thread.thread_id,
            **self._thread_params(cwd=cwd, model=model, options=options),
        }
        resumed = self._thread_from_result(await self._request_ready("thread/resume", params))
        if resumed != thread:
            raise CodexAppServerError("Codex app-server resumed a different thread")
        await self._observe_thread(resumed)
        return resumed

    async def start_turn(self, thread: CodexThread, prompt: str) -> CodexTurn:
        """Start one regular turn with an explicit thread identity."""

        if not prompt.strip():
            raise ValueError("Codex turn prompt cannot be empty")
        result = await self._request_ready(
            "turn/start",
            {
                "threadId": thread.thread_id,
                "input": [{"type": "text", "text": prompt}],
            },
        )
        turn = result.get("turn")
        if not isinstance(turn, dict):
            raise CodexAppServerError("Codex turn/start response has no turn")
        turn_id = turn.get("id")
        if not isinstance(turn_id, str) or not turn_id.strip():
            raise CodexAppServerError("Codex turn/start response has no turn identity")
        started = CodexTurn(thread.thread_id, turn_id)
        await self._observe_turn(started)
        return started

    async def steer(
        self,
        turn: CodexTurn,
        text: str,
        *,
        client_message_id: str = "",
    ) -> None:
        """Append input only to the exact active turn.

        ``expectedTurnId`` is always sent. This method cannot silently create a
        second turn after the mission turn has completed.
        """

        if not text.strip():
            raise ValueError("Codex steering text cannot be empty")
        params: JsonObject = {
            "threadId": turn.thread_id,
            "expectedTurnId": turn.turn_id,
            "input": [{"type": "text", "text": text}],
        }
        if client_message_id:
            params["clientUserMessageId"] = client_message_id
        result = await self._request_ready("turn/steer", params)
        if result.get("turnId") != turn.turn_id:
            raise CodexAppServerError("Codex steering was accepted by a different turn")

    async def interrupt(self, turn: CodexTurn) -> None:
        """Request interruption of one exact in-flight turn."""

        await self._request_ready(
            "turn/interrupt",
            {"threadId": turn.thread_id, "turnId": turn.turn_id},
        )

    async def wait_for_turn(self, turn: CodexTurn) -> CodexTurnCompletion:
        """Wait for the exact ``turn/completed`` notification.

        Caller cancellation never cancels the shared completion future, so a
        replacement observer can rejoin the same in-flight turn.
        Provider cleanup failure raises ``CodexTurnCompletionBarrierError``
        carrying the exact authoritative completion instead of erasing it.
        """

        key = (turn.thread_id, turn.turn_id)
        barrier_failure = self._completion_barrier_failures.get(key)
        if barrier_failure is not None:
            raise barrier_failure
        completed = self._completed_turns.get(key)
        if completed is not None and key not in self._completion_barriers_pending:
            return completed
        self._require_ready()
        waiter = self._turn_waiters.get(key)
        if waiter is None:
            waiter = asyncio.get_running_loop().create_future()
            self._turn_waiters[key] = waiter
        return await asyncio.shield(waiter)

    async def aclose(self) -> None:
        """Close the connection and fail every unresolved operation."""

        if self._closed:
            return
        self._closed = True
        failure = CodexAppServerError("Codex app-server client closed")
        self._fail_waiters(failure)
        reader = self._reader
        if reader is not None:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        await self._transport.aclose()

    async def _request_ready(self, method: str, params: JsonObject) -> JsonObject:
        self._require_ready()
        return await self._request(method, params)

    async def _request(self, method: str, params: JsonObject) -> JsonObject:
        if self._closed:
            raise CodexAppServerError("Codex app-server client is closed")
        if self._terminal_failure is not None:
            raise self._terminal_failure
        async with self._send_lock:
            if self._terminal_failure is not None:
                raise self._terminal_failure
            request_id = self._next_id
            self._next_id += 1
            future: asyncio.Future[JsonObject] = asyncio.get_running_loop().create_future()
            self._pending[request_id] = future
            self._pending_calls[request_id] = (method, params)
            try:
                await self._transport.send({"method": method, "id": request_id, "params": params})
            except BaseException:
                self._pending.pop(request_id, None)
                self._pending_calls.pop(request_id, None)
                raise
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # The request is already on the wire. Keep its response identity
            # registered so the reader does not treat a late reply as an
            # unknown response, but consume its eventual result because the
            # cancelled caller no longer owns an awaiter.
            future.add_done_callback(self._consume_abandoned_response)
            raise

    def _ensure_reader(self) -> None:
        if self._reader is None:
            self._reader = asyncio.create_task(self._read_messages())

    async def _read_messages(self) -> None:
        try:
            while True:
                message = await self._transport.receive()
                request_id = message.get("id")
                if isinstance(request_id, int) and "method" not in message:
                    self._resolve_response(request_id, message)
                    continue
                method = message.get("method")
                params = message.get("params", {})
                if not isinstance(method, str) or not isinstance(params, dict):
                    raise CodexAppServerError("Codex app-server message envelope is invalid")
                if isinstance(request_id, int):
                    await self._transport.send(
                        {
                            "id": request_id,
                            "error": {
                                "code": -32601,
                                "message": f"unsupported server request {method}",
                            },
                        }
                    )
                    continue
                await self._observe_notification(method, params)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            failure = (
                exc
                if isinstance(exc, CodexAppServerError)
                else CodexAppServerError(
                    f"Codex app-server transport failed ({type(exc).__name__})"
                )
            )
            self._terminal_failure = failure
            self._fail_waiters(failure)

    def _resolve_response(self, request_id: int, message: JsonObject) -> None:
        future = self._pending.pop(request_id, None)
        call = self._pending_calls.pop(request_id, None)
        if future is None or call is None:
            raise CodexAppServerError(f"Codex app-server returned unknown response id {request_id}")
        error = message.get("error")
        if error is not None:
            detail = error.get("message") if isinstance(error, dict) else str(error)
            future.set_exception(CodexAppServerError(f"Codex app-server request failed: {detail}"))
            return
        result = message.get("result")
        if not isinstance(result, dict):
            future.set_exception(CodexAppServerError("Codex app-server result must be an object"))
            return
        method, params = call
        if method == "turn/start":
            thread_id = params.get("threadId")
            turn = result.get("turn")
            turn_id = turn.get("id") if isinstance(turn, dict) else None
            if isinstance(thread_id, str) and isinstance(turn_id, str):
                # Record authority before waking start_turn: a terminal
                # notification can already be queued behind this response.
                self._controlled_turns.add((thread_id, turn_id))
        future.set_result(result)

    async def _observe_notification(self, method: str, params: JsonObject) -> None:
        if method == "item/completed":
            self._observe_completed_item(params)
        if method == "turn/completed":
            completion = self._completion(params)
            key = (completion.thread_id, completion.turn_id)
            observed_message = self._last_agent_messages.get(key, "")
            if (
                completion.final_message
                and observed_message
                and completion.final_message != observed_message
            ):
                raise CodexAppServerError(
                    "Codex app-server returned conflicting final agent messages"
                )
            if not completion.final_message and observed_message:
                completion = CodexTurnCompletion(
                    thread_id=completion.thread_id,
                    turn_id=completion.turn_id,
                    status=completion.status,
                    final_message=observed_message,
                    error=completion.error,
                )
            self._terminal_turns_seen.add(key)
            existing = self._completed_turns.get(key)
            if existing is not None and existing != completion:
                raise CodexAppServerError("Codex app-server returned conflicting turn completion")
            if existing is None:
                # Persist the app-server's authoritative fact before invoking
                # provider cleanup. A failed descendant-quiescence barrier is
                # operational substrate; it cannot erase this completion or
                # poison unrelated requests in the shared reader.
                self._completion_barriers_pending.add(key)
                self._completed_turns[key] = completion
                barrier_failure: CodexTurnCompletionBarrierError | None = None
                try:
                    if self._completion_barrier is not None and key in self._controlled_turns:
                        await self._completion_barrier(completion)
                except Exception as exc:
                    barrier_failure = CodexTurnCompletionBarrierError(completion, exc)
                    self._completion_barrier_failures[key] = barrier_failure
                self._completion_barriers_pending.discard(key)
                waiter = self._turn_waiters.pop(key, None)
                if waiter is not None and not waiter.done():
                    if barrier_failure is None:
                        waiter.set_result(completion)
                    else:
                        waiter.set_exception(barrier_failure)
        if self._observer is not None:
            try:
                self._observer(method, params)
            except Exception:
                # Viewport/telemetry observers are operational conveniences;
                # they cannot become turn-completion authority.
                pass

    def _observe_completed_item(self, params: JsonObject) -> None:
        thread_id = params.get("threadId")
        turn_id = params.get("turnId")
        item = params.get("item")
        if not isinstance(thread_id, str) or not isinstance(turn_id, str):
            raise CodexAppServerError("Codex item/completed identity is invalid")
        if not isinstance(item, dict) or item.get("type") != "agentMessage":
            return
        item_id = item.get("id")
        text = item.get("text")
        if not isinstance(item_id, str) or not item_id or not isinstance(text, str):
            raise CodexAppServerError("Codex completed agent message is invalid")
        item_key = (thread_id, turn_id, item_id)
        existing = self._completed_agent_items.get(item_key)
        if existing is not None and existing != text:
            raise CodexAppServerError(
                "Codex app-server returned a conflicting completed agent message"
            )
        self._completed_agent_items[item_key] = text
        self._last_agent_messages[(thread_id, turn_id)] = text

    @staticmethod
    def _completion(params: JsonObject) -> CodexTurnCompletion:
        turn = params.get("turn")
        if not isinstance(turn, dict):
            raise CodexAppServerError("Codex turn/completed has no turn")
        thread_id = params.get("threadId") or turn.get("threadId")
        turn_id = turn.get("id")
        status = turn.get("status")
        if not all(isinstance(value, str) for value in (thread_id, turn_id, status)):
            raise CodexAppServerError("Codex turn/completed identity is invalid")
        final_message = ""
        items = turn.get("items", [])
        if isinstance(items, list):
            messages = [
                item.get("text")
                for item in items
                if isinstance(item, dict)
                and item.get("type") == "agentMessage"
                and isinstance(item.get("text"), str)
            ]
            if messages:
                final_message = str(messages[-1])
        error_value = turn.get("error")
        error = ""
        if isinstance(error_value, dict):
            message = error_value.get("message")
            if isinstance(message, str):
                error = message
        return CodexTurnCompletion(
            thread_id=str(thread_id),
            turn_id=str(turn_id),
            status=str(status),
            final_message=final_message,
            error=error,
        )

    @staticmethod
    def _thread_params(
        *,
        cwd: str,
        model: str,
        options: Mapping[str, Any] | None,
    ) -> JsonObject:
        if not cwd.startswith("/"):
            raise ValueError("Codex app-server cwd must be absolute")
        params: JsonObject = {
            "cwd": cwd,
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }
        if model:
            params["model"] = model
        if options:
            overlap = set(params).intersection(options)
            if overlap:
                raise ValueError(
                    "Codex app-server thread options duplicate controlled fields: "
                    + ", ".join(sorted(overlap))
                )
            params.update(options)
        return params

    @staticmethod
    def _thread_from_result(result: JsonObject) -> CodexThread:
        thread = result.get("thread")
        if not isinstance(thread, dict):
            raise CodexAppServerError("Codex thread response has no thread")
        thread_id = thread.get("id")
        if not isinstance(thread_id, str):
            raise CodexAppServerError("Codex thread response has no identity")
        return CodexThread(thread_id)

    def _require_ready(self) -> None:
        if not self._initialized or self._closed:
            raise RuntimeError("Codex app-server client is not ready")
        if self._terminal_failure is not None:
            raise self._terminal_failure

    async def _observe_thread(self, thread: CodexThread) -> None:
        if self._thread_observer is not None:
            # Unlike a turn observer, this runs before work is admitted.
            # Failure therefore closes the mission rather than starting an
            # unobservable turn.
            await self._thread_observer(thread)

    async def _observe_turn(self, turn: CodexTurn) -> None:
        if (turn.thread_id, turn.turn_id) in self._terminal_turns_seen:
            # A very fast turn can finish before the request coroutine resumes.
            # Never open a writable terminal after its exact terminal fact.
            return
        if self._turn_observer is not None:
            try:
                await self._turn_observer(turn)
            except Exception:
                # The viewport is operational substrate. It cannot prevent an
                # already-admitted app-server turn from running to completion.
                pass

    def _fail_waiters(self, failure: BaseException) -> None:
        for future in (*self._pending.values(), *self._turn_waiters.values()):
            if not future.done():
                future.set_exception(failure)
        self._pending.clear()
        self._pending_calls.clear()
        self._turn_waiters.clear()

    @staticmethod
    def _consume_abandoned_response(future: asyncio.Future[JsonObject]) -> None:
        if not future.cancelled():
            future.exception()


@runtime_checkable
class CodexAppServerConnector(Protocol):
    """Provider-owned authenticated connection to a sandbox app-server."""

    def connect(
        self,
        session: SandboxSession,
    ) -> AbstractAsyncContextManager[CodexAppServerConnection]: ...


@dataclass(slots=True)
class CodexAppServerConnection:
    """One controller plus provider-owned ephemeral execution evidence."""

    client: CodexAppServerClient
    trace_uri: str = ""


@dataclass(frozen=True)
class CodexAppServerDriver:
    """Coding-agent driver backed by app-server thread/turn facts."""

    connector: CodexAppServerConnector
    driver_id: ClassVar[str] = "codex"
    model: str = ""
    workspace: str = "/workspace/repo"
    timeout_seconds: int = 45 * 60

    def __post_init__(self) -> None:
        if not self.workspace.startswith("/") or self.workspace == "/":
            raise ValueError("Codex app-server workspace must be a non-root absolute path")
        if self.timeout_seconds < 1:
            raise ValueError("Codex app-server timeout must be positive")

    async def run(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        prompt: str,
    ) -> AgentProcessObservation:
        del request
        return await run_codex_app_server_turn(
            session,
            connector=self.connector,
            workspace=self.workspace,
            prompt=prompt,
            model=self.model,
            timeout_seconds=self.timeout_seconds,
        )


async def run_codex_app_server_turn(
    session: SandboxSession,
    *,
    connector: CodexAppServerConnector,
    workspace: str,
    prompt: str,
    model: str = "",
    timeout_seconds: int = 45 * 60,
) -> AgentProcessObservation:
    """Run one exact app-server turn for an admitted Modal agent role.

    Author and critic drivers share this path so the pinned CLI interface,
    OAuth scrub barrier, completion authority, and timeout behavior cannot
    drift between roles.
    """

    if not workspace.startswith("/") or workspace == "/":
        raise ValueError("Codex app-server workspace must be a non-root absolute path")
    if timeout_seconds < 1:
        raise ValueError("Codex app-server timeout must be positive")
    if not prompt.strip():
        raise ValueError("Codex app-server prompt cannot be empty")

    options = {
        "config": {
            "cli_auth_credentials_store": "file",
            "shell_environment_policy": {
                "inherit": "core",
                "exclude": ["*KEY*", "*SECRET*", "*TOKEN*"],
            },
        }
    }
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    async with AsyncExitStack() as stack:
        connection = await _await_app_server_phase(
            stack.enter_async_context(connector.connect(session)),
            deadline=deadline,
            phase="connect",
        )
        client = connection.client
        await _await_app_server_phase(
            client.initialize(),
            deadline=deadline,
            phase="initialize",
        )
        # Modal Activity executions use fresh operation-scoped sandboxes and
        # deliberately scrub CODEX_HOME before checkpointing. A prior thread
        # id is durable evidence, not a resumable local rollout.
        thread = await _await_app_server_phase(
            client.start_thread(
                cwd=workspace,
                model=model,
                options=options,
            ),
            deadline=deadline,
            phase="thread/start",
        )
        turn = await _await_app_server_phase(
            client.start_turn(thread, prompt),
            deadline=deadline,
            phase="turn/start",
        )
        # Interruption and its exact terminal fact share the configured
        # exchange budget. Reserve at most the historical 30-second grace,
        # and half of a short remaining budget, so timeout handling can never
        # extend a paid Activity beyond its configured agent deadline.
        remaining = max(0.0, deadline - loop.time())
        interrupt_grace = min(30.0, remaining / 2)
        turn_deadline = deadline - interrupt_grace
        try:
            try:
                completed = await _await_app_server_phase(
                    client.wait_for_turn(turn),
                    deadline=turn_deadline,
                    phase="turn completion",
                    propagate_timeout=True,
                )
            except TimeoutError:
                completed = await _interrupt_for_terminal_completion(
                    client,
                    turn,
                    deadline=deadline,
                )
        except CodexTurnCompletionBarrierError as exc:
            # The app-server fact is authoritative, but it is not safe to
            # release the observation until provider context cleanup retries
            # quiescence successfully. A persistent __aexit__ failure still
            # propagates and prevents validation of a live-mutating worktree.
            completed = exc.completion
    return AgentProcessObservation(
        returncode={"completed": 0, "interrupted": 130, "failed": 1}[completed.status],
        stdout=completed.final_message,
        stderr=completed.error,
        session_id=thread.thread_id,
        trace_uri=connection.trace_uri,
    )


__all__ = [
    "CodexAppServerClient",
    "CodexAppServerConnection",
    "CodexAppServerConnector",
    "CodexAppServerDriver",
    "CodexAppServerError",
    "CodexAppServerSocketTransport",
    "CodexAppServerTransport",
    "CodexThread",
    "CodexTurn",
    "CodexTurnCompletion",
    "CodexTurnCompletionBarrierError",
    "run_codex_app_server_turn",
]
