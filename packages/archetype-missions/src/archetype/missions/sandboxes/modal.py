# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal Backend and Session implementation for mission sandboxes."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import shlex
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

from archetype.missions.sandboxes._image import (
    BASE_IMAGE_REF,
    codex_install_command,
    codex_package,
    ttyd_install_command,
    verify_coding_agent_environment,
)
from archetype.missions.sandboxes.contracts import (
    CheckpointLocality,
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    RepositoryPublicationRequest,
    SandboxCapabilities,
    SandboxEvent,
    SandboxEventType,
    SandboxIdentity,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
    live_observation_paths,
    validate_checkpoint_for_spec,
)
from archetype.missions.sandboxes.versions import load_version_inventory

if TYPE_CHECKING:
    from archetype.missions.coding_agents.app_server import (
        CodexAppServerClient,
        CodexAppServerConnection,
        CodexThread,
        CodexTurn,
        CodexTurnCompletion,
    )

_AUTH_MOUNT = "/auth"
_AUTH_VOLUME_PATH = f"{_AUTH_MOUNT}/auth.json"
_CODEX_HOME = "/root/.codex"
_MISSION_AUTH_PATH = f"{_CODEX_HOME}/auth.json"
_GITHUB_SECRET = "github"
_MAX_PUBLICATION_BUNDLE_BYTES = 512 * 1024 * 1024
_PUBLICATION_ROOT = "/tmp/archetype-agent-missions/publication"
_PUBLICATION_REF_PREFIX = "refs/archetype/publication"
_PUBLICATION_MEASUREMENT_SCRIPT = (
    'set -eu; path="$1"; set -- $(sha256sum -- "$path"); '
    'size=$(wc -c < "$path"); printf "%s %s\\n" "$1" "$size"'
)
_INTERACTIVE_SESSION_ENV = "ARCHETYPE_MODAL_INTERACTIVE_SESSION"
_CLEAN_GIT_ENV = (
    ("GIT_CONFIG_COUNT", "0"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_SYSTEM", "/dev/null"),
    ("GIT_TERMINAL_PROMPT", "0"),
)
_CLEAN_GIT_ARGS = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "credential.helper=",
    "-c",
    "protocol.ext.allow=never",
)
_MAX_PROVIDER_OPERATION_ID_LENGTH = 1024
_MAX_PROVIDER_OBJECT_ID_LENGTH = 256
_MAX_PROVIDER_COHORT_ID_LENGTH = 256
_MODAL_NAMESPACE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}")
_MODAL_OPERATION_COHORT = re.compile(r"cohort-v1:[0-9a-f]{32}")
_SPECTATE_PORT = 7681
_TAKEOVER_PORT = 7682
_TMUX_EVENTS_HELPER = """#!/bin/sh
printf '{"event":"%s","session":"%s","ts_ms":%s}\\n' "$1" "$2" "$(($(date +%s) * 1000))" >> "$3"
"""
_UNIX_WEBSOCKET_BRIDGE = r"""#!/usr/bin/env python3
import argparse
import asyncio
import base64
import hashlib
import os
import struct
import sys


GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


async def connect(path):
    reader, writer = await asyncio.open_unix_connection(path)
    key = base64.b64encode(os.urandom(16))
    request = (
        b"GET / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Key: " + key + b"\r\n"
        b"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    writer.write(request)
    await writer.drain()
    response = await reader.readuntil(b"\r\n\r\n")
    lines = response.split(b"\r\n")
    if not lines or b" 101 " not in lines[0]:
        raise RuntimeError("app-server rejected WebSocket upgrade")
    headers = {}
    for line in lines[1:]:
        name, separator, value = line.partition(b":")
        if separator:
            headers[name.strip().lower()] = value.strip()
    expected = base64.b64encode(hashlib.sha1(key + GUID).digest())
    if headers.get(b"sec-websocket-accept") != expected:
        raise RuntimeError("app-server returned an invalid WebSocket accept value")
    return reader, writer


async def send_frame(writer, opcode, payload=b""):
    first = 0x80 | opcode
    size = len(payload)
    if size < 126:
        header = bytes((first, 0x80 | size))
    elif size < 65536:
        header = bytes((first, 0x80 | 126)) + struct.pack("!H", size)
    else:
        header = bytes((first, 0x80 | 127)) + struct.pack("!Q", size)
    mask = os.urandom(4)
    masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    writer.write(header + mask + masked)
    await writer.drain()


async def receive_frame(reader):
    first, second = await reader.readexactly(2)
    final = bool(first & 0x80)
    opcode = first & 0x0F
    masked = bool(second & 0x80)
    size = second & 0x7F
    if size == 126:
        size = struct.unpack("!H", await reader.readexactly(2))[0]
    elif size == 127:
        size = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(size)
    if masked:
        payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return final, opcode, payload


async def stdin_reader():
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    return reader


async def run(path):
    websocket_reader, websocket_writer = await connect(path)
    stdin = await stdin_reader()

    async def send_input():
        while line := await stdin.readline():
            await send_frame(websocket_writer, 0x1, line.rstrip(b"\n"))
        await send_frame(websocket_writer, 0x8)

    async def receive_output():
        fragments = bytearray()
        fragment_opcode = None
        while True:
            final, opcode, payload = await receive_frame(websocket_reader)
            if opcode == 0x8:
                return
            if opcode == 0x9:
                await send_frame(websocket_writer, 0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode in {0x1, 0x2}:
                fragments = bytearray(payload)
                fragment_opcode = opcode
            elif opcode == 0x0 and fragment_opcode is not None:
                fragments.extend(payload)
            else:
                raise RuntimeError(f"unsupported WebSocket opcode {opcode}")
            if final:
                if fragment_opcode != 0x1:
                    raise RuntimeError("app-server returned a non-text WebSocket message")
                sys.stdout.buffer.write(bytes(fragments) + b"\n")
                sys.stdout.buffer.flush()
                fragments.clear()
                fragment_opcode = None

    sender = asyncio.create_task(send_input())
    receiver = asyncio.create_task(receive_output())
    done, pending = await asyncio.wait(
        (sender, receiver),
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        task.result()
    websocket_writer.close()
    await websocket_writer.wait_closed()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    asyncio.run(run(parser.parse_args().socket))


if __name__ == "__main__":
    main()
"""
_TMUX_HOOKS = ("client-attached", "client-detached", "pane-died", "session-closed")

# Epoch zero and operations without an epoch predate the persistent provider
# barrier. They must never infer retry permission from a missing marker.
MODAL_ACTIVITY_PROTOCOL_EPOCH = 1


def _default_environment() -> str:
    """Return the content identity attested by the default Modal image."""

    pin = load_version_inventory().harness_pin("codex")
    ttyd_x86 = load_version_inventory().resolve("ttyd-x86-64")
    ttyd_arm = load_version_inventory().resolve("ttyd-aarch64")
    material = "\n".join(
        (
            BASE_IMAGE_REF,
            "ca-certificates curl git nodejs npm openssh-client tmux",
            pin.name,
            pin.version,
            pin.source,
            pin.immutable_ref,
            ttyd_x86.name,
            ttyd_x86.version,
            ttyd_x86.source,
            ttyd_x86.immutable_ref,
            ttyd_arm.name,
            ttyd_arm.version,
            ttyd_arm.source,
            ttyd_arm.immutable_ref,
            "user=root",
            "home=/root",
            "workdir=/workspace",
        )
    )
    return f"modal-agent://sha256:{hashlib.sha256(material.encode()).hexdigest()}"


def _require_modal_namespace_name(value: str, *, label: str) -> str:
    if not _MODAL_NAMESPACE_NAME.fullmatch(value):
        raise ValueError(f"Modal {label} is invalid")
    return value


@dataclass(frozen=True)
class ModalSandboxConfig:
    """Provider configuration; repository coordinates arrive in ``SandboxSpec``."""

    app_name: str = "archetype-agent-missions"
    image_id: str = ""
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"
    checkpoint_timeout_seconds: int = 5 * 60
    checkpoint_ttl_seconds: int | None = 30 * 24 * 60 * 60
    heartbeat_seconds: int = 15
    login_timeout_seconds: int = 15 * 60
    workspace_name: str | None = None
    environment_name: str | None = None
    operation_protocol_epoch: int | None = None

    def __post_init__(self) -> None:
        if not self.app_name.strip():
            raise ValueError("Modal app_name must not be empty")
        if self.image_id and not self.image_id.startswith("im-"):
            raise ValueError("Modal image_id must be an immutable im-... identity")
        if not self.auth_volume_name.strip():
            raise ValueError("Modal Codex auth volume must not be empty")
        if not self.github_secret_name.strip():
            raise ValueError("commit-and-push requires a GitHub secret")
        if self.checkpoint_timeout_seconds < 1:
            raise ValueError("checkpoint timeout must be positive")
        if self.checkpoint_ttl_seconds is not None and self.checkpoint_ttl_seconds < 1:
            raise ValueError("checkpoint TTL must be positive when configured")
        if self.heartbeat_seconds < 1:
            raise ValueError("heartbeat interval must be positive")
        if self.login_timeout_seconds < 1:
            raise ValueError("login timeout must be positive")
        if self.workspace_name is not None:
            _require_modal_namespace_name(self.workspace_name, label="workspace_name")
        if self.environment_name is not None:
            _require_modal_namespace_name(self.environment_name, label="environment_name")
        if self.operation_protocol_epoch is not None and self.operation_protocol_epoch < 0:
            raise ValueError("Modal operation protocol epoch must not be negative")


class ModalViewportMode(StrEnum):
    """The exact server-enforced input capability of one terminal grant."""

    SPECTATE = "spectate"
    TAKEOVER = "takeover"


@dataclass(frozen=True, slots=True)
class ModalViewportGrant:
    """One transient, port-scoped Modal connection capability.

    The token and browser URL are deliberately hidden from repr so routine
    logging cannot disclose the bearer capability.
    """

    sandbox_id: str
    mode: ModalViewportMode
    base_url: str
    token: str = field(repr=False)
    grant_id: str

    def __post_init__(self) -> None:
        if not self.sandbox_id.startswith("sb-"):
            raise ValueError("Modal viewport grant requires an sb-... sandbox identity")
        if not self.base_url.startswith("https://") or not self.token.strip():
            raise ValueError("Modal viewport grant requires HTTPS credentials")
        if not re.fullmatch(r"[0-9a-f]{32}", self.grant_id):
            raise ValueError("Modal viewport grant identity is invalid")

    @property
    def browser_url(self) -> str:
        """Return the cookie-bootstrapping browser URL for this one grant."""

        parsed = urlsplit(self.base_url)
        query = (
            f"{parsed.query}&_modal_connect_token={quote(self.token, safe='')}"
            if parsed.query
            else f"_modal_connect_token={quote(self.token, safe='')}"
        )
        path = f"{parsed.path.rstrip('/')}/"
        return urlunsplit((parsed.scheme, parsed.netloc, path, query, parsed.fragment))


@dataclass(slots=True)
class _ModalInteractiveSession:
    trace_id: str
    directory: str
    tmux_socket: str
    app_socket: str
    app_session: str
    tui_session: str
    spectate_session: str
    takeover_session: str
    thread_id: str = ""
    tui_started: bool = False
    spectate_started: bool = False
    takeover_started: bool = False
    stopped: bool = False


_TRANSPORT_EOF = object()


async def _finish_cleanup_preserving_cancellation(
    task: asyncio.Task[None],
    *,
    cancellation: asyncio.CancelledError | None,
) -> None:
    """Join exact cleanup before surfacing the caller's cancellation."""

    caller_cancellation = cancellation
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as interrupted:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                caller_cancellation = caller_cancellation or interrupted
            if task.done():
                break
        except BaseException:
            break

    try:
        task.result()
    except BaseException as cleanup_error:
        if caller_cancellation is not None:
            raise caller_cancellation from cleanup_error
        raise
    if caller_cancellation is not None:
        raise caller_cancellation


class _ModalAppServerTransport:
    """Adapt one Modal process's JSONL streams to the app-server transport."""

    def __init__(self, process: Any) -> None:
        self._process = process
        self._incoming: asyncio.Queue[object] = asyncio.Queue()
        self._closed = False
        self._stdout = asyncio.create_task(self._pump_stdout())
        self._stderr = asyncio.create_task(process.stderr.read.aio())

    async def send(self, message: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("Modal app-server proxy is closed")
        payload = (
            json.dumps(message, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode()
        self._process.stdin.write(payload)
        await self._process.stdin.drain.aio()

    async def receive(self) -> dict[str, Any]:
        value = await self._incoming.get()
        if value is _TRANSPORT_EOF:
            detail = await self._stderr_text()
            suffix = f": {detail[-1000:]}" if detail else ""
            raise RuntimeError(f"Modal app-server proxy closed{suffix}")
        if isinstance(value, BaseException):
            raise RuntimeError(
                f"Modal app-server proxy stream failed ({type(value).__name__})"
            ) from value
        if not isinstance(value, dict):
            raise RuntimeError("Modal app-server proxy message must be an object")
        return cast(dict[str, Any], value)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._process.stdin.write_eof()
            await self._process.stdin.drain.aio()
        except Exception:
            pass
        try:
            await asyncio.wait_for(self._process.wait.aio(), timeout=5)
        except (Exception, TimeoutError):
            pass
        if not self._stdout.done():
            self._stdout.cancel()
        await asyncio.gather(self._stdout, return_exceptions=True)
        if not self._stderr.done():
            self._stderr.cancel()
        await asyncio.gather(self._stderr, return_exceptions=True)

    async def _pump_stdout(self) -> None:
        buffered = ""
        try:
            async for chunk in self._process.stdout:
                buffered += (
                    chunk.decode(errors="replace") if isinstance(chunk, bytes) else str(chunk)
                )
                while "\n" in buffered:
                    line, buffered = buffered.split("\n", 1)
                    if line:
                        await self._incoming.put(self._decode(line))
            if buffered.strip():
                await self._incoming.put(self._decode(buffered))
            await self._incoming.put(_TRANSPORT_EOF)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await self._incoming.put(exc)

    @staticmethod
    def _decode(line: str) -> dict[str, Any]:
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Modal app-server proxy returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise RuntimeError("Modal app-server proxy message must be an object")
        return value

    async def _stderr_text(self) -> str:
        if not self._stderr.done():
            return ""
        try:
            value = self._stderr.result()
        except BaseException:
            return ""
        return value.decode(errors="replace") if isinstance(value, bytes) else str(value)


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationIdentity:
    """Stable full provider identity known before any Modal effect."""

    workspace_name: str
    environment_name: str
    app_name: str
    operation_id: str
    protocol_epoch: int

    def __post_init__(self) -> None:
        _require_modal_namespace_name(self.workspace_name, label="workspace_name")
        _require_modal_namespace_name(self.environment_name, label="environment_name")
        _require_modal_namespace_name(self.app_name, label="app_name")
        if not self.operation_id.strip():
            raise ValueError("Modal sandbox operation_id must not be empty")
        if self.operation_id != self.operation_id.strip():
            raise ValueError("Modal sandbox operation_id must not have surrounding whitespace")
        if "\x00" in self.operation_id:
            raise ValueError("Modal sandbox operation_id must not contain NUL")
        if len(self.operation_id) > _MAX_PROVIDER_OPERATION_ID_LENGTH:
            raise ValueError(
                "Modal sandbox operation_id must not exceed "
                f"{_MAX_PROVIDER_OPERATION_ID_LENGTH} characters"
            )
        if self.protocol_epoch < 0:
            raise ValueError("Modal operation protocol epoch must not be negative")

    @property
    def digest(self) -> str:
        """Return the complete namespaced content identity."""

        payload = json.dumps(
            {
                "schema_version": 2,
                "provider": "modal",
                "workspace_name": self.workspace_name,
                "environment_name": self.environment_name,
                "app_name": self.app_name,
                "protocol_epoch": self.protocol_epoch,
                "operation_id": self.operation_id,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"

    @property
    def mission_sandbox_name(self) -> str:
        """Return a deterministic provider-safe name for the mission sandbox."""

        return self._sandbox_name("m")

    @property
    def auth_sandbox_name(self) -> str:
        """Return a deterministic provider-safe name for the isolated auth broker."""

        return self._sandbox_name("a")

    def _sandbox_name(self, role: str) -> str:
        digest = bytes.fromhex(self.digest.removeprefix("sha256:"))
        encoded = base64.b32encode(digest).decode().rstrip("=").lower()
        return f"arc-{role}-{encoded}"


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationCleanup:
    """Exact provider resources whose teardown is bound to a durable result."""

    identity: ModalSandboxOperationIdentity
    mission_sandbox_id: str
    auth_sandbox_id: str
    cohort_id: str

    def __post_init__(self) -> None:
        if not self.mission_sandbox_id.strip() or not self.auth_sandbox_id.strip():
            raise ValueError("Modal operation cleanup requires both sandbox identities")
        if (
            len(self.mission_sandbox_id) > _MAX_PROVIDER_OBJECT_ID_LENGTH
            or len(self.auth_sandbox_id) > _MAX_PROVIDER_OBJECT_ID_LENGTH
        ):
            raise ValueError("Modal operation cleanup sandbox identity is too long")
        if self.mission_sandbox_id == self.auth_sandbox_id:
            raise ValueError("Modal operation cleanup sandbox identities must be distinct")
        if (
            not _MODAL_OPERATION_COHORT.fullmatch(self.cohort_id)
            or len(self.cohort_id) > _MAX_PROVIDER_COHORT_ID_LENGTH
        ):
            raise ValueError("Modal operation cleanup cohort identity is invalid")

    def to_payload(self) -> dict[str, str]:
        """Return the bounded result-envelope representation."""

        return {
            "auth_sandbox_id": self.auth_sandbox_id,
            "cohort_id": self.cohort_id,
            "mission_sandbox_id": self.mission_sandbox_id,
        }

    @classmethod
    def from_payload(
        cls,
        identity: ModalSandboxOperationIdentity,
        payload: object,
    ) -> ModalSandboxOperationCleanup:
        """Restore exact cleanup ownership from a provider result envelope."""

        if not isinstance(payload, dict) or set(payload) != {
            "auth_sandbox_id",
            "cohort_id",
            "mission_sandbox_id",
        }:
            raise ValueError("Modal operation cleanup payload is invalid")
        mission_sandbox_id = payload.get("mission_sandbox_id")
        auth_sandbox_id = payload.get("auth_sandbox_id")
        cohort_id = payload.get("cohort_id")
        if not (
            isinstance(mission_sandbox_id, str)
            and isinstance(auth_sandbox_id, str)
            and isinstance(cohort_id, str)
        ):
            raise ValueError("Modal operation cleanup payload is invalid")
        return cls(
            identity=identity,
            mission_sandbox_id=mission_sandbox_id,
            auth_sandbox_id=auth_sandbox_id,
            cohort_id=cohort_id,
        )


type ModalSandboxResourceObserver = Callable[
    [
        ModalSandboxOperationIdentity,
        str,
        Literal["intent", "auth", "mission"],
        str,
    ],
    Awaitable[None],
]


class ModalSandboxSession:
    """One Modal filesystem/process container plus an isolated auth broker."""

    def __init__(
        self,
        *,
        spec: SandboxSpec,
        sandbox: Any,
        auth_sandbox: Any,
        github_secret: Any,
        auth_volume_name: str,
        checkpoint_timeout_seconds: int,
        checkpoint_ttl_seconds: int | None,
        heartbeat_seconds: int,
        operation_identity: ModalSandboxOperationIdentity | None = None,
        operation_cohort_id: str | None = None,
    ) -> None:
        self._spec = spec
        self._sandbox = sandbox
        self._auth_sandbox = auth_sandbox
        self._github_secret = github_secret
        self._auth_volume_name = auth_volume_name
        self._checkpoint_timeout_seconds = checkpoint_timeout_seconds
        self._checkpoint_ttl_seconds = checkpoint_ttl_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._operation_identity = operation_identity
        self._operation_cohort_id = operation_cohort_id
        self._lock = asyncio.Lock()
        self._event_lock = asyncio.Lock()
        self._event_sequence = 0
        self._status = SandboxStatus.READY
        self._close_resources = {
            "mission": sandbox,
            "OAuth broker": auth_sandbox,
        }

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity(
            provider="modal",
            sandbox_id=str(self._sandbox.object_id),
            environment=self._spec.environment,
        )

    @property
    def operation_identity(self) -> ModalSandboxOperationIdentity | None:
        """Return the stable named-operation identity, when this session has one."""

        return self._operation_identity

    @property
    def operation_cohort_id(self) -> str | None:
        """Return the exact two-resource cohort created for this session."""

        return self._operation_cohort_id

    @property
    def operation_cleanup(self) -> ModalSandboxOperationCleanup:
        """Return exact durable teardown ownership for a named operation."""

        if self._operation_identity is None or self._operation_cohort_id is None:
            raise RuntimeError("ordinary Modal sandbox has no named-operation cleanup identity")
        return ModalSandboxOperationCleanup(
            identity=self._operation_identity,
            mission_sandbox_id=str(self._sandbox.object_id),
            auth_sandbox_id=str(self._auth_sandbox.object_id),
            cohort_id=self._operation_cohort_id,
        )

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(
            checkpoints=True,
            live_output=True,
            interactive_sessions=True,
            secret_names=(),
        )

    async def status(self) -> SandboxStatus:
        return self._status

    @asynccontextmanager
    async def codex_app_server(self) -> AsyncIterator[CodexAppServerConnection]:
        """Yield an authenticated app-server controller with a real Codex TUI."""

        from archetype.missions.coding_agents.app_server import (
            CodexAppServerClient,
            CodexAppServerConnection,
            CodexThread,
        )

        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Modal sandbox session is {self._status.value}")
            state = self._interactive_state(uuid4().hex)
            client: CodexAppServerClient | None = None
            connection: CodexAppServerConnection | None = None
            heartbeat: asyncio.Task[None] | None = None
            operation_error: BaseException | None = None
            try:
                await self._stage_oauth()
                await self._prepare_interactive_session(state)
                heartbeat = asyncio.create_task(self._heartbeat())
                process = await self._start_app_server_proxy(state)
                transport = _ModalAppServerTransport(process)

                async def open_viewports(turn: CodexTurn) -> None:
                    # A newly created app-server thread has no resumable rollout
                    # until turn/start succeeds. Attach the real TUI only after
                    # that exact turn exists, then open both views over its PTY.
                    await self._start_codex_tui(state, CodexThread(turn.thread_id))
                    await self._start_codex_takeover(state, turn)

                async def scrub_oauth(_thread: CodexThread) -> None:
                    # thread/start is the last operation that may read the
                    # staged subscription credential. The observer is awaited
                    # before start_thread returns, so turn/start, the TUI, and
                    # every agent-controlled tool run after this exact-file
                    # scrub has completed.
                    await self._scrub_mission_oauth()

                client = CodexAppServerClient(
                    transport,
                    thread_observer=scrub_oauth,
                    turn_observer=open_viewports,
                    completion_barrier=lambda completion: self._finish_codex_turn(
                        state,
                        completion,
                    ),
                )
                connection = CodexAppServerConnection(client)
                yield connection
            except asyncio.CancelledError as exc:
                operation_error = exc
                self._status = SandboxStatus.INTERRUPTED
                raise
            except BaseException as exc:
                operation_error = exc
                self._status = SandboxStatus.ERRORED
                raise
            finally:

                async def cleanup() -> None:
                    failures: list[BaseException] = []
                    if heartbeat is not None:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
                    if client is not None:
                        try:
                            await client.aclose()
                        except BaseException as exc:
                            failures.append(exc)
                    try:
                        await self._stop_interactive_session(state)
                    except BaseException as exc:
                        failures.append(exc)
                    if connection is not None:
                        connection.trace_uri = await self._capture_live_output_best_effort(
                            state.trace_id
                        )
                    try:
                        await self._remove_oauth()
                    except BaseException as exc:
                        failures.append(exc)
                    if failures:
                        raise BaseExceptionGroup(
                            "failed to close Modal interactive session",
                            failures,
                        )

                cleanup_task = asyncio.create_task(cleanup())
                try:
                    await _finish_cleanup_preserving_cancellation(
                        cleanup_task,
                        cancellation=(
                            operation_error
                            if isinstance(operation_error, asyncio.CancelledError)
                            else None
                        ),
                    )
                except asyncio.CancelledError:
                    if cleanup_task.done():
                        try:
                            cleanup_task.result()
                        except BaseException:
                            self._status = SandboxStatus.ERRORED
                    raise
                except BaseException as exc:
                    self._status = SandboxStatus.ERRORED
                    if operation_error is not None:
                        raise exc from operation_error
                    raise

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        unknown = set(request.secret_names) - set(self.capabilities.secret_names)
        if unknown:
            raise ValueError(f"unsupported Modal sandbox secret(s): {', '.join(sorted(unknown))}")
        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Modal sandbox session is {self._status.value}")
            is_agent = request.close_stdin
            heartbeat: asyncio.Task[None] | None = None
            trace_uri = ""
            try:
                actual_request = request
                if is_agent:
                    live_directory_ready = False
                    try:
                        await self._ensure_live_directory()
                        await self._clear_live_output()
                        live_directory_ready = True
                    except Exception:
                        pass
                    await self._emit_event_best_effort(
                        SandboxEventType.PROCESS_STARTED,
                        operation=request.argv[0],
                    )
                    if live_directory_ready:
                        actual_request = self._trace_request(request)
                        heartbeat = asyncio.create_task(self._heartbeat())
                result = await self._exec_on(
                    self._sandbox,
                    actual_request,
                )
                if is_agent:
                    await self._emit_event_best_effort(
                        SandboxEventType.PROCESS_FINISHED,
                        operation=request.argv[0],
                        returncode=result.returncode,
                    )
                    if live_directory_ready:
                        trace_uri = await self._capture_live_output_best_effort(uuid4().hex)
            except asyncio.CancelledError:
                self._status = SandboxStatus.INTERRUPTED
                raise
            except BaseException:
                self._status = SandboxStatus.ERRORED
                raise
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
            return ProcessResult(
                argv=request.argv,
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                trace_uri=trace_uri,
            )

    async def publish_repository(
        self,
        request: RepositoryPublicationRequest,
    ) -> ProcessResult:
        """Publish one exact revision from the non-agent auth broker.

        The mission sandbox creates an uncredentialed Git bundle. The
        controller transfers those bytes to the separate broker sandbox,
        verifies the requested object identity there, and attaches the Modal
        GitHub Secret only to the final trusted push process. Agent-created
        processes, hooks, remotes, helpers, and Git configuration therefore
        never share an execution boundary with ``GITHUB_TOKEN``.
        """

        if request.secret_name != _GITHUB_SECRET:
            raise ValueError("Modal publication requires the symbolic GitHub capability")
        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Modal sandbox session is {self._status.value}")
            publication_id = uuid4().hex
            mission_directory = f"{_PUBLICATION_ROOT}/{publication_id}"
            broker_directory = f"{_PUBLICATION_ROOT}/{publication_id}"
            mission_bundle = f"{mission_directory}/validated.bundle"
            broker_bundle = f"{broker_directory}/validated.bundle"
            broker_repository = f"{broker_directory}/repository.git"
            transfer_ref = f"{_PUBLICATION_REF_PREFIX}/{publication_id}"
            operation_error: BaseException | None = None
            try:
                await self._publication_checked(
                    self._sandbox,
                    ProcessRequest(
                        ("mkdir", "-p", mission_directory),
                        timeout_seconds=60,
                    ),
                    "create mission publication directory",
                )
                await self._publication_checked(
                    self._sandbox,
                    ProcessRequest(
                        (
                            "git",
                            *_CLEAN_GIT_ARGS,
                            "update-ref",
                            transfer_ref,
                            request.revision,
                        ),
                        workdir=request.worktree,
                        timeout_seconds=60,
                        env=_CLEAN_GIT_ENV,
                    ),
                    "bind validated publication revision",
                )
                await self._publication_checked(
                    self._sandbox,
                    ProcessRequest(
                        (
                            "git",
                            *_CLEAN_GIT_ARGS,
                            "bundle",
                            "create",
                            mission_bundle,
                            transfer_ref,
                        ),
                        workdir=request.worktree,
                        timeout_seconds=request.timeout_seconds,
                        env=_CLEAN_GIT_ENV,
                    ),
                    "bundle validated publication revision",
                )
                await self._publication_checked(
                    self._auth_sandbox,
                    ProcessRequest(
                        ("mkdir", "-p", broker_directory),
                        timeout_seconds=60,
                    ),
                    "create broker publication directory",
                )
                bundle_size, bundle_digest = await self._stream_publication_bundle(
                    source=mission_bundle,
                    destination=broker_bundle,
                    timeout_seconds=request.timeout_seconds,
                )
                broker_measurement = await self._publication_checked(
                    self._auth_sandbox,
                    ProcessRequest(
                        (
                            "sh",
                            "-c",
                            _PUBLICATION_MEASUREMENT_SCRIPT,
                            "measure-publication-bundle",
                            broker_bundle,
                        ),
                        timeout_seconds=60,
                    ),
                    "verify streamed publication bundle",
                )
                measured = broker_measurement.stdout.strip().split()
                if measured != [bundle_digest, str(bundle_size)]:
                    raise RuntimeError(
                        "publication bundle changed during bounded controller transfer"
                    )
                await self._publication_checked(
                    self._auth_sandbox,
                    ProcessRequest(
                        (
                            "git",
                            *_CLEAN_GIT_ARGS,
                            "init",
                            "--bare",
                            broker_repository,
                        ),
                        timeout_seconds=60,
                        env=_CLEAN_GIT_ENV,
                    ),
                    "initialize broker publication repository",
                )
                await self._publication_checked(
                    self._auth_sandbox,
                    ProcessRequest(
                        (
                            "git",
                            *_CLEAN_GIT_ARGS,
                            f"--git-dir={broker_repository}",
                            "fetch",
                            "--no-tags",
                            "--no-recurse-submodules",
                            "--",
                            broker_bundle,
                            f"+{transfer_ref}:{transfer_ref}",
                        ),
                        timeout_seconds=request.timeout_seconds,
                        env=_CLEAN_GIT_ENV,
                    ),
                    "import broker publication bundle",
                )
                resolved = await self._publication_checked(
                    self._auth_sandbox,
                    ProcessRequest(
                        (
                            "git",
                            *_CLEAN_GIT_ARGS,
                            f"--git-dir={broker_repository}",
                            "rev-parse",
                            "--verify",
                            transfer_ref,
                        ),
                        timeout_seconds=60,
                        env=_CLEAN_GIT_ENV,
                    ),
                    "verify broker publication revision",
                )
                if resolved.stdout.strip() != request.revision:
                    raise RuntimeError("broker resolved a different publication revision")

                helper = (
                    '!f() { echo "username=x-access-token"; echo "password=$GITHUB_TOKEN"; }; f'
                )
                return await self._exec_on(
                    self._auth_sandbox,
                    ProcessRequest(
                        (
                            "git",
                            *_CLEAN_GIT_ARGS,
                            "-c",
                            f"credential.helper={helper}",
                            f"--git-dir={broker_repository}",
                            "push",
                            "--porcelain",
                            "--",
                            request.repository,
                            f"{transfer_ref}:{request.branch_ref}",
                        ),
                        timeout_seconds=request.timeout_seconds,
                        env=_CLEAN_GIT_ENV,
                    ),
                    secrets=[self._github_secret],
                )
            except BaseException as exc:
                operation_error = exc
                raise
            finally:

                async def cleanup() -> None:
                    cleanup_results = await asyncio.gather(
                        self._exec_on(
                            self._sandbox,
                            ProcessRequest(
                                (
                                    "git",
                                    *_CLEAN_GIT_ARGS,
                                    "update-ref",
                                    "-d",
                                    transfer_ref,
                                ),
                                workdir=request.worktree,
                                timeout_seconds=60,
                                env=_CLEAN_GIT_ENV,
                            ),
                        ),
                        self._exec_on(
                            self._sandbox,
                            ProcessRequest(
                                ("rm", "-rf", "--", mission_directory),
                                timeout_seconds=60,
                            ),
                        ),
                        self._exec_on(
                            self._auth_sandbox,
                            ProcessRequest(
                                ("rm", "-rf", "--", broker_directory),
                                timeout_seconds=60,
                            ),
                        ),
                        return_exceptions=True,
                    )
                    cleanup_failures: list[BaseException] = []
                    for result in cleanup_results:
                        if isinstance(result, BaseException):
                            cleanup_failures.append(result)
                        elif result.returncode != 0:
                            cleanup_failures.append(
                                RuntimeError(
                                    "repository publication cleanup failed with exit code "
                                    f"{result.returncode}: {result.stderr or result.stdout}"
                                )
                            )
                    if cleanup_failures:
                        raise BaseExceptionGroup(
                            "failed to clean repository publication state",
                            cleanup_failures,
                        )

                cleanup_task = asyncio.create_task(cleanup())
                try:
                    await _finish_cleanup_preserving_cancellation(
                        cleanup_task,
                        cancellation=(
                            operation_error
                            if isinstance(operation_error, asyncio.CancelledError)
                            else None
                        ),
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    if operation_error is not None:
                        raise exc from operation_error
                    raise

    async def _stream_publication_bundle(
        self,
        *,
        source: str,
        destination: str,
        timeout_seconds: int,
    ) -> tuple[int, str]:
        """Stream one hostile-sandbox file through a hard controller byte cap.

        Modal's convenient ``read_bytes`` API materializes the entire remote
        file. Publication instead opens the source once and forwards bounded
        chunks directly into the broker process. The controller stops reading
        after the first byte beyond the limit, so a replaced/growing source or
        a compromised producer cannot force an unbounded allocation.
        """

        destination_process = await self._auth_sandbox.exec.aio(
            "sh",
            "-c",
            'umask 077; cat > "$1"',
            "write-publication-bundle",
            destination,
            timeout=timeout_seconds,
            text=False,
        )
        try:
            source_process = await self._sandbox.exec.aio(
                "head",
                "-c",
                str(_MAX_PUBLICATION_BUNDLE_BYTES + 1),
                "--",
                source,
                timeout=timeout_seconds,
                text=False,
            )
        except BaseException as source_error:

            async def reap_destination() -> None:
                failures: list[BaseException] = []
                try:
                    destination_process.stdin.write_eof()
                except BaseException as exc:
                    failures.append(exc)
                try:
                    await destination_process.stdin.drain.aio()
                except BaseException as exc:
                    failures.append(exc)
                results = await asyncio.gather(
                    destination_process.wait.aio(),
                    destination_process.stdout.read.aio(),
                    destination_process.stderr.read.aio(),
                    return_exceptions=True,
                )
                failures.extend(result for result in results if isinstance(result, BaseException))
                if failures:
                    raise BaseExceptionGroup(
                        "failed to close repository publication destination",
                        failures,
                    )

            reap_task = asyncio.create_task(reap_destination())
            try:
                await _finish_cleanup_preserving_cancellation(
                    reap_task,
                    cancellation=(
                        source_error if isinstance(source_error, asyncio.CancelledError) else None
                    ),
                )
            except asyncio.CancelledError as cancellation:
                if cancellation is source_error:
                    raise
                reap_error: BaseException | None = None
                if reap_task.done():
                    try:
                        reap_task.result()
                    except BaseException as exc:
                        reap_error = exc
                if reap_error is not None:
                    raise cancellation from BaseExceptionGroup(
                        "repository publication source start and destination cleanup failed",
                        [source_error, reap_error],
                    )
                raise cancellation from source_error
            except BaseException as cleanup_error:
                raise source_error from cleanup_error
            raise
        size = 0
        digest = hashlib.sha256()
        oversized = False
        destination_closed = False
        try:
            async for raw_chunk in source_process.stdout:
                if not isinstance(raw_chunk, (bytes, bytearray, memoryview)):
                    raise RuntimeError("publication bundle stream returned non-binary data")
                chunk = bytes(raw_chunk)
                if not chunk:
                    continue
                size += len(chunk)
                if size > _MAX_PUBLICATION_BUNDLE_BYTES:
                    oversized = True
                    # Do not keep consuming an attacker-controlled producer.
                    # The destination receives EOF in finally and both
                    # sandboxes are torn down by the owning session.
                    break
                digest.update(chunk)
                destination_process.stdin.write(chunk)
                await destination_process.stdin.drain.aio()
        finally:
            destination_process.stdin.write_eof()
            await destination_process.stdin.drain.aio()
            destination_closed = True

        if oversized:
            # The trusted broker writer has received EOF; reap it before
            # publication cleanup removes its destination. The hostile source
            # is deliberately not awaited after the cap because it may keep
            # producing forever.
            await asyncio.gather(
                destination_process.wait.aio(),
                destination_process.stdout.read.aio(),
                destination_process.stderr.read.aio(),
            )
            raise RuntimeError("publication bundle exceeds the 512 MiB controller-transfer limit")
        if size == 0:
            raise RuntimeError("publication bundle is empty")

        source_stderr_task = asyncio.create_task(source_process.stderr.read.aio())
        destination_stdout_task = asyncio.create_task(destination_process.stdout.read.aio())
        destination_stderr_task = asyncio.create_task(destination_process.stderr.read.aio())
        (
            source_returncode,
            source_stderr,
            destination_returncode,
            _,
            destination_stderr,
        ) = await asyncio.gather(
            source_process.wait.aio(),
            source_stderr_task,
            destination_process.wait.aio(),
            destination_stdout_task,
            destination_stderr_task,
        )
        if int(source_returncode) != 0:
            raise RuntimeError(
                "read publication bundle failed with exit code "
                f"{int(source_returncode)}: {source_stderr}"
            )
        if int(destination_returncode) != 0:
            raise RuntimeError(
                "write publication bundle failed with exit code "
                f"{int(destination_returncode)}: {destination_stderr}"
            )
        if not destination_closed:  # pragma: no cover - defensive invariant
            raise RuntimeError("publication bundle destination did not receive EOF")
        return size, digest.hexdigest()

    async def _publication_checked(
        self,
        sandbox: Any,
        request: ProcessRequest,
        label: str,
    ) -> ProcessResult:
        result = await self._exec_on(sandbox, request)
        self._raise(result, label)
        return result

    @classmethod
    async def issue_spectate_grant(
        cls,
        sandbox_id: str,
    ) -> ModalViewportGrant:
        """Mint one authenticated read-only browser capability."""

        return await cls._issue_viewport_grant(sandbox_id, ModalViewportMode.SPECTATE)

    @classmethod
    async def issue_takeover_grant(
        cls,
        sandbox_id: str,
    ) -> ModalViewportGrant:
        """Mint one authenticated writable browser capability."""

        return await cls._issue_viewport_grant(sandbox_id, ModalViewportMode.TAKEOVER)

    @classmethod
    async def _issue_viewport_grant(
        cls,
        sandbox_id: str,
        mode: ModalViewportMode,
    ) -> ModalViewportGrant:
        if not re.fullmatch(r"sb-[A-Za-z0-9_-]+", sandbox_id):
            raise ValueError("Modal viewport requires an sb-... sandbox identity")
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc
        sandbox = await modal.Sandbox.from_id.aio(sandbox_id)
        grant_id = uuid4().hex
        port = _SPECTATE_PORT if mode is ModalViewportMode.SPECTATE else _TAKEOVER_PORT
        credentials = await sandbox.create_connect_token.aio(
            user_metadata={
                "schema": 1,
                "mode": mode.value,
                "grant_id": grant_id,
            },
            port=port,
        )
        return ModalViewportGrant(
            sandbox_id=sandbox_id,
            mode=mode,
            base_url=str(credentials.url),
            token=str(credentials.token),
            grant_id=grant_id,
        )

    async def checkpoint(self) -> CheckpointRef:
        """Capture a provider-native filesystem image after credentials are absent."""

        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Modal sandbox session is {self._status.value}")
            absent = await self._exec_on(
                self._sandbox,
                ProcessRequest(
                    ("test", "!", "-e", _MISSION_AUTH_PATH),
                    timeout_seconds=60,
                ),
            )
            self._raise(absent, "verify credential-free checkpoint")
            try:
                await self._ensure_live_directory()
            except Exception:
                pass
            await self._emit_event_best_effort(SandboxEventType.CHECKPOINT_STARTED)
            try:
                await self._scrub_live_output()
                image = await self._sandbox.snapshot_filesystem.aio(
                    timeout=self._checkpoint_timeout_seconds,
                    ttl=self._checkpoint_ttl_seconds,
                )
            except Exception as exc:
                await self._emit_event_best_effort(
                    SandboxEventType.CHECKPOINT_FAILED,
                    message=type(exc).__name__,
                )
                raise
            image_id = str(image.object_id)
            if not image_id.startswith("im-"):
                raise RuntimeError(f"Modal checkpoint returned invalid image ID: {image_id!r}")
            created_at_ms = int(time.time() * 1000)
            checkpoint = CheckpointRef(
                provider="modal",
                checkpoint_id=image_id,
                uri=f"modal-image://{image_id}",
                created_at_ms=created_at_ms,
                environment=self._spec.environment,
                source_sandbox_id=self.identity.sandbox_id,
                owner_id=self._spec.metadata_dict().get("mission", ""),
                locality=CheckpointLocality.PROVIDER,
                expires_at_ms=(
                    created_at_ms + self._checkpoint_ttl_seconds * 1000
                    if self._checkpoint_ttl_seconds is not None
                    else None
                ),
            )
            await self._emit_event_best_effort(
                SandboxEventType.CHECKPOINT_FINISHED,
                checkpoint_uri=checkpoint.uri,
            )
            return checkpoint

    async def close(self) -> None:
        async with self._lock:
            if self._status is SandboxStatus.CLOSED:
                return
            await self._emit_event_best_effort(SandboxEventType.CLOSING)
            resources = tuple(self._close_resources.items())
            results = await asyncio.gather(
                *(self._terminate(resource) for _label, resource in resources),
                return_exceptions=True,
            )
            failures: list[BaseException] = []
            for (label, _resource), result in zip(resources, results, strict=True):
                if isinstance(result, BaseException):
                    failures.append(result)
                else:
                    self._close_resources.pop(label, None)
            if failures:
                self._status = SandboxStatus.ERRORED
                raise BaseExceptionGroup(
                    f"failed to close {len(failures)} Modal resource(s)", failures
                )
            self._status = SandboxStatus.CLOSED

    def _interactive_state(self, trace_id: str) -> _ModalInteractiveSession:
        paths = self._live_observation_paths(trace_id)
        short = trace_id[:12]
        directory = paths["trace_directory"]
        return _ModalInteractiveSession(
            trace_id=trace_id,
            directory=directory,
            tmux_socket=f"{directory}/tmux.sock",
            app_socket=f"{directory}/app-server.sock",
            app_session=f"app-{short}",
            tui_session=f"tui-{short}",
            spectate_session=f"watch-{short}",
            takeover_session=f"take-{short}",
        )

    async def _prepare_interactive_session(self, state: _ModalInteractiveSession) -> None:
        await self._ensure_live_directory()
        await self._clear_live_output()
        await self._checked(ProcessRequest(("mkdir", "-p", state.directory), timeout_seconds=60))
        helper = f"{state.directory}/tmux-events.sh"
        bridge = f"{state.directory}/unix-websocket-bridge.py"
        events = f"{state.directory}/tmux.events.jsonl"
        await self._sandbox.filesystem.write_text.aio(_TMUX_EVENTS_HELPER, helper)
        await self._sandbox.filesystem.write_text.aio(_UNIX_WEBSOCKET_BRIDGE, bridge)
        await self._checked(ProcessRequest(("chmod", "700", helper), timeout_seconds=60))
        await self._checked(ProcessRequest(("chmod", "700", bridge), timeout_seconds=60))

        setup: list[str] = [
            "tmux",
            "-S",
            state.tmux_socket,
            "start-server",
            ";",
            "set-option",
            "-s",
            "exit-empty",
            "off",
            ";",
            "set-option",
            "-g",
            "remain-on-exit",
            "on",
            ";",
            "set-option",
            "-g",
            "window-size",
            "latest",
            ";",
            "set-option",
            "-g",
            "prefix",
            "None",
            ";",
            "set-option",
            "-g",
            "prefix2",
            "None",
            ";",
            "unbind-key",
            "-a",
        ]
        for hook in _TMUX_HOOKS:
            setup.extend(
                (
                    ";",
                    "set-hook",
                    "-g",
                    hook,
                    "run-shell '"
                    f"{shlex.quote(helper)} {hook} #{{session_name}} {shlex.quote(events)}'",
                )
            )
        await self._checked(ProcessRequest(tuple(setup), timeout_seconds=60))

        codex_home = f"{self.capabilities.home_directory.rstrip('/')}/.codex"
        app_argv = (
            "env",
            f"CODEX_HOME={codex_home}",
            "NO_COLOR=1",
            f"{_INTERACTIVE_SESSION_ENV}={state.trace_id}",
            "codex",
            "app-server",
            "-c",
            'shell_environment_policy.inherit="core"',
            "-c",
            'shell_environment_policy.exclude=["*KEY*","*SECRET*","*TOKEN*"]',
            "-c",
            'cli_auth_credentials_store="file"',
            "--listen",
            f"unix://{state.app_socket}",
        )
        await self._checked(
            ProcessRequest(
                (
                    "tmux",
                    "-S",
                    state.tmux_socket,
                    "new-session",
                    "-d",
                    "-s",
                    state.app_session,
                    "-x",
                    "220",
                    "-y",
                    "50",
                    "-c",
                    self._spec.workdir,
                    *app_argv,
                ),
                timeout_seconds=60,
            )
        )
        await self._record_tmux_pane(
            state,
            state.app_session,
            f"{state.directory}/app-server.stream.log",
        )
        wait_script = (
            'socket="$1"; i=0; while [ "$i" -lt 100 ]; do '
            '[ -S "$socket" ] && exit 0; i=$((i + 1)); sleep 0.1; done; exit 1'
        )
        ready = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                ("sh", "-c", wait_script, "wait-app-server", state.app_socket),
                timeout_seconds=15,
            ),
        )
        self._raise(ready, "wait for Codex app-server")
        await self._emit_event_best_effort(
            SandboxEventType.PROCESS_STARTED,
            operation="codex-app-server",
        )

    async def _start_app_server_proxy(self, state: _ModalInteractiveSession) -> Any:
        paths = self._live_observation_paths()
        bridge = f"{state.directory}/unix-websocket-bridge.py"
        script = (
            'stdout="$1"; stderr="$2"; shift 2; '
            '"$@" > >(tee -a "$stdout") 2> >(tee -a "$stderr" >&2)'
        )
        codex_home = f"{self.capabilities.home_directory.rstrip('/')}/.codex"
        return await self._sandbox.exec.aio(
            "bash",
            "-o",
            "pipefail",
            "-c",
            script,
            "archetype-app-server-proxy",
            paths["stdout"],
            paths["stderr"],
            "python3",
            bridge,
            "--socket",
            state.app_socket,
            workdir=self._spec.workdir,
            timeout=self._spec.timeout_seconds,
            env={"CODEX_HOME": codex_home, "NO_COLOR": "1"},
        )

    async def _start_codex_tui(
        self,
        state: _ModalInteractiveSession,
        thread: CodexThread,
    ) -> None:
        if state.stopped:
            raise RuntimeError("Modal interactive session is already stopped")
        if state.tui_started:
            raise RuntimeError("Modal Codex TUI is already attached")
        codex_home = f"{self.capabilities.home_directory.rstrip('/')}/.codex"
        await self._checked(
            ProcessRequest(
                (
                    "tmux",
                    "-S",
                    state.tmux_socket,
                    "new-session",
                    "-d",
                    "-s",
                    state.tui_session,
                    "-x",
                    "220",
                    "-y",
                    "50",
                    "-c",
                    self._spec.workdir,
                    "env",
                    f"CODEX_HOME={codex_home}",
                    "NO_COLOR=1",
                    f"{_INTERACTIVE_SESSION_ENV}={state.trace_id}",
                    "codex",
                    "resume",
                    "--remote",
                    f"unix://{state.app_socket}",
                    "--no-alt-screen",
                    "--dangerously-bypass-approvals-and-sandbox",
                    "-C",
                    self._spec.workdir,
                    thread.thread_id,
                ),
                timeout_seconds=60,
            )
        )
        await self._record_tmux_pane(
            state,
            state.tui_session,
            f"{state.directory}/codex-tui.stream.log",
        )
        wait_script = (
            'socket="$1"; session="$2"; i=0; stable=0; while [ "$i" -lt 600 ]; do '
            'dead="$(tmux -S "$socket" display-message -p -t "$session" '
            "'#{pane_dead}' 2>/dev/null || printf 1)\"; "
            'screen="$(tmux -S "$socket" capture-pane -p -t "$session" -S -50 '
            '2>/dev/null | tr -d "[:space:]")"; '
            '[ "$dead" = 1 ] && exit 2; '
            'if [ "$dead" = 0 ] && printf %s "$screen" | grep -F "esctointerrupt" >/dev/null; then '
            "stable=$((stable + 1)); else stable=0; fi; "
            '[ "$stable" -ge 3 ] && exit 0; '
            "i=$((i + 1)); sleep 0.1; done; exit 1"
        )
        ready = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                (
                    "sh",
                    "-c",
                    wait_script,
                    "wait-codex-tui",
                    state.tmux_socket,
                    state.tui_session,
                ),
                timeout_seconds=65,
            ),
        )
        if ready.returncode != 0:
            pane = await self._exec_on(
                self._sandbox,
                ProcessRequest(
                    (
                        "tmux",
                        "-S",
                        state.tmux_socket,
                        "capture-pane",
                        "-p",
                        "-t",
                        state.tui_session,
                        "-S",
                        "-200",
                    ),
                    timeout_seconds=60,
                ),
            )
            status = await self._exec_on(
                self._sandbox,
                ProcessRequest(
                    (
                        "tmux",
                        "-S",
                        state.tmux_socket,
                        "display-message",
                        "-p",
                        "-t",
                        state.tui_session,
                        "pane_dead=#{pane_dead} pane_status=#{pane_dead_status}",
                    ),
                    timeout_seconds=60,
                ),
            )
            detail = "\n".join(
                item.strip() for item in (status.stdout, pane.stdout, ready.stderr) if item.strip()
            )[-4000:]
            raise RuntimeError(f"wait for Codex TUI failed: {detail or 'no pane output'}")
        state.thread_id = thread.thread_id
        state.tui_started = True
        await self._start_ttyd_lane(
            state,
            lane_session=state.spectate_session,
            port=_SPECTATE_PORT,
            writable=False,
        )
        state.spectate_started = True
        await self._emit_event_best_effort(
            SandboxEventType.SESSION_READY,
            operation="codex-spectate",
            message=f"thread_id={thread.thread_id}",
        )

    async def _start_codex_takeover(
        self,
        state: _ModalInteractiveSession,
        turn: CodexTurn,
    ) -> None:
        if state.stopped:
            raise RuntimeError("Modal interactive session is already stopped")
        if not state.tui_started or not state.spectate_started:
            raise RuntimeError("Modal Codex TUI is not ready for takeover")
        if state.thread_id != turn.thread_id:
            raise RuntimeError("Modal Codex takeover belongs to a different thread")
        if state.takeover_started:
            raise RuntimeError("Modal Codex takeover is already open")
        await self._start_ttyd_lane(
            state,
            lane_session=state.takeover_session,
            port=_TAKEOVER_PORT,
            writable=True,
        )
        state.takeover_started = True
        await self._emit_event_best_effort(
            SandboxEventType.SESSION_READY,
            operation="codex",
            message=f"thread_id={turn.thread_id} turn_id={turn.turn_id}",
        )

    async def _finish_codex_turn(
        self,
        state: _ModalInteractiveSession,
        completion: CodexTurnCompletion,
    ) -> None:
        if state.thread_id and completion.thread_id != state.thread_id:
            raise RuntimeError("Modal Codex completion belongs to a different thread")
        # A holder of the privileged takeover capability can still race input
        # during the provider transit between emitting turn/completed and this
        # kill reaching tmux. The lane is therefore trusted-maintainer-only.
        # Crucially, the app-server client does not release the exact completion
        # waiter (and the harness cannot begin validators) until this returns.
        await self._stop_interactive_session(state)

    async def _start_ttyd_lane(
        self,
        state: _ModalInteractiveSession,
        *,
        lane_session: str,
        port: int,
        writable: bool,
    ) -> None:
        ttyd: list[str] = [
            "env",
            f"{_INTERACTIVE_SESSION_ENV}={state.trace_id}",
            "ttyd",
            "--port",
            str(port),
            "--interface",
            "0.0.0.0",
        ]
        if writable:
            ttyd.extend(("--writable", "--max-clients", "1"))
        ttyd.extend(("tmux", "-S", state.tmux_socket, "attach-session"))
        if not writable:
            ttyd.append("-r")
        ttyd.extend(("-t", state.tui_session))
        await self._checked(
            ProcessRequest(
                (
                    "tmux",
                    "-S",
                    state.tmux_socket,
                    "new-session",
                    "-d",
                    "-s",
                    lane_session,
                    *ttyd,
                ),
                timeout_seconds=60,
            )
        )
        wait_script = (
            'port="$1"; i=0; while [ "$i" -lt 100 ]; do '
            'curl --fail --silent --max-time 1 "http://127.0.0.1:$port/" '
            ">/dev/null && exit 0; i=$((i + 1)); sleep 0.1; done; exit 1"
        )
        ready = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                ("sh", "-c", wait_script, "wait-ttyd", str(port)),
                timeout_seconds=15,
            ),
        )
        self._raise(ready, f"wait for ttyd port {port}")

    async def _record_tmux_pane(
        self,
        state: _ModalInteractiveSession,
        session_name: str,
        path: str,
    ) -> None:
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                (
                    "tmux",
                    "-S",
                    state.tmux_socket,
                    "pipe-pane",
                    "-o",
                    "-t",
                    session_name,
                    f"cat >> {shlex.quote(path)}",
                ),
                timeout_seconds=60,
            ),
        )
        if result.returncode != 0 and "has exited" not in result.stderr:
            self._raise(result, "record tmux pane")

    async def _stop_interactive_session(self, state: _ModalInteractiveSession) -> None:
        if state.stopped:
            return
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                ("tmux", "-S", state.tmux_socket, "kill-server"),
                timeout_seconds=60,
            ),
        )
        if result.returncode != 0 and not any(
            detail in result.stderr
            for detail in ("no server running", "No such file", "error connecting to")
        ):
            self._raise(result, "stop tmux server")
        await self._quiesce_interactive_processes(state)
        state.stopped = True
        await self._emit_event_best_effort(
            SandboxEventType.PROCESS_FINISHED,
            operation="codex-app-server",
        )

    async def _quiesce_interactive_processes(self, state: _ModalInteractiveSession) -> None:
        """Terminate and then fail closed on marked detached agent descendants."""

        marker = f"{_INTERACTIVE_SESSION_ENV}={state.trace_id}"
        script = (
            'marker="$1"; test -r /proc/self/environ || exit 3; '
            "scan() { "
            "for environment in /proc/[0-9]*/environ; do "
            'grep -z -F -x -- "$marker" "$environment" >/dev/null 2>&1 || continue; '
            'pid="${environment#/proc/}"; pid="${pid%/environ}"; printf "%s " "$pid"; '
            "done; }; "
            'pids="$(scan)"; [ -z "$pids" ] && exit 0; '
            "kill -TERM $pids >/dev/null 2>&1 || true; "
            'i=0; stable=0; while [ "$i" -lt 50 ]; do '
            'pids="$(scan)"; '
            'if [ -z "$pids" ]; then stable=$((stable + 1)); else stable=0; fi; '
            '[ "$stable" -ge 3 ] && exit 0; '
            '[ "$i" -ge 20 ] && kill -KILL $pids >/dev/null 2>&1 || true; '
            "i=$((i + 1)); sleep 0.1; done; "
            'pids="$(scan)"; printf "interactive descendants survived: %s\\n" "$pids" >&2; '
            "exit 1"
        )
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                ("sh", "-c", script, "quiesce-interactive-processes", marker),
                timeout_seconds=15,
            ),
        )
        self._raise(result, "quiesce detached interactive processes")

    async def _stage_oauth(self) -> None:
        try:
            payload = await self._auth_sandbox.filesystem.read_text.aio(_AUTH_VOLUME_PATH)
        except Exception as exc:
            raise RuntimeError(
                f"Modal OAuth volume {self._auth_volume_name!r} has no Codex credential"
            ) from exc
        self._validate_oauth(payload)
        await self._checked(
            ProcessRequest(
                ("sh", "-c", f"rm -rf {_CODEX_HOME} && install -d -m 700 {_CODEX_HOME}"),
                timeout_seconds=60,
            )
        )
        await self._sandbox.filesystem.write_text.aio(payload, _MISSION_AUTH_PATH)
        await self._checked(
            ProcessRequest(("chmod", "600", _MISSION_AUTH_PATH), timeout_seconds=60)
        )

    async def _scrub_mission_oauth(self) -> None:
        """Remove only auth.json after thread admission, preserving rollout state."""

        await self._checked(
            ProcessRequest(("rm", "-f", "--", _MISSION_AUTH_PATH), timeout_seconds=60)
        )
        await self._checked(
            ProcessRequest(("test", "!", "-e", _MISSION_AUTH_PATH), timeout_seconds=60)
        )

    async def _remove_oauth(self) -> None:
        await self._checked(ProcessRequest(("rm", "-rf", _CODEX_HOME), timeout_seconds=60))

    @classmethod
    def live_observation_paths(cls, trace_id: str = "") -> dict[str, str]:
        return live_observation_paths(trace_id=trace_id)

    def _live_observation_paths(self, trace_id: str = "") -> dict[str, str]:
        return live_observation_paths(self.capabilities.observation_directory, trace_id)

    async def _ensure_live_directory(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(("mkdir", "-p", paths["directory"]), timeout_seconds=60),
        )
        self._raise(result, "create live observation directory")

    async def _scrub_live_output(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                (
                    "rm",
                    "-rf",
                    paths["stdout"],
                    paths["stderr"],
                    f"{paths['directory']}/executions",
                ),
                timeout_seconds=60,
            ),
        )
        self._raise(result, "remove raw live output before checkpoint")

    async def _clear_live_output(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(("rm", "-f", paths["stdout"], paths["stderr"]), timeout_seconds=60),
        )
        self._raise(result, "clear stale live output before process")

    async def _capture_live_output_best_effort(self, trace_id: str) -> str:
        try:
            current = self._live_observation_paths()
            captured = self._live_observation_paths(trace_id)
            mkdir = await self._exec_on(
                self._sandbox,
                ProcessRequest(("mkdir", "-p", captured["trace_directory"]), timeout_seconds=60),
            )
            self._raise(mkdir, "create execution trace directory")
            for stream in ("stdout", "stderr"):
                copied = await self._exec_on(
                    self._sandbox,
                    ProcessRequest(
                        ("cp", "--", current[stream], captured[stream]),
                        timeout_seconds=60,
                    ),
                )
                self._raise(copied, f"capture execution {stream}")
            return f"modal-sandbox://{self.identity.sandbox_id}{captured['stdout']}"
        except Exception:
            return ""

    def _trace_request(self, request: ProcessRequest) -> ProcessRequest:
        paths = self._live_observation_paths()
        script = (
            'stdout="$1"; stderr="$2"; shift 2; '
            '"$@" > >(tee -a "$stdout") 2> >(tee -a "$stderr" >&2)'
        )
        return ProcessRequest(
            (
                "bash",
                "-o",
                "pipefail",
                "-c",
                script,
                "archetype-agent-trace",
                paths["stdout"],
                paths["stderr"],
                *request.argv,
            ),
            workdir=request.workdir,
            timeout_seconds=request.timeout_seconds,
            env=request.env,
            close_stdin=True,
        )

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            await self._emit_event_best_effort(SandboxEventType.HEARTBEAT)

    async def _emit_event_best_effort(
        self,
        event_type: SandboxEventType,
        *,
        operation: str = "",
        returncode: int | None = None,
        checkpoint_uri: str = "",
        message: str = "",
    ) -> None:
        """Emit non-authoritative live observation without changing provider outcomes."""

        try:
            await self._emit_event(
                event_type,
                operation=operation,
                returncode=returncode,
                checkpoint_uri=checkpoint_uri,
                message=message,
            )
        except Exception:
            pass

    async def _emit_event(
        self,
        event_type: SandboxEventType,
        *,
        operation: str = "",
        returncode: int | None = None,
        checkpoint_uri: str = "",
        message: str = "",
    ) -> None:
        paths = self._live_observation_paths()
        async with self._event_lock:
            self._event_sequence += 1
            event = SandboxEvent(
                kind=event_type,
                sandbox=self.identity,
                timestamp_ms=int(time.time() * 1000),
                sequence=self._event_sequence,
                operation=operation,
                returncode=returncode,
                checkpoint_uri=checkpoint_uri,
                message=message,
            ).record()
            line = json.dumps(event, sort_keys=True) + "\n"
            try:
                existing = str(await self._sandbox.filesystem.read_text.aio(paths["events"]))
            except Exception as exc:
                if not self._is_missing_path(exc):
                    raise
                existing = ""
            await self._sandbox.filesystem.write_text.aio(existing + line, paths["events"])
            await self._sandbox.filesystem.write_text.aio(line, paths["status"])

    @classmethod
    async def monitor(
        cls,
        sandbox_id: str,
        *,
        follow: bool = True,
        poll_seconds: float = 1.0,
        disconnect_grace_seconds: float = 180.0,
        stdout_target: Any = None,
        stderr_target: Any = None,
        on_monitor_event: Callable[[dict[str, str]], None] | None = None,
    ) -> dict[str, Any]:
        """Attach to one Modal sandbox's durable live observation files."""

        if not sandbox_id.startswith("sb-"):
            raise ValueError("Modal sandbox IDs must start with 'sb-'")
        if poll_seconds <= 0 or disconnect_grace_seconds <= 0:
            raise ValueError("monitor timing values must be positive")
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc
        sandbox = await modal.Sandbox.from_id.aio(sandbox_id)
        paths = cls.live_observation_paths()
        offsets: dict[str, int] = {}
        status: dict[str, Any] = {}
        disconnected_at: float | None = None

        async def read(path: str) -> str:
            try:
                return str(await sandbox.filesystem.read_text.aio(path))
            except Exception as exc:
                if cls._is_missing_path(exc):
                    return ""
                raise

        while True:
            try:
                status_text, events, stdout, stderr = await asyncio.gather(
                    read(paths["status"]),
                    read(paths["events"]),
                    read(paths["stdout"]),
                    read(paths["stderr"]),
                )
                if status_text:
                    try:
                        parsed = json.loads(status_text)
                    except json.JSONDecodeError:
                        parsed = {}
                    if isinstance(parsed, dict):
                        status = parsed
                cls._write_delta(paths["events"], events, offsets, stdout_target)
                cls._write_delta(paths["stdout"], stdout, offsets, stdout_target)
                cls._write_delta(paths["stderr"], stderr, offsets, stderr_target)
                provider_returncode = await sandbox.poll.aio()
            except Exception as exc:
                if not follow:
                    raise
                now = time.monotonic()
                disconnected_at = disconnected_at or now
                if now - disconnected_at >= disconnect_grace_seconds:
                    if on_monitor_event is not None:
                        on_monitor_event(
                            {
                                "type": "monitor_disconnected",
                                "sandbox_id": sandbox_id,
                                "error": str(exc)[-1000:],
                            }
                        )
                    return status
                await asyncio.sleep(poll_seconds)
                continue
            if disconnected_at is not None:
                if on_monitor_event is not None:
                    on_monitor_event({"type": "monitor_reconnected", "sandbox_id": sandbox_id})
                disconnected_at = None
            if provider_returncode is not None:
                status = {**status, "provider_returncode": int(provider_returncode)}
                return status
            if not follow or status.get("type") == SandboxEventType.CLOSING.value:
                return status
            await asyncio.sleep(poll_seconds)

    @staticmethod
    def _write_delta(path: str, value: str, offsets: dict[str, int], target: Any) -> None:
        previous = offsets.get(path, 0)
        if previous > len(value):
            previous = 0
        delta = value[previous:]
        offsets[path] = len(value)
        if delta and target is not None:
            target.write(delta)
            target.flush()

    @staticmethod
    def _is_missing_path(exc: BaseException) -> bool:
        return isinstance(exc, FileNotFoundError) or type(exc).__name__ == (
            "SandboxFilesystemNotFoundError"
        )

    async def _checked(self, request: ProcessRequest) -> ProcessResult:
        result = await self._exec_on(self._sandbox, request)
        self._raise(result, request.argv[0])
        return result

    @staticmethod
    async def _exec_on(
        sandbox: Any,
        request: ProcessRequest,
        *,
        secrets: list[Any] | None = None,
    ) -> ProcessResult:
        process = await sandbox.exec.aio(
            *request.argv,
            workdir=request.workdir,
            timeout=request.timeout_seconds,
            secrets=secrets or [],
            env=request.environment_dict() or None,
        )
        if request.close_stdin:
            # Modal exposes stdin as an open pipe. Codex otherwise waits for
            # optional prompt input even when the prompt is in argv.
            process.stdin.write_eof()
            await process.stdin.drain.aio()
        stdout_task = asyncio.create_task(process.stdout.read.aio())
        stderr_task = asyncio.create_task(process.stderr.read.aio())
        returncode, stdout, stderr = await asyncio.gather(
            process.wait.aio(), stdout_task, stderr_task
        )
        return ProcessResult(
            argv=request.argv,
            returncode=int(returncode),
            stdout=str(stdout),
            stderr=str(stderr),
        )

    @staticmethod
    def _validate_oauth(payload: str) -> None:
        try:
            value = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Codex OAuth credential is not valid JSON") from exc
        if not isinstance(value, dict) or not value:
            raise RuntimeError("Codex OAuth credential must be a non-empty JSON object")

    @staticmethod
    def _raise(result: ProcessResult, label: str) -> None:
        if result.returncode != 0:
            detail = result.stderr or result.stdout
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")

    @staticmethod
    async def _terminate(sandbox: Any) -> None:
        try:
            await sandbox.terminate.aio(wait=True)
        finally:
            await sandbox.detach.aio()


@dataclass(frozen=True, slots=True)
class ModalCodexAppServerConnector:
    """Bind the app-server driver to one live Modal session."""

    def connect(self, session: SandboxSession) -> Any:
        if not isinstance(session, ModalSandboxSession):
            raise TypeError("Modal app-server connector requires a Modal sandbox session")
        return session.codex_app_server()


class ModalSandboxBackend:
    """Create Modal sessions; task and repository policy stay in the harness."""

    name = "modal"

    def __init__(self, config: ModalSandboxConfig | None = None) -> None:
        self.config = config or ModalSandboxConfig()

    @property
    def environment(self) -> str:
        """Declared identity of the named image or complete default recipe."""

        if self.config.image_id:
            return f"modal-image://{self.config.image_id}"
        return _default_environment()

    async def create(self, spec: SandboxSpec) -> SandboxSession:
        if spec.provider != self.name:
            raise ValueError("Modal backend received a non-Modal sandbox spec")
        if spec.environment != self.environment:
            raise ValueError(
                f"Modal environment must be {self.environment!r}, got {spec.environment!r}"
            )
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc

        client = await self._verify_context_binding(modal, phase="sandbox create")
        image = (
            modal.Image.from_id(self.config.image_id)
            if self.config.image_id
            else self._default_image(modal)
        )
        return await self._start(modal, spec, image, client=client)

    async def login_codex(self) -> None:
        """Persist an interactive Codex subscription login in the broker volume."""

        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc
        client = await self._verify_context_binding(modal, phase="Codex login")
        app = await modal.App.lookup.aio(
            self.config.app_name,
            create_if_missing=True,
            environment_name=self.config.environment_name,
            client=client,
        )
        volume = modal.Volume.from_name(
            self.config.auth_volume_name,
            create_if_missing=True,
            version=2,
            environment_name=self.config.environment_name,
            client=client,
        )
        await volume.hydrate.aio()
        image = (
            modal.Image.from_id(self.config.image_id)
            if self.config.image_id
            else self._default_image(modal)
        )
        sandbox = await modal.Sandbox.create.aio(
            "sleep",
            "infinity",
            app=app,
            image=image,
            timeout=self.config.login_timeout_seconds,
            idle_timeout=self.config.login_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: volume},
            tags={"kind": "archetype-agent-oauth-login"},
            client=client,
        )
        try:
            process = await sandbox.exec.aio(
                "codex",
                "login",
                "--device-auth",
                "-c",
                'cli_auth_credentials_store="file"',
                workdir=_AUTH_MOUNT,
                timeout=self.config.login_timeout_seconds,
                env={"CODEX_HOME": _AUTH_MOUNT, "NO_COLOR": "1"},
                pty=True,
            )
            returncode = await self._passthrough_process(process)
            if returncode != 0:
                raise RuntimeError(f"Codex device login failed with exit code {returncode}")
            result = await ModalSandboxSession._exec_on(
                sandbox,
                ProcessRequest(
                    ("codex", "login", "status"),
                    workdir=_AUTH_MOUNT,
                    timeout_seconds=60,
                    env=(("CODEX_HOME", _AUTH_MOUNT),),
                ),
            )
            ModalSandboxSession._raise(result, "Codex device login verification")
        finally:
            cleanup = asyncio.create_task(self._cleanup_login(sandbox))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise

    async def _verify_context_binding(self, modal: Any, *, phase: str) -> Any | None:
        """Verify configured ambient workspace/environment before mutation."""

        expected_workspace = self.config.workspace_name
        expected_environment = self.config.environment_name
        if expected_workspace is None and expected_environment is None:
            return None
        client: Any | None = None
        try:
            if expected_workspace is not None:
                workspace = modal.Workspace.from_context()
                await workspace.hydrate.aio()
                observed_workspace = str(workspace.name or "")
                if observed_workspace != expected_workspace:
                    raise RuntimeError(
                        f"Modal {phase} workspace identity does not match configured namespace"
                    )
                client = workspace.client
            if expected_environment is not None:
                environment = modal.Environment.from_context(client=client)
                await environment.hydrate.aio()
                observed_environment = str(environment.name or "")
                if observed_environment != expected_environment:
                    raise RuntimeError(
                        f"Modal {phase} environment identity does not match configured namespace"
                    )
                if client is None:
                    client = environment.client
        except asyncio.CancelledError:
            raise
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Modal {phase} namespace lookup failed ({type(exc).__name__[:128]})"
            ) from exc
        return client

    @staticmethod
    async def _cleanup_login(sandbox: Any) -> None:
        try:
            result = await ModalSandboxSession._exec_on(
                sandbox,
                ProcessRequest(
                    (
                        "sh",
                        "-c",
                        f"find {_AUTH_MOUNT} -mindepth 1 -maxdepth 1 "
                        "! -name auth.json -exec rm -rf -- {} + "
                        f"&& chmod 600 {_AUTH_VOLUME_PATH} && sync {_AUTH_MOUNT}",
                    ),
                    timeout_seconds=60,
                ),
            )
            ModalSandboxSession._raise(result, "Codex OAuth volume cleanup")
        finally:
            await ModalSandboxSession._terminate(sandbox)

    @staticmethod
    async def _passthrough_process(process: Any) -> int:
        """Bridge a remote device-login PTY without retaining its output."""

        loop = asyncio.get_running_loop()
        writes: set[asyncio.Task[Any]] = set()
        stdin_fd: int | None = None

        async def write_remote(data: bytes) -> None:
            process.stdin.write(data)
            await process.stdin.drain.aio()

        def stdin_ready() -> None:
            assert stdin_fd is not None
            try:
                data = os.read(stdin_fd, 4096)
            except OSError:
                data = b""
            if not data:
                loop.remove_reader(stdin_fd)
                return
            task = asyncio.create_task(write_remote(data))
            writes.add(task)
            task.add_done_callback(writes.discard)

        async def pump(reader: Any, target: Any) -> None:
            async for chunk in reader:
                value = chunk.decode(errors="replace") if isinstance(chunk, bytes) else str(chunk)
                target.write(value)
                target.flush()

        try:
            try:
                stdin_fd = sys.stdin.fileno()
                loop.add_reader(stdin_fd, stdin_ready)
            except (AttributeError, OSError, ValueError, NotImplementedError):
                stdin_fd = None
            stdout = asyncio.create_task(pump(process.stdout, sys.stdout))
            stderr = asyncio.create_task(pump(process.stderr, sys.stderr))
            returncode = int(await process.wait.aio())
            await asyncio.gather(stdout, stderr)
            return returncode
        finally:
            if stdin_fd is not None:
                try:
                    loop.remove_reader(stdin_fd)
                except (OSError, ValueError):
                    pass
            if writes:
                await asyncio.gather(*writes, return_exceptions=True)

    async def _start(
        self,
        modal: Any,
        spec: SandboxSpec,
        image: Any,
        *,
        operation_identity: ModalSandboxOperationIdentity | None = None,
        resource_observer: ModalSandboxResourceObserver | None = None,
        client: Any | None = None,
    ) -> SandboxSession:
        if operation_identity is not None:
            self._validate_operation_identity(operation_identity)
        app = await modal.App.lookup.aio(
            self.config.app_name,
            create_if_missing=True,
            environment_name=self.config.environment_name,
            client=client,
        )
        auth_volume = modal.Volume.from_name(
            self.config.auth_volume_name,
            create_if_missing=False,
            version=2,
            environment_name=self.config.environment_name,
            client=client,
        )
        await auth_volume.hydrate.aio()
        github_secret = modal.Secret.from_name(
            self.config.github_secret_name,
            required_keys=["GITHUB_TOKEN"],
            environment_name=self.config.environment_name,
            client=client,
        )
        metadata = spec.metadata_dict()
        operation_cohort_id = f"cohort-v1:{uuid4().hex}" if operation_identity is not None else None
        if resource_observer is not None:
            if operation_identity is None or operation_cohort_id is None:
                raise ValueError("Modal resource observation requires a named operation")
            await resource_observer(
                operation_identity,
                operation_cohort_id,
                "intent",
                "",
            )
        operation_tags = (
            {
                "operation_digest": operation_identity.digest,
                "operation_cohort": str(operation_cohort_id),
                "operation_protocol_epoch": str(operation_identity.protocol_epoch),
            }
            if operation_identity is not None
            else {}
        )
        auth_name = (
            {"name": operation_identity.auth_sandbox_name} if operation_identity is not None else {}
        )
        auth_sandbox = await modal.Sandbox.create.aio(
            "sleep",
            "infinity",
            app=app,
            image=image,
            timeout=spec.timeout_seconds,
            idle_timeout=spec.idle_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: auth_volume},
            tags={**metadata, "kind": "archetype-agent-auth", **operation_tags},
            client=client,
            **auth_name,
        )
        try:
            if resource_observer is not None:
                assert operation_identity is not None
                assert operation_cohort_id is not None
                await resource_observer(
                    operation_identity,
                    operation_cohort_id,
                    "auth",
                    str(auth_sandbox.object_id),
                )
            mission_name = (
                {"name": operation_identity.mission_sandbox_name}
                if operation_identity is not None
                else {}
            )
            sandbox = await modal.Sandbox.create.aio(
                "sleep",
                "infinity",
                app=app,
                image=image,
                timeout=spec.timeout_seconds,
                idle_timeout=spec.idle_timeout_seconds,
                workdir=str(PurePosixPath(spec.workdir).parent),
                tags={**metadata, "kind": "archetype-agent-mission", **operation_tags},
                client=client,
                **mission_name,
            )
            if resource_observer is not None:
                assert operation_identity is not None
                assert operation_cohort_id is not None
                try:
                    await resource_observer(
                        operation_identity,
                        operation_cohort_id,
                        "mission",
                        str(sandbox.object_id),
                    )
                except BaseException:
                    await ModalSandboxSession._terminate(sandbox)
                    raise
        except BaseException:
            await ModalSandboxSession._terminate(auth_sandbox)
            raise
        session = ModalSandboxSession(
            spec=spec,
            sandbox=sandbox,
            auth_sandbox=auth_sandbox,
            github_secret=github_secret,
            auth_volume_name=self.config.auth_volume_name,
            checkpoint_timeout_seconds=self.config.checkpoint_timeout_seconds,
            checkpoint_ttl_seconds=self.config.checkpoint_ttl_seconds,
            heartbeat_seconds=self.config.heartbeat_seconds,
            operation_identity=operation_identity,
            operation_cohort_id=operation_cohort_id,
        )
        try:
            await verify_coding_agent_environment(
                session,
                spec,
                expected_user="root",
                verify_environment=not self.config.image_id,
            )
        except BaseException:
            await session.close()
            raise
        return session

    def _validate_operation_identity(
        self,
        identity: ModalSandboxOperationIdentity,
    ) -> None:
        expected = (
            self.config.workspace_name,
            self.config.environment_name,
            self.config.app_name,
            self.config.operation_protocol_epoch,
        )
        observed = (
            identity.workspace_name,
            identity.environment_name,
            identity.app_name,
            identity.protocol_epoch,
        )
        if observed != expected:
            raise ValueError("Modal operation identity does not match backend namespace")

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> SandboxSession:
        if spec.provider != self.name:
            raise ValueError("Modal backend received a non-Modal sandbox spec")
        if spec.environment != self.environment:
            raise ValueError(
                f"Modal environment must be {self.environment!r}, got {spec.environment!r}"
            )
        validate_checkpoint_for_spec(checkpoint, spec)
        image_id = self._checkpoint_image(checkpoint)
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc
        client = await self._verify_context_binding(modal, phase="sandbox restore")
        image = modal.Image.from_id(image_id)
        session = await self._start(modal, spec, image, client=client)
        try:
            result = await session.exec(
                ProcessRequest(("test", "-d", spec.workdir), timeout_seconds=60)
            )
            ModalSandboxSession._raise(result, "verify restored workspace")
        except BaseException:
            await session.close()
            raise
        return session

    @staticmethod
    def _checkpoint_image(checkpoint: CheckpointRef) -> str:
        if checkpoint.provider != "modal":
            raise ValueError("Modal checkpoint provider does not match")
        if checkpoint.locality is not CheckpointLocality.PROVIDER:
            raise ValueError("Modal checkpoint locality does not match")
        prefix = "modal-image://"
        if not checkpoint.uri.startswith(prefix) or "#" in checkpoint.uri:
            raise ValueError("invalid Modal image checkpoint")
        image_id = checkpoint.uri.removeprefix(prefix)
        if image_id != checkpoint.checkpoint_id or not image_id.startswith("im-"):
            raise ValueError("invalid Modal image checkpoint")
        return image_id

    @staticmethod
    def _default_image(modal: Any) -> Any:
        return (
            modal.Image.from_registry(BASE_IMAGE_REF)
            .apt_install(
                "ca-certificates",
                "curl",
                "git",
                "nodejs",
                "npm",
                "openssh-client",
                "tmux",
            )
            .run_commands(
                "mkdir -p /workspace",
                f"# {codex_package()}\n{codex_install_command()}",
                f"# ttyd {load_version_inventory().resolve('ttyd-x86-64').version}\n"
                f"{ttyd_install_command()}",
            )
            .env({"ARCHETYPE_SANDBOX_ENVIRONMENT": _default_environment()})
        )


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationRunning:
    """Bounded evidence that one coherent named cohort was running.

    This value deliberately carries no executable session. Reconciliation
    cannot grant a second worker access to the inner ``exec`` boundary.
    """

    identity: ModalSandboxOperationIdentity
    mission_sandbox_id: str
    auth_sandbox_id: str
    cohort_id: str

    def __post_init__(self) -> None:
        if not self.mission_sandbox_id.strip() or not self.auth_sandbox_id.strip():
            raise ValueError("running Modal operation requires both sandbox identities")
        if (
            len(self.mission_sandbox_id) > _MAX_PROVIDER_OBJECT_ID_LENGTH
            or len(self.auth_sandbox_id) > _MAX_PROVIDER_OBJECT_ID_LENGTH
        ):
            raise ValueError("running Modal sandbox identity is too long")
        if self.mission_sandbox_id == self.auth_sandbox_id:
            raise ValueError("mission and auth sandbox identities must be distinct")
        if (
            not _MODAL_OPERATION_COHORT.fullmatch(self.cohort_id)
            or len(self.cohort_id) > _MAX_PROVIDER_COHORT_ID_LENGTH
        ):
            raise ValueError("running Modal operation cohort identity is invalid")


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationUnknown:
    """Provider evidence cannot prove a complete live pair or safe absence."""

    identity: ModalSandboxOperationIdentity
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("unknown Modal sandbox reconciliation requires a reason")
        if len(self.reason) > 512:
            raise ValueError("unknown Modal sandbox reconciliation reason is too long")


type ModalSandboxOperationReconciliation = (
    ModalSandboxOperationRunning | ModalSandboxOperationUnknown
)


@dataclass(frozen=True, slots=True)
class _ModalSandboxLookupFailure:
    reason: str


_MODAL_SANDBOX_MISSING = object()


class ModalSandboxOperationCapability:
    """Start and observe one namespaced, cohort-tagged Modal sandbox pair.

    Identity includes the authenticated workspace, explicit Environment, App,
    protocol epoch, and logical operation. Epoch zero and any operation that
    existed before the persistent barrier are legacy: missing barrier evidence
    for them is always Unknown and must never authorize this endpoint.

    ``start`` is an initial execution endpoint, not an idempotent retry
    endpoint. A running provider name prevents a second ``start`` from
    acquiring the same pair, but Modal releases names when sandboxes stop.
    Consequently reconciliation never returns replay permission, a retry
    guard, or an executable session. General name-based cleanup is deliberately
    absent because a late cleanup could terminate a newer name generation.
    Completed Activities may retry cleanup only from the exact mission/auth
    object IDs and cohort persisted with their first durable result.

    This is provider-resource substrate, not full Activity parity. The current
    multi-step author harness still invokes inner processes after pair
    creation; those invocations are not one atomic named Modal operation. An
    Activity adapter must use ``ModalProviderStartBarrier.start_initial`` or
    ``start_retry``. The barrier owns both acknowledged marker acquisitions and
    the one allowed call into this capability; no structural value is accepted
    as start authority.
    """

    def __init__(
        self,
        backend: ModalSandboxBackend,
        *,
        resource_observer: ModalSandboxResourceObserver | None = None,
    ) -> None:
        self._backend = backend
        self._resource_observer = resource_observer
        config = backend.config
        if config.workspace_name is None:
            raise ValueError("Modal named operations require an explicit workspace_name")
        if config.environment_name is None:
            raise ValueError("Modal named operations require an explicit environment_name")
        _require_modal_namespace_name(config.workspace_name, label="workspace_name")
        _require_modal_namespace_name(config.environment_name, label="environment_name")
        _require_modal_namespace_name(config.app_name, label="app_name")
        if config.operation_protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            raise ValueError(
                "Modal named operations require the barrier-aware protocol epoch "
                f"{MODAL_ACTIVITY_PROTOCOL_EPOCH}"
            )

    def identity(self, operation_id: str) -> ModalSandboxOperationIdentity:
        """Derive provider names without contacting Modal."""

        config = self._backend.config
        assert config.workspace_name is not None
        assert config.environment_name is not None
        assert config.operation_protocol_epoch is not None
        return ModalSandboxOperationIdentity(
            workspace_name=config.workspace_name,
            environment_name=config.environment_name,
            app_name=config.app_name,
            operation_id=operation_id,
            protocol_epoch=config.operation_protocol_epoch,
        )

    async def _start_after_provider_barrier(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        spec: SandboxSpec,
    ) -> ModalSandboxSession:
        """Create the pair after the barrier acknowledged its permanent marker.

        This is deliberately private. The ``ModalProviderStartBarrier`` start
        methods are the public family contract and never release transferable
        start authority. The first named sandbox create additionally
        serializes the live provider cohort, but it is not a retry guard
        because Modal releases sandbox names when they stop.
        """

        if self.identity(identity.operation_id) != identity:
            raise ValueError("Modal provider barrier belongs to another operation capability")
        self._validate_spec(spec)
        modal = self._load_modal()
        client = await self._verified_client(modal, phase="start")
        if isinstance(client, _ModalSandboxLookupFailure):
            raise RuntimeError(client.reason)
        image = (
            modal.Image.from_id(self._backend.config.image_id, client=client)
            if self._backend.config.image_id
            else self._backend._default_image(modal)
        )
        session = await self._backend._start(
            modal,
            spec,
            image,
            operation_identity=identity,
            resource_observer=self._resource_observer,
            client=client,
        )
        if not isinstance(session, ModalSandboxSession):
            raise TypeError("Modal named operation returned a non-Modal session")
        return session

    async def reconcile(
        self,
        *,
        operation_id: str,
        spec: SandboxSpec,
    ) -> ModalSandboxOperationReconciliation:
        """Observe a complete running pair without granting execution authority."""

        identity = self.identity(operation_id)
        self._validate_spec(spec)
        modal = self._load_modal()
        client = await self._verified_client(modal, phase="reconciliation")
        if isinstance(client, _ModalSandboxLookupFailure):
            return ModalSandboxOperationUnknown(identity, client.reason)
        mission, auth = await asyncio.gather(
            self._lookup(
                modal,
                client=client,
                role="mission",
                name=identity.mission_sandbox_name,
            ),
            self._lookup(
                modal,
                client=client,
                role="auth",
                name=identity.auth_sandbox_name,
            ),
        )
        lookups = {"mission": mission, "auth": auth}
        failures = [
            value.reason
            for value in lookups.values()
            if isinstance(value, _ModalSandboxLookupFailure)
        ]
        if failures:
            return ModalSandboxOperationUnknown(
                identity,
                "; ".join(sorted(failures)),
            )
        missing = [role for role, value in lookups.items() if value is _MODAL_SANDBOX_MISSING]
        if len(missing) == len(lookups):
            return ModalSandboxOperationUnknown(
                identity,
                "no running named Modal sandbox pair; absence is not retry authorization",
            )
        if missing:
            return ModalSandboxOperationUnknown(
                identity,
                "named Modal sandbox pair is partial; absent=" + ",".join(sorted(missing)),
            )

        mission_tags, auth_tags, mission_poll, auth_poll = await asyncio.gather(
            self._tags(mission, role="mission"),
            self._tags(auth, role="auth"),
            self._poll(mission, role="mission"),
            self._poll(auth, role="auth"),
        )
        tags = {"mission": mission_tags, "auth": auth_tags}
        polls = {"mission": mission_poll, "auth": auth_poll}
        observation_failures = [
            value.reason
            for value in (*tags.values(), *polls.values())
            if isinstance(value, _ModalSandboxLookupFailure)
        ]
        if observation_failures:
            return ModalSandboxOperationUnknown(
                identity,
                "; ".join(sorted(observation_failures)),
            )
        stopped = [role for role, value in polls.items() if value is not None]
        if stopped:
            return ModalSandboxOperationUnknown(
                identity,
                "named Modal sandbox is not running; stopped=" + ",".join(sorted(stopped)),
            )

        expected_tags = {
            "operation_digest": identity.digest,
            "operation_protocol_epoch": str(identity.protocol_epoch),
        }
        for role, values in tags.items():
            if not isinstance(values, dict):
                return ModalSandboxOperationUnknown(
                    identity,
                    f"{role} sandbox returned invalid cohort evidence",
                )
            if any(values.get(key) != value for key, value in expected_tags.items()):
                return ModalSandboxOperationUnknown(
                    identity,
                    f"{role} sandbox does not match the full provider operation identity",
                )
            if values.get("kind") != f"archetype-agent-{role}":
                return ModalSandboxOperationUnknown(
                    identity,
                    f"{role} sandbox role evidence does not match",
                )
        mission_cohort = mission_tags.get("operation_cohort")
        auth_cohort = auth_tags.get("operation_cohort")
        if (
            not isinstance(mission_cohort, str)
            or not _MODAL_OPERATION_COHORT.fullmatch(mission_cohort)
            or mission_cohort != auth_cohort
        ):
            return ModalSandboxOperationUnknown(
                identity,
                "named Modal sandbox pair has missing or mixed-generation cohort evidence",
            )

        return ModalSandboxOperationRunning(
            identity=identity,
            mission_sandbox_id=str(mission.object_id),
            auth_sandbox_id=str(auth.object_id),
            cohort_id=mission_cohort,
        )

    async def cleanup_completed(
        self,
        *,
        cleanup: ModalSandboxOperationCleanup,
        spec: SandboxSpec,
    ) -> None:
        """Retry teardown of the exact pair bound into a durable result.

        Provider names alone are not cleanup authority. The durable result
        supplies both immutable provider object IDs and the cohort tag, so a
        later process can reject a reused or mixed-generation name before
        terminating either exact handle.
        """

        identity = cleanup.identity
        if self.identity(identity.operation_id) != identity:
            raise ValueError("Modal cleanup belongs to another operation capability")
        self._validate_spec(spec)
        modal = self._load_modal()
        client = await self._verified_client(modal, phase="cleanup")
        if isinstance(client, _ModalSandboxLookupFailure):
            raise RuntimeError(client.reason)
        mission, auth = await asyncio.gather(
            self._lookup(
                modal,
                client=client,
                role="mission",
                name=identity.mission_sandbox_name,
            ),
            self._lookup(
                modal,
                client=client,
                role="auth",
                name=identity.auth_sandbox_name,
            ),
        )
        handles = {"mission": mission, "auth": auth}
        failures = [
            value.reason
            for value in handles.values()
            if isinstance(value, _ModalSandboxLookupFailure)
        ]
        if failures:
            raise RuntimeError("; ".join(sorted(failures)))

        expected_ids = {
            "mission": cleanup.mission_sandbox_id,
            "auth": cleanup.auth_sandbox_id,
        }
        present = {
            role: handle for role, handle in handles.items() if handle is not _MODAL_SANDBOX_MISSING
        }
        if not present:
            return
        for role, handle in present.items():
            if str(handle.object_id) != expected_ids[role]:
                raise RuntimeError(f"{role} cleanup resolved a different Modal sandbox generation")

        tags = await asyncio.gather(
            *(self._tags(handle, role=role) for role, handle in present.items())
        )
        for (role, _handle), observed in zip(present.items(), tags, strict=True):
            if isinstance(observed, _ModalSandboxLookupFailure):
                raise RuntimeError(observed.reason)
            expected = {
                "kind": f"archetype-agent-{role}",
                "operation_digest": identity.digest,
                "operation_cohort": cleanup.cohort_id,
                "operation_protocol_epoch": str(identity.protocol_epoch),
            }
            if not isinstance(observed, dict) or any(
                observed.get(key) != value for key, value in expected.items()
            ):
                raise RuntimeError(f"{role} cleanup evidence does not match the durable result")

        results = await asyncio.gather(
            *(ModalSandboxSession._terminate(handle) for handle in present.values()),
            return_exceptions=True,
        )
        cleanup_failures = [result for result in results if isinstance(result, BaseException)]
        if cleanup_failures:
            raise BaseExceptionGroup(
                f"failed to close {len(cleanup_failures)} completed Modal resource(s)",
                cleanup_failures,
            )

    def _validate_spec(self, spec: SandboxSpec) -> None:
        if spec.provider != self._backend.name:
            raise ValueError("Modal operation capability received a non-Modal sandbox spec")
        if spec.environment != self._backend.environment:
            raise ValueError(
                f"Modal environment must be {self._backend.environment!r}, got {spec.environment!r}"
            )

    @staticmethod
    def _load_modal() -> Any:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                'Modal support is optional; install it with `uv add "archetype-missions[modal]"`'
            ) from exc
        return modal

    async def _verified_client(
        self,
        modal: Any,
        *,
        phase: str,
    ) -> Any | _ModalSandboxLookupFailure:
        expected = self._backend.config.workspace_name
        assert expected is not None
        try:
            workspace = modal.Workspace.from_context()
            await workspace.hydrate.aio()
            observed = str(workspace.name or "")
            client = workspace.client
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _ModalSandboxLookupFailure(
                f"Modal {phase} workspace lookup failed ({type(exc).__name__[:128]})"
            )
        if observed != expected:
            return _ModalSandboxLookupFailure(
                f"Modal {phase} workspace identity does not match configured namespace"
            )
        return client

    async def _lookup(
        self,
        modal: Any,
        *,
        client: Any,
        role: str,
        name: str,
    ) -> Any:
        try:
            return await modal.Sandbox.from_name.aio(
                self._backend.config.app_name,
                name,
                environment_name=self._backend.config.environment_name,
                client=client,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            not_found = getattr(getattr(modal, "exception", None), "NotFoundError", None)
            if isinstance(not_found, type) and isinstance(exc, not_found):
                return _MODAL_SANDBOX_MISSING
            return _ModalSandboxLookupFailure(f"{role} lookup failed ({type(exc).__name__[:128]})")

    @staticmethod
    async def _tags(sandbox: Any, *, role: str) -> Any:
        try:
            return await sandbox.get_tags.aio()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _ModalSandboxLookupFailure(
                f"{role} cohort lookup failed ({type(exc).__name__[:128]})"
            )

    @staticmethod
    async def _poll(sandbox: Any, *, role: str) -> Any:
        try:
            return await sandbox.poll.aio()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _ModalSandboxLookupFailure(f"{role} poll failed ({type(exc).__name__[:128]})")


__all__ = [
    "MODAL_ACTIVITY_PROTOCOL_EPOCH",
    "ModalSandboxBackend",
    "ModalSandboxConfig",
    "ModalSandboxOperationCapability",
    "ModalSandboxOperationCleanup",
    "ModalSandboxOperationIdentity",
    "ModalSandboxOperationReconciliation",
    "ModalSandboxOperationRunning",
    "ModalSandboxOperationUnknown",
    "ModalSandboxResourceObserver",
    "ModalSandboxSession",
]
