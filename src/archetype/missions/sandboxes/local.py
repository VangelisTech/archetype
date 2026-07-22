# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Local tmux-supervised Backend and Session for mission sandboxes.

The local backend isolates by working directory, not by kernel namespace;
its distinguishing capability is interactivity: any process can run inside
a supervised, recorded tmux session with spectate/takeover web lanes.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import uuid
from dataclasses import dataclass
from pathlib import Path

from archetype.missions.sandboxes.contracts import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxSpec,
    SandboxStatus,
)
from archetype.missions.sessions import (
    SessionLanes,
    SessionRecording,
    TmuxSessionSupervisor,
)


@dataclass(frozen=True)
class InteractiveHandle:
    """One supervised interactive process: lanes to join, records on disk."""

    session_name: str
    lanes: SessionLanes
    recording: SessionRecording
    takeover_credential: str


class LocalTmuxSession:
    """One workdir-scoped local session with tmux-supervised interactivity."""

    def __init__(self, *, spec: SandboxSpec, supervisor: TmuxSessionSupervisor) -> None:
        self._spec = spec
        self._supervisor = supervisor
        self._status = SandboxStatus.READY
        self._sessions: list[str] = []
        self._identity = SandboxIdentity(
            provider="local",
            sandbox_id=f"local-{uuid.uuid4().hex[:12]}",
            environment=spec.environment,
        )
        Path(spec.workdir).mkdir(parents=True, exist_ok=True)

    @property
    def identity(self) -> SandboxIdentity:
        return self._identity

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=False)

    async def status(self) -> SandboxStatus:
        return self._status

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        """Run one batch process in the sandbox workdir."""

        if self._status is not SandboxStatus.READY:
            raise RuntimeError(f"local sandbox is {self._status}")
        env = {**os.environ, **request.environment_dict()}
        proc = await asyncio.create_subprocess_exec(
            *request.argv,
            cwd=request.workdir or self._spec.workdir,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL if request.close_stdin else None,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=request.timeout_seconds
            )
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise
        return ProcessResult(
            argv=request.argv,
            returncode=proc.returncode if proc.returncode is not None else -1,
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
        )

    async def start_interactive(
        self,
        argv: tuple[str, ...],
        *,
        session_name: str | None = None,
        serve_lanes: bool = True,
    ) -> InteractiveHandle:
        """Run ``argv`` in a recorded tmux session; optionally serve lanes."""

        if self._status is not SandboxStatus.READY:
            raise RuntimeError(f"local sandbox is {self._status}")
        name = session_name or f"{self._identity.sandbox_id}-{len(self._sessions)}"
        recording = await asyncio.to_thread(
            self._supervisor.start, name, argv, cwd=self._spec.workdir
        )
        self._sessions.append(name)
        credential = f"operator:{secrets.token_urlsafe(9)}"
        if serve_lanes:
            lanes = await asyncio.to_thread(
                self._supervisor.lanes, name, takeover_credential=credential
            )
        else:
            lanes = SessionLanes("", "", 0, 0)
        return InteractiveHandle(
            session_name=name,
            lanes=lanes,
            recording=recording,
            takeover_credential=credential,
        )

    def session_alive(self, session_name: str) -> bool:
        return self._supervisor.alive(session_name)

    async def checkpoint(self) -> CheckpointRef:
        raise RuntimeError("local sandbox does not support checkpoints")

    async def close(self) -> None:
        for name in self._sessions:
            await asyncio.to_thread(self._supervisor.kill, name)
        self._sessions.clear()
        self._status = SandboxStatus.CLOSED


class LocalTmuxBackend:
    """Provider adapter for workdir-scoped, tmux-supervised local sandboxes.

    Each backend owns a private tmux server by default: ``shutdown()`` kills
    the whole server on its socket, so two backends (or two archetype
    processes) sharing one ``socket_name`` would destroy each other's live
    sessions. Pass an explicit ``socket_name`` only to adopt a supervisor
    you own exclusively.
    """

    name = "local"

    def __init__(self, *, run_dir: Path | str, socket_name: str | None = None) -> None:
        if socket_name is None:
            socket_name = f"archetype-sessions-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._supervisor = TmuxSessionSupervisor(socket_name=socket_name, run_dir=run_dir)

    @property
    def supervisor(self) -> TmuxSessionSupervisor:
        return self._supervisor

    async def create(self, spec: SandboxSpec) -> LocalTmuxSession:
        if spec.provider != self.name:
            raise ValueError(f"spec provider {spec.provider!r} is not {self.name!r}")
        return LocalTmuxSession(spec=spec, supervisor=self._supervisor)

    async def restore(self, spec: SandboxSpec, checkpoint: CheckpointRef) -> LocalTmuxSession:
        raise RuntimeError("local sandbox does not support checkpoint restore")

    def shutdown(self) -> None:
        self._supervisor.shutdown()
