# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""tmux-backed session supervisor: spectate, take over, and record PTYs."""

from __future__ import annotations

import re
import shutil
import socket
import subprocess
from dataclasses import dataclass
from pathlib import Path

_EVENTS_HELPER = """#!/bin/sh
# Append one session lifecycle event as a JSON line. Args: event session file
printf '{"event":"%s","session":"%s","ts_ms":%s}\\n' "$1" "$2" "$(($(date +%s) * 1000))" >> "$3"
"""

_HOOKS = ("client-attached", "client-detached", "pane-died", "session-closed")


@dataclass(frozen=True)
class SessionLanes:
    """Web viewports over one PTY; access is enforced server-side per lane."""

    spectate_url: str
    takeover_url: str
    spectate_port: int
    takeover_port: int


@dataclass(frozen=True)
class SessionRecording:
    """On-disk record of one session: raw PTY stream + lifecycle events."""

    stream_path: Path
    events_path: Path


class TmuxSessionSupervisor:
    """Owns interactive sessions on one dedicated tmux server socket.

    The PTY lives in the tmux server, so it survives every client death —
    a crashed operator, a dropped spectator, or a dead harness process all
    leave the session (and its crash scene, via ``remain-on-exit``) intact.
    Recording starts when ``pipe-pane`` attaches, immediately after session
    creation: a very fast command may emit output before the pipe lands, so
    the stream is near-complete rather than byte-exact from process start;
    ``capture()`` with history covers that window. tmux hooks append
    attach/detach/death events as JSONL.

    Two supervisors must never share a ``socket_name``: ``shutdown()`` kills
    the whole tmux server on its socket.
    """

    def __init__(
        self,
        *,
        socket_name: str = "archetype-sessions",
        run_dir: Path | str,
        tmux_bin: str = "tmux",
        ttyd_bin: str = "ttyd",
    ) -> None:
        if not shutil.which(tmux_bin):
            raise RuntimeError(f"tmux binary {tmux_bin!r} not found on PATH")
        self._socket = socket_name
        self._tmux = tmux_bin
        self._ttyd = ttyd_bin
        self._run_dir = Path(run_dir)
        self._run_dir.mkdir(parents=True, exist_ok=True)
        self._ttyd_procs: dict[tuple[str, bool], subprocess.Popen[bytes]] = {}
        helper = self._run_dir / "events.sh"
        helper.write_text(_EVENTS_HELPER)
        helper.chmod(0o755)
        self._events_helper = helper
        # Persistent server so global options and hooks exist before any
        # session spawns — otherwise a fast-exiting command dies before
        # remain-on-exit can preserve its crash scene. One chained call:
        # a bare started server exits again before a second call connects.
        setup: list[str] = [
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
        ]
        for hook in _HOOKS:
            setup += [
                ";",
                "set-hook",
                "-g",
                hook,
                "run-shell '"
                f"{self._events_helper} {hook} #{{session_name}} "
                f"{self._run_dir}/#{{session_name}}.events.jsonl'",
            ]
        self._tm(*setup)

    # -- lifecycle ---------------------------------------------------------

    def start(
        self,
        name: str,
        argv: tuple[str, ...],
        *,
        cwd: str,
        width: int = 220,
        height: int = 50,
    ) -> SessionRecording:
        """Spawn ``argv`` inside a detached, recorded tmux session."""

        if not argv:
            raise ValueError("session start requires a command")
        recording = self.recording(name)
        self._tm(
            "new-session",
            "-d",
            "-s",
            name,
            "-x",
            str(width),
            "-y",
            str(height),
            "-c",
            cwd,
            *argv,
        )
        pipe = subprocess.run(
            [
                self._tmux,
                "-L",
                self._socket,
                "pipe-pane",
                "-o",
                "-t",
                name,
                f"cat >> {shquote(str(recording.stream_path))}",
            ],
            capture_output=True,
            text=True,
        )
        # A command that exits before the pipe attaches leaves no stream,
        # but remain-on-exit still preserves its crash scene for capture().
        if pipe.returncode != 0 and "has exited" not in pipe.stderr:
            raise RuntimeError(f"tmux pipe-pane failed: {pipe.stderr.strip()}")
        return recording

    def alive(self, name: str) -> bool:
        return (
            subprocess.run(
                [self._tmux, "-L", self._socket, "has-session", "-t", name],
                capture_output=True,
            ).returncode
            == 0
        )

    def capture(self, name: str, *, history: bool = True) -> str:
        """Return the session's screen contents, scrollback included.

        History is included by default so a crashed pane's final output —
        which may have scrolled off the visible region — stays readable
        for forensics after the process exits.
        """

        args = ["capture-pane", "-p", "-t", name]
        if history:
            args[2:2] = ["-S", "-"]
        return self._tm(*args)

    def kill(self, name: str) -> None:
        for writable in (False, True):
            proc = self._ttyd_procs.pop((name, writable), None)
            if proc is not None and proc.poll() is None:
                proc.terminate()
        subprocess.run(
            [self._tmux, "-L", self._socket, "kill-session", "-t", name],
            capture_output=True,
        )

    def shutdown(self) -> None:
        for proc in self._ttyd_procs.values():
            if proc.poll() is None:
                proc.terminate()
        self._ttyd_procs.clear()
        subprocess.run([self._tmux, "-L", self._socket, "kill-server"], capture_output=True)

    # -- viewports ---------------------------------------------------------

    def serve(
        self,
        name: str,
        *,
        writable: bool = False,
        port: int = 0,
        credential: str | None = None,
    ) -> tuple[str, int]:
        """Serve one web lane over the session PTY; returns (url, port).

        Read-only is double-gated (ttyd default plus ``attach -r``); the
        writable lane requires ttyd ``-W`` and should sit behind an
        authorization decision recorded by the caller.
        """

        _validate_session_name(name)
        if writable and not credential:
            raise ValueError("the writable lane requires a credential")
        if not shutil.which(self._ttyd):
            raise RuntimeError(f"ttyd binary {self._ttyd!r} not found on PATH")
        if not self.alive(name):
            raise RuntimeError(f"session {name!r} is not running")
        key = (name, writable)
        existing = self._ttyd_procs.get(key)
        if existing is not None and existing.poll() is None:
            raise RuntimeError(f"lane already served for session {name!r}")
        if port == 0:
            port = _free_port()
        # Loopback-only: ttyd binds all interfaces when -i is omitted, which
        # would expose both lanes off-host while the URL claims localhost.
        argv = [self._ttyd, "-p", str(port), "-i", "127.0.0.1"]
        if writable:
            argv += ["-W", "-c", credential or ""]
        argv += [
            self._tmux,
            "-L",
            self._socket,
            "attach",
            "-rt" if not writable else "-t",
            name,
        ]
        self._ttyd_procs[key] = subprocess.Popen(
            argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return f"http://localhost:{port}", port

    def lanes(self, name: str, *, takeover_credential: str) -> SessionLanes:
        spectate_url, spectate_port = self.serve(name)
        takeover_url, takeover_port = self.serve(
            name, writable=True, credential=takeover_credential
        )
        return SessionLanes(
            spectate_url=spectate_url,
            takeover_url=takeover_url,
            spectate_port=spectate_port,
            takeover_port=takeover_port,
        )

    # -- records -----------------------------------------------------------

    def recording(self, name: str) -> SessionRecording:
        _validate_session_name(name)
        return SessionRecording(
            stream_path=self._run_dir / f"{name}.stream.log",
            events_path=self._run_dir / f"{name}.events.jsonl",
        )

    # -- internals ---------------------------------------------------------

    def _tm(self, *args: str) -> str:
        result = subprocess.run(
            [self._tmux, "-L", self._socket, *args],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"tmux {' '.join(args[:2])} failed: {result.stderr.strip()}")
        return result.stdout


_SESSION_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _validate_session_name(name: str) -> None:
    """Reject names that could escape run_dir or confuse tmux targets."""

    if not _SESSION_NAME_RE.fullmatch(name):
        raise ValueError(
            f"session name {name!r} must match [A-Za-z0-9_-]+ — it becomes "
            "a recording filename and a tmux target"
        )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def shquote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"
