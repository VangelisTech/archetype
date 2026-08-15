#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Guard nested process groups owned by one operational scenario.

``serve`` runs outside the scenario session and treats EOF on its parent-owned
stdin pipe as the cleanup signal. ``exec`` is a fail-closed launch wrapper: it
registers its own new-session PID/PGID, waits for the live guardian to
acknowledge ownership, retains session leadership, and reports target exit
without abandoning any descendants that still belong to the group.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import selectors
import signal
import socket
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

LEASE_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_FILE"
ACK_DIR_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_ACK_DIR"
READY_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_READY_FILE"
CLOSED_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_CLOSED_FILE"
WRAPPER_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_WRAPPER"
TARGET_STATUS_DIR_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_TARGET_STATUS_DIR"
LEASE_SCHEMA = "archetype.operational-process-lease/v1"
RESULT_SCHEMA = "archetype.operational-process-lease-cleanup/v1"
_TERM_GRACE_SECONDS = 5.0
_KILL_GRACE_SECONDS = 5.0
_PORT_GRACE_SECONDS = 5.0
_ACK_TIMEOUT_SECONDS = 5.0
_LEASE_PREFIX = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")


@dataclass(frozen=True)
class ProcessLease:
    lease_id: str
    pid: int
    process_group: int
    host: str
    port: int
    birth_identity: str


def _strict_json_object(encoded: str, *, line_number: int) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-standard JSON constant {value}")

    payload = json.loads(encoded, parse_constant=reject_constant)
    if not isinstance(payload, dict):
        raise TypeError(f"lease line {line_number} is not a JSON object")
    return payload


def _append_record(path: Path, payload: dict[str, object]) -> None:
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise RuntimeError("process lease journal is not a regular file")
        if os.write(descriptor, encoded) != len(encoded):
            raise RuntimeError("process lease journal accepted a partial record")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _parse_lease(payload: dict[str, Any], *, line_number: int) -> ProcessLease:
    if payload.get("schema") != LEASE_SCHEMA or payload.get("operation") != "acquire":
        raise ValueError(f"lease line {line_number} has an invalid acquire envelope")
    lease_id = payload.get("lease_id")
    pid = payload.get("pid")
    process_group = payload.get("process_group")
    host = payload.get("host")
    port = payload.get("port")
    birth_identity = payload.get("birth_identity")
    if not isinstance(lease_id, str) or not lease_id:
        raise TypeError(f"lease line {line_number} has an invalid lease_id")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 1:
        raise TypeError(f"lease line {line_number} has an invalid pid")
    if not isinstance(process_group, int) or isinstance(process_group, bool):
        raise TypeError(f"lease line {line_number} has an invalid process_group")
    if process_group <= 1 or process_group != pid:
        raise ValueError(f"lease line {line_number} must name an owned session leader")
    if not isinstance(host, str) or host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValueError(f"lease line {line_number} must name a loopback listener")
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise TypeError(f"lease line {line_number} has an invalid port")
    if not isinstance(birth_identity, str) or not birth_identity:
        raise TypeError(f"lease line {line_number} has an invalid birth_identity")
    return ProcessLease(lease_id, pid, process_group, host, port, birth_identity)


def _lease_history(
    path: Path,
) -> tuple[dict[str, ProcessLease], set[str], list[str]]:
    leases: dict[str, ProcessLease] = {}
    released: set[str] = set()
    errors: list[str] = []
    if not path.exists():
        return leases, released, errors
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return leases, released, [f"could not read lease journal: {type(exc).__name__}: {exc}"]

    for line_number, encoded in enumerate(lines, start=1):
        if not encoded.strip():
            continue
        try:
            payload = _strict_json_object(encoded, line_number=line_number)
            if payload.get("schema") != LEASE_SCHEMA:
                raise ValueError(f"lease line {line_number} has an invalid schema")
            operation = payload.get("operation")
            lease_id = payload.get("lease_id")
            if operation == "acquire":
                lease = _parse_lease(payload, line_number=line_number)
                if lease.lease_id in leases:
                    raise ValueError(f"lease line {line_number} reacquires an active lease")
                leases[lease.lease_id] = lease
            elif operation == "release":
                if not isinstance(lease_id, str) or not lease_id:
                    raise TypeError(f"lease line {line_number} has an invalid lease_id")
                if lease_id not in leases:
                    raise ValueError(f"lease line {line_number} releases an unknown lease")
                if lease_id in released:
                    raise ValueError(f"lease line {line_number} releases a lease twice")
                released.add(lease_id)
            else:
                raise ValueError(f"lease line {line_number} has an invalid operation")
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
    return leases, released, errors


def _marker_path(directory: Path, lease_id: str) -> Path:
    digest = hashlib.sha256(lease_id.encode()).hexdigest()
    return directory / f"{digest}.ack"


def target_status_path(directory: Path, lease_id: str) -> Path:
    """Return the stable private target-status path for one acknowledged lease."""

    digest = hashlib.sha256(lease_id.encode()).hexdigest()
    return directory / f"{digest}.target.json"


def _write_marker(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _acknowledge_active(lease_file: Path, ack_dir: Path) -> None:
    leases, released, errors = _lease_history(lease_file)
    if errors:
        return
    for lease_id, lease in leases.items():
        if lease_id in released:
            continue
        marker = _marker_path(ack_dir, lease.lease_id)
        if not marker.exists():
            _write_marker(
                marker,
                {
                    "schema": LEASE_SCHEMA,
                    "lease_id": lease.lease_id,
                    "pid": lease.pid,
                    "process_group": lease.process_group,
                    "birth_identity": lease.birth_identity,
                    "status": "accepted",
                },
            )


def _acknowledgement_is_exact(path: Path, lease: ProcessLease) -> bool:
    try:
        payload = _strict_json_object(path.read_text(encoding="utf-8"), line_number=1)
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        return False
    return payload == {
        "schema": LEASE_SCHEMA,
        "lease_id": lease.lease_id,
        "pid": lease.pid,
        "process_group": lease.process_group,
        "birth_identity": lease.birth_identity,
        "status": "accepted",
    }


def _process_birth_identity(process_id: int) -> str | None:
    """Return a kernel process-instance token, not a wall-clock display value."""

    if sys.platform == "darwin":
        return _darwin_process_birth_identity(process_id)
    if sys.platform.startswith("linux"):
        try:
            # The command name is parenthesized and may contain spaces. Field
            # 22 (index 19 after the closing parenthesis) is the kernel start
            # time in clock ticks since boot.
            fields = Path(f"/proc/{process_id}/stat").read_text().rsplit(")", 1)[1].split()
            start_ticks = fields[19]
        except (IndexError, OSError):
            return None
        return f"linux-start-ticks:{start_ticks}"
    return None


def _darwin_process_birth_identity(process_id: int) -> str | None:
    class ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("pbi_flags", ctypes.c_uint32),
            ("pbi_status", ctypes.c_uint32),
            ("pbi_xstatus", ctypes.c_uint32),
            ("pbi_pid", ctypes.c_uint32),
            ("pbi_ppid", ctypes.c_uint32),
            ("pbi_uid", ctypes.c_uint32),
            ("pbi_gid", ctypes.c_uint32),
            ("pbi_ruid", ctypes.c_uint32),
            ("pbi_rgid", ctypes.c_uint32),
            ("pbi_svuid", ctypes.c_uint32),
            ("pbi_svgid", ctypes.c_uint32),
            ("rfu_1", ctypes.c_uint32),
            ("pbi_comm", ctypes.c_char * 16),
            ("pbi_name", ctypes.c_char * 32),
            ("pbi_nfiles", ctypes.c_uint32),
            ("pbi_pgid", ctypes.c_uint32),
            ("pbi_pjobc", ctypes.c_uint32),
            ("e_tdev", ctypes.c_uint32),
            ("e_tpgid", ctypes.c_uint32),
            ("pbi_nice", ctypes.c_int32),
            ("pbi_start_tvsec", ctypes.c_uint64),
            ("pbi_start_tvusec", ctypes.c_uint64),
        ]

    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError:
        return None
    libproc.proc_pidinfo.argtypes = (
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    )
    libproc.proc_pidinfo.restype = ctypes.c_int
    info = ProcBsdInfo()
    size = ctypes.sizeof(info)
    if libproc.proc_pidinfo(process_id, 3, 0, ctypes.byref(info), size) != size:
        return None
    if info.pbi_pid != process_id:
        return None
    return f"darwin-start-time:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}"


def _process_group_alive(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _signal_process_group(process_group: int, requested_signal: signal.Signals) -> None:
    try:
        os.killpg(process_group, requested_signal)
    except (PermissionError, ProcessLookupError):
        pass


def _wait_for_group_close(process_group: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_group_alive(process_group):
            return True
        time.sleep(0.05)
    return not _process_group_alive(process_group)


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            return True
    except OSError:
        return False


def _wait_for_port_close(host: str, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _port_open(host, port):
            return True
        time.sleep(0.05)
    return not _port_open(host, port)


def _reap(lease: ProcessLease, *, released: bool) -> dict[str, object]:
    group_was_alive = _process_group_alive(lease.process_group)
    port_was_open = _port_open(lease.host, lease.port)
    current_birth_identity = (
        _process_birth_identity(lease.pid) if group_was_alive else lease.birth_identity
    )
    # The launch wrapper remains the session leader for the target's complete
    # lifetime. Missing or changed leader identity is therefore not evidence
    # of ownership and must fail closed rather than signal a reused PGID.
    ownership_matches = current_birth_identity == lease.birth_identity
    if group_was_alive and ownership_matches:
        _signal_process_group(lease.process_group, signal.SIGTERM)
    group_closed = not group_was_alive or (
        ownership_matches and _wait_for_group_close(lease.process_group, _TERM_GRACE_SECONDS)
    )
    if not group_closed and ownership_matches:
        _signal_process_group(lease.process_group, signal.SIGKILL)
        group_closed = _wait_for_group_close(lease.process_group, _KILL_GRACE_SECONDS)
    port_closed = _wait_for_port_close(lease.host, lease.port, _PORT_GRACE_SECONDS)
    release_was_truthful = None if not released else not group_was_alive and not port_was_open
    return {
        "lease_id": lease.lease_id,
        "pid": lease.pid,
        "process_group": lease.process_group,
        "host": lease.host,
        "port": lease.port,
        "birth_identity": lease.birth_identity,
        "ownership_matches": ownership_matches,
        "released": released,
        "release_was_truthful": release_was_truthful,
        "group_was_alive": group_was_alive,
        "port_was_open": port_was_open,
        "group_closed": group_closed,
        "port_closed": port_closed,
    }


def guard(lease_file: Path, result_file: Path) -> bool:
    history, released, errors = _lease_history(lease_file)
    leases = [
        _reap(history[lease_id], released=lease_id in released) for lease_id in sorted(history)
    ]
    closed = not errors and all(
        lease["group_closed"]
        and lease["port_closed"]
        and lease["ownership_matches"]
        and lease["release_was_truthful"] is not False
        for lease in leases
    )
    _write_marker(
        result_file,
        {
            "schema": RESULT_SCHEMA,
            "status": "closed" if closed else "leaked",
            "active_lease_count": len(history) - len(released),
            "leases": leases,
            "errors": errors,
        },
    )
    return closed


def serve(
    *,
    lease_file: Path,
    ack_dir: Path,
    ready_file: Path,
    closed_file: Path,
    result_file: Path,
) -> bool:
    """Acknowledge live leases until parent EOF, then close every remainder."""

    shutdown_requested = False

    def request_shutdown(_signum: int, _frame: object) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    selector = selectors.DefaultSelector()
    try:
        selector.register(sys.stdin.buffer, selectors.EVENT_READ)
        ack_dir.mkdir(parents=True, exist_ok=True)
        _write_marker(
            ready_file,
            {"schema": LEASE_SCHEMA, "status": "ready", "pid": os.getpid()},
        )
        while not shutdown_requested:
            _acknowledge_active(lease_file, ack_dir)
            events = selector.select(timeout=0.05)
            if events and os.read(sys.stdin.fileno(), 4096) == b"":
                break
    finally:
        # Once cleanup begins, a second CI cancellation/escalation signal must
        # not interrupt TERM/KILL, port proof, or result publication.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        selector.close()
    _write_marker(closed_file, {"schema": LEASE_SCHEMA, "status": "closed"})
    return guard(lease_file, result_file)


def _required_env_path(name: str) -> Path:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"guarded exec requires {name}")
    return Path(value)


def execute(*, lease_prefix: str, host: str, port: int, command: list[str]) -> NoReturn:
    """Acquire ownership, then retain session leadership while the target runs."""

    if _LEASE_PREFIX.fullmatch(lease_prefix) is None:
        raise ValueError("lease prefix must be a safe lowercase identifier")
    if not command:
        raise ValueError("guarded exec requires a command")
    lease_file = _required_env_path(LEASE_ENV)
    ack_dir = _required_env_path(ACK_DIR_ENV)
    ready_file = _required_env_path(READY_ENV)
    closed_file = _required_env_path(CLOSED_ENV)
    target_status_dir = _required_env_path(TARGET_STATUS_DIR_ENV)
    process_id = os.getpid()
    if os.getpgrp() != process_id:
        raise RuntimeError("guarded exec must be launched as a new session leader")
    lease_id = f"{lease_prefix}:{process_id}"
    birth_identity = _process_birth_identity(process_id)
    if birth_identity is None:
        raise RuntimeError("guarded exec could not bind the process birth identity")
    if not ready_file.is_file() or closed_file.exists():
        raise RuntimeError("process lease guardian is not accepting registrations")
    _append_record(
        lease_file,
        {
            "schema": LEASE_SCHEMA,
            "operation": "acquire",
            "lease_id": lease_id,
            "pid": process_id,
            "process_group": process_id,
            "host": host,
            "port": port,
            "birth_identity": birth_identity,
        },
    )
    lease = ProcessLease(lease_id, process_id, process_id, host, port, birth_identity)
    acknowledgement = _marker_path(ack_dir, lease_id)
    deadline = time.monotonic() + _ACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if closed_file.exists():
            raise RuntimeError("process lease guardian closed before acknowledging the launch")
        if acknowledgement.is_file() and _acknowledgement_is_exact(acknowledgement, lease):
            break
        time.sleep(0.02)
    else:
        raise RuntimeError("process lease guardian did not acknowledge the launch")
    if closed_file.exists():
        raise RuntimeError("process lease guardian closed before target exec")
    target = subprocess.Popen(command, env=os.environ.copy())
    target_returncode = target.wait()
    try:
        _write_marker(
            target_status_path(target_status_dir, lease_id),
            {
                "schema": LEASE_SCHEMA,
                "lease_id": lease_id,
                "pid": process_id,
                "process_group": process_id,
                "status": "target_exited",
                "target_pid": target.pid,
                "returncode": target_returncode,
            },
        )
    except OSError:
        # Failure to report must not abandon the kernel ownership anchor. The
        # caller will time out and the independent guardian will still reap it.
        pass
    # The wrapper intentionally remains the kernel-visible session leader even
    # after an unexpected target exit. Its owner will observe failed readiness
    # or I/O, close the group through ``terminate()``, and only then release the
    # lease. Exiting here could leave target descendants under an ownerless,
    # eventually reusable numeric PGID.
    while True:
        time.sleep(3600)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    server = commands.add_parser("serve")
    server.add_argument("--lease-file", type=Path, required=True)
    server.add_argument("--ack-dir", type=Path, required=True)
    server.add_argument("--ready-file", type=Path, required=True)
    server.add_argument("--closed-file", type=Path, required=True)
    server.add_argument("--result-file", type=Path, required=True)
    executor = commands.add_parser("exec")
    executor.add_argument("--lease-prefix", required=True)
    executor.add_argument("--host", required=True)
    executor.add_argument("--port", type=int, required=True)
    executor.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.mode == "serve":
        return (
            0
            if serve(
                lease_file=args.lease_file,
                ack_dir=args.ack_dir,
                ready_file=args.ready_file,
                closed_file=args.closed_file,
                result_file=args.result_file,
            )
            else 1
        )
    command = list(args.command)
    if command[:1] == ["--"]:
        command.pop(0)
    execute(
        lease_prefix=args.lease_prefix,
        host=args.host,
        port=args.port,
        command=command,
    )


if __name__ == "__main__":  # pragma: no cover - subprocess entry point
    raise SystemExit(main())
