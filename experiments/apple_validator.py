# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Credential-free Apple Container boundary for experimental validators."""

from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path, PurePosixPath

IMAGE = (
    "ghcr.io/astral-sh/uv@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58"
)
OUTPUT_LIMIT = 1_000_000


class _OutputLimitError(RuntimeError):
    pass


def safe_path(root: Path, raw: str) -> Path:
    relative = PurePosixPath(raw)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError(f"unsafe repository path: {raw}")
    resolved = (root / relative.as_posix()).resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise ValueError(f"repository path escaped its root: {raw}")
    return resolved


def _run(
    args: list[str],
    *,
    timeout: int,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(args)} failed: {detail[-4_000:]}")
    return completed


def _run_capped(
    args: list[str],
    *,
    timeout: int,
    check: bool,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    streams = (process.stdout, process.stderr)
    assert all(stream is not None for stream in streams)
    buffers = (bytearray(), bytearray())
    lock = threading.Lock()
    overflow = threading.Event()
    used = 0

    def drain(index: int) -> None:
        nonlocal used
        stream = streams[index]
        assert stream is not None
        while chunk := stream.read(65_536):
            with lock:
                remaining = OUTPUT_LIMIT - used
                buffers[index].extend(chunk[:remaining])
                used += min(len(chunk), remaining)
                if len(chunk) > remaining:
                    overflow.set()
                    process.kill()
                    return

    readers = [threading.Thread(target=drain, args=(index,)) for index in range(2)]
    for reader in readers:
        reader.start()
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise
    finally:
        for reader in readers:
            reader.join(timeout=5)
    stdout, stderr = (value.decode(errors="replace") for value in buffers)
    if overflow.is_set():
        raise _OutputLimitError("validator output exceeded 1,000,000 bytes")
    completed = subprocess.CompletedProcess(args, returncode, stdout, stderr)
    if check and returncode:
        detail = stderr.strip() or stdout.strip()
        raise RuntimeError(f"validator failed: {detail[-4_000:]}")
    return completed


def start(workspace: Path, name: str) -> str:
    """Prepare trusted dependencies, then start one networkless validator VM."""

    mount = f"type=bind,source={workspace.resolve()},target=/workspace"
    uid = f"{os.getuid()}:{os.getgid()}"
    _run(
        [
            "container",
            "run",
            "--remove",
            "--user",
            uid,
            "--env",
            "HOME=/tmp",
            "--env",
            "UV_CACHE_DIR=/tmp/uv-cache",
            "--mount",
            mount,
            "--workdir",
            "/workspace",
            IMAGE,
            "uv",
            "sync",
            "--locked",
        ],
        timeout=1_200,
    )
    _run(
        [
            "container",
            "run",
            "--detach",
            "--init",
            "--name",
            name,
            "--user",
            uid,
            "--cpus",
            "4",
            "--memory",
            "8g",
            "--cap-drop",
            "ALL",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--mount",
            f"{mount},readonly",
            IMAGE,
            "sleep",
            "infinity",
        ],
        timeout=120,
    )
    return name


def execute(
    validator: str,
    args: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        return _run_capped(
            [
                "container",
                "exec",
                "--env",
                "HOME=/tmp",
                "--env",
                "PYTHONDONTWRITEBYTECODE=1",
                "--env",
                "PYTHONPATH=/workspace/src",
                "--env",
                "PYTEST_ADDOPTS=-p no:cacheprovider",
                "--env",
                "RUFF_NO_CACHE=1",
                "--env",
                "XDG_CACHE_HOME=/tmp/cache",
                "--workdir",
                "/workspace",
                validator,
                "/workspace/.venv/bin/python",
                *args,
            ],
            timeout=600,
            check=check,
        )
    except (_OutputLimitError, subprocess.TimeoutExpired):
        stop(validator, check=False)
        raise


def stop(validator: str, *, check: bool = True) -> None:
    _run(
        ["container", "delete", "--force", validator],
        timeout=60,
        check=check,
    )
