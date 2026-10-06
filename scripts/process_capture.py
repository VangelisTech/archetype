# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded, credential-masked evidence for repository-owned child processes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import threading
import time
from pathlib import Path

TAIL_BYTES = 16 << 10


def _secret_environment_values(env):
    return sorted(
        (
            (name, value)
            for name, value in env.items()
            if re.search(r"(?:KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)", name, re.I)
            and len(value) >= 8
        ),
        key=lambda item: len(item[1]),
        reverse=True,
    )


def _mask(raw, secret_values):
    for name, value in secret_values:
        raw = raw.replace(value.encode(), ("***" + name + "***").encode())
    return raw


def _write_captured_log(*, raw, destination, redacted, failed, secret_values=()):
    masked = _mask(raw, secret_values)
    if redacted and not failed:
        written = b"Output omitted by redacted_receipt policy.\n"
    elif redacted:
        written = b"Redacted run FAILED, so the output tail is retained.\n" + masked[-TAIL_BYTES:]
    else:
        written = masked
    destination.write_bytes(written)
    return dict(
        path=str(destination),
        bytes=len(raw),
        sha256=hashlib.sha256(raw).hexdigest(),
        redacted=redacted,
        failure_tail_retained=redacted and failed,
    )


class _Capture:
    """Mask across chunk boundaries before retaining a bounded tail."""

    def __init__(self, secrets):
        self.secrets = [
            (value.encode(), ("***" + name + "***").encode()) for name, value in secrets
        ]
        self.width = max([len(value) for value, _ in self.secrets] + [1])
        self.pending = b""
        self.tail = b""
        self.digest = hashlib.sha256()
        self.size = 0
        self.error = False

    def feed(self, block, *, final=False):
        self.size += len(block)
        self.digest.update(block)
        self.pending += block
        stop = len(self.pending) if final else max(0, len(self.pending) - self.width + 1)
        position = 0
        masked = bytearray()
        while position < stop:
            for value, replacement in self.secrets:
                if self.pending.startswith(value, position):
                    masked.extend(replacement)
                    position += len(value)
                    break
            else:
                masked.append(self.pending[position])
                position += 1
        self.pending = self.pending[position:]
        self.tail = (self.tail + masked)[-TAIL_BYTES:]

    def read(self, stream):
        try:
            while block := stream.read(8192):
                self.feed(block)
            self.feed(b"", final=True)
        except Exception:
            self.error = True
        finally:
            stream.close()

    def write(self, path, redacted, failed):
        # Already masked before truncation; raw digest/count remain separate.
        if redacted and not failed:
            path.write_bytes(b"Output omitted by redacted_receipt policy.\n")
        else:
            prefix = b"Redacted run FAILED, so the output tail is retained.\n" if redacted else b""
            path.write_bytes(prefix + self.tail)
        return dict(
            path=str(path),
            bytes=self.size,
            sha256=self.digest.hexdigest(),
            redacted=redacted,
            failure_tail_retained=redacted and failed,
            bounded_tail=True,
            capture_failed=self.error,
        )


def _signal_owned(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _run_process(command, *, cwd, env, timeout_seconds, log_prefix, redacted=False):
    if not 0 < timeout_seconds <= 7200:
        raise ValueError("Expected bounded timeout")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    secrets = _secret_environment_values(env)
    captures = [_Capture(secrets), _Capture(secrets)]
    threads = [
        threading.Thread(target=capture.read, args=(stream,), daemon=True)
        for capture, stream in zip(captures, (process.stdout, process.stderr), strict=True)
    ]
    for thread in threads:
        thread.start()
    timed_out = False
    started = time.monotonic()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        # We created and still own this session. No shell, global ps or unrelated
        # process lease is consulted; arguments are deliberately omitted.
        diagnostic = (
            f"operational timeout: no exit within {timeout_seconds:g}s\n"
            f"process-group snapshot at SIGTERM: pid={process.pid}, owned_session={process.pid}, age={time.monotonic() - started:.1f}s\n"
        )
        _signal_owned(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            _signal_owned(process.pid, signal.SIGKILL)
            process.wait(timeout=2)
    for thread in threads:
        thread.join(timeout=2)
    if any(thread.is_alive() for thread in threads):
        # A child can exit while its descendants retain the pipes. The session
        # was created by this runner; terminate only that original group.
        _signal_owned(process.pid, signal.SIGTERM)
        for thread in threads:
            thread.join(timeout=1)
        if any(thread.is_alive() for thread in threads):
            _signal_owned(process.pid, signal.SIGKILL)
            for thread in threads:
                thread.join(timeout=1)
    failed = (
        timed_out
        or process.returncode != 0
        or any(t.is_alive() for t in threads)
        or any(c.error for c in captures)
    )
    if timed_out:
        captures[1].feed(diagnostic.encode(), final=True)
    logs = {
        name: capture.write(Path(str(log_prefix) + f".{name}.log"), redacted, failed)
        for name, capture in zip(("stdout", "stderr"), captures, strict=True)
    }
    result = dict(
        returncode=process.returncode,
        timed_out=timed_out,
        capture_failed=any(t.is_alive() for t in threads) or any(c.error for c in captures),
        logs=logs,
    )
    Path(str(log_prefix) + ".capture.json").write_text(json.dumps(result, indent=2))
    return result


def execute(command, *, environment, cwd, log, timeout_seconds=2700, redacted=False):
    result = _run_process(
        command,
        cwd=cwd,
        env=environment,
        timeout_seconds=timeout_seconds,
        log_prefix=log,
        redacted=redacted,
    )
    log.write_bytes(
        Path(str(log) + ".stdout.log").read_bytes() + Path(str(log) + ".stderr.log").read_bytes()
    )
    if result["timed_out"] or result["capture_failed"] or result["returncode"] != 0:
        raise RuntimeError("Owned child failed; inspect captured stage evidence") from None
