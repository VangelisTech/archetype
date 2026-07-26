# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for operational-scenario output capture.

Two incidents motivated these: the #675 R2 flake and the #678 merge-group
stall were both undiagnosable because the ``redacted_receipt`` policy
discarded the entire output of FAILED runs, and the stall additionally
produced no output at all until the runner's timeout killed it. The
contracts:

1. A redacted receipt on a failed run retains a bounded output tail with
   credential values masked — redact secrets, not the assertion.
2. A redacted receipt on a successful run still discards everything.
3. A timed-out run's stderr names the processes that were still running
   (the group snapshot taken before SIGTERM).
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.run_operational_scenarios import (
    _run_process,
    _secret_environment_values,
    _write_captured_log,
)


def test_redacted_failure_retains_masked_tail(tmp_path: Path) -> None:
    secret = "AKIA-example-secret-value-1234"
    raw = (
        b"collected 2 items\n"
        b"FAILED test_r2_artifact_context.py::test_roundtrip - "
        b"AssertionError: manifest missing\n"
        b"credential leaked into output: " + secret.encode() + b"\n"
    )
    destination = tmp_path / "stderr.log"

    entry = _write_captured_log(
        raw=raw,
        destination=destination,
        redacted=True,
        failed=True,
        secret_values=[("R2_SECRET_ACCESS_KEY", secret)],
    )

    written = destination.read_text(encoding="utf-8")
    assert "AssertionError: manifest missing" in written, "the assertion is the diagnosis"
    assert secret not in written, "credential values must never survive into the receipt"
    assert "***R2_SECRET_ACCESS_KEY***" in written
    assert entry["failure_tail_retained"] is True
    assert entry["redacted"] is True
    # Digest and byte count still describe the full raw output, not the tail.
    assert entry["bytes"] == len(raw)


def test_redacted_success_still_discards_output(tmp_path: Path) -> None:
    destination = tmp_path / "stdout.log"

    entry = _write_captured_log(
        raw=b"2 passed in 97.9s\npresigned url: https://bucket/...\n",
        destination=destination,
        redacted=True,
        failed=False,
    )

    assert destination.read_text(encoding="utf-8") == (
        "Output omitted by redacted_receipt policy.\n"
    )
    assert entry["failure_tail_retained"] is False


def test_secret_values_filter_names_and_lengths() -> None:
    env = {
        "R2_ACCESS_KEY_ID": "key-value-long-enough",
        "R2_SECRET_ACCESS_KEY": "an-even-longer-secret-value",
        "LOGFIRE_TOKEN": "tokenvalue",
        "HOME": "/home/runner",
        "SHORT_KEY": "abc",
    }

    values = _secret_environment_values(env)

    names = [name for name, _ in values]
    assert set(names) == {"R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "LOGFIRE_TOKEN"}
    # Longest value first, so embedded substrings are masked after their
    # containers.
    lengths = [len(value) for _, value in values]
    assert lengths == sorted(lengths, reverse=True)


def test_timed_out_run_records_a_process_group_snapshot(tmp_path: Path) -> None:
    result = _run_process(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        cwd=tmp_path,
        env={"PATH": "/usr/bin:/bin"},
        timeout_seconds=1,
        log_prefix=tmp_path / "capture",
        redacted=False,
    )

    assert result["timed_out"] is True
    stderr_text = (tmp_path / "capture.stderr.log").read_text(encoding="utf-8")
    assert "operational timeout: no exit within 1s" in stderr_text
    assert "process-group snapshot at SIGTERM" in stderr_text
    # The stalled interpreter itself should be visible in the snapshot.
    assert "time.sleep(60)" in stderr_text or "pid=" in stderr_text


def test_timed_out_redacted_run_still_explains_itself(tmp_path: Path) -> None:
    """The #678 shape exactly: silent hang + redacted receipt."""
    result = _run_process(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        cwd=tmp_path,
        env={"PATH": "/usr/bin:/bin", "FAKE_SECRET_KEY": "irrelevant-but-long"},
        timeout_seconds=1,
        log_prefix=tmp_path / "capture",
        redacted=True,
    )

    assert result["timed_out"] is True
    stderr_text = (tmp_path / "capture.stderr.log").read_text(encoding="utf-8")
    assert "run FAILED, so the output tail is retained" in stderr_text
    assert "process-group snapshot at SIGTERM" in stderr_text
