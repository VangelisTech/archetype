# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Negative controls for the runtime-loopback cleanup probe.

``provider_children`` in the loopback receipt claims to count processes that
outlive the scenario. A probe for that claim is only worth having if it can
actually observe a leak, so every test here asserts against a *real* leaked
process rather than against the constant the receipt happens to print.

The previous implementation counted unreaped direct children via
``os.waitpid(pid, WNOHANG)``. A leaked provider child is not a direct child, so
``waitpid`` raised ``ChildProcessError`` and the probe read it as "reaped" —
and every tracked pid was reaped before the probe ran anyway. The count was
structurally always zero. ``test_leaked_grandchild_is_counted`` fails against
that implementation.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from run_runtime_loopback import (  # noqa: E402
    _process_group_alive,
    _surviving_process_groups,
)


@pytest.fixture(autouse=True)
def _fast_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the probe's semantics, shorten only its waiting."""
    monkeypatch.setattr("run_runtime_loopback._PROCESS_EXIT_GRACE_SECONDS", 1.0)


def _spawn_group(child_code: str) -> subprocess.Popen[bytes]:
    """Start a session leader so its pid is also its process-group id."""
    return subprocess.Popen(
        [sys.executable, "-c", child_code],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def test_leaked_grandchild_is_counted() -> None:
    """A process outliving its reaped leader is cleanup debt, and must be seen.

    This is the regression that the pid-based probe could not express: the
    leader exits and is reaped, but it leaves a descendant in its process
    group. Nothing about the leader's exit status reveals that.
    """
    leader = _spawn_group(
        "import subprocess, sys;"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']);"
    )
    group = leader.pid
    leader.wait()  # leader reaped, exactly as the loopback reaps its processes
    # Let the orphan settle so we are asserting against a genuinely live process.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not _process_group_alive(group):
        time.sleep(0.05)
    assert _process_group_alive(group), "test setup failed to leak a process"

    assert _surviving_process_groups([group]) == 1


def test_clean_process_group_reports_no_survivors() -> None:
    """Positive control: a group that exits fully is not reported as leaked."""
    leader = _spawn_group("pass")
    group = leader.pid
    leader.wait()

    assert _surviving_process_groups([group]) == 0


def test_survivors_are_terminated_after_being_counted() -> None:
    """The probe records the debt and still leaves the machine clean."""
    leader = _spawn_group(
        "import subprocess, sys;"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']);"
    )
    group = leader.pid
    leader.wait()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not _process_group_alive(group):
        time.sleep(0.05)

    assert _surviving_process_groups([group]) == 1

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and _process_group_alive(group):
        time.sleep(0.05)
    assert not _process_group_alive(group), "counted survivors must also be reaped"


def test_empty_group_list_reports_no_survivors() -> None:
    assert _surviving_process_groups([]) == 0


def test_process_group_liveness_tracks_a_real_process() -> None:
    """The underlying probe distinguishes a live group from a dead one."""
    leader = _spawn_group("import time; time.sleep(60)")
    group = leader.pid

    assert _process_group_alive(group)

    os.killpg(group, 9)
    leader.wait()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and _process_group_alive(group):
        time.sleep(0.05)

    assert not _process_group_alive(group)
