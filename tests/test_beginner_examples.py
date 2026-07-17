# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end contracts for the two beginner-facing Python examples."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _run_example(name: str, tmp_path: Path) -> str:
    env = os.environ.copy()
    env["DO_NOT_TRACK"] = "1"
    result = subprocess.run(
        [sys.executable, str(_ROOT / "examples" / name)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def test_quickstart_runs_in_one_command(tmp_path: Path) -> None:
    assert _run_example("00_quickstart.py", tmp_path).strip() == "3"


def test_simulation_script_reports_the_current_state(tmp_path: Path) -> None:
    output = _run_example("simulation_script.py", tmp_path)
    assert "Alice: skill=3.0, experience=60, rating=18.00" in output
    assert "Bob: skill=2.0, experience=40, rating=8.00" in output
    assert "Charlie: skill=1.5, experience=30, rating=4.50" in output
