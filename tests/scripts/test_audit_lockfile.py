# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the blocking lockfile dependency audit."""

from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.audit_lockfile import audit_lockfile


def test_lockfile_audit_fails_on_known_bad_injection(
    tmp_path: Path,
    capsys,
) -> None:
    calls: list[list[str]] = []

    def run(
        command: list[str],
        **_kwargs,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["uv", "export"]:
            output = Path(command[command.index("--output-file") + 1])
            output.write_text("known-bad-package==0\n", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, "", "")
        assert command[1:3] == ["-m", "pip_audit"]
        assert "known-bad-package==0" in Path(
            command[command.index("--requirement") + 1]
        ).read_text(encoding="utf-8")
        return subprocess.CompletedProcess(
            command,
            1,
            "Found 1 known vulnerability in 1 package\n",
            "",
        )

    status = audit_lockfile(root=tmp_path, run=run)

    assert status == 1
    assert len(calls) == 2
    assert "--locked" in calls[0]
    assert "--no-emit-project" in calls[0]
    assert "--no-deps" in calls[1]
    assert "Found 1 known vulnerability" in capsys.readouterr().out
