#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Audit every locked dependency with pip-audit."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
Run = Callable[..., subprocess.CompletedProcess[str]]


def _emit(process: subprocess.CompletedProcess[str]) -> None:
    if process.stdout:
        print(process.stdout, end="")
    if process.stderr:
        print(process.stderr, end="", file=sys.stderr)


def audit_lockfile(
    *,
    root: Path = ROOT,
    run: Run = subprocess.run,
) -> int:
    """Export the exact lock and return pip-audit's blocking status."""
    with TemporaryDirectory(prefix="archetype-lockfile-audit-") as temporary:
        requirements = Path(temporary) / "requirements.txt"
        exported = run(
            [
                "uv",
                "export",
                "--locked",
                "--all-extras",
                "--all-groups",
                "--no-emit-workspace",
                "--format",
                "requirements-txt",
                "--output-file",
                str(requirements),
            ],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if exported.returncode != 0:
            _emit(exported)
            return exported.returncode

        contents = requirements.read_text(encoding="utf-8")
        if any(line.lstrip().startswith(("-e ", "--editable ")) for line in contents.splitlines()):
            print(
                "Lockfile audit refused an editable project requirement",
                file=sys.stderr,
            )
            return 1

        audited = run(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "--requirement",
                str(requirements),
                "--no-deps",
                "--disable-pip",
            ],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        _emit(audited)
        return audited.returncode


def main(_argv: Sequence[str] | None = None) -> int:
    return audit_lockfile()


if __name__ == "__main__":
    raise SystemExit(main())
