# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the local documentation quality gate."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def _install_capture_stub(bin_dir: Path, command: str) -> None:
    stub = bin_dir / command
    stub.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > "$DOCS_LINT_CAPTURE_DIR/{command}.args"\n')
    stub.chmod(0o755)


def _run_docs_lint_with_stubs(tmp_path: Path, commands: tuple[str, ...]) -> Path:
    bin_dir = tmp_path / "bin"
    capture_dir = tmp_path / "captures"
    bin_dir.mkdir()
    capture_dir.mkdir()

    for command in commands:
        _install_capture_stub(bin_dir, command)

    env = os.environ.copy()
    env["PATH"] = str(bin_dir)
    env["DOCS_LINT_CAPTURE_DIR"] = str(capture_dir)
    make = shutil.which("make")
    assert make is not None, "make is required to exercise the target"
    result = subprocess.run(
        [make, "docs-lint"],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    return capture_dir


def _captured_args(capture_dir: Path, command: str) -> list[str]:
    return (capture_dir / f"{command}.args").read_text().splitlines()


def test_docs_lint_invokes_ci_equivalent_document_scopes(tmp_path: Path) -> None:
    """The Make target must pass paths each tool accepts and CI actually checks."""
    capture_dir = _run_docs_lint_with_stubs(
        tmp_path,
        ("typos", "markdownlint-cli2", "lychee"),
    )
    expected = {
        "typos": ["--config", "./_typos.toml", "."],
        "markdownlint-cli2": [
            "--config",
            ".markdownlint.yaml",
            "docs/**/*.md",
            "*.md",
        ],
        "lychee": [
            "--config",
            "lychee.toml",
            "docs/",
            "README.md",
            "CONTRIBUTING.md",
            "AGENTS.md",
        ],
    }
    for command, arguments in expected.items():
        captured = _captured_args(capture_dir, command)
        assert captured == arguments, f"{command}: {captured} != {arguments}"


def test_docs_lint_npx_fallback_does_not_probe_nonzero_help(tmp_path: Path) -> None:
    """A usable npx fallback must run even though markdownlint --help exits 2."""
    capture_dir = _run_docs_lint_with_stubs(
        tmp_path,
        ("typos", "npx", "lychee"),
    )
    captured = _captured_args(capture_dir, "npx")
    assert captured == [
        "--yes",
        "markdownlint-cli2",
        "--config",
        ".markdownlint.yaml",
        "docs/**/*.md",
        "*.md",
    ]
