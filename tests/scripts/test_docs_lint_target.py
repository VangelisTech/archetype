# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Smoke contract for the aggregate local documentation gate."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _write_recorder(bin_dir: Path, name: str) -> None:
    tool = bin_dir / name
    tool.write_text(
        "#!/bin/sh\n"
        f'for arg in "$@"; do printf "%s\\n" "$arg"; done > "$DOC_LINT_RECORD_DIR/{name}"\n',
        encoding="utf-8",
    )
    tool.chmod(0o755)


def test_docs_lint_invokes_tools_with_ci_equivalent_inputs(tmp_path) -> None:
    bin_dir = tmp_path / "bin"
    records = tmp_path / "records"
    bin_dir.mkdir()
    records.mkdir()
    for tool in ("typos", "markdownlint-cli2", "lychee"):
        _write_recorder(bin_dir, tool)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["DOC_LINT_RECORD_DIR"] = str(records)

    result = subprocess.run(
        ["make", "docs-lint"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (records / "typos").read_text().splitlines() == []
    assert (records / "markdownlint-cli2").read_text().splitlines() == [
        "docs/**/*.md",
        "*.md",
    ]
    assert (records / "lychee").read_text().splitlines() == [
        "--config",
        "lychee.toml",
        "docs/",
        "README.md",
        "CONTRIBUTING.md",
        "AGENTS.md",
    ]
