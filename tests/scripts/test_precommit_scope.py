# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for multi-distribution pre-commit trigger coverage."""

from __future__ import annotations

import re
from pathlib import Path

_CONFIG = Path(".pre-commit-config.yaml")


def _hook_block(config: str, hook_id: str) -> str:
    marker = f"- id: {hook_id}"
    block = config.split(marker, maxsplit=1)[1]
    return block.split("- id:", maxsplit=1)[0]


def test_python_hooks_trigger_for_workspace_package_sources() -> None:
    """Removing the monolith root must not make changed package code invisible."""
    config = _CONFIG.read_text()

    for hook_id in ("ruff", "ruff-format"):
        block = _hook_block(config, hook_id)
        assert "packages" in block
        assert "^(src|" not in block

    for hook_id in ("ty", "check-license-headers", "lazy-audit"):
        block = _hook_block(config, hook_id)
        assert "packages/[^/]+/src/" in block
        assert "files: '^src/" not in block

    assert "(packages/[^/]+/)?pyproject\\.toml" in _hook_block(config, "ty")


def test_precommit_ruff_matches_the_locked_ci_line() -> None:
    """Developer commits and CI must not apply different Ruff rule versions."""
    config = _CONFIG.read_text()
    lock = Path("uv.lock").read_text()

    match = re.search(r'^name = "ruff"\nversion = "([^"]+)"', lock, re.MULTILINE)
    assert match is not None
    assert f"rev: v{match.group(1)}" in config
