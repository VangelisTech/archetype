# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small JSON snapshots shared by local benchmark suites."""

from __future__ import annotations

import json
import os
import platform
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

_PACKAGES = ("archetype-ecs", "daft", "lancedb", "pyiceberg")


def capture_environment(*, runner_id: str | None = None) -> dict[str, Any]:
    """Capture enough context to reproduce or interpret one local run."""
    packages: dict[str, str] = {}
    for package in _PACKAGES:
        try:
            packages[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            packages[package] = "not-installed"

    return {
        "runner_id": (
            runner_id or os.environ.get("ARCHETYPE_BENCH_RUNNER") or platform.node() or "unknown"
        ),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "packages": packages,
    }


def capture_revision() -> dict[str, Any]:
    """Capture the measured Git revision when Git is available."""
    commit = os.environ.get("GITHUB_SHA") or _git_output("rev-parse", "HEAD") or "unknown"
    dirty_output = _git_output("status", "--short")
    return {
        "commit": commit,
        "dirty": None if dirty_output is None else bool(dirty_output),
    }


def build_report(
    results: Sequence[Mapping[str, Any]],
    *,
    suite: str,
    config: Mapping[str, Any],
    environment: Mapping[str, Any] | None = None,
    revision: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Attach reproduction context without imposing a cross-suite metric schema."""
    if not suite:
        raise ValueError("benchmark suite must not be empty")
    if not results:
        raise ValueError("benchmark results must not be empty")

    report = {
        "suite": suite,
        "created_at": created_at or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "revision": dict(revision or capture_revision()),
        "environment": dict(environment or capture_environment()),
        "config": dict(config),
        "results": [dict(result) for result in results],
    }
    _render(report)
    return report


def write_report(report: Mapping[str, Any], path: str | Path) -> Path:
    """Write one readable snapshot; retention belongs to the caller."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(_render(report))
    return output


def _render(report: Mapping[str, Any]) -> str:
    try:
        return json.dumps(report, allow_nan=False, indent=2, sort_keys=True) + "\n"
    except (TypeError, ValueError) as exc:
        raise ValueError(f"benchmark report must contain finite JSON data: {exc}") from exc


def _git_output(*args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()
