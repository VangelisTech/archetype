# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Common provenance envelope for retained repository evidence."""

from __future__ import annotations

import os
import platform
import subprocess
import sys
from datetime import UTC, datetime
from importlib import metadata
from typing import Any

SCHEMA_VERSION = 1
_PACKAGES = ("archetype-ecs", "daft", "lancedb", "pyiceberg", "pytest")


def utc_now() -> str:
    """Return an RFC 3339 UTC timestamp."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def capture_revision() -> dict[str, Any]:
    """Capture the exact source revision without requiring a Git checkout."""
    commit = os.environ.get("GITHUB_SHA") or _git("rev-parse", "HEAD") or "unknown"
    dirty = _git("status", "--short")
    return {"commit": commit, "dirty": None if dirty is None else bool(dirty)}


def capture_environment() -> dict[str, Any]:
    """Capture the execution identity needed to interpret retained evidence."""
    packages: dict[str, str] = {}
    for package in _PACKAGES:
        try:
            packages[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            packages[package] = "not-installed"
    return {
        "runner_id": (
            os.environ.get("ARCHETYPE_QUALITY_RUNNER")
            or os.environ.get("GITHUB_RUN_ID")
            or platform.node()
            or "unknown"
        ),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "packages": packages,
    }


def build_result_envelope(
    *,
    kind: str,
    profile: str,
    suites: list[str],
    failure_policy: str,
    started_at: str,
    duration_s: float,
    outcome: str,
    configuration: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Wrap a tool-native result set in the shared repository envelope."""
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "profile": profile,
        "suites": suites,
        "failure_policy": failure_policy,
        "started_at": started_at,
        "duration_s": round(duration_s, 6),
        "outcome": outcome,
        "revision": capture_revision(),
        "environment": capture_environment(),
        "invocation": list(sys.argv),
        "configuration": configuration,
        "results": results,
    }


def _git(*args: str) -> str | None:
    try:
        process = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if process.returncode != 0:
        return None
    return process.stdout.strip()
