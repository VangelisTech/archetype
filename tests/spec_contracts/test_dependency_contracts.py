# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for security-sensitive dependency floors."""

from __future__ import annotations

import tomllib
from pathlib import Path

_PATCHED_THRIFT_RELEASE = (0, 23, 0)


def _is_affected_thrift(version: str) -> bool:
    release = version.split("+", 1)[0]
    core, prerelease_separator, _prerelease = release.partition("-")
    parts = tuple(int(part) for part in core.split("."))
    return parts < _PATCHED_THRIFT_RELEASE or (
        parts == _PATCHED_THRIFT_RELEASE and bool(prerelease_separator)
    )


def test_thrift_advisory_boundary_matches_semver_ordering() -> None:
    expected = {
        "0.17.0": True,
        "0.22.0": True,
        "0.23.0-rc.1": True,
        "0.23.0": False,
        "0.24.0": False,
    }

    assert {version: _is_affected_thrift(version) for version in expected} == expected


def test_cargo_lock_excludes_thrift_affected_by_cve_2026_43868() -> None:
    lock = tomllib.loads(Path("Cargo.lock").read_text())
    affected = [
        package["version"]
        for package in lock["package"]
        if package["name"] == "thrift" and _is_affected_thrift(package["version"])
    ]

    assert not affected, (
        f"Cargo.lock reintroduced thrift before 0.23.0 (CVE-2026-43868): {', '.join(affected)}"
    )
