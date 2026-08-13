# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed inputs for compiling a mission-factory blueprint."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BugFixLineInputs:
    """Parameters bound when one reusable ``BugFixLine`` becomes a mission."""

    repository: str
    branch: str
    issue: str
    test_path: str
    name: str = "bugfix-line"
    base_ref: str = "main"

    def __post_init__(self) -> None:
        for field_name in ("repository", "branch", "issue", "test_path", "name", "base_ref"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.test_path.startswith("/") or ".." in self.test_path.split("/"):
            raise ValueError("test_path must be repository-relative")
