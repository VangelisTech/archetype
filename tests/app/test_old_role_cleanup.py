# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Old-role cleanup enforcement.

Verifies that the deprecated role strings "maintainer" and "coder" no longer
appear anywhere in src/, tests/, or examples/.  This prevents accidental
re-introduction of removed role names.
"""

from __future__ import annotations

import re
from pathlib import Path

_PROJECT = Path(__file__).resolve().parents[2]
_SEARCH_DIRS = [_PROJECT / "src", _PROJECT / "tests", _PROJECT / "examples"]

# Files that are allowed to mention the role strings (this test itself, docs).
_EXCLUDE_PATTERNS = {
    "test_old_role_cleanup.py",
    "CHANGELOG",
    "MIGRATION",
    "migration",
}

# Regex: match the string literals "maintainer" or "coder" (with quotes).
_ROLE_RE = re.compile(r"""["'](?:maintainer|coder)["']""")


def _scan_for_old_roles() -> list[str]:
    """Return human-readable violation strings."""
    violations: list[str] = []
    for search_dir in _SEARCH_DIRS:
        if not search_dir.is_dir():
            continue
        for py in sorted(search_dir.rglob("*")):
            if not py.is_file():
                continue
            if any(pat in py.name for pat in _EXCLUDE_PATTERNS):
                continue
            # Only check text files we can reasonably read.
            if py.suffix not in (
                ".py",
                ".toml",
                ".yaml",
                ".yml",
                ".json",
                ".cfg",
                ".ini",
                ".txt",
                ".md",
                ".rst",
            ):
                continue
            try:
                text = py.read_text(errors="ignore")
            except Exception:
                continue
            for i, line in enumerate(text.splitlines(), 1):
                if _ROLE_RE.search(line):
                    rel = py.relative_to(_PROJECT)
                    violations.append(f"{rel}:{i}  {line.strip()}")
    return violations


class TestOldRoleCleanup:
    """Ensure deprecated role strings are fully removed."""

    def test_no_maintainer_or_coder_role_strings(self):
        violations = _scan_for_old_roles()
        assert not violations, (
            'Found deprecated role strings ("maintainer" / "coder"):\n  ' + "\n  ".join(violations)
        )
