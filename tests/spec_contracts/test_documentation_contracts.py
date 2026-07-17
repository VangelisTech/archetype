# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for claims made by the hand-authored guides."""

from __future__ import annotations

import re
from pathlib import Path

from archetype.app.auth.permissions import ROLES_BY_COMMAND
from archetype.app.models import CommandType

_GUIDE_ROOT = Path("docs/guide")
_COMMAND_TOTAL_PATTERNS = (
    re.compile(r"\b(\d+)\s+command\s+types?\b", re.IGNORECASE),
    re.compile(r"\bcommand\s+types?\*{0,2}\s*\((\d+)\s+total\b", re.IGNORECASE),
)
_DESIGN_ONLY_MESSAGING_TYPES = (
    "MessageDeliveryProcessor",
    "DeliveryReceipt",
    "ChatGraphRegistry",
)


def test_numeric_command_type_claims_match_the_enum() -> None:
    """A guide may omit a volatile total, but any total it states must be true."""
    expected = len(CommandType)
    stale: list[str] = []

    for guide in sorted(_GUIDE_ROOT.glob("*.md")):
        text = guide.read_text()
        for pattern in _COMMAND_TOTAL_PATTERNS:
            for match in pattern.finditer(text):
                claimed = int(match.group(1))
                if claimed != expected:
                    line = text.count("\n", 0, match.start()) + 1
                    stale.append(f"{guide}:{line} claims {claimed}; CommandType has {expected}")

    assert not stale, "stale command-type totals:\n" + "\n".join(stale)


def test_curated_example_command_roles_follow_permissions() -> None:
    """The examples table may be selective, but every listed role must be authoritative."""
    guide = (_GUIDE_ROOT / "examples.md").read_text()
    heading = "**Gated operations in this example (curated, not exhaustive):**"
    lines = guide.splitlines()
    start = lines.index(heading)
    role_order = ("viewer", "player", "operator", "admin")
    rows: list[str] = []

    for line in lines[start + 1 :]:
        if line.startswith("|"):
            if "---" not in line and "Gate command" not in line:
                rows.append(line)
        elif rows:
            break

    assert rows, "the curated command table is missing"
    for row in rows:
        _runtime_call, command_text, roles_text = [
            cell.strip() for cell in row.strip("|").split("|")
        ]
        command = CommandType(command_text.strip("`"))
        documented = tuple(role.strip() for role in roles_text.split(","))
        expected = tuple(role for role in role_order if role in ROLES_BY_COMMAND[command])
        assert documented == expected, (
            f"{command.value}: documented {documented}, expected {expected}"
        )


def test_current_robotics_guides_point_to_extracted_libero_harness() -> None:
    """Current guidance must point into robot-evals, not the removed bench tree."""
    guides = (Path("LEARNINGS.md"), _GUIDE_ROOT / "autoresearch.md")

    text_by_path = {path: path.read_text() for path in guides}
    stale = [str(path) for path, text in text_by_path.items() if "bench/libero/" in text]
    missing_successor = [
        str(path) for path, text in text_by_path.items() if "robot-evals" not in text
    ]

    assert not stale, f"current guides reference the extracted bench/libero tree: {stale}"
    assert not missing_successor, f"current guides omit robot-evals: {missing_successor}"


def test_docs_do_not_claim_design_only_messaging_types() -> None:
    """Unimplemented design sketches must not read as framework contracts."""
    paths = [Path("LEARNINGS.md"), *sorted(_GUIDE_ROOT.glob("*.md"))]
    stale: list[str] = []

    for path in paths:
        text = path.read_text()
        for type_name in _DESIGN_ONLY_MESSAGING_TYPES:
            if type_name in text:
                stale.append(f"{path}: presents design-only {type_name}")

    assert not stale, "stale messaging infrastructure claims:\n" + "\n".join(stale)
