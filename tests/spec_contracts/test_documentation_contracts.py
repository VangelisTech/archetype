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


def test_current_robotics_guides_do_not_reference_extracted_libero_paths() -> None:
    """Current guidance must point into robot-evals, not the removed bench tree."""
    guides = (Path("LEARNINGS.md"), _GUIDE_ROOT / "autoresearch.md")

    stale = [str(path) for path in guides if "bench/libero/" in path.read_text()]

    assert not stale, f"current guides reference the extracted bench/libero tree: {stale}"


def test_messaging_examples_are_labeled_as_application_defined() -> None:
    """Design-sketch names must not read like exported framework contracts."""
    guides = (Path("LEARNINGS.md"), _GUIDE_ROOT / "system-execution.md")
    example_names = ("DeliveryReceipt", "MessageDeliveryProcessor", "ChatGraphRegistry")

    for path in guides:
        text = path.read_text()
        boundary_match = re.search(r"does not\s+export", text)
        assert boundary_match is not None, f"{path} is missing the export boundary"
        boundary = boundary_match.start()
        first_example = min(text.find(name) for name in example_names if name in text)
        assert boundary < first_example, (
            f"{path} must label messaging examples before presenting their type names"
        )
