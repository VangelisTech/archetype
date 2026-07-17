# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for claims made by the hand-authored guides."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from archetype.app.auth.permissions import ROLES_BY_COMMAND
from archetype.app.models import CommandType
from evals.run import build_harness

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
_EVAL_MANIFEST = re.compile(
    r"<!-- eval-task-manifest:start -->(.*?)<!-- eval-task-manifest:end -->",
    re.DOTALL,
)
_EVAL_TASK_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|", re.MULTILINE)
_BEGINNER_GUIDES = (
    _GUIDE_ROOT / "quickstart.md",
    _GUIDE_ROOT / "building-simulations.md",
    _GUIDE_ROOT / "processors.md",
)
_BEGINNER_EXAMPLES = (
    Path("examples/00_quickstart.py"),
    Path("examples/simulation_script.py"),
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


def test_eval_guide_manifest_matches_registered_tasks() -> None:
    """The guide's task inventory is exact, not a hand-maintained sample."""
    guide = (_GUIDE_ROOT / "evals.md").read_text()
    manifest = _EVAL_MANIFEST.search(guide)
    assert manifest is not None, "eval task manifest markers are missing"

    rows = _EVAL_TASK_ROW.findall(manifest.group(1))
    documented = {(suite, task_id) for suite, task_id in rows}
    assert len(rows) == len(documented), "eval task manifest contains duplicate rows"

    harness = build_harness(trials=1)
    registered = {(suite, task_id) for task_id, suite, _, _ in harness._tasks}
    assert documented == registered, (
        f"eval guide manifest drifted; missing={registered - documented}, "
        f"stale={documented - registered}"
    )


def test_beginner_surfaces_do_not_route_through_engine_internals() -> None:
    """Beginner paths compose the public runtime instead of wiring core objects."""
    forbidden_guide_terms = ("archetype.core", "AsyncWorld", "AsyncSystem", "world.system")
    stale = [
        f"{path}: contains {term}"
        for path in _BEGINNER_GUIDES
        for term in forbidden_guide_terms
        if term in path.read_text()
    ]

    for path in _BEGINNER_EXAMPLES:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "archetype.core"
            ):
                stale.append(f"{path}:{node.lineno} imports {node.module}")

    assert not stale, "beginner surfaces expose engine internals:\n" + "\n".join(stale)


def test_quickstart_example_stays_under_thirty_source_lines() -> None:
    """The copy-and-run path remains small enough to understand in one screen."""
    source_lines = [
        line
        for line in _BEGINNER_EXAMPLES[0].read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert len(source_lines) < 30, f"quickstart grew to {len(source_lines)} source lines"


def test_quickstart_variants_stage_initial_state_before_running() -> None:
    """Async and sync snippets must spend the same number of processor ticks."""
    guide = (_GUIDE_ROOT / "quickstart.md").read_text()
    async_section, sync_section = guide.split("## Synchronous scripts", maxsplit=1)

    for name, section in (("async", async_section), ("sync", sync_section)):
        spawn = section.rfind("world.spawn(")
        step = section.find("world.step()", spawn)
        run = section.find("world.run(steps=3)", spawn)
        assert -1 not in (spawn, step, run), f"{name} quickstart lifecycle is incomplete"
        assert spawn < step < run, f"{name} quickstart must persist spawns before its run"
