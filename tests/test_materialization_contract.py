# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract: every .collect(), .to_pydict(), .to_pylist() call in src/ must be justified.

Archetype is data-centric: materialization breaks the lazy DAG and loses plan
optimization. The only justified reason to pull data out of Daft is cross-row
context that cannot be expressed columnarly. This test enforces that every
materialization call is documented with a "justified" comment on the same line
or within two lines above, so reviewers can audit each escape hatch.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).parent.parent / "src" / "archetype"

# Matches .collect(), .to_pydict(), .to_pylist() at a call site (allows whitespace)
CALL_RE = re.compile(r"\.(collect|to_pydict|to_pylist)\s*\(\s*\)")

# Files exempt from the contract — e.g., the contract test itself.
EXEMPT_FILES: set[str] = set()

# "justified" appearing within 2 lines above or below the call counts as valid.
# Below is allowed because chained-call expressions commonly put the closing-paren
# annotation on the line after the last call:
#     rows = (
#         df.sort(...)
#         .collect()
#         .to_pydict()
#     )  # justified .collect(): need full ordered sequence
JUSTIFIED_WINDOW = 2


def _iter_python_files(root: Path):
    for p in root.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        yield p


def _find_unjustified_calls(path: Path) -> list[tuple[int, str]]:
    """Return (line_no, line_text) for each materialization call lacking justification."""
    text = path.read_text()
    lines = text.splitlines()
    unjustified: list[tuple[int, str]] = []

    for i, line in enumerate(lines):
        # Skip strings/docstrings is not trivial without AST — but these tokens in
        # strings would be dead weight anyway. The test tolerates false positives
        # by letting the author add "justified" to any nearby comment.
        match = CALL_RE.search(line)
        if not match:
            continue

        # Look JUSTIFIED_WINDOW lines above and below this line for "justified"
        window_start = max(0, i - JUSTIFIED_WINDOW)
        window_end = min(len(lines), i + JUSTIFIED_WINDOW + 1)
        window = "\n".join(lines[window_start:window_end])
        if "justified" in window.lower():
            continue

        unjustified.append((i + 1, line.strip()))

    return unjustified


def test_all_materializations_are_justified():
    """Every .collect() / .to_pydict() / .to_pylist() in src/ must have a justification comment."""
    violations: list[str] = []

    for path in _iter_python_files(SRC_ROOT):
        rel = path.relative_to(SRC_ROOT.parent.parent)
        if str(rel) in EXEMPT_FILES:
            continue
        for line_no, line_text in _find_unjustified_calls(path):
            violations.append(f"{rel}:{line_no}: {line_text}")

    if violations:
        msg = (
            "Unjustified materialization call(s) detected. Every .collect(), .to_pydict(),\n"
            "and .to_pylist() must have a comment containing the word 'justified' on the\n"
            "same line or within 2 lines above, explaining why the lazy DAG must be broken.\n\n"
            "Violations:\n  " + "\n  ".join(violations)
        )
        pytest.fail(msg)


def test_contract_detects_unjustified_call(tmp_path):
    """The contract scanner must flag an unjustified materialization."""
    bad = tmp_path / "bad.py"
    bad.write_text("import daft\ndf = daft.from_pydict({'a': [1]})\ndf.collect()\n")
    assert _find_unjustified_calls(bad) == [(3, "df.collect()")]


def test_contract_accepts_same_line_justification(tmp_path):
    good = tmp_path / "good.py"
    good.write_text(
        "import daft\n"
        "df = daft.from_pydict({'a': [1]})\n"
        "df.collect()  # justified .collect(): test fixture\n"
    )
    assert _find_unjustified_calls(good) == []


def test_contract_accepts_comment_above(tmp_path):
    good = tmp_path / "good.py"
    good.write_text(
        "import daft\n"
        "df = daft.from_pydict({'a': [1]})\n"
        "# justified .collect(): needed for cross-row ordering\n"
        "df.collect()\n"
    )
    assert _find_unjustified_calls(good) == []
