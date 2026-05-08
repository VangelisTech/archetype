# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Detection-only DRY eval: agent enumerates duplicates, oracle is truth.

Inspired by unclebob/dry4clj.  We give the agent a small fixture of
Python files, ask it to list every pair of duplicated function-level
forms, and grade its answer against the oracle's pair set (Jaccard >=
:data:`THRESHOLD` on normalized AST edges).

Graceful degradation:
  - No ``ANTHROPIC_API_KEY`` -> task records a "skipped" grader and
    keeps CI green; ``score`` is 0.0 so the skip is visible in reports.
  - ``anthropic`` SDK not installed -> same skip behavior.
  - LLM call raises -> single failing grader with the exception message.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

from evals.dry import find_duplicates, fingerprint_paths
from evals.dry.oracle import DEFAULT_MIN_NODES, DEFAULT_THRESHOLD
from evals.graders import threshold
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"

# F1 floor for the agent's pair-set vs. the oracle.  0.70 leaves room
# for the agent to miss one of the five ground-truth pairs and still
# pass; tighten as the model improves.
F1_MIN = 0.70

MODEL = "claude-opus-4-7"
MAX_TOKENS = 2048

_FIXTURE_DIR = Path(__file__).resolve().parent.parent / "dry" / "fixtures"


def _fixture_paths() -> list[Path]:
    return sorted(p for p in _FIXTURE_DIR.glob("*.py") if p.name != "__init__.py")


def _build_prompt(paths: list[Path]) -> str:
    sections = []
    for p in paths:
        sections.append(f"### FILE: {p.name}\n```python\n{p.read_text()}```")
    files_block = "\n\n".join(sections)
    return f"""You are auditing a small Python codebase for duplicated function-level forms.

Two functions are "duplicates" when they share structure (same control flow, \
same operator/call shape, same field access pattern) even if their names, \
parameter names, and literal values differ.  Trivially short helpers do not \
count.  Pure naming changes still count as duplicates.

For every pair of duplicate functions, emit ONE JSON object:
  {{"file_a": "<basename>", "name_a": "<func name>", "line_a": <def line>,
   "file_b": "<basename>", "name_b": "<func name>", "line_b": <def line>}}

Return ONLY a JSON array of those objects (no prose, no fences). \
Use the def line number (the line containing `def` or `async def`).  \
Each unordered pair must appear exactly once.

{files_block}
"""


_PAIR_RE = re.compile(r"\[.*\]", re.DOTALL)


def _parse_agent_pairs(
    text: str,
    form_by_loc: dict[tuple[str, str, int], str],
) -> set[frozenset[str]] | None:
    """Parse the LLM response into a set of frozenset pairs of form_ids.

    Returns None if the response can't be parsed at all (graded as 0.0).
    Pairs that don't resolve to known forms are silently dropped — that
    just hurts the agent's precision, which is the right behavior.
    """
    match = _PAIR_RE.search(text)
    if not match:
        return None
    try:
        raw = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, list):
        return None

    out: set[frozenset[str]] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            fa = str(item["file_a"])
            fb = str(item["file_b"])
            la = int(item["line_a"])
            lb = int(item["line_b"])
            na = str(item["name_a"])
            nb = str(item["name_b"])
        except (KeyError, ValueError, TypeError):
            continue
        a_id = form_by_loc.get((fa, na, la))
        b_id = form_by_loc.get((fb, nb, lb))
        if a_id is None or b_id is None or a_id == b_id:
            continue
        out.add(frozenset({a_id, b_id}))
    return out


def _skipped(reason: str) -> list[GraderResult]:
    return [
        GraderResult(
            grader_name="dry_detection",
            passed=True,
            score=0.0,
            details=f"skipped: {reason}",
        )
    ]


def task_dry_detection() -> list[GraderResult]:
    """Have the agent enumerate duplicate pairs; grade vs. oracle."""
    paths = _fixture_paths()
    forms = fingerprint_paths(paths)
    truth_pairs = find_duplicates(forms, threshold=DEFAULT_THRESHOLD, min_nodes=DEFAULT_MIN_NODES)
    truth_set = {frozenset({p.a.form_id, p.b.form_id}) for p in truth_pairs}

    # Map (basename, name, lineno) -> canonical form_id, since the agent
    # sees only basenames.
    form_by_loc = {(Path(f.path).name, f.name, f.lineno): f.form_id for f in forms}

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return _skipped("ANTHROPIC_API_KEY not set")
    try:
        import anthropic  # noqa: F401
    except ImportError:
        return _skipped("anthropic SDK not installed (uv add anthropic)")

    prompt = _build_prompt(paths)
    try:
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(block.text for block in response.content if getattr(block, "text", None))
    except Exception as e:  # noqa: BLE001 - report any LLM error verbatim
        return [
            GraderResult(
                grader_name="dry_detection",
                passed=False,
                score=0.0,
                details=f"LLM call failed: {type(e).__name__}: {e}",
            )
        ]

    agent_pairs = _parse_agent_pairs(text, form_by_loc)
    if agent_pairs is None:
        return [
            GraderResult(
                grader_name="dry_detection",
                passed=False,
                score=0.0,
                details=f"unparseable response: {text[:120]!r}",
            )
        ]

    tp = len(agent_pairs & truth_set)
    precision = tp / len(agent_pairs) if agent_pairs else 0.0
    recall = tp / len(truth_set) if truth_set else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    detail = f"tp={tp}, |agent|={len(agent_pairs)}, |oracle|={len(truth_set)}"
    return [
        threshold(f1, min_val=F1_MIN, name="f1"),
        GraderResult(
            grader_name="precision",
            passed=True,
            score=precision,
            details=detail,
        ),
        GraderResult(
            grader_name="recall",
            passed=True,
            score=recall,
            details=detail,
        ),
    ]


def register(harness: EvalHarness) -> None:
    """Register the DRY detection task on the harness."""
    harness.add(
        "dry_detection",
        suite=SUITE,
        fn=task_dry_detection,
        desc=(
            "Agent enumerates duplicate Python forms in a small fixture; "
            "graded vs. dry4clj-style AST-edge Jaccard oracle (F1 >= "
            f"{F1_MIN})"
        ),
    )
