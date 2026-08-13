# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Guard the canonical 0.6 world-library documentation surface."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CANONICAL_SURFACES = (
    ROOT / "README.md",
    ROOT / "docs/index.md",
    ROOT / "docs/guide/api-stability.md",
    ROOT / "docs/guide/artifacts.md",
    ROOT / "docs/guide/autoresearch.md",
    ROOT / "docs/guide/examples.md",
    ROOT / "docs/guide/physical-ai.md",
    ROOT / "docs/guide/runtime.md",
    ROOT / "docs/guide/trajectories.md",
    ROOT / "docs/guide/world-libraries.md",
    ROOT / "examples/06_trajectory_analysis.py",
    ROOT / "examples/10_autoresearch.py",
    *sorted((ROOT / "experiments").glob("*.py")),
)
REMOVED_SURFACE = re.compile(
    r"\bRuntimeMissions\b|"
    r"\bruntime\.missions\(|"
    r"\b(?:world|fork|runtime_world|sync_world)\."
    r"(?:autoresearch|grade_trajectory|ingest_claude_transcript|"
    r"query_trajectory|run_hosted_episode|transcript_rows)\(|"
    r"\bCandidateContext\b"
)


def test_canonical_world_library_docs_do_not_teach_removed_compatibility() -> None:
    stale: dict[Path, list[str]] = {}
    for path in CANONICAL_SURFACES:
        content = path.read_text(encoding="utf-8")
        matches = sorted(set(REMOVED_SURFACE.findall(content)))
        if matches:
            stale[path.relative_to(ROOT)] = matches

    assert stale == {}


def test_autoresearch_guide_uses_generic_terminal_states() -> None:
    guide = (ROOT / "docs/guide/autoresearch.md").read_text(encoding="utf-8")

    assert "`SUCCEEDED`" in guide
    assert "`FAILED`" in guide
    assert "`STOPPED`" not in guide
    assert "`CRASHED`" not in guide


def test_clean_break_release_note_is_reader_visible() -> None:
    release_note = (ROOT / "docs/guide/release-0.6.md").read_text(encoding="utf-8")
    navigation = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "There are no world-library import shims" in release_note
    assert "Pre-0.6 Research ledgers are unsupported" in release_note
    assert "guide/release-0.6.md" in navigation


def test_missions_reference_renders_the_primary_workflow_methods() -> None:
    runtime = (ROOT / "packages/archetype-missions/src/archetype/missions/runtime.py").read_text(
        encoding="utf-8"
    )

    assert '"""Persist one coding mission and return its durable identity."""' in runtime
    assert '"""Run a submitted mission to a terminal result."""' in runtime
    assert '"""Release this mission workflow handle and its world reservation."""' in runtime

    rendered = ROOT / "site/docs/reference/python/missions/index.html"
    if rendered.exists():
        page = rendered.read_text(encoding="utf-8")
        assert "Persist one coding mission and return its durable identity." in page
        assert "Run a submitted mission to a terminal result." in page
        assert "Release this mission workflow handle and its world reservation." in page


def test_world_library_signature_contracts_are_in_the_reference_inventory() -> None:
    research = (ROOT / "docs/reference/python/autoresearch.md").read_text(encoding="utf-8")
    physical = (ROOT / "docs/reference/python/physical-ai.md").read_text(encoding="utf-8")
    optimization = (ROOT / "docs/reference/python/physical-ai-optimization.md").read_text(
        encoding="utf-8"
    )
    host = (ROOT / "docs/reference/python/physical-ai-host.md").read_text(encoding="utf-8")

    assert "::: archetype.research.Evaluator" in research
    assert "::: archetype.research.CandidatePreparer" in research
    assert "::: archetype.physical_ai.PhysicalAIExtensionConfig" in host
    assert "::: archetype.physical_ai.optimization.PerturbationStrategy" in optimization
    assert "::: archetype.physical_ai.interfaces.EnvClient" not in physical
    assert "::: archetype.physical_ai.interfaces.PolicyClient" not in physical
