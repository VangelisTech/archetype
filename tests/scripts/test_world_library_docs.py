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
    ROOT / "docs/guide/api-layer.md",
    ROOT / "docs/guide/api-stability.md",
    ROOT / "docs/guide/application-architecture.md",
    ROOT / "docs/guide/artifacts.md",
    ROOT / "docs/guide/autoresearch.md",
    ROOT / "docs/guide/examples.md",
    ROOT / "docs/guide/runtime.md",
    ROOT / "docs/guide/storage-migration.md",
    ROOT / "docs/guide/world-libraries.md",
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


def test_pending_trusted_publishers_do_not_claim_new_project_names() -> None:
    release_docs = (
        ROOT / "CONTRIBUTING.md",
        ROOT / "docs/guide/release-0.6.md",
        ROOT / "docs/guide/repository-harness.md",
    )

    for path in release_docs:
        content = " ".join(path.read_text(encoding="utf-8").lower().split())
        assert "register pending trusted publishers" in content
        assert "preconfigur" in content
        assert "does not reserve or claim" in content
        assert "remains claimable until the first successful oidc publication" in content


def test_contributing_names_each_trusted_publisher_workflow() -> None:
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")

    for workflow in (
        "release.yml",
        "publish-archetype-research.yml",
    ):
        assert f"`{workflow}`" in contributing


def test_split_rest_references_are_navigable() -> None:
    navigation = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "- REST API: reference/rest-api.md" in navigation
    assert "rest-api-missions.md" not in navigation


def test_split_research_and_evaluation_references_are_navigable() -> None:
    navigation = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "- Python API: reference/python/autoresearch.md" in navigation
    assert "- Framework Evaluation: reference/python/evaluation.md" in navigation
    assert "AutoResearch and Evaluation" not in navigation


def test_world_library_signature_contracts_are_in_the_reference_inventory() -> None:
    research = (ROOT / "docs/reference/python/autoresearch.md").read_text(encoding="utf-8")
    evaluation = (ROOT / "docs/reference/python/evaluation.md").read_text(encoding="utf-8")
    assert "::: archetype.research.Evaluator" in research
    assert "::: archetype.research.CandidatePreparer" in research
    assert "::: archetype.evaluation.models.FrameGrader" not in research
    assert "::: archetype.evaluation.models.FrameGrader" in evaluation
