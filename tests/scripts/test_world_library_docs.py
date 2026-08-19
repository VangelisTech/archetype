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
    ROOT / "docs/guide/agent-missions.md",
    ROOT / "docs/guide/api-layer.md",
    ROOT / "docs/guide/api-stability.md",
    ROOT / "docs/guide/application-architecture.md",
    ROOT / "docs/guide/artifacts.md",
    ROOT / "docs/guide/autoresearch.md",
    ROOT / "docs/guide/examples.md",
    ROOT / "docs/guide/physical-ai.md",
    ROOT / "docs/guide/runtime.md",
    ROOT / "docs/guide/storage-migration.md",
    ROOT / "docs/guide/trajectories.md",
    ROOT / "docs/guide/world-libraries.md",
    ROOT / "docs/missions/recovery.md",
    ROOT / "docs/missions/transcripts.md",
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


def test_agent_missions_guide_uses_the_0_6_adapter_and_contract() -> None:
    guide = (ROOT / "docs/guide/agent-missions.md").read_text(encoding="utf-8")

    assert "archetype/runtime/missions.py" not in guide
    assert "packages/archetype-missions/src/archetype/missions/runtime.py" in guide
    assert "v0.5 Mission workflow" not in guide
    assert "normative for v0.5" not in guide


def test_missions_contract_pages_are_the_single_normative_owners() -> None:
    agent_guide = (ROOT / "docs/guide/agent-missions.md").read_text(encoding="utf-8")
    recovery = (ROOT / "docs/missions/recovery.md").read_text(encoding="utf-8")
    trajectories = (ROOT / "docs/guide/trajectories.md").read_text(encoding="utf-8")
    transcripts = (ROOT / "docs/missions/transcripts.md").read_text(encoding="utf-8")
    contracts = (ROOT / "quality/contracts.toml").read_text(encoding="utf-8")

    assert "| Crash window | Durable evidence after restart |" not in agent_guide
    assert "[Mission Activity recovery](../missions/recovery.md)" in agent_guide
    assert "| Crash window | Durable evidence after restart |" in recovery
    assert "`TranscriptIngestionService` preserves this exact order:" not in trajectories
    assert "[Transcript ingestion contract](../missions/transcripts.md)" in trajectories
    assert "`TranscriptIngestionService` preserves this exact order:" in transcripts
    assert 'source = "docs/missions/recovery.md"' in contracts
    assert contracts.count('source = "docs/missions/transcripts.md"') == 3


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


def test_first_publication_runbook_stages_pending_publishers_without_reruns() -> None:
    release_note = " ".join(
        (ROOT / "docs/guide/release-0.6.md").read_text(encoding="utf-8").split()
    )
    harness = " ".join(
        (ROOT / "docs/guide/repository-harness.md").read_text(encoding="utf-8").split()
    )

    assert "at most three pending projects" in release_note
    assert "two waves within the same release attempt" in release_note
    assert "including `archetype-smol`, were claimed by v0.6.1" in release_note
    assert "leave the remaining child waiting at" in harness
    assert "an intentional child failure or parent rerun is not the bootstrap mechanism" in harness


def test_contributing_names_each_trusted_publisher_workflow() -> None:
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")

    for workflow in (
        "release.yml",
        "publish-archetype-missions.yml",
        "publish-archetype-physical-ai.yml",
        "publish-archetype-research.yml",
    ):
        assert f"`{workflow}`" in contributing


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


def test_split_rest_references_are_navigable() -> None:
    navigation = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "- REST API: reference/rest-api.md" in navigation
    assert "- REST API: reference/rest-api-missions.md" in navigation


def test_split_research_and_evaluation_references_are_navigable() -> None:
    navigation = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "- Python API: reference/python/autoresearch.md" in navigation
    assert "- Framework Evaluation: reference/python/evaluation.md" in navigation
    assert "AutoResearch and Evaluation" not in navigation


def test_world_library_signature_contracts_are_in_the_reference_inventory() -> None:
    research = (ROOT / "docs/reference/python/autoresearch.md").read_text(encoding="utf-8")
    evaluation = (ROOT / "docs/reference/python/evaluation.md").read_text(encoding="utf-8")
    physical = (ROOT / "docs/reference/python/physical-ai.md").read_text(encoding="utf-8")
    optimization = (ROOT / "docs/reference/python/physical-ai-optimization.md").read_text(
        encoding="utf-8"
    )
    host = (ROOT / "docs/reference/python/physical-ai-host.md").read_text(encoding="utf-8")

    assert "::: archetype.research.Evaluator" in research
    assert "::: archetype.research.CandidatePreparer" in research
    assert "::: archetype.evaluation.models.FrameGrader" not in research
    assert "::: archetype.evaluation.models.FrameGrader" in evaluation
    assert "::: archetype.physical_ai.PhysicalAIExtensionConfig" in host
    assert "::: archetype.physical_ai.hosted_activity_contracts.HostedEpisodeProvider" in host
    assert "::: archetype.physical_ai.hosted_activity_contracts.HostedEpisodeReconciliation" in host
    assert "::: archetype.physical_ai.optimization.PerturbationStrategy" in optimization
    assert "::: archetype.physical_ai.interfaces.EnvClient" not in physical
    assert "::: archetype.physical_ai.interfaces.PolicyClient" not in physical


def test_embedded_physical_ai_host_example_closes_its_runtime() -> None:
    guide = (ROOT / "docs/guide/physical-ai.md").read_text(encoding="utf-8")

    section = guide.split("For an embedded host", maxsplit=1)[1].split(
        "## Committed-state sequence", maxsplit=1
    )[0]
    assert "async with ArchetypeRuntime(" in section
