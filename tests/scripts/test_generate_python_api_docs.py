# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated Python reference."""

from __future__ import annotations

import archetype
from scripts.generate_python_api_docs import (
    PAGES_DIR,
    WORLD_LIBRARY_FACADE_TIERS,
    _validate_coverage,
    _validate_world_library_facades,
    main,
)

WORLD_LIBRARY_EXPORTS = {
    "AgentMissionConfig",
    "AgentTask",
    "AutoResearchConfig",
    "AutoResearchResult",
    "CandidatePreparer",
    "CommandValidator",
    "CriticPolicy",
    "EvaluationResult",
    "Evaluator",
    "HostedEpisodeObservation",
    "HostedEpisodeRequest",
    "MissionResult",
    "MissionWorld",
    "Missions",
    "ModalHostedEpisodeConfig",
    "PhysicalAI",
    "PhysicalAIExtensionConfig",
    "Research",
    "RepositoryPublicationPolicy",
    "ResearchCandidateContext",
    "SubmittedMission",
    "MissionRun",
    "TaskResult",
}


def test_committed_python_reference_is_current() -> None:
    assert main(["--check"]) == 0


def test_artifact_context_reference_names_selected_evidence() -> None:
    reference = (PAGES_DIR / "artifacts.md").read_text(encoding="utf-8")

    assert (
        "| `artifact_ids` | `tuple[str, ...]` | `required` | "
        "UUIDv7 occurrence identities selected as evidence |"
    ) in reference


def test_world_library_exports_are_documented_without_entering_framework_all() -> None:
    locations = _validate_coverage()

    assert WORLD_LIBRARY_EXPORTS.isdisjoint(archetype.__all__)
    assert WORLD_LIBRARY_EXPORTS <= set(locations)
    assert locations["AutoResearchConfig"] == (
        "archetype.research",
        "AutoResearchConfig",
    )
    assert locations["HostedEpisodeRequest"] == (
        "archetype.physical_ai",
        "HostedEpisodeRequest",
    )
    assert locations["IterationResult"] == ("archetype.research", "IterationResult")

    missions = (PAGES_DIR / "missions.md").read_text(encoding="utf-8")
    for name in ("CriticPolicy", "RepositoryPublicationPolicy", "TaskResult"):
        assert f"::: archetype.missions.{name}" in missions

    reference = (PAGES_DIR.parent / "python-api.md").read_text(encoding="utf-8")
    assert "`CandidateContext`" not in reference
    assert "Compatibility-tier root attributes" not in reference
    assert "| Surface | Distribution | Use it for |" in reference


def test_blocking_facade_is_runtime_api_not_a_second_engine() -> None:
    runtime = (PAGES_DIR / "runtime.md").read_text(encoding="utf-8")
    reference = (PAGES_DIR.parent / "python-api.md").read_text(encoding="utf-8")

    assert "::: archetype.runtime.SyncArchetypeRuntime" in runtime
    assert "::: archetype.runtime.SyncRuntimeWorld" in runtime
    assert "::: archetype.runtime.run_sync" in runtime
    assert "Compatibility API" not in reference
    assert "Compatibility aliases" not in reference
    assert not (PAGES_DIR / "compatibility.md").exists()


def test_research_and_framework_evaluation_have_distinct_owners() -> None:
    research = (PAGES_DIR / "autoresearch.md").read_text(encoding="utf-8")
    evaluation = (PAGES_DIR / "evaluation.md").read_text(encoding="utf-8")

    assert "| Distribution | `archetype-research` |" in research
    assert "::: archetype.research.Research" in research
    assert "::: archetype.evaluation.models.FrameGrader" not in research
    assert "| Distribution | `archetype-ecs` |" in evaluation
    assert "::: archetype.evaluation.models.FrameGrader" in evaluation
    assert "::: archetype.research.Research" not in evaluation


def test_physical_ai_provider_factory_signature_is_documented() -> None:
    reference = (PAGES_DIR / "physical-ai-host.md").read_text(encoding="utf-8")

    for name in (
        "HostedEpisodeProvider",
        "HostedEpisodeProviderResult",
        "HostedEpisodeRetryGuard",
        "HostedEpisodeReconciliation",
        "HostedEpisodeRecovered",
        "HostedEpisodeConfirmedAbsent",
        "HostedEpisodeRecoveryUnknown",
    ):
        assert f"::: archetype.physical_ai.hosted_activity_contracts.{name}" in reference


def test_world_library_facade_exports_have_exact_stability_tiers() -> None:
    import importlib

    _validate_world_library_facades()

    for module_name, tiers in WORLD_LIBRARY_FACADE_TIERS.items():
        module = importlib.import_module(module_name)
        classified = [name for names in tiers.values() for name in names]
        assert len(classified) == len(set(classified))
        assert set(classified) == set(module.__all__)

    reference = (PAGES_DIR.parent / "python-api.md").read_text(encoding="utf-8")
    assert "## World-library facade classifications" in reference
    assert "### `archetype.missions`" in reference
    assert "**Recommended:** `Missions`, `MissionWorld`" in reference
    assert "**Extension:** `MISSION_COMPONENTS`" in reference
