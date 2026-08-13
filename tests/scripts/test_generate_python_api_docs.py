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
