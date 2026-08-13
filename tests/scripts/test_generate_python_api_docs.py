# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated Python reference."""

from __future__ import annotations

import archetype
from scripts.generate_python_api_docs import (
    PAGES_DIR,
    _validate_coverage,
    main,
)

WORLD_LIBRARY_EXPORTS = {
    "AgentMissionConfig",
    "AgentTask",
    "AutoResearchConfig",
    "AutoResearchResult",
    "CommandValidator",
    "CriticPolicy",
    "EvaluationResult",
    "HostedEpisodeObservation",
    "HostedEpisodeRequest",
    "MissionResult",
    "MissionWorld",
    "Missions",
    "ModalHostedEpisodeConfig",
    "PhysicalAI",
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
    assert "CandidateContext" not in reference
    assert "Compatibility-tier root attributes" not in reference
