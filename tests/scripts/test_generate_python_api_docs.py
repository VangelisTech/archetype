# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated Python reference."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import archetype
from scripts.generate_python_api_docs import (
    PAGES_DIR,
    _manifest_root_exports,
    _validate_coverage,
    main,
)

DYNAMIC_ROOT_EXPORTS = {
    "AutoResearchConfig",
    "AutoResearchResult",
    "CandidateContext",
    "EvaluationResult",
    "HostedEpisodeObservation",
    "HostedEpisodeRequest",
    "ModalHostedEpisodeConfig",
    "ResearchCandidateContext",
}


def test_committed_python_reference_is_current() -> None:
    assert main(["--check"]) == 0


def test_artifact_context_reference_names_selected_evidence() -> None:
    reference = (PAGES_DIR / "artifacts.md").read_text(encoding="utf-8")

    assert (
        "| `artifact_ids` | `tuple[str, ...]` | `required` | "
        "UUIDv7 occurrence identities selected as evidence |"
    ) in reference


def test_installed_manifest_exports_are_documented_without_entering_framework_all() -> None:
    locations = _validate_coverage()

    assert DYNAMIC_ROOT_EXPORTS.isdisjoint(archetype.__all__)
    assert DYNAMIC_ROOT_EXPORTS <= set(locations)
    assert locations["AutoResearchConfig"] == (
        "archetype.research.models",
        "AutoResearchConfig",
    )
    assert locations["HostedEpisodeRequest"] == (
        "archetype.physical_ai.models",
        "HostedEpisodeRequest",
    )


def test_manifest_root_export_merge_preserves_framework_and_library_collisions() -> None:
    framework = {"Component": ("archetype.core", "Component")}
    with pytest.raises(RuntimeError, match="collides with the framework"):
        _manifest_root_exports(
            [
                SimpleNamespace(
                    name="rogue",
                    root_exports={"Component": ("rogue", "Component")},
                )
            ],
            framework_locations=framework,
        )

    with pytest.raises(RuntimeError, match="duplicate world-library root export"):
        _manifest_root_exports(
            [
                SimpleNamespace(
                    name="alpha",
                    root_exports={"Shared": ("alpha", "Shared")},
                ),
                SimpleNamespace(
                    name="beta",
                    root_exports={"Shared": ("beta", "Shared")},
                ),
            ],
            framework_locations=framework,
        )
