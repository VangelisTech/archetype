# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Guard distribution ownership and trusted world-library composition guidance."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUIDE_ROOT = ROOT / "docs" / "guide"


def test_current_authority_layout_keeps_world_libraries_out_of_framework_tree() -> None:
    guide = (GUIDE_ROOT / "application-architecture.md").read_text(encoding="utf-8")
    remainder = guide.split("The current distribution and authority layout is:", 1)[1]
    layout = remainder.split("```text", 1)[1].split("```", 1)[0]
    framework_tree = layout.split(
        "packages/archetype-missions/src/archetype/missions/",
        1,
    )[0]

    for stale_directory in ("  missions/", "  physical_ai/", "  research/"):
        assert stale_directory not in framework_tree

    for distribution_root in (
        "packages/archetype-missions/src/archetype/missions/",
        "packages/archetype-physical-ai/src/archetype/physical_ai/",
        "packages/archetype-research/src/archetype/research/",
    ):
        assert distribution_root in layout


def test_guides_assign_library_construction_to_private_extension_adapters() -> None:
    architecture = (GUIDE_ROOT / "application-architecture.md").read_text(encoding="utf-8")
    services = (GUIDE_ROOT / "services.md").read_text(encoding="utf-8")
    protocols = (GUIDE_ROOT / "service-protocols.md").read_text(encoding="utf-8")
    research = (GUIDE_ROOT / "autoresearch.md").read_text(encoding="utf-8")
    composition_guidance = "\n".join((architecture, services, protocols, research))

    for adapter in (
        "archetype.missions._extension",
        "archetype.physical_ai._extension",
        "archetype.research._extension",
    ):
        assert adapter in composition_guidance

    for stale_claim in (
        "Wiring constructs one process-shared `AutoResearchAdmissions`",
        "Wiring injects exact world cleanup and one process-shared keyed-admission",
        "Wiring closes its two free handlers",
        "wiring registers exact submit/run/restore handlers",
    ):
        assert stale_claim not in composition_guidance
