# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for claims made by the hand-authored guides."""

from __future__ import annotations

import ast
import re
from pathlib import Path

_GUIDE_ROOT = Path("docs/guide")
_COMMAND_TOTAL_PATTERNS = (
    re.compile(r"\b(\d+)\s+command\s+types?\b", re.IGNORECASE),
    re.compile(r"\bcommand\s+types?\*{0,2}\s*\((\d+)\s+total\b", re.IGNORECASE),
)
_DESIGN_ONLY_MESSAGING_TYPES = (
    "MessageDeliveryProcessor",
    "DeliveryReceipt",
    "ChatGraphRegistry",
)
_BEGINNER_GUIDES = (
    _GUIDE_ROOT / "quickstart.md",
    _GUIDE_ROOT / "building-simulations.md",
    _GUIDE_ROOT / "processors.md",
)
_BEGINNER_EXAMPLES = (
    Path("examples/00_quickstart.py"),
    Path("examples/simulation_script.py"),
)


def test_guides_do_not_freeze_numeric_command_type_claims() -> None:
    """Exact family operations make a global command-type total meaningless."""
    stale: list[str] = []

    for guide in sorted(_GUIDE_ROOT.glob("*.md")):
        text = guide.read_text()
        for pattern in _COMMAND_TOTAL_PATTERNS:
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                stale.append(f"{guide}:{line} freezes a global command-type total")

    assert not stale, "global command-type totals are no longer a contract:\n" + "\n".join(stale)


def test_trusted_runtime_example_keeps_rbac_at_the_adapter_boundary() -> None:
    """Trusted scripting must not imply that it constructs an authorization actor."""
    guide = (_GUIDE_ROOT / "examples.md").read_text()
    world_mutations = guide.split("## 2. Fork for Counterfactuals", maxsplit=1)[0]

    assert "actor-free `ArchetypeRuntime`" in world_mutations
    assert "trusted dispatcher\nentry" in world_mutations
    assert "does not construct an `ActorCtx`" in world_mutations
    assert "untrusted-adapter permission matrix" in world_mutations
    assert "CommandGateway" not in world_mutations
    assert "Runtime operations in this example" in world_mutations
    assert "Gated operations in this example" not in world_mutations


def test_current_robotics_guides_point_to_extracted_libero_harness() -> None:
    """Current guidance must point into robot-evals, not the removed bench tree."""
    guides = (Path("LEARNINGS.md"), _GUIDE_ROOT / "autoresearch.md")

    text_by_path = {path: path.read_text() for path in guides}
    stale = [str(path) for path, text in text_by_path.items() if "bench/libero/" in text]
    missing_successor = [
        str(path) for path, text in text_by_path.items() if "robot-evals" not in text
    ]

    assert not stale, f"current guides reference the extracted bench/libero tree: {stale}"
    assert not missing_successor, f"current guides omit robot-evals: {missing_successor}"


def test_docs_do_not_claim_design_only_messaging_types() -> None:
    """Unimplemented design sketches must not read as framework contracts."""
    paths = [Path("LEARNINGS.md"), *sorted(_GUIDE_ROOT.glob("*.md"))]
    stale: list[str] = []

    for path in paths:
        text = path.read_text()
        for type_name in _DESIGN_ONLY_MESSAGING_TYPES:
            if type_name in text:
                stale.append(f"{path}: presents design-only {type_name}")

    assert not stale, "stale messaging infrastructure claims:\n" + "\n".join(stale)


def test_eval_guide_uses_live_inventory_instead_of_embedded_taxonomy() -> None:
    """Task registration stays executable instead of becoming a prose taxonomy."""
    guide = (_GUIDE_ROOT / "evals.md").read_text()

    assert "python -m evals.run --list" in guide
    assert "eval-task-manifest" not in guide
    assert "Registered task manifest" not in guide


def test_beginner_surfaces_do_not_route_through_engine_internals() -> None:
    """Beginner paths compose the public runtime instead of wiring core objects."""
    forbidden_guide_terms = ("archetype.core", "AsyncWorld", "AsyncSystem", "world.system")
    stale = [
        f"{path}: contains {term}"
        for path in _BEGINNER_GUIDES
        for term in forbidden_guide_terms
        if term in path.read_text()
    ]

    for path in _BEGINNER_EXAMPLES:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "archetype.core"
            ):
                stale.append(f"{path}:{node.lineno} imports {node.module}")

    assert not stale, "beginner surfaces expose engine internals:\n" + "\n".join(stale)


def test_quickstart_example_stays_under_thirty_source_lines() -> None:
    """The copy-and-run path remains small enough to understand in one screen."""
    source_lines = [
        line
        for line in _BEGINNER_EXAMPLES[0].read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert len(source_lines) < 30, f"quickstart grew to {len(source_lines)} source lines"


def test_quickstart_variants_stage_initial_state_before_running() -> None:
    """Async and sync snippets must spend the same number of processor ticks."""
    guide = (_GUIDE_ROOT / "quickstart.md").read_text()
    async_section, sync_section = guide.split("## Synchronous scripts", maxsplit=1)

    for name, section in (("async", async_section), ("sync", sync_section)):
        spawn = section.rfind("world.spawn(")
        step = section.find("world.step()", spawn)
        run = section.find("world.run(steps=3)", spawn)
        assert -1 not in (spawn, step, run), f"{name} quickstart lifecycle is incomplete"
        assert spawn < step < run, f"{name} quickstart must persist spawns before its run"


def test_world_library_installation_and_native_boundary_are_explicit() -> None:
    """The split must be understandable without reading workspace metadata."""
    readme = Path("README.md").read_text()
    guide = (_GUIDE_ROOT / "world-libraries.md").read_text()

    for package_specifier in (
        "uv add archetype-ecs",
        "uv add archetype-missions",
        "uv add archetype-physical-ai",
        "uv add archetype-research",
        'uv add "archetype-ecs[all]"',
        'uv add "archetype-ecs[missions,research]"',
    ):
        assert package_specifier in readme
        assert package_specifier in guide

    assert "archetype.world_libraries" in guide
    assert "trusted Python manifest" in guide
    assert "`archetype-ffi` v1" in guide
    assert "Arrow `RecordBatch`" in guide
    assert "not the native processor ABI" in guide
    assert re.search(r"not\s+the discovery or lifecycle protocol", guide)


def test_world_library_examples_use_typed_adapters() -> None:
    """Current examples teach library-owned adapters, not migration aliases."""
    paths = (
        _GUIDE_ROOT / "autoresearch.md",
        _GUIDE_ROOT / "agent-missions.md",
        _GUIDE_ROOT / "runtime.md",
        Path("examples/10_autoresearch.py"),
        Path("examples/11_coding_agent_mission.py"),
    )
    text = "\n".join(path.read_text() for path in paths)

    assert "Research(base).autoresearch(" in text
    assert "Missions(\n" in text
    assert "runtime.missions(" not in text
    assert "base.autoresearch(" not in text
    assert "Research(world).run(" not in text
    assert "--extra coding-agent" not in text


def test_mkdocs_resolves_all_distribution_source_roots() -> None:
    """Generated API references must merge the split import-package trees."""
    config = Path("mkdocs.yml").read_text()
    extension = Path("scripts/griffe_world_libraries.py").read_text()

    assert "packages/archetype-ecs/src" in config
    assert "scripts/griffe_world_libraries.py:WorldLibraryPackages" in config
    for library_source_root in (
        "packages/archetype-missions/src",
        "packages/archetype-physical-ai/src",
        "packages/archetype-research/src",
    ):
        assert library_source_root in extension

    assert "- Install World Libraries: guide/world-libraries.md" in config
