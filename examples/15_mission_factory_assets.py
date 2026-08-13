# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Author and instantiate an Agent Missions factory asset library.

The example stores nine AI-ready visual briefs plus one semantic ``BugFixLine``
as ECS prefab graphs.  It copies the line with generic ``ChildOf`` semantics,
then an example-local trusted driver interprets durable ``DependsOn`` and
``Guards`` rule entities into a supported ``MissionSubmission``.  No agent or
external provider is started.

Usage:
    uv run python examples/15_mission_factory_assets.py
    uv run python examples/15_mission_factory_assets.py --briefs-json
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from mission_factory import (
    BugFixLineInputs,
    author_mission_factory_library,
    compile_bugfix_line,
    export_visual_briefs,
    register_mission_factory,
)

from archetype import ArchetypeRuntime, StorageConfig
from archetype.graph import IsA, edges, instantiate


async def _build(storage_uri: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    registration = register_mission_factory()
    storage = StorageConfig(uri=storage_uri, namespace="mission_factory_assets")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "mission-factory-assets",
            storage=storage,
            **registration.world_options(),
        )
        library = await author_mission_factory_library(world)
        await world.step()

        blueprint = await instantiate(world, registration.view, library.bugfix_line)
        await world.step()
        latest = (await world.info()).tick - 1

        submission = await compile_bugfix_line(
            world,
            blueprint,
            BugFixLineInputs(
                repository="VangelisTech/archetype",
                branch="agent/example-bugfix",
                issue="https://github.com/VangelisTech/archetype/issues/603",
                test_path="tests/examples/test_mission_factory_contract.py",
            ),
            at=latest,
        )
        briefs = await export_visual_briefs(world, at=0)
        lineage = (await edges(world, IsA, at=latest)).to_pylist()

    receipt: dict[str, Any] = {
        "library": "mission_factory",
        "visual_assets": [brief["key"] for brief in briefs],
        "line": "bugfix_line",
        "copied_entities": len(lineage),
        "tasks": [
            {
                "name": task.name,
                "depends_on": list(task.depends_on),
                "validators": [validator.name for validator in task.validators],
                "max_dispatches": task.max_dispatches,
            }
            for task in submission.tasks
        ],
        "relation_rules": ["DependsOn", "Guards"],
        "model_contract": {
            "format": sorted({brief["model"]["format"] for brief in briefs}),
            "status": sorted({brief["model"]["status"] for brief in briefs}),
            "coordinate_system": sorted({brief["model"]["coordinate_system"] for brief in briefs}),
            "origin": sorted({brief["model"]["origin"] for brief in briefs}),
        },
        "protected_interactions": sorted(
            {
                interaction["action"]
                for brief in briefs
                for interaction in brief["interactions"]
                if interaction["confirmation_required"]
            }
        ),
    }
    return receipt, briefs


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Return stable semantic evidence for the credential-free example."""

    receipt, _briefs = await _build(storage_uri)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-uri", type=Path, default=Path("./archetype_data"))
    parser.add_argument(
        "--briefs-json",
        action="store_true",
        help="print the committed AI-ready geometry, socket, state, and behavior contracts",
    )
    return parser


async def main() -> None:
    args = _parser().parse_args()
    receipt, briefs = await _build(str(args.storage_uri))
    if args.briefs_json:
        print(json.dumps(briefs, indent=2, sort_keys=True))
        return

    print(f"1. visual asset briefs: {len(receipt['visual_assets'])}")
    print(f"2. copied BugFixLine entities: {receipt['copied_entities']}")
    print(
        "3. mission tasks:",
        [(task["name"], task["depends_on"], task["validators"]) for task in receipt["tasks"]],
    )
    print("4. explicit relation rules:", receipt["relation_rules"])
    print("5. model contract:", receipt["model_contract"])
    print("6. confirmed operator actions:", receipt["protected_interactions"])


if __name__ == "__main__":
    asyncio.run(main())
