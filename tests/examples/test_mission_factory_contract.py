# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the example-local Agent Missions factory prefab library."""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path

import pytest
from daft import col

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from mission_factory import (  # noqa: E402
    AssetChildOf,
    BugFixLineInputs,
    ConnectionRule,
    author_mission_factory_library,
    compile_bugfix_line,
    export_visual_briefs,
    register_mission_factory,
)

from archetype import ArchetypeRuntime, StorageConfig  # noqa: E402
from archetype.graph import IsA, edges, instantiate  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "factory"), namespace="mission_factory_tests")


def test_bugfix_line_compiles_copied_rule_entities_into_supported_authoring(tmp_path: Path) -> None:
    async def go():
        registration = register_mission_factory()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "mission-factory-contract",
                storage=_storage(tmp_path),
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
                    branch="agent/fix-603",
                    issue="https://github.com/VangelisTech/archetype/issues/603",
                    test_path="tests/examples/test_mission_factory_contract.py",
                ),
                at=latest,
            )
            lineage = (await edges(world, IsA, at=latest)).to_pylist()
            catalog = (await edges(world, AssetChildOf, at=latest)).to_pylist()
            copied_ids = [row["isa__source"] for row in lineage]
            rules = (
                (await world.query(ConnectionRule))
                .where(col("tick") == latest)
                .where(col("entity_id").is_in(copied_ids))
                .to_pylist()
            )
            return submission, lineage, catalog, rules

    submission, lineage, catalog, rules = _run(go())

    assert submission.name == "bugfix-line"
    assert submission.repository == "VangelisTech/archetype"
    assert [task.name for task in submission.tasks] == ["reproduction", "implementation"]
    reproduction, implementation = submission.tasks
    assert reproduction.depends_on == ()
    assert implementation.depends_on == ("reproduction",)
    assert [validator.name for validator in reproduction.validators] == [
        "regression_is_red",
        "regression_diff_check",
    ]
    assert reproduction.validators[0].expected_returncode == 1
    assert reproduction.validators[0].command[-1] == (
        "tests/examples/test_mission_factory_contract.py"
    )
    assert [validator.name for validator in implementation.validators] == [
        "focused_contract",
        "architecture",
        "implementation_diff_check",
    ]
    assert implementation.max_dispatches == 3
    assert implementation.critic_policy.max_reviews == 2

    # Root + six line slots + five validators + six explicit rule entities.
    assert len(lineage) == 18
    # AssetChildOf organizes four collections, nine visuals, and one line. It
    # is not copied into the instance; generic instantiation copied ChildOf.
    assert len(catalog) == 14
    assert len(rules) == 6
    assert {row["connectionrule__relation"] for row in rules} == {"DependsOn", "Guards"}


def test_visual_briefs_are_complete_ai_generation_contracts(tmp_path: Path) -> None:
    async def go():
        registration = register_mission_factory()
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "mission-factory-visuals",
                storage=_storage(tmp_path),
                **registration.world_options(),
            )
            await author_mission_factory_library(world)
            await world.step()
            return await export_visual_briefs(world, at=0)

    briefs = _run(go())

    assert [brief["key"] for brief in briefs] == [
        "agent_unit",
        "agent_workcell",
        "artifact_depot",
        "critic_gate",
        "dependency_conduit",
        "evidence_capsule",
        "mission_core",
        "publication_uplink",
        "validator_gate",
    ]
    for brief in briefs:
        model = brief["model"]
        assert model["format"] == "glb"
        assert model["status"] == "brief"
        assert model["coordinate_system"] == "y_up"
        assert model["origin"] == "ground_center"
        assert all(value > 0 for value in model["footprint"])
        assert all(value > 0 for value in model["dimensions_m"])
        assert model["max_triangles"] > 0
        assert brief["prompt"]
        assert "No letters" in brief["negative_prompt"]
        socket_names = [socket["name"] for socket in brief["sockets"]]
        assert socket_names
        assert len(socket_names) == len(set(socket_names))
        assert brief["behaviors"]
        assert brief["states"]
        for behavior in brief["behaviors"]:
            module_name, separator, symbol = behavior["authority"].rpartition(".")
            assert separator
            authority = getattr(importlib.import_module(module_name), symbol)
            assert authority.__module__ == module_name

    workcell = next(brief for brief in briefs if brief["key"] == "agent_workcell")
    assert {state["signal"] for state in workcell["states"]} >= {
        "task.pending",
        "task.ready",
        "execution.running",
        "task.candidate",
        "critic.running",
        "task.accepted",
        "task.failed",
    }
    assert {interaction["action"] for interaction in workcell["interactions"]} == {
        "task.inspect",
        "terminal.spectate",
        "terminal.takeover",
    }
    takeover = next(
        interaction
        for interaction in workcell["interactions"]
        if interaction["action"] == "terminal.takeover"
    )
    assert takeover == {
        "name": "takeover",
        "permission": "operator",
        "action": "terminal.takeover",
        "confirmation_required": True,
    }


def test_bugfix_inputs_reject_paths_outside_the_repository() -> None:
    with pytest.raises(ValueError, match="repository-relative"):
        BugFixLineInputs(
            repository="VangelisTech/archetype",
            branch="agent/fix",
            issue="issue-603",
            test_path="../outside.py",
        )
