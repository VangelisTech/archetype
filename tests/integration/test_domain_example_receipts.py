# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Semantic receipts for the credential-free domain examples."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

import pytest

from scripts.run_example_receipt import captured_receipt_or_run

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))


def _load_example(filename: str) -> ModuleType:
    module_name = f"domain_receipt_{Path(filename).stem}"
    spec = importlib.util.spec_from_file_location(module_name, _EXAMPLES / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


trajectory = _load_example("06_trajectory_analysis.py")
htn = _load_example("08_htn_resolution.py")
cloud_storage = _load_example("09_cloud_storage.py")
autoresearch = _load_example("10_autoresearch.py")
coding_agent_mission = _load_example("11_coding_agent_mission.py")
graph_relationships = _load_example("11_graph_relationships.py")
prefabs = _load_example("12_prefabs.py")
biome_rts = _load_example("13_biome_rts.py")


@pytest.mark.asyncio
async def test_trajectory_receipt_is_semantic_and_stable(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        trajectory.run_demo,
        str(tmp_path / "trajectory"),
    )

    assert result == {
        "episode_id": "episode-cache-1",
        "roles": ["user", "assistant"],
        "grade": {"samples": 1, "total_reward": -1.0},
    }


@pytest.mark.asyncio
async def test_htn_receipt_pins_fanout_and_persisted_solutions(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        htn.run_demo,
        str(tmp_path / "htn"),
    )

    assert result["solutions"] == [
        {
            "plan_id": "r.0.0.0.0.0",
            "operations": [
                "read_code",
                "write_failing_test",
                "edit_code",
                "run_tests",
                "open_pr",
            ],
            "depth": 5,
        },
        {
            "plan_id": "r.0.0.1.0.0",
            "operations": [
                "read_code",
                "edit_code_direct",
                "run_tests",
                "open_pr",
            ],
            "depth": 5,
        },
    ]
    assert result["persisted_solutions"] == [
        {
            "plan_id": "r.0.0.0.0.0",
            "plan_length": 5,
            "final_state": [
                "code_edited",
                "issue_open",
                "pr_open",
                "test_written",
                "tests_pass",
                "understood",
            ],
        },
        {
            "plan_id": "r.0.0.1.0.0",
            "plan_length": 4,
            "final_state": [
                "code_edited",
                "issue_open",
                "pr_open",
                "tests_pass",
                "understood",
            ],
        },
    ]
    assert [
        (
            item["tick"],
            item["live"],
            item["solved"],
            item["expansions"],
            item["frontier"],
        )
        for item in result["tick_trace"]
    ] == [
        (0, 1, 0, 0, {"": 1}),
        (1, 1, 0, 1, {"resolve_issue": 1}),
        (2, 1, 0, 0, {"": 1}),
        (3, 1, 0, 1, {"understand": 1}),
        (4, 1, 0, 0, {"": 1}),
        (5, 1, 0, 0, {"read_code": 1}),
        (6, 1, 0, 1, {"fix": 1}),
        (7, 2, 0, 0, {"": 2}),
        (8, 2, 0, 0, {"edit_code_direct": 1, "write_failing_test": 1}),
        (9, 2, 0, 1, {"edit_code": 1, "verify": 1}),
        (10, 2, 0, 1, {"": 1, "verify": 1}),
        (11, 2, 0, 0, {"": 1, "run_tests": 1}),
        (12, 2, 0, 1, {"run_tests": 1, "ship": 1}),
        (13, 2, 0, 1, {"": 1, "ship": 1}),
        (14, 1, 1, 0, {"": 1}),
        (15, 0, 2, 0, {}),
    ]


@pytest.mark.asyncio
async def test_local_cloud_storage_receipt_pins_runtime_roundtrip(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        cloud_storage.run_demo,
        str(tmp_path / "cloud"),
    )

    assert result == {
        "backend": "lancedb",
        "provider": "local",
        "note": "same storage API",
        "spawned_entities": 1,
        "ticks_completed": 1,
    }


@pytest.mark.asyncio
async def test_autoresearch_receipt_pins_improvement_and_ledger_order(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        autoresearch.run_demo,
        str(tmp_path / "research"),
    )

    assert result == {
        "iterations": [
            {
                "iteration": 0,
                "score": -9.0,
                "incumbent_score": -9.0,
                "improved": True,
            },
            {
                "iteration": 1,
                "score": -4.0,
                "incumbent_score": -4.0,
                "improved": True,
            },
            {
                "iteration": 2,
                "score": -1.0,
                "incumbent_score": -1.0,
                "improved": True,
            },
            {
                "iteration": 3,
                "score": 0.0,
                "incumbent_score": 0.0,
                "improved": True,
            },
        ],
        "final_score": 0.0,
        "improved": True,
        "ledger": [
            {"run_id": "knob-tuning-demo:iter0", "status": "stopped"},
            {"run_id": "knob-tuning-demo:iter1", "status": "stopped"},
            {"run_id": "knob-tuning-demo:iter2", "status": "stopped"},
            {"run_id": "knob-tuning-demo:iter3", "status": "stopped"},
        ],
        "ledger_tick": 8,
    }


@pytest.mark.asyncio
async def test_coding_agent_dry_run_receipt_is_typed_and_starts_no_work(
    tmp_path: Path,
) -> None:
    result = await captured_receipt_or_run(
        coding_agent_mission.run_demo,
        str(tmp_path / "mission-authoring"),
    )

    assert result == {
        "mode": "dry_run",
        "repository": "VangelisTech/archetype",
        "backend": "modal",
        "backend_type": "ModalSandboxBackend",
        "environment_is_pinned": True,
        "tasks": [
            {
                "name": "regression",
                "depends_on": [],
                "validators": [
                    {"name": "regression_is_red", "expected_returncode": 1},
                    {"name": "regression_file_only", "expected_returncode": 0},
                ],
            },
            {
                "name": "implementation",
                "depends_on": ["regression"],
                "validators": [
                    {"name": "focused_contract", "expected_returncode": 0},
                    {"name": "architecture", "expected_returncode": 0},
                    {"name": "lazy_audit", "expected_returncode": 0},
                    {"name": "ruff", "expected_returncode": 0},
                    {"name": "diff_check", "expected_returncode": 0},
                ],
            },
        ],
        "task_paths": {
            "implementation": {
                "path": "src/archetype/world/query.py",
            },
            "regression": {
                "path": "tests/world/test_query_schema_evolution.py",
            },
        },
        "external_work_started": False,
    }


@pytest.mark.asyncio
async def test_graph_receipt_pins_temporal_edges_and_cascade(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        graph_relationships.run_demo,
        str(tmp_path / "graph"),
    )

    assert result == {
        "subtree_hops": [0, 1, 2],
        "edge_count_before_build": 0,
        "edge_count_after_build": 2,
        "cascade_deleted_counts": [1, 0],
        "edge_count_after_cascade": 1,
    }


@pytest.mark.asyncio
async def test_prefab_receipt_pins_lineage_and_copy_semantics(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        prefabs.run_demo,
        str(tmp_path / "prefabs"),
    )

    assert result == {
        "first_generation_lineage_count": 2,
        "total_lineage_count": 4,
        "first_instance_armor": 42,
        "new_instance_armor": 99,
        "copy_on_instantiate": True,
    }


@pytest.mark.asyncio
async def test_biome_receipt_pins_composed_scene_outcome(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        biome_rts.run_demo,
        str(tmp_path / "biome"),
    )

    assert result == {
        "asset_descendant_count": 5,
        "live_units": [
            {"role": "harvester", "x": 2.0, "health": 80},
            {"role": "harvester", "x": 6.0, "health": 80},
            {"role": "turret", "x": 4.0, "health": 150},
        ],
        "minimap_population": [
            {"tick": 1, "population": 2},
            {"tick": 2, "population": 2},
            {"tick": 3, "population": 3},
        ],
        "visible_roles": ["harvester", "turret"],
        "possessed_relations": [
            {"relation": "assignedto", "direction": "out"},
            {"relation": "childof", "direction": "in"},
            {"relation": "childof", "direction": "out"},
            {"relation": "supplyline", "direction": "out"},
            {"relation": "targets", "direction": "in"},
            {"relation": "visibleto", "direction": "out"},
        ],
        "command_order": [
            {"name": "first-army", "depth": 0},
            {"name": "alpha", "depth": 1},
            {"name": "ada", "depth": 2},
            {"name": "harvester-1", "depth": 2},
            {"name": "turret-1", "depth": 2},
        ],
        "edge_counts": {
            "asset_child_of": 5,
            "child_of": 8,
            "assigned_to": 2,
            "commanded_by": 1,
            "supply_line": 1,
            "targets": 1,
            "visible_to": 1,
            "is_a": 5,
        },
        "upgrade": {
            "old_rate": 3,
            "new_rate": 5,
            "lineage_recorded": True,
        },
        "cascade_deleted_count": 4,
    }
