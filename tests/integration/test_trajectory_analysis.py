# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contract for the normalized trajectory example."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from archetype.missions.trajectories import Trajectory

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "06_trajectory_analysis.py"
_SPEC = importlib.util.spec_from_file_location("trajectory_example", _EXAMPLE)
assert _SPEC is not None and _SPEC.loader is not None
trajectory_example = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = trajectory_example
_SPEC.loader.exec_module(trajectory_example)


def test_example_authors_normalized_trajectory_headers() -> None:
    authored = trajectory_example.make_trajectories()

    assert len(authored) == 2
    assert all(isinstance(item.header, Trajectory) for item in authored)
    assert [item.header.outcome for item in authored] == ["accepted", "rejected"]
    assert [item.header.total_turns for item in authored] == [2, 2]
    assert all(not hasattr(item.header, "turns_json") for item in authored)


@pytest.mark.asyncio
async def test_example_runs_through_runtime_trajectory_service(tmp_path) -> None:
    result = await trajectory_example.run_demo(str(tmp_path / "store"))

    assert result == {
        "trajectory_id": "mission-42:cache:attempt-1",
        "outcome": "rejected",
        "grade": {"samples": 1, "total_reward": -1.0},
    }
