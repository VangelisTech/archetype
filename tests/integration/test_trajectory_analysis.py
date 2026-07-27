# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contract for the normalized episode-evidence example."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import daft
import pytest
from daft import col

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.missions.trajectories import TrajectoryTurn, turns_to_components
from archetype.physical_ai.hosted_episode import hosted_episode_id

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "06_trajectory_analysis.py"
_SPEC = importlib.util.spec_from_file_location("trajectory_example", _EXAMPLE)
assert _SPEC is not None and _SPEC.loader is not None
trajectory_example = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = trajectory_example
_SPEC.loader.exec_module(trajectory_example)


def test_example_authors_normalized_episode_evidence() -> None:
    authored = trajectory_example.make_episodes()

    assert len(authored) == 2
    assert [item.episode_id for item in authored] == ["episode-auth-1", "episode-cache-1"]
    rows = turns_to_components(authored[0].episode_id, list(authored[0].turns))
    assert all(isinstance(row, TrajectoryTurn) for row in rows)
    assert {row.episode_id for row in rows} == {"episode-auth-1"}


@pytest.mark.asyncio
async def test_example_runs_through_runtime_trajectory_service(tmp_path) -> None:
    result = await trajectory_example.run_demo(str(tmp_path / "store"))

    assert result == {
        "episode_id": "episode-cache-1",
        "roles": ["user", "assistant"],
        "grade": {"samples": 1, "total_reward": -1.0},
    }


@pytest.mark.asyncio
async def test_fresh_world_mission_and_hosted_evidence_join_on_episode_id(tmp_path) -> None:
    """Mission evidence and hosted Physical-AI evidence share one persistent key."""
    episode_id = hosted_episode_id("op-join-1", 0)
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="episode_join")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("episode-join", storage=storage)
        await world.spawn(TrajectoryTurn(episode_id=episode_id, seq=0, role="user", content="pick"))
        await world.run(steps=1)
        mission_frame = await world.query_trajectory(TrajectoryTurn)

        hosted_frame = daft.from_pylist(
            [
                {
                    "episode_id": episode_id,
                    "operation_id": "op-join-1",
                    "trial_id": 0,
                    "terminal_reason": "success",
                }
            ]
        )
        joined = mission_frame.join(
            hosted_frame,
            left_on=col("trajectoryturn__episode_id"),
            right_on=col("episode_id"),
        )
        rows = joined.collect().to_pylist()

    assert len(rows) == 1
    assert rows[0]["trajectoryturn__episode_id"] == episode_id
    assert rows[0]["terminal_reason"] == "success"
