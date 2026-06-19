# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: E402

"""Toy EvalService trajectory-scoring example.

Persist trajectory reward rows, query them back as a Daft DataFrame, then run
existing eval graders over that DataFrame.

Usage:
    uv run python examples/09_eval_service_toy.py
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import asdict
from pathlib import Path

from daft import DataFrame

_ROOT = str(Path(__file__).parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.trajectories import Trajectory, TrajectoryReward
from evals.graders import exact_match, threshold
from evals.types import GraderResult


def reward_graders(df: DataFrame) -> list[GraderResult]:
    rows = df.collect().to_pylist()
    total_reward = sum(row["trajectoryreward__reward"] for row in rows)
    return [
        exact_match(len(rows), 2, name="reward_rows"),
        threshold(total_reward, min_val=1.0, name="total_reward"),
    ]


async def main() -> None:
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri="./archetype_data", namespace="eval_toy")
        world = await container.world_service.create_world(WorldConfig(name="eval-toy"), storage)
        await world.create_entity(
            [
                Trajectory(
                    trajectory_id="toy-traj",
                    run_id="toy-run",
                    episode_id="episode-1",
                    task_id="reach",
                    trial_idx=0,
                    terminal=True,
                    outcome="success",
                )
            ]
        )
        await world.create_entity([TrajectoryReward(trajectory_id="toy-traj", seq=0, reward=0.25)])
        await world.create_entity([TrajectoryReward(trajectory_id="toy-traj", seq=1, reward=1.0)])

        run = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))
        rewards = await container.eval_service.query_trajectory_component(
            TrajectoryReward,
            world_id=world.world_id,
            run_id=run.run_id,
            trajectory_ids=["toy-traj"],
            storage_config=storage,
        )
        grader_results = await container.eval_service.run_graders(rewards, [reward_graders])

        print(rewards.collect().to_pylist())
        print([asdict(result) for result in grader_results])
    finally:
        await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
