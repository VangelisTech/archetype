# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: E402

"""Evaluate a scripted manipulation episode set from persisted rows.

This example uses the existing manipulation components and EnvStepProcessor.
EvalService reads the final persisted ManipStatus/ManipTask rows and computes
success-rate metrics without re-running the simulation.

Usage:
    uv run python examples/10_eval_service_manipulation.py
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
from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.manipulation import (
    ACTION_DIM,
    EnvStepProcessor,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
)
from evals.graders import exact_match, threshold
from evals.types import GraderResult


async def spawn_reach_trial(
    world,
    env: ScriptedReachEnv,
    *,
    env_key: int,
    seed: int,
    action: list[float],
) -> int:
    obs = env.reset(env_key, seed)
    return await world.create_entity(
        [
            ManipProprio(
                eef_pos=obs["eef_pos"],
                eef_quat=obs["eef_quat"],
                gripper=obs["gripper"],
            ),
            ManipAction(values=action),
            ManipStatus(),
            ManipTask(
                suite="scripted_reach",
                task_id=0,
                instruction="reach target",
                seed=seed,
                env_key=env_key,
            ),
        ]
    )


def manipulation_graders(df: DataFrame) -> list[GraderResult]:
    rows = df.collect().to_pylist()
    successes = [bool(row["manipstatus__success"]) for row in rows]
    env_steps = [int(row["manipstatus__env_step"]) for row in rows]
    success_rate = sum(successes) / len(rows)
    mean_env_step = sum(env_steps) / len(env_steps)
    return [
        exact_match(len(rows), 3, name="trial_count"),
        threshold(success_rate, min_val=0.5, name="success_rate"),
        threshold(mean_env_step, max_val=6.0, name="mean_env_step"),
    ]


async def main() -> None:
    env = ScriptedReachEnv(
        targets={
            0: (0.10, 0.0, 0.5),
            1: (0.25, 0.0, 0.5),
            2: (0.80, 0.0, 0.5),
        },
        tolerance=0.06,
    )
    actions = {
        0: [0.05, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3),
        1: [0.05, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3),
        2: [0.02, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3),
    }

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri="./archetype_data", namespace="eval_manipulation")
        system = AsyncSystem()
        await system.add_processor(EnvStepProcessor(env))
        world = await container.world_service.create_world(
            WorldConfig(name="eval-manipulation"),
            storage_config=storage,
            system=system,
        )

        for env_key, action in actions.items():
            await spawn_reach_trial(world, env, env_key=env_key, seed=env_key + 1, action=action)

        run = await container.simulation_service.run(world.world_id, RunConfig(num_steps=6))
        final_rows = await container.eval_service.query_components(
            [ManipStatus, ManipTask],
            world_id=world.world_id,
            run_id=run.run_id,
            ticks=[run.final_tick - 1],
            storage_config=storage,
        )
        grader_results = await container.eval_service.run_graders(
            final_rows, [manipulation_graders]
        )

        print(final_rows.collect().to_pylist())
        print([asdict(result) for result in grader_results])
    finally:
        await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
