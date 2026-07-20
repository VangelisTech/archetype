# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Query and grade normalized mission trajectory evidence.

The example is deterministic and credential-free. It writes two trajectory
headers plus separate turn and reward rows, selects one failed trial, and asks
the trajectory application service to grade that trial's rewards.

Usage:
    uv run python examples/06_trajectory_analysis.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from daft import DataFrame

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.missions.trajectories import (
    Trajectory,
    TrajectoryReward,
    TrajectorySelection,
    Turn,
    turns_to_components,
)


@dataclass(frozen=True)
class ExampleTrajectory:
    """One header and its normalized child values before spawning."""

    header: Trajectory
    turns: tuple[Turn, ...]
    rewards: tuple[float, ...]


def make_trajectories() -> tuple[ExampleTrajectory, ...]:
    """Build two small synthetic mission attempts."""
    accepted_turns = (
        Turn(role="user", content="Fix the login regression", tokens=6),
        Turn(role="assistant", content="Patched and validated", tokens=18),
    )
    rejected_turns = (
        Turn(role="user", content="Fix the cache regression", tokens=6),
        Turn(role="assistant", content="Validator still fails", tokens=14),
    )
    return (
        ExampleTrajectory(
            header=Trajectory.from_turns(
                "mission-42:auth:attempt-1",
                list(accepted_turns),
                run_id="mission-42",
                task_id="auth",
                trial_idx=0,
                source="coding-agent",
                terminal=True,
                outcome="accepted",
            ),
            turns=accepted_turns,
            rewards=(0.25, 1.0),
        ),
        ExampleTrajectory(
            header=Trajectory.from_turns(
                "mission-42:cache:attempt-1",
                list(rejected_turns),
                run_id="mission-42",
                task_id="cache",
                trial_idx=1,
                source="coding-agent",
                terminal=True,
                outcome="rejected",
            ),
            turns=rejected_turns,
            rewards=(-1.0,),
        ),
    )


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Persist, select, and grade the synthetic trajectory evidence."""
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "trajectory-analysis",
            storage=StorageConfig(uri=storage_uri, namespace="trajectory_example"),
        )
        for authored in make_trajectories():
            await world.spawn(authored.header)
            await world.spawn_many(
                [
                    [turn]
                    for turn in turns_to_components(
                        authored.header.trajectory_id,
                        list(authored.turns),
                    )
                ]
            )
            await world.spawn_many(
                [
                    [
                        TrajectoryReward(
                            trajectory_id=authored.header.trajectory_id,
                            seq=seq,
                            reward=reward,
                        )
                    ]
                    for seq, reward in enumerate(authored.rewards)
                ]
            )
        await world.run(steps=1)

        rejected = await world.query_trajectory(
            Trajectory,
            selection=TrajectorySelection(task_ids=("cache",), trial_idxs=(1,)),
        )
        rejected_rows = rejected.collect().to_pylist()
        trajectory_id = str(rejected_rows[0]["trajectory__trajectory_id"])

        def reward_summary(frame: DataFrame) -> dict[str, object]:
            rows = frame.collect().to_pylist()
            return {
                "samples": len(rows),
                "total_reward": sum(row["trajectoryreward__reward"] for row in rows),
            }

        outputs = await world.grade_trajectory(
            TrajectoryReward,
            selection=TrajectorySelection(trajectory_ids=(trajectory_id,)),
            graders=[reward_summary],
        )
        return {
            "trajectory_id": trajectory_id,
            "outcome": rejected_rows[0]["trajectory__outcome"],
            "grade": outputs[0],
        }


async def main() -> None:
    result = await run_demo()
    print(f"Selected: {result['trajectory_id']} ({result['outcome']})")
    print(f"Grade:    {result['grade']}")


if __name__ == "__main__":
    asyncio.run(main())
