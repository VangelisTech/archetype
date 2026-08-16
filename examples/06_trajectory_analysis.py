# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Query and grade normalized episode evidence.

The example is deterministic and credential-free. It writes turn and reward
rows for two episodes keyed by ``episode_id``, selects one failed episode,
and asks the trajectory application service to grade that episode's rewards.

Usage:
    uv run python examples/06_trajectory_analysis.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from daft import DataFrame

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.missions import MissionWorld
from archetype.missions.trajectories import (
    TrajectoryReward,
    TrajectorySelection,
    TrajectoryTurn,
    Turn,
    trajectory,
    turns_to_components,
)


@dataclass(frozen=True)
class ExampleEpisode:
    """One episode's normalized evidence values before spawning."""

    episode_id: str
    turns: tuple[Turn, ...]
    rewards: tuple[float, ...]


def make_episodes() -> tuple[ExampleEpisode, ...]:
    """Build two small synthetic mission attempts."""
    return (
        ExampleEpisode(
            episode_id="episode-auth-1",
            turns=(
                Turn(role="user", content="Fix the login regression", tokens=6),
                Turn(role="assistant", content="Patched and validated", tokens=18),
            ),
            rewards=(0.25, 1.0),
        ),
        ExampleEpisode(
            episode_id="episode-cache-1",
            turns=(
                Turn(role="user", content="Fix the cache regression", tokens=6),
                Turn(role="assistant", content="Validator still fails", tokens=14),
            ),
            rewards=(-1.0,),
        ),
    )


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Persist, select, and grade the synthetic episode evidence."""
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "trajectory-analysis",
            storage=StorageConfig(uri=storage_uri, namespace="trajectory_example"),
        )
        for episode in make_episodes():
            await world.spawn_many(
                [
                    [turn]
                    for turn in turns_to_components(
                        episode.episode_id,
                        list(episode.turns),
                    )
                ]
            )
            await world.spawn_many(
                [
                    [
                        TrajectoryReward(
                            episode_id=episode.episode_id,
                            seq=seq,
                            reward=reward,
                        )
                    ]
                    for seq, reward in enumerate(episode.rewards)
                ]
            )
        await world.run(steps=1)

        evidence = MissionWorld(world)
        failed_episode_id = "episode-cache-1"
        turns_frame = await evidence.query_trajectory(TrajectoryTurn)
        ordered = trajectory(turns_frame, TrajectoryTurn, episode_id=failed_episode_id)
        roles = [row["trajectoryturn__role"] for row in ordered.collect().to_pylist()]

        def reward_summary(frame: DataFrame) -> dict[str, object]:
            rows = frame.collect().to_pylist()
            return {
                "samples": len(rows),
                "total_reward": sum(row["trajectoryreward__reward"] for row in rows),
            }

        outputs = await evidence.grade_trajectory(
            TrajectoryReward,
            selection=TrajectorySelection(episode_ids=(failed_episode_id,)),
            graders=[reward_summary],
        )
        return {
            "episode_id": failed_episode_id,
            "roles": roles,
            "grade": outputs[0],
        }


async def main() -> None:
    result = await run_demo()
    print(f"Selected: {result['episode_id']} (roles: {result['roles']})")
    print(f"Grade:    {result['grade']}")


if __name__ == "__main__":
    asyncio.run(main())
