# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helpers that turn existing runtime artifacts into typed trajectory rows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from archetype.app.models import Command, EpisodeResult
from archetype.experiments.trajectories import (
    Trajectory,
    TrajectoryAction,
    TrajectoryCommandEvent,
    TrajectoryObservation,
    TrajectoryReward,
)


def command_to_event(
    command: Command,
    *,
    trajectory_id: str,
    seq: int,
) -> TrajectoryCommandEvent:
    """Serialize a queued command into a typed trajectory event."""
    return TrajectoryCommandEvent(
        trajectory_id=trajectory_id,
        seq=seq,
        command_id=str(command.id),
        tick=command.tick,
        command_type=command.type.value,
        priority=command.priority,
        version=command.version,
    )


def audit_row_to_event(
    row: Mapping[str, Any],
    *,
    trajectory_id: str,
    seq: int,
) -> TrajectoryCommandEvent:
    """Serialize an audit row dict into a typed trajectory event."""
    return TrajectoryCommandEvent(
        trajectory_id=trajectory_id,
        seq=seq,
        audit_id=str(row.get("audit_id") or ""),
        command_id=str(row.get("command_id") or ""),
        world_id=str(row.get("world_id") or ""),
        actor_id=str(row.get("actor_id") or ""),
        command_type=str(row.get("command_type") or ""),
        status=str(row.get("status") or ""),
        accepted_at=str(row.get("accepted_at") or ""),
        applied_at=str(row.get("applied_at") or ""),
    )


def commands_to_events(
    commands: Iterable[Command],
    *,
    trajectory_id: str,
) -> list[TrajectoryCommandEvent]:
    return [
        command_to_event(command, trajectory_id=trajectory_id, seq=seq)
        for seq, command in enumerate(commands)
    ]


def audit_rows_to_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    trajectory_id: str,
) -> list[TrajectoryCommandEvent]:
    return [
        audit_row_to_event(row, trajectory_id=trajectory_id, seq=seq)
        for seq, row in enumerate(rows)
    ]


def trajectory_from_episode_result(
    episode: EpisodeResult,
    *,
    rollout_id: str = "",
    run_id: str = "",
    task_id: str = "",
    trial_idx: int = 0,
    source: str = "archetype.rollout",
) -> Trajectory:
    """Build a durable trajectory header row from one episode result."""
    trajectory_id = (
        f"{rollout_id}:trial-{trial_idx}:{episode.episode_id}"
        if rollout_id
        else str(episode.episode_id)
    )
    return Trajectory(
        trajectory_id=trajectory_id,
        run_id=run_id,
        episode_id=str(episode.episode_id),
        rollout_id=rollout_id,
        task_id=task_id,
        trial_idx=trial_idx,
        source=source,
        terminal=episode.terminated,
        total_steps=episode.duration_steps,
    )


def observations_from_post_tick_events(
    events: Iterable[Mapping[str, Any]],
    *,
    trajectory_id: str,
) -> list[TrajectoryObservation]:
    return [
        TrajectoryObservation(
            trajectory_id=trajectory_id,
            seq=seq,
            world_id=str(event.get("world_id") or ""),
            tick=int(event.get("tick") or 0),
            event_type=str(event.get("event") or ""),
            archetype_count=int(event.get("archetype_count") or 0),
            entity_count=int(event.get("entity_count") or 0),
        )
        for seq, event in enumerate(events)
    ]


def actions_from_observations(
    observations: Iterable[TrajectoryObservation],
    *,
    action_type: str = "step",
) -> list[TrajectoryAction]:
    return [
        TrajectoryAction(
            trajectory_id=observation.trajectory_id,
            seq=seq,
            tick=observation.tick,
            action_type=action_type,
        )
        for seq, observation in enumerate(observations)
    ]


def reward_row(
    *,
    trajectory_id: str,
    reward: float,
    seq: int = 0,
) -> TrajectoryReward:
    return TrajectoryReward(trajectory_id=trajectory_id, seq=seq, reward=reward)
