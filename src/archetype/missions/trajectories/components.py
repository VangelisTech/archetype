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

"""Persistent trajectory schemas for mission and rollout evidence.

Trajectory data is stored as normalized, Arrow-friendly component rows. The
header row identifies the trajectory; turns, commands, observations, actions,
and rewards are separate typed rows keyed by ``trajectory_id`` and ``seq``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.missions.trajectories.contracts import Turn

CLAUDE_TRANSCRIPT_TABLE = "coding_agent_transcript_rows"


class Trajectory(Component):
    """One durable trajectory header row."""

    trajectory_id: str = ""
    run_id: str = ""
    episode_id: str = ""
    rollout_id: str = ""
    task_id: str = ""
    trial_idx: int = 0

    source: str = ""
    model: str = ""
    policy_version: str = ""

    terminal: bool = False
    total_steps: int = 0
    total_turns: int = 0
    total_tokens: int = 0
    duration_seconds: float = 0.0
    outcome: str = ""

    @classmethod
    def from_turns(
        cls,
        trajectory_id: str,
        turns: list[Turn],
        *,
        run_id: str = "",
        episode_id: str = "",
        rollout_id: str = "",
        task_id: str = "",
        trial_idx: int = 0,
        source: str = "",
        model: str = "",
        policy_version: str = "",
        terminal: bool = False,
        outcome: str = "",
    ) -> Trajectory:
        return cls(
            trajectory_id=trajectory_id,
            run_id=run_id,
            episode_id=episode_id,
            rollout_id=rollout_id,
            task_id=task_id,
            trial_idx=trial_idx,
            source=source,
            model=model,
            policy_version=policy_version,
            terminal=terminal,
            total_steps=0,
            total_turns=len(turns),
            total_tokens=sum(turn.tokens for turn in turns),
            duration_seconds=sum(turn.duration_ms for turn in turns) / 1000.0,
            outcome=outcome,
        )


class TrajectoryTurn(Component):
    """One typed turn row belonging to a trajectory."""

    trajectory_id: str = ""
    seq: int = 0
    role: str = ""
    content: str = ""
    tool_name: str = ""
    tool_input: str = ""
    tool_output: str = ""
    tokens: int = 0
    duration_ms: float = 0.0
    error: str = ""


class TranscriptArtifactRef(Component):
    """Lightweight link from a trajectory to redacted transcript artifacts.

    Narrative content never belongs in this Component. Historical
    ``TrajectoryTurn`` rows remain readable, while new transcript ingestion
    writes normalized narrative rows only through the artifact table boundary.
    """

    trajectory_id: str = ""
    mission_id: str = ""
    source_uri: str = ""
    source_content_hash: str = ""
    redaction_policy_id: str = ""
    redaction_status: str = "clean"
    redaction_count: int = 0
    redaction_rule_ids_json: str = "[]"
    table_name: str = CLAUDE_TRANSCRIPT_TABLE


class TrajectoryCommandEvent(Component):
    """One typed command/audit event row belonging to a trajectory."""

    trajectory_id: str = ""
    seq: int = 0
    audit_id: str = ""
    command_id: str = ""
    world_id: str = ""
    actor_id: str = ""
    command_type: str = ""
    status: str = ""
    tick: int = 0
    priority: int = 0
    version: int = 0
    accepted_at: str = ""
    applied_at: str = ""


class TrajectoryObservation(Component):
    """One typed observation/event row belonging to a trajectory."""

    trajectory_id: str = ""
    seq: int = 0
    world_id: str = ""
    tick: int = 0
    event_type: str = ""
    archetype_count: int = 0
    entity_count: int = 0


class TrajectoryAction(Component):
    """One typed action row belonging to a trajectory."""

    trajectory_id: str = ""
    seq: int = 0
    tick: int = 0
    action_type: str = ""


class TrajectoryReward(Component):
    """One typed reward row belonging to a trajectory."""

    trajectory_id: str = ""
    seq: int = 0
    reward: float = 0.0
