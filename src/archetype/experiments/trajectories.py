# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed trajectory components for experiment and rollout data.

Trajectory data is stored as normalized, Arrow-friendly component rows. The
header row identifies the trajectory; turns, commands, observations, actions,
and rewards are separate typed rows keyed by ``trajectory_id`` and ``seq``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from archetype.core.component import Component


@dataclass
class Turn:
    """Python authoring helper for one conversational or tool-use turn."""

    role: str
    content: str = ""
    tool_name: str = ""
    tool_input: str = ""
    tool_output: str = ""
    tokens: int = 0
    duration_ms: float = 0.0
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"role": self.role}
        if self.content:
            data["content"] = self.content
        if self.tool_name:
            data["tool_name"] = self.tool_name
        if self.tool_input:
            data["tool_input"] = self.tool_input
        if self.tool_output:
            data["tool_output"] = self.tool_output
        if self.tokens:
            data["tokens"] = self.tokens
        if self.duration_ms:
            data["duration_ms"] = self.duration_ms
        if self.error:
            data["error"] = self.error
        if self.metadata:
            data["metadata"] = self.metadata
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Turn:
        return cls(
            role=str(data.get("role") or ""),
            content=str(data.get("content") or ""),
            tool_name=str(data.get("tool_name") or ""),
            tool_input=str(data.get("tool_input") or ""),
            tool_output=str(data.get("tool_output") or ""),
            tokens=int(data.get("tokens") or 0),
            duration_ms=float(data.get("duration_ms") or 0.0),
            error=str(data.get("error") or ""),
            metadata=dict(data.get("metadata") or {}),
        )

    def to_component(self, trajectory_id: str, seq: int) -> TrajectoryTurn:
        return TrajectoryTurn(
            trajectory_id=trajectory_id,
            seq=seq,
            role=self.role,
            content=self.content,
            tool_name=self.tool_name,
            tool_input=self.tool_input,
            tool_output=self.tool_output,
            tokens=self.tokens,
            duration_ms=self.duration_ms,
            error=self.error,
        )


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


def turns_to_components(trajectory_id: str, turns: list[Turn]) -> list[TrajectoryTurn]:
    return [turn.to_component(trajectory_id, seq) for seq, turn in enumerate(turns)]
