# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Non-persistent authoring and structural input contracts for episode evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from archetype.episodes.contracts import (
    TrajectorySelection as TrajectorySelection,
)
from archetype.episodes.contracts import (
    TranscriptIngestionResult as TranscriptIngestionResult,
)

if TYPE_CHECKING:
    from archetype.missions.trajectories.components import TrajectoryTurn


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

    def to_component(self, episode_id: str, seq: int) -> TrajectoryTurn:
        """Materialize the typed turn row without storing metadata."""
        from archetype.missions.trajectories.components import TrajectoryTurn

        return TrajectoryTurn(
            episode_id=episode_id,
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


class CommandRecord(Protocol):
    """Structural command fields required by evidence transforms."""

    id: object
    tick: int
    type: object
    priority: int
    version: int
