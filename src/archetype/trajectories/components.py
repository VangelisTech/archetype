# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Components
=====================

Trajectory: The raw agent trajectory — every turn, tool call, output, and timing.
Label:      An evaluation result attached to a trajectory.

Both are Arrow-serializable. Complex nested data (turns, tool calls)
is stored as JSON strings for LanceDB compatibility.

Note: Named 'Trajectory' not 'Session' to avoid collision with daft.session.Session.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from archetype.core.component import Component


@dataclass
class Turn:
    """A single turn in an agent trajectory. Not a Component — just a data class
    for building Trajectory.turns JSON."""

    role: str  # "user", "assistant", "tool_call", "tool_result", "system"
    content: str = ""
    tool_name: str | None = None
    tool_input: str | None = None  # JSON string
    tool_output: str | None = None  # JSON string
    tokens: int = 0
    duration_ms: float = 0.0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "role": self.role,
            "content": self.content,
            "tokens": self.tokens,
            "duration_ms": self.duration_ms,
        }
        if self.tool_name is not None:
            d["tool_name"] = self.tool_name
        if self.tool_input is not None:
            d["tool_input"] = self.tool_input
        if self.tool_output is not None:
            d["tool_output"] = self.tool_output
        if self.error is not None:
            d["error"] = self.error
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Turn:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class Trajectory(Component):
    """A complete agent trajectory.

    Stores the full history of an agent session — every turn, tool call,
    reasoning step, output, error, and timing — as a JSON-encoded list of turns.

    Fields:
        trajectory_id:    External reference (e.g., Claude Code session ID)
        source:           Origin system ("claude-code", "api", "custom")
        turns:            JSON list of Turn dicts — the full trajectory
        total_turns:      Count of turns (denormalized for filtering)
        total_tokens:     Total token usage across all turns
        duration_seconds: Wall-clock duration of the session
        outcome:          Final outcome summary (success/failure/partial + description)
        tags:             JSON list of string tags for categorization
        metadata:         JSON dict of arbitrary session metadata
    """

    trajectory_id: str = ""
    source: str = ""
    turns: str = "[]"
    total_turns: int = 0
    total_tokens: int = 0
    duration_seconds: float = 0.0
    outcome: str = ""
    tags: str = "[]"
    metadata: str = "{}"

    @classmethod
    def from_turns(
        cls,
        trajectory_id: str,
        turns: list[Turn],
        *,
        source: str = "",
        outcome: str = "",
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Trajectory:
        """Build a Trajectory from a list of Turn objects."""
        total_tokens = sum(t.tokens for t in turns)
        duration = sum(t.duration_ms for t in turns) / 1000.0
        return cls(
            trajectory_id=trajectory_id,
            source=source,
            turns=json.dumps([t.to_dict() for t in turns]),
            total_turns=len(turns),
            total_tokens=total_tokens,
            duration_seconds=duration,
            outcome=outcome,
            tags=json.dumps(tags or []),
            metadata=json.dumps(metadata or {}),
        )

    def get_turns(self) -> list[Turn]:
        """Deserialize turns JSON back to Turn objects."""
        return [Turn.from_dict(d) for d in json.loads(self.turns)]


class Label(Component):
    """An evaluation label attached to a trajectory.

    Each (Trajectory, Label) entity represents one labeling technique
    applied to one trajectory. To compare techniques, fork the world
    and swap the LabelingProcessor's description.

    Fields:
        technique:   Name of the labeling technique (e.g., "efficiency", "correctness")
        description: Natural language description of what to evaluate.
                     This is the prompt — describe it and the LLM applies it.
        value:       The label result (filled by LabelingProcessor)
        score:       Numeric score 0.0-1.0 (filled by LabelingProcessor)
        rationale:   Why this label was assigned (filled by LabelingProcessor)
        sampled:     Whether this entity was selected by the SamplingProcessor
    """

    technique: str = ""
    description: str = ""
    value: str = ""
    score: float = 0.0
    rationale: str = ""
    sampled: bool = True
