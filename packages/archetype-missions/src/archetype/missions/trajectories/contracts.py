# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission-owned transcript and trajectory authoring contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import quote

from archetype.artifacts.models import ArtifactRef

if TYPE_CHECKING:
    from archetype.missions.trajectories.components import TrajectoryTurn


@dataclass(frozen=True)
class ClaudeTranscriptSource:
    """Local Claude JSONL source and its stable logical linkage."""

    path: Path
    mission_id: str = ""
    project: str = ""
    session_id: str = ""
    max_content_chars: int = 4000
    include_sidechains: bool = False

    def __post_init__(self) -> None:
        path = Path(self.path)
        object.__setattr__(self, "path", path)
        if path.suffix.lower() != ".jsonl":
            raise ValueError("Claude transcript sources must use the .jsonl suffix")
        if self.max_content_chars < 1:
            raise ValueError("max_content_chars must be at least 1")
        project = self.project.strip() or path.parent.name
        session_id = self.session_id.strip() or path.stem
        if not project or not session_id:
            raise ValueError("Claude transcript source needs project and session identity")
        object.__setattr__(self, "project", project)
        object.__setattr__(self, "session_id", session_id)

    @property
    def source_uri(self) -> str:
        """Canonical source identity without leaking a local filesystem path."""

        return f"claude-session://{quote(self.project, safe='')}/{quote(self.session_id, safe='')}"

    @property
    def episode_id(self) -> str:
        """Stable episode identity for normalized rows and the lightweight index."""

        return self.source_uri


@dataclass(frozen=True)
class TrajectorySelection:
    """Typed episode filter applied to one mission-evidence component table."""

    episode_ids: tuple[str, ...] = ()

    def requested(self) -> dict[str, tuple[str, ...]]:
        """Return only active field filters."""

        return {"episode_id": self.episode_ids} if self.episode_ids else {}


@dataclass(frozen=True)
class TranscriptIngestionResult:
    """Durable outputs from sanitizing and indexing one coding-agent session."""

    world_id: str
    run_id: str
    episode_id: str
    mission_id: str
    source_uri: str
    artifact: ArtifactRef
    rows_written: int
    redaction_policy_id: str
    redaction_status: str
    redaction_count: int
    redaction_rule_ids: tuple[str, ...]


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


__all__ = [
    "ClaudeTranscriptSource",
    "CommandRecord",
    "TrajectorySelection",
    "TranscriptIngestionResult",
    "Turn",
]
