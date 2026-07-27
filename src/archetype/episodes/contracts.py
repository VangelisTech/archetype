# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned episode and trajectory authoring contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from archetype.artifacts.models import ArtifactRef


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
    """Typed episode filter applied to one evidence component table."""

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


__all__ = [
    "ClaudeTranscriptSource",
    "TrajectorySelection",
    "TranscriptIngestionResult",
]
