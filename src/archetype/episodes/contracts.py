# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned episode and trajectory authoring contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from archetype.artifacts.contracts import ArtifactRef


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
    def trajectory_id(self) -> str:
        """Stable trajectory linkage for normalized rows and the lightweight index."""

        return self.source_uri


@dataclass(frozen=True)
class TrajectorySelection:
    """Typed filters applied to one trajectory component table."""

    trajectory_ids: tuple[str, ...] = ()
    episode_ids: tuple[str, ...] = ()
    rollout_ids: tuple[str, ...] = ()
    task_ids: tuple[str, ...] = ()
    trial_idxs: tuple[int, ...] = ()

    def requested(self) -> dict[str, tuple[str, ...] | tuple[int, ...]]:
        """Return only active field filters."""

        return {
            name: values
            for name, values in (
                ("trajectory_id", self.trajectory_ids),
                ("episode_id", self.episode_ids),
                ("rollout_id", self.rollout_ids),
                ("task_id", self.task_ids),
                ("trial_idx", self.trial_idxs),
            )
            if values
        }


@dataclass(frozen=True)
class TranscriptIngestionResult:
    """Durable outputs from sanitizing and indexing one coding-agent session."""

    world_id: str
    run_id: str
    trajectory_id: str
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
