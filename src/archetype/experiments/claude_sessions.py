# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claude Code session transcripts -> trajectory rows.

Claude Code records every session as JSONL under ``~/.claude/projects/
<project-slug>/<session-id>.jsonl``: one line per event, with ``user`` and
``assistant`` message lines carrying content blocks (text, thinking,
tool_use, tool_result) plus timestamps, git branch, model, and token
usage. This module transcribes those transcripts into the shared
trajectory schema so an agent's own session history becomes rows in a
world — queryable, labelable, forkable, and eventually trainable-on.

This is a loader, not an archive: the JSONL files on disk remain the
source artifacts; the trajectory rows are the analytical view. (The
lesson of Chronicle: ideas survive here as components plus a loader.)

Mapping:
    session file        -> one LoadedSession: a Trajectory header plus
                           Turn rows (normalized via turns_to_components)
    user text           -> Turn(role="user")
    assistant text      -> Turn(role="assistant", tokens=output_tokens)
    assistant tool_use  -> Turn(role="tool_call", tool_name, tool_input)
    user tool_result    -> Turn(role="tool_result", error when is_error)
    thinking blocks     -> skipped (private scratchpad, not dialogue)
    sidechain/meta rows -> skipped, counted on the LoadedSession

Content fields are truncated to ``max_content_chars`` — tool outputs
dominate transcript bytes and the full text stays on disk.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from archetype.core.component import Component
from archetype.experiments.trajectories import Trajectory, Turn, turns_to_components

logger = logging.getLogger(__name__)

DEFAULT_PROJECTS_DIR = Path.home() / ".claude" / "projects"

_TRUNCATION_MARK = "…[truncated]"


@dataclass
class LoadedSession:
    """One transcribed session: the header, its turns, and session facts."""

    trajectory: Trajectory
    turns: list[Turn]
    project: str = ""
    cwd: str = ""
    git_branch: str = ""
    client_version: str = ""
    models: list[str] = field(default_factory=list)
    started_at: str = ""
    ended_at: str = ""
    sidechain_turns_skipped: int = 0

    def components(self) -> list[list[Component]]:
        """Spawnable entities: [header] plus one [turn-row] per turn."""
        rows: list[list[Component]] = [[self.trajectory]]
        rows.extend(
            [turn_row]
            for turn_row in turns_to_components(self.trajectory.trajectory_id, self.turns)
        )
        return rows


def _truncate(text: str, limit: int) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + _TRUNCATION_MARK


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _block_text(content: Any) -> str:
    """Flatten a content value (string, block list, or nested) to text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "\n".join(p for p in parts if p)
    return str(content)


def iter_session_files(projects_dir: str | Path | None = None) -> list[Path]:
    """All session transcript files, sorted for deterministic ingest order."""
    root = Path(projects_dir) if projects_dir is not None else DEFAULT_PROJECTS_DIR
    if not root.exists():
        return []
    return sorted(root.glob("*/*.jsonl"))


def load_claude_session(
    path: str | Path,
    *,
    max_content_chars: int = 4000,
    include_sidechains: bool = False,
) -> LoadedSession | None:
    """Transcribe one session transcript.

    Returns None for sessions with no dialogue turns (queue noise,
    empty files, unparseable transcripts).
    """
    path = Path(path)
    turns: list[Turn] = []
    models: set[str] = set()
    git_branch = ""
    cwd = ""
    version = ""
    sidechain_skipped = 0
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    prev_ts: datetime | None = None
    tool_names_by_use_id: dict[str, str] = {}

    try:
        raw_lines = path.read_text(errors="replace").splitlines()
    except OSError as e:
        logger.warning("unreadable session %s: %s", path, e)
        return None

    for raw in raw_lines:
        try:
            line = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(line, dict):
            continue
        line_type = line.get("type")
        if line_type not in ("user", "assistant"):
            continue
        if line.get("isMeta"):
            continue
        if line.get("isSidechain") and not include_sidechains:
            sidechain_skipped += 1
            continue

        message = line.get("message") or {}
        ts = _parse_ts(line.get("timestamp"))
        if ts is not None:
            first_ts = first_ts or ts
            last_ts = ts
        duration_ms = (
            (ts - prev_ts).total_seconds() * 1000.0
            if ts is not None and prev_ts is not None
            else 0.0
        )
        prev_ts = ts or prev_ts

        git_branch = line.get("gitBranch") or git_branch
        cwd = line.get("cwd") or cwd
        version = line.get("version") or version

        if line_type == "assistant":
            if message.get("model"):
                models.add(message["model"])
            usage = message.get("usage") or {}
            tokens = int(usage.get("output_tokens") or 0)
            content = message.get("content") or []
            if isinstance(content, str):
                content = [{"type": "text", "text": content}]
            emitted_tokens = False
            for block in content:
                if not isinstance(block, dict):
                    continue
                btype = block.get("type")
                if btype == "text":
                    text = block.get("text", "")
                    if not text.strip():
                        continue
                    turns.append(
                        Turn(
                            role="assistant",
                            content=_truncate(text, max_content_chars),
                            tokens=0 if emitted_tokens else tokens,
                            duration_ms=duration_ms,
                        )
                    )
                    emitted_tokens = True
                    duration_ms = 0.0
                elif btype == "tool_use":
                    if block.get("id"):
                        tool_names_by_use_id[str(block["id"])] = str(block.get("name", ""))
                    turns.append(
                        Turn(
                            role="tool_call",
                            tool_name=str(block.get("name", "")),
                            tool_input=_truncate(
                                json.dumps(block.get("input", {})), max_content_chars
                            ),
                            tokens=0 if emitted_tokens else tokens,
                            duration_ms=duration_ms,
                        )
                    )
                    emitted_tokens = True
                    duration_ms = 0.0
                # thinking blocks: skipped by design
        else:  # user line
            content = message.get("content")
            if isinstance(content, str):
                if content.strip():
                    turns.append(
                        Turn(
                            role="user",
                            content=_truncate(content, max_content_chars),
                            duration_ms=duration_ms,
                        )
                    )
                continue
            for block in content or []:
                if not isinstance(block, dict):
                    continue
                btype = block.get("type")
                if btype == "tool_result":
                    is_error = bool(block.get("is_error"))
                    turns.append(
                        Turn(
                            role="tool_result",
                            content=_truncate(_block_text(block.get("content")), max_content_chars),
                            tool_name=tool_names_by_use_id.get(
                                str(block.get("tool_use_id", "")), ""
                            ),
                            error="tool_error" if is_error else "",
                            duration_ms=duration_ms,
                        )
                    )
                    duration_ms = 0.0
                elif btype == "text":
                    text = block.get("text", "")
                    if text.strip():
                        turns.append(
                            Turn(
                                role="user",
                                content=_truncate(text, max_content_chars),
                                duration_ms=duration_ms,
                            )
                        )
                        duration_ms = 0.0

    if not turns:
        return None

    project = path.parent.name
    trajectory = Trajectory.from_turns(
        path.stem,
        turns,
        source="claude-code",
        model=sorted(models)[0] if models else "",
        task_id=project,
        terminal=True,
    )
    return LoadedSession(
        trajectory=trajectory,
        turns=turns,
        project=project,
        cwd=cwd,
        git_branch=git_branch,
        client_version=version,
        models=sorted(models),
        started_at=first_ts.isoformat() if first_ts else "",
        ended_at=last_ts.isoformat() if last_ts else "",
        sidechain_turns_skipped=sidechain_skipped,
    )


def load_claude_sessions(
    projects_dir: str | Path | None = None,
    *,
    max_content_chars: int = 4000,
    include_sidechains: bool = False,
    limit: int | None = None,
) -> list[LoadedSession]:
    """Transcribe every session under projects_dir."""
    sessions: list[LoadedSession] = []
    for path in iter_session_files(projects_dir):
        if limit is not None and len(sessions) >= limit:
            break
        session = load_claude_session(
            path,
            max_content_chars=max_content_chars,
            include_sidechains=include_sidechains,
        )
        if session is not None:
            sessions.append(session)
    return sessions
