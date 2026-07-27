# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure Claude JSONL parsing into in-memory trajectory authoring values.

This module performs no file I/O and writes no Components. The mission-owned
transcript workflow is responsible for source snapshots, redaction, quarantine,
and artifact durability before calling :func:`parse_claude_transcript`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from archetype.episodes.contracts import ClaudeTranscriptSource
from archetype.missions.trajectories.contracts import Turn

_TRUNCATION_MARK = "…[truncated]"


@dataclass(frozen=True)
class LoadedSession:
    """One parsed session held in memory until an artifact adapter publishes it."""

    episode_id: str
    turns: tuple[Turn, ...]
    source_uri: str
    mission_id: str = ""
    project: str = ""
    session_id: str = ""
    cwd: str = ""
    git_branch: str = ""
    client_version: str = ""
    models: tuple[str, ...] = ()
    started_at: str = ""
    ended_at: str = ""
    sidechain_turns_skipped: int = 0

    @property
    def model(self) -> str:
        return self.models[0] if self.models else ""

    @property
    def total_turns(self) -> int:
        return len(self.turns)

    @property
    def total_tokens(self) -> int:
        return sum(turn.tokens for turn in self.turns)

    @property
    def duration_seconds(self) -> float:
        return sum(turn.duration_ms for turn in self.turns) / 1000.0


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + _TRUNCATION_MARK


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _block_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        return "\n".join(part for part in parts if part)
    return str(content)


def parse_claude_transcript(
    text: str,
    source: ClaudeTranscriptSource,
) -> LoadedSession | None:
    """Parse already-sanitized Claude JSONL without performing I/O or persistence."""

    turns: list[Turn] = []
    models: set[str] = set()
    git_branch = ""
    cwd = ""
    version = ""
    sidechain_skipped = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    previous_timestamp: datetime | None = None
    tool_names_by_use_id: dict[str, str] = {}

    for raw in text.splitlines():
        try:
            line = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(line, dict):
            continue
        line_type = line.get("type")
        if line_type not in ("user", "assistant") or line.get("isMeta"):
            continue
        if line.get("isSidechain") and not source.include_sidechains:
            sidechain_skipped += 1
            continue

        message = line.get("message") or {}
        if not isinstance(message, dict):
            continue
        timestamp = _parse_timestamp(line.get("timestamp"))
        if timestamp is not None:
            first_timestamp = first_timestamp or timestamp
            last_timestamp = timestamp
        duration_ms = (
            (timestamp - previous_timestamp).total_seconds() * 1000.0
            if timestamp is not None and previous_timestamp is not None
            else 0.0
        )
        previous_timestamp = timestamp or previous_timestamp

        git_branch = str(line.get("gitBranch") or git_branch)
        cwd = str(line.get("cwd") or cwd)
        version = str(line.get("version") or version)

        if line_type == "assistant":
            if message.get("model"):
                models.add(str(message["model"]))
            usage = message.get("usage") or {}
            tokens = int(usage.get("output_tokens") or 0) if isinstance(usage, dict) else 0
            content = message.get("content") or []
            if isinstance(content, str):
                content = [{"type": "text", "text": content}]
            if not isinstance(content, list):
                continue
            emitted_tokens = False
            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    block_text = str(block.get("text", ""))
                    if not block_text.strip():
                        continue
                    turns.append(
                        Turn(
                            role="assistant",
                            content=_truncate(block_text, source.max_content_chars),
                            tokens=0 if emitted_tokens else tokens,
                            duration_ms=duration_ms,
                        )
                    )
                    emitted_tokens = True
                    duration_ms = 0.0
                elif block_type == "tool_use":
                    if block.get("id"):
                        tool_names_by_use_id[str(block["id"])] = str(block.get("name", ""))
                    turns.append(
                        Turn(
                            role="tool_call",
                            tool_name=str(block.get("name", "")),
                            tool_input=_truncate(
                                json.dumps(
                                    block.get("input", {}),
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                                source.max_content_chars,
                            ),
                            tokens=0 if emitted_tokens else tokens,
                            duration_ms=duration_ms,
                        )
                    )
                    emitted_tokens = True
                    duration_ms = 0.0
            continue

        content = message.get("content")
        if isinstance(content, str):
            if content.strip():
                turns.append(
                    Turn(
                        role="user",
                        content=_truncate(content, source.max_content_chars),
                        duration_ms=duration_ms,
                    )
                )
            continue
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "tool_result":
                is_error = bool(block.get("is_error"))
                turns.append(
                    Turn(
                        role="tool_result",
                        tool_output=_truncate(
                            _block_text(block.get("content")),
                            source.max_content_chars,
                        ),
                        tool_name=tool_names_by_use_id.get(str(block.get("tool_use_id", "")), ""),
                        error="tool_error" if is_error else "",
                        duration_ms=duration_ms,
                    )
                )
                duration_ms = 0.0
            elif block_type == "text":
                block_text = str(block.get("text", ""))
                if block_text.strip():
                    turns.append(
                        Turn(
                            role="user",
                            content=_truncate(block_text, source.max_content_chars),
                            duration_ms=duration_ms,
                        )
                    )
                    duration_ms = 0.0

    if not turns:
        return None

    ordered_models = tuple(sorted(models))
    return LoadedSession(
        episode_id=source.episode_id,
        turns=tuple(turns),
        source_uri=source.source_uri,
        mission_id=source.mission_id,
        project=source.project,
        session_id=source.session_id,
        cwd=cwd,
        git_branch=git_branch,
        client_version=version,
        models=ordered_models,
        started_at=first_timestamp.isoformat() if first_timestamp else "",
        ended_at=last_timestamp.isoformat() if last_timestamp else "",
        sidechain_turns_skipped=sidechain_skipped,
    )
