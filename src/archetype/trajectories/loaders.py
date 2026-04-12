# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Loaders
==================

Turn on-disk agent session files into ``Trajectory`` Components the pipeline
can ingest. First-class loader: Claude Code's per-project JSONL format at
``~/.claude/projects/<hashed-cwd>/<session_id>.jsonl``.

Each Claude Code session file contains a heterogenous JSONL stream:

- ``type=user`` records: a prompt from the user, OR a ``tool_result`` list
  from a previously-executed tool
- ``type=assistant`` records: the model's reply, whose ``message.content`` is
  a list of parts typed ``text``, ``thinking``, or ``tool_use``
- ``type=system``, ``type=file-history-snapshot``, ``type=last-prompt``:
  bookkeeping that isn't a conversational turn — skipped

The loader flattens multi-part assistant messages into one ``Turn`` per
content item (text, thinking, and each tool_use become separate turns) so
downstream processors see the full action trace, not a concatenated blob.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from archetype.trajectories.components import Trajectory, Turn

# ── Public API ────────────────────────────────────────────────────────────


def from_claude_session(path: str | Path) -> Trajectory:
    """Load one Claude Code session JSONL file as a ``Trajectory``.

    Args:
        path: Path to a ``*.jsonl`` session file.

    Returns:
        A ``Trajectory`` Component with turns flattened from the session's
        user/assistant records. Duration is computed from first-to-last
        timestamp; token total sums assistant ``usage.output_tokens``.
    """
    path = Path(path)
    turns: list[Turn] = []
    timestamps: list[datetime] = []
    total_output_tokens = 0
    model: str | None = None
    git_branch: str | None = None
    cwd: str | None = None
    session_id = path.stem

    # Stream line-by-line instead of slurping the whole file — keeps memory
    # usage O(longest line) so arbitrarily large sessions don't blow up.
    with path.open() as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                rec = json.loads(raw_line)
            except json.JSONDecodeError:
                # Corrupt line — skip, don't crash the whole session load
                continue

            if not isinstance(rec, dict):
                continue

            # Capture session-level metadata from the first record that has it.
            # All four fields use first-wins semantics for consistency.
            if cwd is None and isinstance(rec.get("cwd"), str):
                cwd = rec["cwd"]
            if git_branch is None and isinstance(rec.get("gitBranch"), str):
                git_branch = rec["gitBranch"]
            if session_id == path.stem and isinstance(rec.get("sessionId"), str):
                session_id = rec["sessionId"]

            ts = _parse_timestamp(rec.get("timestamp"))
            if ts is not None:
                timestamps.append(ts)

            rec_type = rec.get("type")
            if rec_type not in ("user", "assistant"):
                # Skip snapshots, system messages, last-prompt markers
                continue

            message = rec.get("message")
            if not isinstance(message, dict):
                continue

            role = message.get("role")
            content = message.get("content")

            # Track model usage (assistant records carry usage on .message.usage)
            usage = message.get("usage") or {}
            output_tokens = int(usage.get("output_tokens") or 0)
            total_output_tokens += output_tokens
            if model is None and isinstance(message.get("model"), str):
                model = message["model"]

            # Flatten content into 1+ Turns
            turns.extend(_content_to_turns(role, content, output_tokens))

    duration_seconds = 0.0
    if len(timestamps) >= 2:
        duration_seconds = (max(timestamps) - min(timestamps)).total_seconds()

    # Derive coarse tags from the tool_use footprint — cheap categorization
    # signal that doesn't require an LLM call
    tool_names_used = sorted({t.tool_name for t in turns if t.role == "tool_call" and t.tool_name})
    tags = [f"tool:{name}" for name in tool_names_used]
    if git_branch:
        tags.append(f"branch:{git_branch}")

    metadata: dict[str, Any] = {"session_file": str(path)}
    if model:
        metadata["model"] = model
    if cwd:
        metadata["cwd"] = cwd
    if git_branch:
        metadata["git_branch"] = git_branch

    # Bypass Trajectory.from_turns: it computes duration from
    # turn.duration_ms (which we don't track per-turn), but we *do* know the
    # real session duration from first-to-last timestamp. Build directly.
    return Trajectory(
        trajectory_id=session_id,
        source="claude-code",
        turns_json=json.dumps([t.to_dict() for t in turns]),
        total_turns=len(turns),
        total_tokens=total_output_tokens,
        duration_seconds=duration_seconds,
        outcome="",  # Claude Code doesn't record an explicit outcome
        tags_json=json.dumps(tags),
        metadata_json=json.dumps(metadata),
    )


def discover_sessions(project_root: str | Path | None = None) -> list[Path]:
    """Discover Claude Code session files on disk.

    Args:
        project_root: Specific Claude Code project directory to scan
            (e.g., ``~/.claude/projects/-Users-you-git-myproject``). If
            ``None``, scans all projects under ``~/.claude/projects/``.

    Returns:
        Sorted list of ``.jsonl`` session paths. Empty if the root doesn't
        exist — callers should check.
    """
    if project_root is not None:
        root = Path(project_root).expanduser()
    else:
        root = Path.home() / ".claude" / "projects"
    if not root.exists():
        return []
    # Only top-level .jsonl files — subdirs contain snapshot blobs, not sessions
    if root.is_file():
        return [root] if root.suffix == ".jsonl" else []
    if root.is_dir():
        # When scanning a single project dir: only top-level .jsonl
        sessions = sorted(p for p in root.glob("*.jsonl") if p.is_file())
        if sessions:
            return sessions
        # When scanning the root ~/.claude/projects: recurse one level
        return sorted(
            p for sub in root.iterdir() if sub.is_dir() for p in sub.glob("*.jsonl") if p.is_file()
        )
    return []


def discover_sessions_for_cwd(cwd: str | Path) -> list[Path]:
    """Discover sessions for a specific working directory.

    Claude Code hashes the cwd into a project folder name by replacing ``/``
    with ``-``. This helper resolves that convention so callers can ask
    "show me every session I've had in *this* repo".
    """
    cwd = Path(cwd).expanduser().resolve()
    slug = str(cwd).replace("/", "-")
    project_dir = Path.home() / ".claude" / "projects" / slug
    return discover_sessions(project_dir)


# ── Helpers ───────────────────────────────────────────────────────────────


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    # Claude Code uses ISO 8601 with 'Z' suffix
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _content_to_turns(
    role: str | None,
    content: Any,
    output_tokens: int,
) -> list[Turn]:
    """Flatten a ``message.content`` field into one or more ``Turn``s.

    Claude Code's ``content`` is either a plain string (legacy simple prompts)
    or a list of typed parts. The token budget from the parent record is
    attributed to the first assistant ``text`` part we emit, or the first
    turn overall if there's no text part — this keeps the per-row token
    counts summable without double-counting.
    """
    if role not in ("user", "assistant"):
        return []

    # Simple string content
    if isinstance(content, str):
        return [Turn(role=role, content=content, tokens=output_tokens)]

    if not isinstance(content, list):
        return []

    turns: list[Turn] = []
    token_attributed = False
    for item in content:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")

        if item_type == "text":
            tokens = 0 if token_attributed else output_tokens
            token_attributed = True
            turns.append(Turn(role=role, content=str(item.get("text", "")), tokens=tokens))

        elif item_type == "thinking":
            turns.append(
                Turn(
                    role=role,
                    content=str(item.get("thinking", "")),
                    tokens=0,
                    metadata={"kind": "thinking"},
                )
            )

        elif item_type == "tool_use":
            tool_input = item.get("input")
            turns.append(
                Turn(
                    role="tool_call",
                    tool_name=str(item.get("name", "")),
                    tool_input=json.dumps(tool_input) if tool_input is not None else None,
                    content="",
                    tokens=0,
                )
            )

        elif item_type == "tool_result":
            result_content = item.get("content")
            if isinstance(result_content, list):
                # Sometimes tool_result.content is itself a list of parts;
                # join the text parts
                parts = [
                    str(p.get("text", ""))
                    for p in result_content
                    if isinstance(p, dict) and p.get("type") == "text"
                ]
                result_text = "\n".join(parts)
            else:
                result_text = str(result_content) if result_content is not None else ""
            turns.append(
                Turn(
                    role="tool_result",
                    tool_output=result_text,
                    content="",
                    tokens=0,
                    # Review fix: replaced `and/or` ternary antipattern with
                    # a plain conditional expression.
                    error="tool reported error" if item.get("is_error") else None,
                )
            )

        # Unknown content types are silently skipped — the format evolves

    # If we didn't attribute the token count to any text turn, attribute it
    # to the first turn so the total still reconciles
    if not token_attributed and turns and output_tokens:
        turns[0].tokens = output_tokens

    return turns
