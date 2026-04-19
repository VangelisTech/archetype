# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Load Claude Code session JSONL transcripts into Daft DataFrames.

Session JSONL format (one JSON object per line):
    type: "user" | "assistant" | "system" | "progress" | "queue-operation" | ...
    message.role: "user" | "assistant"
    message.content: list[{type: "text", text: str} | {type: "tool_use", name: str, ...} | ...]
    timestamp: ISO 8601
    sessionId: str
    uuid: str
"""

from __future__ import annotations

import json
from pathlib import Path

import daft
from daft import DataFrame


def _parse_session_file(path: str | Path) -> list[dict]:
    """Parse a single JSONL session file into flat turn records."""
    path = Path(path)
    if not path.exists():
        return []

    turns: list[dict] = []
    seq = 0

    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        row_type = obj.get("type", "")
        if row_type not in ("user", "assistant"):
            continue

        message = obj.get("message", {})
        if not isinstance(message, dict):
            continue

        role = message.get("role", row_type)
        content_blocks = message.get("content", [])
        if isinstance(content_blocks, str):
            content_blocks = [{"type": "text", "text": content_blocks}]

        # Extract text content
        texts = []
        tool_uses = []
        tool_results = []
        has_error = False

        for block in content_blocks:
            if not isinstance(block, dict):
                continue
            btype = block.get("type", "")
            if btype == "text":
                texts.append(block.get("text", ""))
            elif btype == "tool_use":
                tool_uses.append(block.get("name", "unknown"))
            elif btype == "tool_result":
                tool_results.append(block.get("tool_use_id", ""))
                if block.get("is_error", False):
                    has_error = True
                # Also check content for error indicators
                result_content = str(block.get("content", ""))
                if any(
                    kw in result_content.lower()
                    for kw in ["error", "exit code", "traceback", "failed"]
                ):
                    has_error = True

        text = "\n".join(texts)

        # Skip system-injected messages (skill loads, system instructions, reminders)
        is_system_injected = (
            text.lstrip().startswith("<system_instruction>")
            or text.lstrip().startswith("<system-reminder>")
            or text.lstrip().startswith("Base directory for this skill:")
            or text.lstrip().startswith("# ")
            and "skill" in text[:200].lower()
        )
        if role == "user" and is_system_injected and not tool_results:
            continue

        # A user turn is "user-authored" if it has actual text content
        # and isn't purely a tool_result delivery.
        is_user_authored = (
            role == "user"
            and len(texts) > 0
            and len(text.strip()) > 0
            and not tool_results  # pure tool-result turns aren't user-typed
        )

        turns.append(
            {
                "session_id": obj.get("sessionId", ""),
                "turn_seq": seq,
                "timestamp": obj.get("timestamp", ""),
                "role": role,
                "text": text,
                "text_len": len(text),
                "is_user_authored": is_user_authored,
                "tool_names": tool_uses,
                "tool_result_ids": tool_results,
                "has_error": has_error,
                "n_tool_uses": len(tool_uses),
                "n_tool_results": len(tool_results),
            }
        )
        seq += 1

    return turns


def load_session(path: str | Path) -> DataFrame:
    """Load a single session JSONL file into a DataFrame of conversation turns.

    Columns:
        session_id: str — session identifier
        turn_seq: int — sequential turn number (0-indexed)
        timestamp: str — ISO 8601 timestamp
        role: str — "user" or "assistant"
        text: str — concatenated text content
        text_len: int — character count of text
        tool_names: list[str] — tools invoked in this turn
        tool_result_ids: list[str] — tool result IDs received
        has_error: bool — whether any tool result contained an error
        n_tool_uses: int — number of tool invocations
        n_tool_results: int — number of tool results
    """
    turns = _parse_session_file(path)
    if not turns:
        return daft.from_pydict(
            {
                "session_id": [],
                "turn_seq": [],
                "timestamp": [],
                "role": [],
                "text": [],
                "text_len": [],
                "is_user_authored": [],
                "tool_names": [],
                "tool_result_ids": [],
                "has_error": [],
                "n_tool_uses": [],
                "n_tool_results": [],
            }
        )
    return daft.from_pydict(
        {k: [row[k] for row in turns] for k in turns[0]}
    )


def load_sessions(paths: list[str | Path]) -> DataFrame:
    """Load multiple session JSONL files and concatenate into one DataFrame."""
    dfs = [load_session(p) for p in paths]
    if not dfs:
        return load_session("/dev/null")  # empty
    result = dfs[0]
    for df in dfs[1:]:
        result = result.concat(df)
    return result
