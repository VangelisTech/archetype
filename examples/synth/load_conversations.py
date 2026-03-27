#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Load conversation data from claude-shared repo into a Daft DataFrame
for the synth engine pipeline.

Two data sources:
  1. history.jsonl — user prompts with project context and timestamps
  2. projects/{path}/{session}.jsonl — full conversations (user + assistant)

Extracts user messages, strips XML/command noise, labels with user_id
and project context. Output is a DataFrame ready for generate_triplets().

Usage:
    from examples.synth.load_conversations import load_conversations
    df = load_conversations("/path/to/claude-shared")
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import daft


def _strip_noise(text: str) -> str:
    """Remove XML tags, system reminders, command scaffolding."""
    # Remove <system-reminder>...</system-reminder> blocks
    text = re.sub(r"<system-reminder>.*?</system-reminder>", "", text, flags=re.DOTALL)
    # Remove <local-command-caveat>...</local-command-caveat>
    text = re.sub(r"<local-command-caveat>.*?</local-command-caveat>", "", text, flags=re.DOTALL)
    # Remove <command-name>...</command-name> and siblings
    text = re.sub(r"<command-\w+>.*?</command-\w+>", "", text, flags=re.DOTALL)
    # Remove <local-command-stdout>...</local-command-stdout>
    text = re.sub(r"<local-command-stdout>.*?</local-command-stdout>", "", text, flags=re.DOTALL)
    # Remove standalone XML-ish tags
    text = re.sub(r"</?[a-z_-]+>", "", text)
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def _extract_text(content) -> str:
    """Extract text from message content (string or list of parts)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(part.get("text", ""))
        return " ".join(parts)
    return ""


def load_history(claude_shared_dir: str | Path, min_length: int = 30) -> daft.DataFrame:
    """Load user prompts from history.jsonl files across all users.

    Returns DataFrame with: user_id, segment__content, project, session_id, timestamp
    """
    root = Path(claude_shared_dir) / "users"
    rows: dict[str, list] = {
        "user_id": [],
        "segment__content": [],
        "project": [],
        "session_id": [],
        "timestamp": [],
    }

    for user_dir in sorted(root.iterdir()):
        if not user_dir.is_dir():
            continue
        history = user_dir / "history.jsonl"
        if not history.exists():
            continue
        user_id = user_dir.name
        with open(history) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                text = _strip_noise(obj.get("display", ""))
                if len(text) < min_length:
                    continue
                rows["user_id"].append(user_id)
                rows["segment__content"].append(text)
                rows["project"].append(_project_name(obj.get("project", "")))
                rows["session_id"].append(obj.get("sessionId", ""))
                rows["timestamp"].append(obj.get("timestamp", 0))

    return daft.from_pydict(rows)


def load_sessions(
    claude_shared_dir: str | Path,
    min_length: int = 30,
    max_per_session: int = 50,
    include_assistant: bool = False,
) -> daft.DataFrame:
    """Load messages from per-session JSONL files across all users.

    Args:
        claude_shared_dir: Path to claude-shared repo
        min_length: Minimum text length to include
        max_per_session: Cap messages per session to avoid huge sessions dominating
        include_assistant: If True, include assistant responses (doubles the data)

    Returns DataFrame with: user_id, segment__content, role, project, session_id, timestamp
    """
    root = Path(claude_shared_dir) / "users"
    rows: dict[str, list] = {
        "user_id": [],
        "segment__content": [],
        "role": [],
        "project": [],
        "session_id": [],
        "timestamp": [],
    }

    allowed_types = {"user"}
    if include_assistant:
        allowed_types.add("assistant")

    for user_dir in sorted(root.iterdir()):
        if not user_dir.is_dir():
            continue
        projects_dir = user_dir / "projects"
        if not projects_dir.exists():
            continue
        user_id = user_dir.name

        for session_file in projects_dir.rglob("*.jsonl"):
            # Skip subagent files
            if "subagent" in str(session_file):
                continue
            project = _project_name(session_file.parent.name)
            session_id = session_file.stem
            count = 0

            with open(session_file) as f:
                for line in f:
                    if count >= max_per_session:
                        break
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    msg_type = obj.get("type", "")
                    if msg_type not in allowed_types:
                        continue
                    msg = obj.get("message", {})
                    if not isinstance(msg, dict):
                        continue
                    text = _strip_noise(_extract_text(msg.get("content", "")))
                    if len(text) < min_length:
                        continue

                    rows["user_id"].append(user_id)
                    rows["segment__content"].append(text)
                    rows["role"].append(msg_type)
                    rows["project"].append(project)
                    rows["session_id"].append(session_id)
                    rows["timestamp"].append(obj.get("timestamp", 0))
                    count += 1

    return daft.from_pydict(rows)


def load_conversations(
    claude_shared_dir: str | Path,
    min_length: int = 30,
    max_per_session: int = 50,
    source: str = "both",
) -> daft.DataFrame:
    """Load conversation data from claude-shared repo.

    Args:
        claude_shared_dir: Path to the cloned claude-shared repo
        min_length: Minimum text length to keep
        max_per_session: Cap per session for session data
        source: "history" (prompts only), "sessions" (full convos), or "both"

    Returns DataFrame with: user_id, segment__content, project, session_id, timestamp
    """
    dfs = []

    if source in ("history", "both"):
        hist = load_history(claude_shared_dir, min_length=min_length)
        # Add role column to match session schema
        hist = hist.with_columns({"role": daft.lit("user")})
        dfs.append(hist)

    if source in ("sessions", "both"):
        sess = load_sessions(
            claude_shared_dir,
            min_length=min_length,
            max_per_session=max_per_session,
        )
        dfs.append(sess)

    if not dfs:
        return daft.from_pydict({
            "user_id": [],
            "segment__content": [],
            "role": [],
            "project": [],
            "session_id": [],
            "timestamp": [],
        })

    result = dfs[0]
    for extra in dfs[1:]:
        result = result.union_all_by_name(extra)
    return result


def _project_name(raw: str) -> str:
    """Extract a readable project name from path-encoded directory names."""
    # "-Users-sam-projects-daft-cloud" → "daft-cloud"
    parts = raw.replace("-", "/").strip("/").split("/")
    # Take last meaningful segment
    for part in reversed(parts):
        if part and part not in ("Users", "git", "work", "projects", "other", "sprint"):
            return part
    return raw or "unknown"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load claude-shared conversations")
    parser.add_argument("path", help="Path to claude-shared repo")
    parser.add_argument("--source", default="history", choices=["history", "sessions", "both"])
    parser.add_argument("--min-length", type=int, default=30)
    parser.add_argument("--max-per-session", type=int, default=50)
    args = parser.parse_args()

    df = load_conversations(
        args.path,
        source=args.source,
        min_length=args.min_length,
        max_per_session=args.max_per_session,
    )
    n = df.count_rows()
    print(f"Loaded {n} segments from {args.path} (source={args.source})")

    # Per-user breakdown
    counts = df.groupby("user_id").agg(
        daft.col("segment__content").count().alias("segments"),
    )
    counts.sort("segments", desc=True).show()

    # Per-project breakdown (top 10)
    proj_counts = df.groupby("project").agg(
        daft.col("segment__content").count().alias("segments"),
    )
    proj_counts.sort("segments", desc=True).limit(10).show()
