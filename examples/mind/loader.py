# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Load .claude conversation JSONLs into Segment entities."""

import json
from pathlib import Path

from .components import Extraction, Perspective, Segment, Voice


def parse_content(content) -> str:
    """Extract text from message content (string or array of parts)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                texts.append(part.get("text", ""))
        return "\n".join(texts)
    return ""


def load_conversations(
    directory: str,
    max_segments: int = 200,
    min_content_length: int = 20,
) -> list[list]:
    """
    Load user messages from .claude conversation JSONL files.

    Returns a list of component lists, one per entity:
        [[Segment(...), Extraction(), Voice(), Perspective()], ...]
    """
    entities = []
    jsonl_dir = Path(directory)

    for jsonl_path in sorted(jsonl_dir.glob("*.jsonl")):
        session_id = jsonl_path.stem
        turn_index = 0

        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    print(f"  warning: skipped malformed JSON in {jsonl_path.name}")
                    continue

                # Only process user messages
                if record.get("type") != "user":
                    continue

                msg = record.get("message", {})
                if msg.get("role") != "user":
                    continue

                content = parse_content(msg.get("content", ""))

                # Skip trivial messages
                if len(content) < min_content_length:
                    continue

                # Skip system injections and hook outputs
                if content.startswith("<system-reminder>"):
                    continue
                if "[Request interrupted" in content:
                    continue

                entities.append(
                    [
                        Segment(
                            role="user",
                            content=content[:2000],  # cap for LLM context
                            session_id=session_id,
                            timestamp=record.get("timestamp", ""),
                            turn_index=turn_index,
                        ),
                        Extraction(),
                        Voice(),
                        Perspective(),
                    ]
                )
                turn_index += 1

                if len(entities) >= max_segments:
                    return entities

    return entities
