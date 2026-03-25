#!/usr/bin/env python3
"""Export Claude transcript events to Agent Trace records (best effort).

This script scans transcript JSONL files and emits Agent Trace v0.1.0 records.
It focuses on extracting file-attribution ranges from tool use events that
perform edits. Some tools do not expose exact line ranges, so those entries
fall back to line range (1, 1).
"""

from __future__ import annotations

import argparse
import json
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PATCH_HEADER_RE = re.compile(r"^\*\*\* (?:Update|Add) File: (.+)$")
UNIFIED_HUNK_RE = re.compile(r"@@\s*-[0-9]+(?:,[0-9]+)?\s+\+([0-9]+)(?:,([0-9]+))?\s*@@")

PATH_KEYS = {
    "path",
    "file_path",
    "filepath",
    "target_file",
    "target_notebook",
    "filename",
}

PATCH_KEYS = {"patch", "input", "changes", "diff", "new_string", "old_string"}

LINE_START_KEYS = {"start_line", "line_start", "from_line", "offset"}
LINE_END_KEYS = {"end_line", "line_end", "to_line"}
LINE_LEN_KEYS = {"length", "line_count", "limit"}

EDIT_TOOL_NAMES = {
    "ApplyPatch",
    "Edit",
    "Write",
    "MultiEdit",
    "EditNotebook",
}


@dataclass
class SessionContext:
    session_id: str | None
    cwd: str | None
    tool_name: str | None
    tool_version: str | None
    model_id: str | None
    timestamp: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Agent Trace records from Claude transcripts.")
    parser.add_argument(
        "--source-dir",
        default=str(Path("~/.claude/projects").expanduser()),
        help="Directory containing transcript JSONL files.",
    )
    parser.add_argument(
        "--output",
        default="data/agent_trace/trace_records.jsonl",
        help="Output JSONL path for Agent Trace records.",
    )
    parser.add_argument(
        "--include-subagents",
        action="store_true",
        help="Include transcript files from /subagents directories.",
    )
    return parser.parse_args()


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file_obj:
        for line in file_obj:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def normalize_model_id(model_name: str | None) -> str | None:
    if not model_name:
        return None
    name = model_name.strip()
    if "/" in name:
        return name
    if name.startswith("claude-"):
        return f"anthropic/{name}"
    return name


def parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def walk_values(obj: Any) -> list[tuple[str, Any]]:
    items: list[tuple[str, Any]] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            items.append((key, value))
            items.extend(walk_values(value))
    elif isinstance(obj, list):
        for value in obj:
            items.extend(walk_values(value))
    return items


def extract_paths(input_obj: dict[str, Any]) -> list[str]:
    paths: set[str] = set()
    for key, value in walk_values(input_obj):
        if key in PATH_KEYS and isinstance(value, str) and value.strip():
            paths.add(value.strip())
    return sorted(paths)


def infer_range_from_input(input_obj: dict[str, Any]) -> tuple[int, int] | None:
    values = dict(walk_values(input_obj))
    start = None
    end = None
    length = None

    for key in LINE_START_KEYS:
        if isinstance(values.get(key), int):
            start = int(values[key])
            break
    for key in LINE_END_KEYS:
        if isinstance(values.get(key), int):
            end = int(values[key])
            break
    for key in LINE_LEN_KEYS:
        if isinstance(values.get(key), int):
            length = int(values[key])
            break

    if start is None:
        return None
    # Offset is sometimes 0-indexed.
    if start <= 0:
        start = 1
    if end is not None:
        end = max(start, end)
        return (start, end)
    if length is not None and length > 0:
        return (start, start + length - 1)
    return (start, start)


def extract_patch_text(input_obj: dict[str, Any]) -> str | None:
    for key, value in walk_values(input_obj):
        if key in PATCH_KEYS and isinstance(value, str) and "*** Begin Patch" in value:
            return value
    return None


def extract_patch_ranges(patch_text: str) -> dict[str, list[tuple[int, int]]]:
    by_file: dict[str, list[tuple[int, int]]] = defaultdict(list)
    current_file: str | None = None
    seen_hunk_for_file: set[str] = set()

    for line in patch_text.splitlines():
        header_match = PATCH_HEADER_RE.match(line.strip())
        if header_match:
            current_file = header_match.group(1).strip()
            continue
        if current_file is None:
            continue

        hunk_match = UNIFIED_HUNK_RE.search(line)
        if hunk_match:
            start = int(hunk_match.group(1))
            length = int(hunk_match.group(2) or "1")
            end = max(start, start + length - 1)
            by_file[current_file].append((start, end))
            seen_hunk_for_file.add(current_file)

    # If file sections exist but no unified hunk headers, still emit fallback range.
    for line in patch_text.splitlines():
        header_match = PATCH_HEADER_RE.match(line.strip())
        if header_match:
            file_path = header_match.group(1).strip()
            if file_path not in seen_hunk_for_file:
                by_file[file_path].append((1, 1))

    return by_file


def normalize_path(path_value: str, cwd: str | None) -> str:
    value = path_value.strip()
    if not value:
        return value

    p = Path(value)
    if p.is_absolute():
        if cwd:
            try:
                return str(p.relative_to(Path(cwd)))
            except ValueError:
                return str(p)
        return str(p)
    return value


def merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not ranges:
        return []
    ordered = sorted((max(1, s), max(1, e)) for s, e in ranges)
    merged: list[list[int]] = [[ordered[0][0], ordered[0][1]]]
    for start, end in ordered[1:]:
        prev = merged[-1]
        if start <= prev[1] + 1:
            prev[1] = max(prev[1], end)
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def extract_tool_uses(event: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    tool_uses: list[tuple[str, dict[str, Any]]] = []
    if event.get("type") != "assistant":
        return tool_uses
    message = event.get("message")
    if not isinstance(message, dict):
        return tool_uses
    content = message.get("content")
    if not isinstance(content, list):
        return tool_uses
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "tool_use":
            continue
        name = item.get("name")
        input_obj = item.get("input")
        if isinstance(name, str) and isinstance(input_obj, dict):
            tool_uses.append((name, input_obj))
    return tool_uses


def context_from_events(events: list[dict[str, Any]], fallback_timestamp: str) -> SessionContext:
    session_id = None
    cwd = None
    tool_name = "claude-code"
    tool_version = None
    model_id = None
    timestamp = fallback_timestamp

    latest_time: datetime | None = None
    for event in events:
        session_id = session_id or event.get("sessionId")
        cwd = cwd or event.get("cwd")
        if tool_version is None and isinstance(event.get("version"), str):
            tool_version = event["version"]

        msg = event.get("message")
        if isinstance(msg, dict):
            if model_id is None and isinstance(msg.get("model"), str):
                model_id = normalize_model_id(msg.get("model"))

        dt = parse_iso_timestamp(event.get("timestamp"))
        if dt and (latest_time is None or dt > latest_time):
            latest_time = dt
            timestamp = dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

    return SessionContext(
        session_id=session_id,
        cwd=cwd,
        tool_name=tool_name,
        tool_version=tool_version,
        model_id=model_id,
        timestamp=timestamp,
    )


def record_from_session(path: Path, events: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None

    ctx = context_from_events(events, datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"))
    file_ranges: dict[str, list[tuple[int, int]]] = defaultdict(list)

    for event in events:
        for tool_name, input_obj in extract_tool_uses(event):
            if tool_name not in EDIT_TOOL_NAMES:
                continue

            patch_text = extract_patch_text(input_obj)
            if patch_text:
                for changed_file, ranges in extract_patch_ranges(patch_text).items():
                    normalized = normalize_path(changed_file, ctx.cwd)
                    file_ranges[normalized].extend(ranges)
                continue

            paths = extract_paths(input_obj)
            if not paths:
                continue
            inferred = infer_range_from_input(input_obj) or (1, 1)
            for raw_path in paths:
                normalized = normalize_path(raw_path, ctx.cwd)
                file_ranges[normalized].append(inferred)

    if not file_ranges:
        return None

    conversation_url = (
        f"claude://session/{ctx.session_id}"
        if ctx.session_id
        else f"file://{path.as_posix()}"
    )

    contributor: dict[str, Any] = {"type": "ai"}
    if ctx.model_id:
        contributor["model_id"] = ctx.model_id

    files: list[dict[str, Any]] = []
    for changed_file in sorted(file_ranges):
        merged = merge_ranges(file_ranges[changed_file])
        ranges = [
            {"start_line": start, "end_line": end}
            for start, end in merged
        ]
        files.append(
            {
                "path": changed_file,
                "conversations": [
                    {
                        "url": conversation_url,
                        "contributor": contributor,
                        "ranges": ranges,
                    }
                ],
            }
        )

    record: dict[str, Any] = {
        "version": "0.1.0",
        "id": str(uuid.uuid4()),
        "timestamp": ctx.timestamp,
        "tool": {
            "name": ctx.tool_name or "claude-code",
            "version": ctx.tool_version or "unknown",
        },
        "files": files,
        "metadata": {
            "source_transcript": str(path),
            "generator": "export_agent_trace.py",
            "range_mode": "best_effort",
        },
    }

    return record


def main() -> None:
    args = parse_args()

    source_dir = Path(args.source_dir).expanduser().resolve()
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    transcript_paths = sorted(source_dir.rglob("*.jsonl"))
    if not args.include_subagents:
        transcript_paths = [p for p in transcript_paths if "subagents" not in p.parts]

    records: list[dict[str, Any]] = []
    skipped_no_events = 0
    skipped_no_edits = 0
    for transcript_path in transcript_paths:
        events = iter_jsonl(transcript_path)
        if not events:
            skipped_no_events += 1
            continue
        record = record_from_session(transcript_path, events)
        if record is None:
            skipped_no_edits += 1
            continue
        records.append(record)

    with output_path.open("w", encoding="utf-8") as file_obj:
        for record in records:
            file_obj.write(json.dumps(record, ensure_ascii=True) + "\n")

    print(f"source_dir={source_dir}")
    print(f"output={output_path}")
    print(f"records={len(records)}")
    print(f"transcripts_scanned={len(transcript_paths)}")
    print(f"skipped_no_events={skipped_no_events}")
    print(f"skipped_no_detected_edits={skipped_no_edits}")


if __name__ == "__main__":
    main()
