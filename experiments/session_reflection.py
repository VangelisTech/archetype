# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reflect over Claude Code history through the supported artifact boundary.

The script discovers local source files, but Archetype owns every safety and
durability concern: each transcript is snapshotted, redacted, stored as a
sanitized artifact, and normalized into a typed ingestion table before
analysis. Raw narrative is
never written as mission Component state.

Usage:
    uv run python experiments/session_reflection.py [--limit N]
        [--projects-dir DIR] [--storage DIR] [--report PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

from daft import col
from daft.functions import lower, startswith

from archetype import ArchetypeRuntime, StorageConfig
from archetype.core.config import StorageBackend
from archetype.missions import MissionWorld
from archetype.missions.trajectories import ClaudeTranscriptSource

CORRECTION_OPENERS = (
    "no ",
    "no,",
    "no.",
    "nope",
    "wait",
    "stop",
    "don't",
    "dont ",
    "actually",
    "that's not",
    "thats not",
    "wrong",
    "not what",
    "undo",
    "revert",
    "hold on",
    "you're not",
    "youre not",
)


def _correction_predicate():
    lowered = lower(col("content"))
    predicate = startswith(lowered, CORRECTION_OPENERS[0])
    for opener in CORRECTION_OPENERS[1:]:
        predicate = predicate | startswith(lowered, opener)
    return (col("role") == "user") & predicate


def _session_files(projects_dir: str | None, limit: int | None) -> list[Path]:
    root = Path(projects_dir) if projects_dir else Path.home() / ".claude" / "projects"
    paths = sorted(root.glob("*/*.jsonl")) if root.exists() else []
    return paths if limit is None else paths[:limit]


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--projects-dir", default=None, help="defaults to ~/.claude/projects")
    parser.add_argument("--storage", default="./session_data")
    parser.add_argument("--report", default="./session_reflection_report.md")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-content-chars", type=int, default=2000)
    args = parser.parse_args()

    paths = _session_files(args.projects_dir, args.limit)
    if not paths:
        print("No sessions found.")
        return

    storage = StorageConfig(
        uri=args.storage,
        namespace="sessions",
        backend=StorageBackend.ICEBERG,
    )
    started = time.time()
    ingested = skipped = redactions = 0

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("claude-sessions", storage=storage)
        missions = MissionWorld(world)
        for path in paths:
            try:
                receipt = await missions.ingest_claude_transcript(
                    ClaudeTranscriptSource(
                        path=path,
                        max_content_chars=args.max_content_chars,
                    )
                )
            except ValueError as exc:
                if str(exc) != "Claude transcript contains no dialogue turns":
                    raise
                skipped += 1
                continue
            ingested += 1
            redactions += receipt.redaction_count

        if not ingested:
            print(f"No dialogue sessions found ({skipped} empty/noise files skipped).")
            return

        rows = await missions.transcript_rows()
        sessions = rows.where(col("row_kind") == "session")
        turns = rows.where(col("row_kind") == "turn")

        totals = sessions.agg(
            col("total_tokens").sum().alias("tokens"),
            col("total_turns").sum().alias("turns"),
            col("duration_seconds").sum().alias("seconds"),
        ).to_pylist()[0]
        by_project = (
            sessions.groupby("project")
            .agg(
                col("trajectory_id").count().alias("sessions"),
                col("total_tokens").sum().alias("tokens"),
            )
            .sort("tokens", desc=True)
            .limit(12)
            .to_pylist()
        )
        by_model = (
            sessions.where(col("model") != "")
            .groupby("model")
            .agg(col("trajectory_id").count().alias("sessions"))
            .sort("sessions", desc=True)
            .to_pylist()
        )
        tool_usage = (
            turns.where(col("role") == "tool_call")
            .groupby("tool_name")
            .agg(col("seq").count().alias("calls"))
            .sort("calls", desc=True)
            .limit(15)
            .to_pylist()
        )
        tool_errors = (
            turns.where(col("error") != "")
            .groupby("tool_name")
            .agg(col("seq").count().alias("errors"))
            .sort("errors", desc=True)
            .limit(10)
            .to_pylist()
        )
        total_tool_results = turns.where(col("role") == "tool_result").count_rows()
        total_tool_errors = turns.where(col("error") != "").count_rows()
        correction_rows = turns.where(_correction_predicate())
        corrections = (
            correction_rows.groupby("trajectory_id", "project")
            .agg(col("seq").count().alias("corrections"))
            .sort("corrections", desc=True)
            .limit(15)
            .to_pylist()
        )
        total_corrections = correction_rows.count_rows()
        total_user_turns = turns.where(col("role") == "user").count_rows()
        biggest = (
            sessions.sort("total_tokens", desc=True)
            .limit(10)
            .select("trajectory_id", "project", "total_tokens", "total_turns")
            .to_pylist()
        )

        lines = [
            "# Session Reflection Report",
            "",
            f"- Sessions: **{ingested}** | Turn rows: **{int(totals['turns'])}** | "
            f"Output tokens: **{int(totals['tokens']):,}** | "
            f"Cumulative session span: **{totals['seconds'] / 3600:.0f}h**",
            f"- Source files skipped as empty/noise: **{skipped}** | "
            f"Secret findings redacted before durability: **{redactions}**",
            f"- Tool errors: **{total_tool_errors}** of {total_tool_results} tool results "
            f"({100 * total_tool_errors / max(total_tool_results, 1):.1f}%)",
            f"- Correction-shaped user turns: **{total_corrections}** of {total_user_turns} "
            f"({100 * total_corrections / max(total_user_turns, 1):.1f}%) — a candidate "
            "heuristic, not a verdict",
            "",
            "## Where the work happens",
            "",
            "| project | sessions | output tokens |",
            "|---|---:|---:|",
            *(
                f"| {row['project']} | {row['sessions']} | {int(row['tokens']):,} |"
                for row in by_project
            ),
            "",
            "## Models",
            "",
            "| model | sessions |",
            "|---|---:|",
            *(f"| {row['model']} | {row['sessions']} |" for row in by_model),
            "",
            "## Tools that carry the work",
            "",
            "| tool | calls |",
            "|---|---:|",
            *(f"| {row['tool_name']} | {row['calls']} |" for row in tool_usage),
            "",
            "## Tool errors",
            "",
            "| tool | errors |",
            "|---|---:|",
            *(f"| {row['tool_name']} | {row['errors']} |" for row in tool_errors),
            "",
            "## Sessions with correction-shaped turns",
            "",
            "| session | project | corrections |",
            "|---|---|---:|",
            *(
                f"| {row['trajectory_id']} | {row['project']} | {row['corrections']} |"
                for row in corrections
            ),
            "",
            "## Biggest sessions",
            "",
            "| session | project | tokens | turns |",
            "|---|---|---:|---:|",
            *(
                f"| {row['trajectory_id']} | {row['project']} | "
                f"{int(row['total_tokens']):,} | {row['total_turns']} |"
                for row in biggest
            ),
            "",
            "---",
            f"World `{world.world_id}`; storage `{args.storage}`. Raw JSONL remains the "
            "source artifact. This report reads world/run-scoped, redacted typed rows.",
        ]
        report = "\n".join(lines)
        Path(args.report).write_text(report, encoding="utf-8")
        print(f"Ingested {ingested} sessions in {time.time() - started:.1f}s")
        print(f"Report written to {args.report}\n")
        print(report)


if __name__ == "__main__":
    asyncio.run(main())
