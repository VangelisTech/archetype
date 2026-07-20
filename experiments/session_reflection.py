# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Self-reflection over Claude Code session history.

Ingests every session transcript under ~/.claude/projects into a world —
one Trajectory header plus normalized TrajectoryTurn rows per session,
all materialized at tick 0 as initial conditions — then reflects with
Daft queries over the corpus: where the time went, which tools carry the
work, where tools errored, and where the human had to push back.

The correction heuristic is deliberately crude (user turns opening with
"no", "wait", "stop", "don't", "actually", ...): it exists to surface
candidate sessions for closer reading and later LLM labeling, not to be
a verdict.

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

from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.claude_sessions import load_claude_sessions
from archetype.missions.trajectories import Trajectory, TrajectoryTurn

CORRECTION_OPENERS = [
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
]


def _correction_predicate():
    lowered = lower(col("trajectoryturn__content"))
    predicate = startswith(lowered, CORRECTION_OPENERS[0])
    for opener in CORRECTION_OPENERS[1:]:
        predicate = predicate | startswith(lowered, opener)
    return (col("trajectoryturn__role") == "user") & predicate


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--projects-dir", default=None, help="defaults to ~/.claude/projects")
    parser.add_argument("--storage", default="./session_data")
    parser.add_argument("--report", default="./session_reflection_report.md")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-content-chars", type=int, default=2000)
    args = parser.parse_args()

    t0 = time.time()
    sessions = load_claude_sessions(
        args.projects_dir, limit=args.limit, max_content_chars=args.max_content_chars
    )
    print(f"Loaded {len(sessions)} sessions in {time.time() - t0:.1f}s")
    if not sessions:
        print("No sessions found.")
        return

    c = ServiceContainer()
    try:
        storage = StorageConfig(uri=args.storage, namespace="sessions")
        world = await c.world_service.create_world(WorldConfig(name="claude-sessions"), storage)

        t0 = time.time()
        n_rows = 0
        for session in sessions:
            for entity_components in session.components():
                await world.create_entity(entity_components)
                n_rows += 1
        await c.simulation_service.step(world.world_id, RunConfig())
        print(f"Materialized {n_rows} rows at tick 0 in {time.time() - t0:.1f}s")

        headers = await world.query_archetype(sig=(Trajectory,), ticks=[0])
        turns = await world.query_archetype(sig=(TrajectoryTurn,), ticks=[0])

        # ── Reflections (Daft queries over the corpus) ─────────────────────
        totals = headers.agg(
            col("trajectory__total_tokens").sum().alias("tokens"),
            col("trajectory__total_turns").sum().alias("turns"),
            col("trajectory__duration_seconds").sum().alias("seconds"),
        ).to_pylist()[0]

        by_project = (
            headers.groupby("trajectory__task_id")
            .agg(
                col("trajectory__trajectory_id").count().alias("sessions"),
                col("trajectory__total_tokens").sum().alias("tokens"),
            )
            .sort("tokens", desc=True)
            .limit(12)
            .to_pylist()
        )

        by_model = (
            headers.where(col("trajectory__model") != "")
            .groupby("trajectory__model")
            .agg(col("trajectory__trajectory_id").count().alias("sessions"))
            .sort("sessions", desc=True)
            .to_pylist()
        )

        tool_usage = (
            turns.where(col("trajectoryturn__role") == "tool_call")
            .groupby("trajectoryturn__tool_name")
            .agg(col("trajectoryturn__seq").count().alias("calls"))
            .sort("calls", desc=True)
            .limit(15)
            .to_pylist()
        )

        tool_errors = (
            turns.where(col("trajectoryturn__error") != "")
            .groupby("trajectoryturn__tool_name")
            .agg(col("trajectoryturn__seq").count().alias("errors"))
            .sort("errors", desc=True)
            .limit(10)
            .to_pylist()
        )
        total_tool_results = turns.where(col("trajectoryturn__role") == "tool_result").count_rows()
        total_tool_errors = turns.where(col("trajectoryturn__error") != "").count_rows()

        corrections = (
            turns.where(_correction_predicate())
            .groupby("trajectoryturn__trajectory_id")
            .agg(col("trajectoryturn__seq").count().alias("corrections"))
            .sort("corrections", desc=True)
            .limit(15)
            .to_pylist()
        )
        total_corrections = turns.where(_correction_predicate()).count_rows()
        total_user_turns = turns.where(col("trajectoryturn__role") == "user").count_rows()

        biggest = (
            headers.sort("trajectory__total_tokens", desc=True)
            .limit(10)
            .select(
                "trajectory__trajectory_id",
                "trajectory__task_id",
                "trajectory__total_tokens",
                "trajectory__total_turns",
            )
            .to_pylist()
        )

        # ── Report ──────────────────────────────────────────────────────────
        project_of = {s.trajectory.trajectory_id: s.project for s in sessions}
        lines = [
            "# Session Reflection Report",
            "",
            f"- Sessions: **{len(sessions)}**  |  Turn rows: **{int(totals['turns'])}**  |  "
            f"Output tokens: **{int(totals['tokens']):,}**  |  "
            f"Cumulative session span: **{totals['seconds'] / 3600:.0f}h** (parallel agents, includes idle)",
            f"- Tool errors: **{total_tool_errors}** of {total_tool_results} tool results "
            f"({100 * total_tool_errors / max(total_tool_results, 1):.1f}%)",
            f"- Correction-shaped user turns: **{total_corrections}** of {total_user_turns} "
            f"({100 * total_corrections / max(total_user_turns, 1):.1f}%) — crude heuristic, "
            "candidates for labeling",
            "",
            "## Where the work happens (top projects by tokens)",
            "",
            "| project | sessions | output tokens |",
            "|---|---|---|",
        ]
        lines += [
            f"| {r['trajectory__task_id']} | {r['sessions']} | {int(r['tokens']):,} |"
            for r in by_project
        ]
        lines += ["", "## Models", "", "| model | sessions |", "|---|---|"]
        lines += [f"| {r['trajectory__model']} | {r['sessions']} |" for r in by_model]
        lines += ["", "## Tools that carry the work", "", "| tool | calls |", "|---|---|"]
        lines += [f"| {r['trajectoryturn__tool_name']} | {r['calls']} |" for r in tool_usage]
        lines += ["", "## Tool errors", "", "| tool | errors |", "|---|---|"]
        lines += [f"| {r['trajectoryturn__tool_name']} | {r['errors']} |" for r in tool_errors]
        lines += [
            "",
            "## Sessions with the most correction-shaped turns",
            "",
            "| session | project | corrections |",
            "|---|---|---|",
        ]
        lines += [
            f"| {r['trajectoryturn__trajectory_id'][:8]} | "
            f"{project_of.get(r['trajectoryturn__trajectory_id'], '?')} | {r['corrections']} |"
            for r in corrections
        ]
        lines += [
            "",
            "## Biggest sessions",
            "",
            "| session | project | tokens | turns |",
            "|---|---|---|---|",
        ]
        lines += [
            f"| {r['trajectory__trajectory_id'][:8]} | {r['trajectory__task_id']} | "
            f"{int(r['trajectory__total_tokens']):,} | {r['trajectory__total_turns']} |"
            for r in biggest
        ]
        lines += [
            "",
            "---",
            f"Corpus world: `{world.world_id}` (run `{world.run_id}`), storage `{args.storage}` — "
            "the rows are durable; every query above is repeatable, and the corpus is forkable.",
        ]

        report = "\n".join(lines)
        Path(args.report).write_text(report)
        print(f"\nReport written to {args.report}\n")
        print(report)
    finally:
        await c.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
