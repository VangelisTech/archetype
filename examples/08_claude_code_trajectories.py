# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Claude Code Trajectory Capture
==============================

Spawn one entity per prompt, run Claude Code on each row in parallel via the
``ClaudeCodeProcessor`` (a ``daft.cls``-backed processor that pipes the
``claude`` CLI's ``--output-format stream-json`` stdout into structured turns),
then export the resulting trajectories as AgentPrism-ready JSON.

Run:
    uv run python examples/08_claude_code_trajectories.py

Requires the ``claude`` CLI on PATH for live capture. Without it the
processor records ``outcome="skipped: no claude CLI on PATH"`` and writes
empty trajectories — useful for shape-checking the pipeline in CI.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from daft import col

from archetype import ArchetypeRuntime
from archetype.contrib.agentprism import claude_code_run_to_trace_record
from archetype.contrib.claude_code import (
    ClaudeCodeProcessor,
    ClaudeCodePrompt,
    ClaudeCodeRun,
)
from archetype.contrib.logfire_observer import logfire_hooks
from archetype.core.config import StorageConfig

PROMPTS = [
    "Summarize this repository's CLAUDE.md in one sentence.",
    "List the top-level directories under src/archetype/ as a comma-separated string.",
    "What does the SamplingProcessor do? Answer in one short paragraph.",
]


async def main() -> None:
    out_dir = Path(os.environ.get("ARCHETYPE_AGENTPRISM_OUT", "./agentprism_traces"))
    out_dir.mkdir(parents=True, exist_ok=True)

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "claude-code-traces",
            storage=StorageConfig(uri="./claude_code_data", namespace="trajectories"),
            processors=[ClaudeCodeProcessor()],
            hooks=logfire_hooks(),
        )

        for prompt in PROMPTS:
            await world.spawn(
                ClaudeCodePrompt(
                    prompt=prompt,
                    cwd=os.getcwd(),
                    max_turns=4,
                ),
                ClaudeCodeRun(),
            )

        await world.run(steps=1)

        df = await world.query(ClaudeCodePrompt, ClaudeCodeRun)
        info = await world.info()
        latest = df.where(col("tick") == info.tick - 1)
        rows = latest.collect().to_pylist()

    print(f"Captured {len(rows)} trajectories")
    for row in rows:
        record = claude_code_run_to_trace_record(row)
        path = out_dir / f"{record['id'] or 'run'}-{row.get('entity_id', 'x')}.json"
        path.write_text(json.dumps(record, indent=2))
        print(
            f"  → {path} ({len(record['spans'])} spans, outcome={row.get('claudecoderun__outcome')!r})"
        )

    print()
    print("Drop the generated JSON files into AgentPrism's <TraceList> / <TraceDetailView>")
    print("components, or POST them to your OTel backend via emit_replay_spans().")


if __name__ == "__main__":
    asyncio.run(main())
