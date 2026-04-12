#!/usr/bin/env python
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Ingest Claude Code sessions into an Archetype world.

One-shot dogfood script. Reads every Claude Code session JSONL for the
given project directory (default: this repo), turns each one into a
``Trajectory`` Component, and spawns them as entities in a new Archetype
world backed by LanceDB. Skips labeling entirely — no API key required,
no LLM calls, no pipeline run step.

Re-running is safe: the default world name is timestamped, so each run
creates a fresh world. Use ``--clean`` to wipe the storage directory first
if you want to reset history.

Usage::

    uv run python scripts/ingest_claude_sessions.py
    uv run python scripts/ingest_claude_sessions.py --cwd /path/to/other/repo
    uv run python scripts/ingest_claude_sessions.py --all-projects
    uv run python scripts/ingest_claude_sessions.py --clean
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
from datetime import datetime
from pathlib import Path

from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.trajectories.loaders import (
    discover_sessions,
    discover_sessions_for_cwd,
    from_claude_session,
)


def _status(msg: str) -> None:
    """Progress messages go to stderr so stdout stays clean for piping."""
    print(msg, file=sys.stderr)


async def ingest(
    project_cwd: str | None,
    all_projects: bool,
    storage_uri: str,
    world_name: str,
    clean: bool,
) -> None:
    # 0. Optional clean — wipe prior trajectory storage so re-runs don't pile
    # up duplicate worlds against the same on-disk partitions.
    if clean:
        traj_dir = Path(storage_uri) / "trajectories"
        if traj_dir.exists():
            _status(f"Removing {traj_dir} (--clean)")
            shutil.rmtree(traj_dir, ignore_errors=True)

    # 1. Discover sessions
    if all_projects:
        paths = discover_sessions()
        scope = "~/.claude/projects (all projects)"
    else:
        cwd = project_cwd or str(Path.cwd())
        paths = discover_sessions_for_cwd(cwd)
        scope = cwd

    if not paths:
        _status(f"No Claude Code sessions found for {scope}.")
        return

    _status(f"Found {len(paths)} session file(s) in {scope}.")

    # 2. Load each session into a Trajectory Component
    trajectories = []
    for path in paths:
        try:
            traj = from_claude_session(path)
            trajectories.append(traj)
            _status(
                f"  loaded {path.name[:8]}  "
                f"turns={traj.total_turns:5d}  "
                f"tokens={traj.total_tokens:7d}  "
                f"dur={traj.duration_seconds:8.1f}s"
            )
        except Exception as e:  # noqa: BLE001
            _status(f"  skipped {path.name}: {type(e).__name__}: {e}")

    if not trajectories:
        _status("No trajectories loaded.")
        return

    # 3. Spin up an Archetype world and spawn one entity per Trajectory.
    # We bypass TrajectoryPipeline entirely because it requires labels +
    # calls OpenAI on .run(). We just want the Trajectory rows in Lance so
    # downstream tools (e.g. daft SQL) can query them.
    container = ServiceContainer()
    try:
        world = await container.world_service.create_world(
            WorldConfig(name=world_name),
            StorageConfig(uri=storage_uri, namespace="trajectories"),
        )

        # Direct spawn — no commands, no broker, no labels
        for traj in trajectories:
            await world.create_entity([traj])

        # One step to materialize spawns and persist to LanceDB.
        # No processors are registered, so nothing else runs.
        await container.simulation_service.step(
            world.world_id,
            RunConfig(num_steps=1),
        )

        _status("")
        _status(f"Ingested {len(trajectories)} trajectories into world {world.world_id}.")
        _status(f"World name: {world_name}")
        _status(f"Storage:    {storage_uri}/trajectories/lance/")

        # Resolve the actual Lance table path so the printed hint is
        # copy-pasteable instead of a placeholder.
        lance_dir = Path(storage_uri) / "trajectories" / "lance"
        candidates = sorted(lance_dir.glob("a_*.lance")) if lance_dir.exists() else []
        table_path = str(candidates[-1]) if candidates else f"{lance_dir}/<a_*.lance>"

        _status("")
        _status("Query with any Daft-capable tool, e.g.:")
        _status(
            f'  uv run python -c "'
            f"import daft; "
            f"daft.read_lance('{table_path}')"
            f".select('trajectory__trajectory_id','trajectory__total_turns',"
            f"'trajectory__total_tokens','trajectory__duration_seconds')"
            f'.show()"'
        )
    finally:
        await container.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Claude Code sessions into an Archetype world."
    )
    parser.add_argument(
        "--cwd",
        help="Ingest sessions for a specific project cwd (default: current dir)",
    )
    parser.add_argument(
        "--all-projects",
        action="store_true",
        help="Ingest sessions from every project under ~/.claude/projects",
    )
    parser.add_argument(
        "--storage-uri",
        default="./trajectory_data",
        help="Storage URI for the trajectory world (default: ./trajectory_data)",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="World name (default: claude-sessions-<timestamp>)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Wipe <storage-uri>/trajectories/ before ingest. "
        "Use this to reset history and avoid accumulating prior runs.",
    )
    args = parser.parse_args()

    # Default to a timestamped world name so re-runs don't collide in the
    # registry. Review fix: the previous default `claude-sessions` silently
    # created duplicate worlds with the same name on every invocation.
    world_name = args.name or datetime.now().strftime("claude-sessions-%Y%m%d-%H%M%S")

    asyncio.run(
        ingest(
            project_cwd=args.cwd,
            all_projects=args.all_projects,
            storage_uri=args.storage_uri,
            world_name=world_name,
            clean=args.clean,
        )
    )


if __name__ == "__main__":
    main()
