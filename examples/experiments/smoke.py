#!/usr/bin/env python3
"""Run the autoresearch loop once. End to end."""

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path

from archetype.app.container import ServiceContainer
from archetype.core.archetype import Archetype
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments import (
    BranchHead,
    Experiment,
    Result,
    Run,
    ingest_runner_state,
)


async def main():
    tmp = Path(tempfile.mkdtemp())
    container = ServiceContainer()

    # 1. Create the world
    world = await container.world_service.create_world(
        WorldConfig(name="smoke"),
        StorageConfig(uri=str(tmp / "store"), namespace="experiments"),
    )
    print(f"World created: {world.world_id}")

    # 2. Experiment
    exp = Experiment.make(
        name="archetype-smoke",
        repo_url="https://github.com/VangelisTech/archetype",
        metadata={"version": "v0"},
    )
    await world.create_entity([exp])
    print("Experiment: archetype-smoke")

    # 3. Stub runner — write a fake state.db
    db = tmp / "state.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE agents ("
        "agent_id TEXT PRIMARY KEY, vm_name TEXT, status TEXT, harness TEXT, "
        "repo_url TEXT, branch TEXT, task TEXT, started_at TEXT, finished_at TEXT, "
        "agent_name TEXT, workspace_path TEXT)"
    )
    conn.execute(
        "INSERT INTO agents VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            "smoke-001", "mb-vm-smoke", "stopped", "claude-code",
            "https://github.com/VangelisTech/archetype", "main",
            "Add a test that asserts Run.get_prefix() == 'run__' (budget: 5 turns)",
            "2026-04-11T12:00:00+00:00", "2026-04-11T12:03:00+00:00",
            "archetype-smoke-claude-code", str(tmp / "ws"),
        ),
    )
    conn.commit()
    conn.close()

    # 4. Ingest
    count = await ingest_runner_state(world, db)
    print(f"Ingested {count} run(s) from runner state.db")

    # 5. Eval — run pytest on this repo as the eval harness
    import subprocess

    proc = subprocess.run(
        ["uv", "run", "pytest", "tests/experiments/test_components.py", "-q"],
        capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[2]),
    )
    pass_rate = 1.0 if proc.returncode == 0 else 0.0
    print(f"Eval: pytest returned {proc.returncode}, pass_rate={pass_rate}")

    result = Result.make(
        run_id="smoke-001",
        evaluator="pytest",
        outputs={"returncode": proc.returncode, "pass_rate": pass_rate, "stdout": proc.stdout[-500:]},
    )
    await world.create_entity([result])

    # 6. BranchHead
    head = BranchHead.make(
        experiment_name="archetype-smoke",
        commit_hash="HEAD",
        run_id="smoke-001",
        descriptor={"pass_rate": pass_rate},
    )
    await world.create_entity([head])

    # 7. Persist
    rc = RunConfig(num_steps=1)
    await container.simulation_service.step(world.world_id, rc)
    print("Simulation stepped. Entities persisted.")

    # 8. Query the fleet
    tick = world.tick - 1
    run_df = await world.query_archetype(
        Archetype.sig_from_components([Run()]), rc, ticks=[tick],
    )
    rows = run_df.to_pylist()
    print(f"\n--- Fleet table ({len(rows)} run(s)) ---")
    for r in rows:
        print(f"  {r['run__run_id']}  status={r['run__status']}  harness={r['run__harness']}")

    head_df = await world.query_archetype(
        Archetype.sig_from_components([BranchHead()]), rc, ticks=[tick],
    )
    for h in head_df.to_pylist():
        d = json.loads(h["branchhead__descriptor_json"])
        print(f"\n--- BranchHead ---")
        print(f"  experiment: {h['branchhead__experiment_name']}")
        print(f"  commit:     {h['branchhead__commit_hash']}")
        print(f"  descriptor: {d}")

    await container.shutdown()
    print("\nLoop closed.")


if __name__ == "__main__":
    asyncio.run(main())
