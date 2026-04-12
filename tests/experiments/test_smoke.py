# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end autoresearch smoke test.

Proves that Experiment, Run, Result, and BranchHead Components travel
through the full round trip:

    1. Create a real archetype world (LanceDB-backed)
    2. Spawn an Experiment entity
    3. Simulate archetype-runner producing one completed run by writing
       a SQLite file that matches runner's state.py schema exactly
    4. Ingest that SQLite into the world via ``ingest_runner_state``
    5. Spawn Result and BranchHead entities representing the outcome
    6. Step the simulation once to persist
    7. Query each Component type back and assert the full payload
       round-tripped correctly, including the JSON-encoded dicts

The autoresearch loop closed once. Every time CI runs, it closes again.

Note on the runner:
    ``archetype-runner`` is not installed locally on CI or typical dev
    machines, and its gVisor sandbox is Linux-only. The runner is
    **stubbed** here by writing the same SQLite schema runner writes —
    the ingestion path (``load_runner_state_db`` → ``Run`` Component →
    ``world.create_entity``) is executed for real against that fixture.
    When runner adoption (see PR description) lands, replacing the
    fixture with a real runner invocation is a one-line change.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

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


def _write_fake_runner_state_db(path: Path, rows: list[tuple]) -> None:
    """Write a SQLite file whose schema matches archetype-runner's state.py.

    This is the stub for the runner: ``ingest_runner_state`` reads it
    the same way it would read the real ``~/.archetype-runner/state.db``.
    """
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE agents (
            agent_id TEXT PRIMARY KEY,
            vm_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'booting',
            harness TEXT NOT NULL,
            repo_url TEXT NOT NULL,
            branch TEXT NOT NULL,
            task TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            agent_name TEXT NOT NULL DEFAULT '',
            workspace_path TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.executemany(
        "INSERT INTO agents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


class TestAutoresearchSmoke:
    """First end-to-end turn of the autoresearch loop.

    Each assertion here is a *bug barrier*: if it fails in the future,
    something in the experiment Component contract, the ingestion path,
    the simulation persistence layer, or the query layer has regressed.
    """

    @pytest.mark.asyncio
    async def test_loop_closes_once(self, tmp_path):
        container = ServiceContainer()
        try:
            storage = StorageConfig(
                uri=str(tmp_path / "world_store"),
                namespace="experiments_smoke",
            )
            world = await container.world_service.create_world(
                WorldConfig(name="smoke"),
                storage,
            )

            # ----------------------------------------------------------
            # Phase 1 — create the Experiment entity
            # ----------------------------------------------------------
            exp = Experiment.make(
                name="archetype-smoke-v0",
                repo_url="https://github.com/VangelisTech/archetype",
                branch="main",
                metadata={"purpose": "smoke test", "version": "v0"},
            )
            exp_entity_id = await world.create_entity([exp])
            assert exp_entity_id >= 0

            # ----------------------------------------------------------
            # Phase 2 — stub runner: write a fake state.db with 1 run
            # ----------------------------------------------------------
            fake_db = tmp_path / "state.db"
            _write_fake_runner_state_db(
                fake_db,
                rows=[
                    (
                        "smoke-001",
                        "mb-vm-smoke",
                        "stopped",
                        "claude-code",
                        "https://github.com/VangelisTech/archetype",
                        "main",
                        "Add a docstring to archetype.experiments.Run",
                        "2026-04-10T12:00:00+00:00",
                        "2026-04-10T12:05:00+00:00",
                        "archetype-smoke-v0-claude-code",
                        str(tmp_path / "workspace"),
                    ),
                ],
            )

            # ----------------------------------------------------------
            # Phase 3 — ingest the legacy-shaped run into the world
            # ----------------------------------------------------------
            # The loader reads runner's current 11-column state.db
            # schema, which predates turn_budget. Rows ingested this
            # way land as unbounded (turn_budget=0). Production
            # autoresearch must never ingest unbounded rows; this one
            # is OK because it's a smoke fixture.
            ingest_count = await ingest_runner_state(world, fake_db)
            assert ingest_count == 1

            # ----------------------------------------------------------
            # Phase 3b — spawn a bounded run directly (evolved contract)
            # ----------------------------------------------------------
            # This is how processors will create runs once the fleet
            # layer exists: construct Run with turn_budget > 0 and
            # create_entity it. We prove the budget fields round-trip
            # through the persist/query path below.
            bounded_run = Run(
                run_id="smoke-002-bounded",
                experiment_name="archetype-smoke-v0",
                vm_name="mb-vm-smoke-2",
                status="stopped",
                harness="claude-code",
                repo_url="https://github.com/VangelisTech/archetype",
                branch="main",
                task="Add a trivial test that asserts Run.get_prefix() == 'run__'",
                turn_budget=5,
                turn_count=3,  # agent finished under budget
                started_at_ms=1_775_822_700_000,
                finished_at_ms=1_775_822_820_000,
                agent_name="archetype-smoke-v0-claude-code",
                workspace_path=str(tmp_path / "workspace-2"),
                commit_hash="bounded1234",
            )
            assert bounded_run.is_bounded is True
            assert bounded_run.exhausted_budget is False
            await world.create_entity([bounded_run])

            # ----------------------------------------------------------
            # Phase 4 — write a Result describing the eval outcome
            # ----------------------------------------------------------
            result = Result.make(
                run_id="smoke-001",
                evaluator="pytest",
                outputs={
                    "returncode": 0,
                    "passed": 48,
                    "failed": 0,
                    "pass_rate": 1.0,
                },
            )
            await world.create_entity([result])

            # ----------------------------------------------------------
            # Phase 5 — declare the first BranchHead (seeds the frontier)
            # ----------------------------------------------------------
            head = BranchHead.make(
                experiment_name="archetype-smoke-v0",
                commit_hash="abc123def456",
                run_id="smoke-001",
                descriptor={
                    "pass_rate": 1.0,
                    "reasoning": "initial smoke — first pass of the loop",
                },
            )
            await world.create_entity([head])

            # ----------------------------------------------------------
            # Phase 6 — step the simulation to materialize and persist
            # ----------------------------------------------------------
            # One RunConfig shared across step() and every query() call
            # below. The query path needs the same run_id the updater
            # stamped onto persisted rows, and it needs an explicit
            # tick — query defaults filter to world.tick which is one
            # past where data lives after stepping.
            #
            # Note: we call simulation_service.step(), NOT run(). run()
            # constructs a fresh RunConfig(num_steps=1) for each step
            # and ignores the one we pass, which means the persisted
            # run_id would differ from our rc.run_id and all queries
            # that filter by run_id would return zero rows.
            rc = RunConfig(num_steps=1)
            await container.simulation_service.step(world.world_id, rc)
            persisted_tick = world.tick - 1

            # ----------------------------------------------------------
            # Phase 7 — query each Component type back from the world
            # ----------------------------------------------------------
            # Experiment
            exp_df = await world.query_archetype(
                Archetype.sig_from_components([Experiment()]),
                rc,
                ticks=[persisted_tick],
            )
            exp_rows = exp_df.to_pylist()
            assert len(exp_rows) >= 1, "Experiment entity was not persisted"
            exp_row = next(
                r for r in exp_rows if r.get("experiment__name") == "archetype-smoke-v0"
            )
            assert exp_row["experiment__repo_url"] == "https://github.com/VangelisTech/archetype"
            assert exp_row["experiment__branch"] == "main"
            exp_metadata = json.loads(exp_row["experiment__metadata_json"])
            assert exp_metadata["purpose"] == "smoke test"
            assert exp_metadata["version"] == "v0"

            # Run — assert both the legacy-ingested row and the
            # bounded directly-spawned row landed, and that the
            # turn_budget / turn_count fields round-tripped on the
            # bounded one.
            run_df = await world.query_archetype(
                Archetype.sig_from_components([Run()]),
                rc,
                ticks=[persisted_tick],
            )
            run_rows = run_df.to_pylist()
            assert len(run_rows) >= 2, (
                f"Expected 2 Run entities (legacy + bounded), got {len(run_rows)}"
            )

            # Legacy-shaped run from the SQLite loader
            legacy_row = next(r for r in run_rows if r.get("run__run_id") == "smoke-001")
            assert legacy_row["run__status"] == "stopped"
            assert legacy_row["run__harness"] == "claude-code"
            assert legacy_row["run__vm_name"] == "mb-vm-smoke"
            assert legacy_row["run__started_at_ms"] > 0
            assert legacy_row["run__finished_at_ms"] > legacy_row["run__started_at_ms"]
            # Loader doesn't set budget fields — they come through as 0
            assert legacy_row["run__turn_budget"] == 0
            assert legacy_row["run__turn_count"] == 0

            # Bounded run — this is where the turn_budget contract
            # evolution lives. If these assertions ever fail, something
            # has stripped the new fields from the persist/query path.
            bounded_row = next(
                r for r in run_rows if r.get("run__run_id") == "smoke-002-bounded"
            )
            assert bounded_row["run__turn_budget"] == 5, (
                "turn_budget did not round-trip through the persist/query layer"
            )
            assert bounded_row["run__turn_count"] == 3
            assert bounded_row["run__commit_hash"] == "bounded1234"

            # Result
            result_df = await world.query_archetype(
                Archetype.sig_from_components([Result()]),
                rc,
                ticks=[persisted_tick],
            )
            result_rows = result_df.to_pylist()
            assert len(result_rows) >= 1, "Result entity was not persisted"
            result_row = next(
                r for r in result_rows if r.get("result__run_id") == "smoke-001"
            )
            assert result_row["result__evaluator"] == "pytest"
            outputs = json.loads(result_row["result__outputs_json"])
            assert outputs["pass_rate"] == 1.0
            assert outputs["passed"] == 48

            # BranchHead
            head_df = await world.query_archetype(
                Archetype.sig_from_components([BranchHead()]),
                rc,
                ticks=[persisted_tick],
            )
            head_rows = head_df.to_pylist()
            assert len(head_rows) >= 1, "BranchHead entity was not persisted"
            head_row = next(
                r
                for r in head_rows
                if r.get("branchhead__experiment_name") == "archetype-smoke-v0"
            )
            assert head_row["branchhead__commit_hash"] == "abc123def456"
            assert head_row["branchhead__run_id"] == "smoke-001"
            descriptor = json.loads(head_row["branchhead__descriptor_json"])
            assert descriptor["pass_rate"] == 1.0
            assert "reasoning" in descriptor

        finally:
            await container.shutdown()
