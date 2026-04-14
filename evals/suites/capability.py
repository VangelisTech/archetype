# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Capability eval suite: tasks that define a hill to climb.

These measure what the system can do well and where it struggles.
Pass rates may start low.  As they improve, tasks can graduate to
the regression suite.

Graders use outcome verification and partial credit (state_check)
to capture nuance in multi-component tasks.
"""

from __future__ import annotations

import asyncio
import tempfile

from daft import col

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"


# ---------------------------------------------------------------------------
# Components & Processors
# ---------------------------------------------------------------------------


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


class Health(Component):
    hp: int


class Stats(Component):
    hp: int
    name: str


class Flags(Component):
    active: bool
    level: int


class ApplyVelocity(AsyncProcessor):
    """Move entities: position += velocity each step."""

    components = (Position, Velocity)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("position__x", col("position__x") + col("velocity__dx")).with_column(
            "position__y", col("position__y") + col("velocity__dy")
        )


# ---------------------------------------------------------------------------
# Task: Storage round-trip integrity
# ---------------------------------------------------------------------------


def task_storage_roundtrip() -> list[GraderResult]:
    """Write entities with mixed field types, flush, read back, verify."""
    return asyncio.run(_task_storage_roundtrip())


async def _task_storage_roundtrip() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_cap")
        orch = WorldService(StorageService())

        try:
            world = await orch.create_world(WorldConfig(name="roundtrip"), storage_cfg)

            expected = []
            for i in range(20):
                comps = [
                    Stats(hp=100 + i, name=f"entity_{i}"),
                    Flags(active=i % 2 == 0, level=i),
                ]
                eid = await world.create_entity(comps)
                expected.append(
                    {
                        "entity_id": eid,
                        "hp": 100 + i,
                        "name": f"entity_{i}",
                        "level": i,
                    }
                )

            # Flush to storage
            rc = RunConfig.benchmark(steps=1)
            await world.run(rc)

            # Read back
            df = await world.get_components([Stats, Flags])
            rows = df.collect().to_pydict()

            # Grade: entity count
            returned_ids = set(rows.get("entity_id", []))
            expected_ids = {e["entity_id"] for e in expected}

            # Grade: field-level integrity
            field_checks = {}
            for idx, eid in enumerate(rows.get("entity_id", [])):
                exp = next((e for e in expected if e["entity_id"] == eid), None)
                if exp is None:
                    field_checks[f"entity_{eid}_found"] = False
                    continue
                field_checks[f"entity_{eid}_hp"] = rows["stats__hp"][idx] == exp["hp"]
                field_checks[f"entity_{eid}_name"] = rows["stats__name"][idx] == exp["name"]
                field_checks[f"entity_{eid}_level"] = rows["flags__level"][idx] == exp["level"]

            return [
                exact_match(returned_ids, expected_ids, name="entity_ids_match"),
                state_check(field_checks, name="field_integrity"),
            ]
        finally:
            await orch.shutdown()


# ---------------------------------------------------------------------------
# Task: Multi-step simulation correctness
# ---------------------------------------------------------------------------

N_ENTITIES = 50
N_STEPS = 3


def task_simulation_correctness() -> list[GraderResult]:
    """Spawn, run processors, verify outcome state is correct."""
    return asyncio.run(_task_simulation_correctness())


async def _task_simulation_correctness() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_sim")
        orch = WorldService(StorageService())

        try:
            world = await orch.create_world(WorldConfig(name="sim-eval"), storage_cfg)

            spawned_ids = []
            init_positions = {}
            for i in range(N_ENTITIES):
                eid = await world.create_entity(
                    [
                        Position(x=i, y=i * 2),
                        Velocity(dx=1, dy=-1),
                        Health(hp=100),
                    ]
                )
                spawned_ids.append(eid)
                init_positions[eid] = (i, i * 2)

            await world.add_processor(ApplyVelocity())
            rc = RunConfig.benchmark(steps=N_STEPS)
            await world.run(rc)

            # Query outcome
            df = await world.get_components([Position, Velocity, Health])
            rows = df.collect().to_pydict()

            entity_ids = rows.get("entity_id", [])
            entity_count = len(entity_ids)

            # Grader 1: Entity preservation
            entity_checks = {
                "correct_count": entity_count == N_ENTITIES,
                "no_duplicates": len(set(entity_ids)) == entity_count,
                "all_present": set(entity_ids) == set(spawned_ids),
            }

            # Grader 2: Processor correctness (outcome verification)
            # After N_STEPS of ApplyVelocity(dx=1, dy=-1):
            #   x_final = x_init + N_STEPS, y_final = y_init - N_STEPS
            position_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                x_init, y_init = init_positions.get(eid, (None, None))
                if x_init is None:
                    position_checks[f"eid_{eid}_found"] = False
                    continue
                position_checks[f"eid_{eid}_x"] = rows["position__x"][idx] == x_init + N_STEPS
                position_checks[f"eid_{eid}_y"] = rows["position__y"][idx] == y_init - N_STEPS

            # Grader 3: Untouched data preserved (velocity, health unchanged)
            preservation_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                preservation_checks[f"eid_{eid}_hp"] = rows["health__hp"][idx] == 100
                preservation_checks[f"eid_{eid}_dx"] = rows["velocity__dx"][idx] == 1
                preservation_checks[f"eid_{eid}_dy"] = rows["velocity__dy"][idx] == -1

            # Grader 4: Query completeness
            expected_cols = {
                "entity_id",
                "tick",
                "world_id",
                "run_id",
                "is_active",
                "position__x",
                "position__y",
                "velocity__dx",
                "velocity__dy",
                "health__hp",
            }
            actual_cols = set(rows.keys())
            col_checks = {c: c in actual_cols for c in expected_cols}

            return [
                state_check(entity_checks, name="entity_preservation"),
                state_check(position_checks, name="processor_correctness"),
                state_check(preservation_checks, name="untouched_data"),
                state_check(col_checks, name="query_completeness"),
            ]
        finally:
            await orch.shutdown()


# ---------------------------------------------------------------------------
# Register all capability tasks
# ---------------------------------------------------------------------------


def register(harness: EvalHarness) -> None:
    """Register all capability tasks on the harness."""
    harness.add(
        "storage_roundtrip",
        suite=SUITE,
        fn=task_storage_roundtrip,
        desc="Write entities with mixed types, flush to storage, read back, verify field integrity",
    )
    harness.add(
        "simulation_correctness",
        suite=SUITE,
        fn=task_simulation_correctness,
        desc="Multi-step simulation: entity preservation, processor correctness, data integrity",
    )
