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
from dataclasses import dataclass

from daft import col

from archetype import ArchetypeRuntime
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


class ForkValue(Component):
    amount: int = 0


@dataclass
class ForkStepSize:
    delta: int = 1


class ApplyVelocity(AsyncProcessor):
    """Move entities: position += velocity each step."""

    components = (Position, Velocity)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("position__x", col("position__x") + col("velocity__dx")).with_column(
            "position__y", col("position__y") + col("velocity__dy")
        )


class AdvanceForkValue(AsyncProcessor):
    """Advance fork state using a mutable resource shared across branches."""

    components = (ForkValue,)
    priority = 1

    async def process(self, df, resources=None, **kwargs):
        step_size = resources.get(ForkStepSize) if resources else None
        delta = step_size.delta if step_size else 0
        return df.with_column("forkvalue__amount", col("forkvalue__amount") + delta)


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
            # Initial-conditions contract (spec World Contracts): the spawn tick
            # persists raw initial state and processors first apply on the
            # following tick, so N_STEPS ticks apply ApplyVelocity(dx=1, dy=-1)
            # exactly N_STEPS - 1 times:
            #   x_final = x_init + (N_STEPS - 1), y_final = y_init - (N_STEPS - 1)
            applied_ticks = N_STEPS - 1
            position_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                x_init, y_init = init_positions.get(eid, (None, None))
                if x_init is None:
                    position_checks[f"eid_{eid}_found"] = False
                    continue
                position_checks[f"eid_{eid}_x"] = rows["position__x"][idx] == x_init + applied_ticks
                position_checks[f"eid_{eid}_y"] = rows["position__y"][idx] == y_init - applied_ticks

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
# Task: Fork-of-fork continuity and divergence
# ---------------------------------------------------------------------------


def task_fork_divergence() -> list[GraderResult]:
    """Compose fork lineage, resource sharing, and branch isolation."""
    return asyncio.run(_task_fork_divergence())


async def _task_fork_divergence() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_fork")
        step_size = ForkStepSize()

        async with ArchetypeRuntime() as runtime:
            base = runtime.world(
                "fork-base",
                storage=storage_cfg,
                processors=[AdvanceForkValue()],
                resources=[step_size],
            )
            entity_id = await base.spawn(ForkValue(amount=0))
            await base.step()  # raw initial condition at tick 0
            await base.step()  # 1 at tick 1
            base_at_fork = await base.info()

            mid = await base.fork("fork-mid")
            mid_at_fork = await mid.info()
            await mid.step()  # continue parent state: 2 at tick 2
            mid_after_first_step = await mid.info()

            leaf = await mid.fork("fork-leaf")
            leaf_at_fork = await leaf.info()
            await leaf.step()  # continue mid state: 3 at tick 3

            resource_name = f"{ForkStepSize.__module__}.{ForkStepSize.__qualname__}"
            resource_sets = [
                {resource.qualname for resource in await world.list_resources()}
                for world in (base, mid, leaf)
            ]

            # Forks intentionally share resource instances. Mutating the
            # original object must affect the next independent step in every
            # branch without changing their lineage cutoffs.
            step_size.delta = 5
            await base.step()  # 1 + 5 = 6 at tick 2
            await mid.step()  # 2 + 5 = 7 at tick 3
            await leaf.step()  # 3 + 5 = 8 at tick 4

            await base.update(entity_id, ForkValue(amount=100))
            await mid.update(entity_id, ForkValue(amount=200))
            await leaf.update(entity_id, ForkValue(amount=300))
            await base.step()
            await mid.step()
            await leaf.step()

            histories = [
                (await world.query(ForkValue, entity_ids=[entity_id])).to_pylist()
                for world in (base, mid, leaf)
            ]

        value_maps = [
            {int(row["tick"]): int(row["forkvalue__amount"]) for row in rows} for rows in histories
        ]
        owner_maps = [
            {int(row["tick"]): str(row["world_id"]) for row in rows} for rows in histories
        ]
        base_id, mid_id, leaf_id = (
            str(base_at_fork.world_id),
            str(mid_at_fork.world_id),
            str(leaf_at_fork.world_id),
        )

        expected_values = [
            {0: 0, 1: 1, 2: 6, 3: 100},
            {0: 0, 1: 1, 2: 2, 3: 7, 4: 200},
            {0: 0, 1: 1, 2: 2, 3: 3, 4: 8, 5: 300},
        ]
        expected_owners = [
            {0: base_id, 1: base_id, 2: base_id, 3: base_id},
            {0: base_id, 1: base_id, 2: mid_id, 3: mid_id, 4: mid_id},
            {
                0: base_id,
                1: base_id,
                2: mid_id,
                3: leaf_id,
                4: leaf_id,
                5: leaf_id,
            },
        ]

        return [
            state_check(
                {
                    "fresh_world_ids": len({base_id, mid_id, leaf_id}) == 3,
                    "fresh_run_ids": len(
                        {
                            str(base_at_fork.run_id),
                            str(mid_at_fork.run_id),
                            str(leaf_at_fork.run_id),
                        }
                    )
                    == 3,
                    "mid_starts_at_base_tick": mid_at_fork.tick == base_at_fork.tick == 2,
                    "leaf_starts_at_mid_tick": (
                        leaf_at_fork.tick == mid_after_first_step.tick == 3
                    ),
                },
                name="fork_identity",
            ),
            state_check(
                {
                    "resource_in_base": resource_name in resource_sets[0],
                    "resource_in_mid": resource_name in resource_sets[1],
                    "resource_in_leaf": resource_name in resource_sets[2],
                    "shared_change_reaches_base": value_maps[0].get(2) == 6,
                    "shared_change_reaches_mid": value_maps[1].get(3) == 7,
                    "shared_change_reaches_leaf": value_maps[2].get(4) == 8,
                },
                name="fork_resource_sharing",
            ),
            state_check(
                {
                    "one_row_per_base_tick": len(histories[0]) == len(value_maps[0]),
                    "one_row_per_mid_tick": len(histories[1]) == len(value_maps[1]),
                    "one_row_per_leaf_tick": len(histories[2]) == len(value_maps[2]),
                    "base_lineage_ownership": owner_maps[0] == expected_owners[0],
                    "mid_lineage_ownership": owner_maps[1] == expected_owners[1],
                    "leaf_lineage_ownership": owner_maps[2] == expected_owners[2],
                },
                name="fork_lineage_cutoffs",
            ),
            exact_match(value_maps[0], expected_values[0], name="base_branch_history"),
            exact_match(value_maps[1], expected_values[1], name="mid_branch_history"),
            exact_match(value_maps[2], expected_values[2], name="leaf_branch_history"),
        ]


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
    harness.add(
        "fork_divergence",
        suite=SUITE,
        fn=task_fork_divergence,
        desc="Fork-of-fork lineage continuity, shared resources, and isolated branch mutations",
    )
