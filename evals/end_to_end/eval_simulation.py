# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end eval: Full simulation lifecycle.

Tests the complete pipeline: world creation → entity spawning → processor
registration → multi-step simulation → query → result verification.

Uses rubric grading across multiple dimensions:
- Entity count preservation (no entities lost)
- Data mutation correctness (processors applied correctly)
- Temporal consistency (tick advances correctly)
- Query result completeness (all fields present)
"""

from __future__ import annotations

import asyncio
import tempfile

from daft import col

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from evals.types import EvalResult, Tier, rubric

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


class ApplyVelocity(AsyncProcessor):
    """Move entities: position += velocity each step."""

    components = (Position, Velocity)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("position__x", col("position__x") + col("velocity__dx")).with_column(
            "position__y", col("position__y") + col("velocity__dy")
        )


# ---------------------------------------------------------------------------
# Eval
# ---------------------------------------------------------------------------

N_ENTITIES = 50
N_STEPS = 3


async def _run() -> list[EvalResult]:
    results = []

    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_e2e")
        orch = WorldService(StorageService())

        try:
            world = await orch.create_world(WorldConfig(name="e2e-eval"), storage_cfg)

            # --- spawn entities with known initial state ---
            spawned_ids = []
            for i in range(N_ENTITIES):
                eid = await world.create_entity(
                    [
                        Position(x=i, y=i * 2),
                        Velocity(dx=1, dy=-1),
                        Health(hp=100),
                    ]
                )
                spawned_ids.append(eid)

            # --- register processor and run ---
            await world.add_processor(ApplyVelocity())
            rc = RunConfig.benchmark(steps=N_STEPS)
            await world.run(rc)

            # --- query results ---
            sig = Archetype.sig_from_components(
                [
                    Position(x=0, y=0),
                    Velocity(dx=0, dy=0),
                    Health(hp=0),
                ]
            )
            df = await world.query_archetype(sig, run_config=rc)
            rows = df.collect().to_pydict()

            entity_ids = rows.get("entity_id", [])
            entity_count = len(entity_ids)

            # =================================================================
            # Rubric 1: Entity preservation
            # =================================================================
            results.append(
                rubric(
                    {
                        "correct_count": entity_count == N_ENTITIES,
                        "no_duplicates": len(set(entity_ids)) == entity_count,
                        "all_spawned_present": set(entity_ids) == set(spawned_ids),
                    },
                    case_name="e2e_entity_preservation",
                    tier=Tier.END_TO_END,
                )
            )

            # =================================================================
            # Rubric 2: Processor correctness (position mutation)
            # After N_STEPS of ApplyVelocity(dx=1, dy=-1):
            #   x_final = x_init + N_STEPS
            #   y_final = y_init - N_STEPS
            # =================================================================
            position_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                actual_x = rows["position__x"][idx]
                actual_y = rows["position__y"][idx]
                expected_x = eid + N_STEPS
                expected_y = (eid * 2) - N_STEPS
                position_checks[f"entity_{eid}_x"] = actual_x == expected_x
                position_checks[f"entity_{eid}_y"] = actual_y == expected_y

            results.append(
                rubric(
                    position_checks,
                    case_name="e2e_processor_correctness",
                    tier=Tier.END_TO_END,
                )
            )

            # =================================================================
            # Rubric 3: Untouched data preserved
            # Health and Velocity should be unchanged by ApplyVelocity.
            # =================================================================
            preservation_checks = {}
            for idx in range(entity_count):
                eid = entity_ids[idx]
                preservation_checks[f"entity_{eid}_hp"] = rows["health__hp"][idx] == 100
                preservation_checks[f"entity_{eid}_dx"] = rows["velocity__dx"][idx] == 1
                preservation_checks[f"entity_{eid}_dy"] = rows["velocity__dy"][idx] == -1

            results.append(
                rubric(
                    preservation_checks,
                    case_name="e2e_untouched_data_preserved",
                    tier=Tier.END_TO_END,
                )
            )

            # =================================================================
            # Rubric 4: Query completeness (all expected columns present)
            # =================================================================
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
            results.append(
                rubric(
                    {col: col in actual_cols for col in expected_cols},
                    case_name="e2e_query_completeness",
                    tier=Tier.END_TO_END,
                )
            )

        finally:
            await orch.shutdown()

    return results


def run() -> list[EvalResult]:
    return asyncio.run(_run())
