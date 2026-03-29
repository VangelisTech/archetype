# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Eval: World lifecycle correctness.

Verifies the full create → populate → step → query pipeline produces
correct results.  Exits non-zero on any assertion failure.
"""

from __future__ import annotations

import asyncio
import sys
import tempfile

from daft import col, lit

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

# ---------------------------------------------------------------------------
# Components & Processors
# ---------------------------------------------------------------------------


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


class ApplyVelocity(AsyncProcessor):
    components = (Position, Velocity)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("position__x", col("position__x") + col("velocity__dx")).with_column(
            "position__y", col("position__y") + col("velocity__dy")
        )


class ScaleVelocity(AsyncProcessor):
    components = (Velocity,)
    priority = 2

    async def process(self, df, **kwargs):
        return df.with_column("velocity__dx", col("velocity__dx") * lit(2)).with_column(
            "velocity__dy", col("velocity__dy") * lit(2)
        )


# ---------------------------------------------------------------------------
# Eval
# ---------------------------------------------------------------------------

PASS = 0
FAIL = 1


async def eval_world_lifecycle() -> int:
    """Create a world, populate it, run processors, and verify results."""
    failures = []

    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval")
        orch = WorldService(StorageService())

        try:
            world = await orch.create_world(WorldConfig(name="lifecycle-eval"), storage_cfg)

            # --- spawn entities ---
            n_entities = 50
            for i in range(n_entities):
                await world.create_entity([Position(x=i, y=i * 2), Velocity(dx=1, dy=-1)])

            await world.add_processor(ApplyVelocity())

            # --- run 3 steps ---
            rc = RunConfig.benchmark(steps=3)
            await world.run(rc)

            # --- verify: positions should have moved by (3, -3) from initial ---
            from archetype.core.archetype import Archetype

            sig = Archetype.sig_from_components([Position(x=0, y=0), Velocity(dx=0, dy=0)])
            df = await world.query_archetype(sig, run_config=rc)
            rows = df.collect().to_pydict()

            if len(rows.get("entity_id", [])) == 0:
                failures.append("FAIL: query returned zero rows after 3 steps")
            else:
                xs = rows["position__x"]
                ys = rows["position__y"]
                entity_count = len(xs)
                if entity_count != n_entities:
                    failures.append(f"FAIL: expected {n_entities} entities, got {entity_count}")

                # After 3 steps of ApplyVelocity (dx=1, dy=-1):
                # x_final = x_init + 3, y_final = y_init - 3
                for idx in range(min(entity_count, n_entities)):
                    eid = rows["entity_id"][idx]
                    # Entity IDs start at 0, initial x=eid, y=eid*2
                    expected_x = eid + 3
                    expected_y = (eid * 2) - 3
                    if xs[idx] != expected_x or ys[idx] != expected_y:
                        failures.append(
                            f"FAIL: entity {eid} expected ({expected_x},{expected_y}), "
                            f"got ({xs[idx]},{ys[idx]})"
                        )

        finally:
            await orch.shutdown()

    if failures:
        for f in failures:
            print(f, file=sys.stderr)
        print(f"\neval_world_lifecycle: {len(failures)} failure(s)", file=sys.stderr)
        return FAIL

    print("eval_world_lifecycle: PASS")
    return PASS


def main() -> int:
    return asyncio.run(eval_world_lifecycle())


if __name__ == "__main__":
    sys.exit(main())
