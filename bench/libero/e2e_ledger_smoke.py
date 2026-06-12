# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end Stage 2 smoke: a real LIBERO episode on the Archetype ledger.

Drives the deployed Modal LIBERO worker through the EnvStepProcessor from
the local (py3.12) harness process: reset obs spawn as the raw tick-0 row,
each subsequent tick records exactly one remote control step.

Prereqs:
    modal deploy bench/libero/modal_worker.py

Usage:
    uv run --with modal python bench/libero/e2e_ledger_smoke.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

sys.path.insert(0, str(Path(__file__).parent))

from modal_worker import ModalEnvClient  # noqa: E402

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.experiments.manipulation import (  # noqa: E402
    EnvStepProcessor,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
)

TICKS = 3


async def main() -> None:
    client = ModalEnvClient(suite="libero_spatial", task_id=0)

    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=str(Path(tmp) / "store"), namespace="libero_e2e")
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "libero-e2e",
                storage=storage,
                processors=[EnvStepProcessor(client)],
            )

            obs = client.reset(0, seed=0)
            print(f"reset obs (will be the raw tick-0 row): {obs['eef_pos']}")
            eid = await world.spawn(
                ManipProprio(
                    eef_pos=obs["eef_pos"], eef_quat=obs["eef_quat"], gripper=obs["gripper"]
                ),
                ManipAction(values=[0.0, 0.0, -0.1, 0.0, 0.0, 0.0, 0.0]),
                ManipStatus(),
                ManipTask(suite="libero_spatial", task_id=0, instruction="", seed=0, env_key=0),
            )

            await world.run(steps=TICKS)

            history = await world.query(ManipProprio)
            rows = sorted(
                history.select("tick", "entity_id", "manipproprio__eef_pos").to_pylist(),
                key=lambda r: r["tick"],
            )
            print(f"\nledger for entity {eid} ({len(rows)} rows):")
            for row in rows:
                print(f"  tick {row['tick']}: eef_pos={row['manipproprio__eef_pos']}")

            assert rows[0]["manipproprio__eef_pos"] == obs["eef_pos"], (
                "tick-0 row must be the raw reset observation"
            )
            zs = [row["manipproprio__eef_pos"][2] for row in rows]
            assert zs[-1] < zs[0], f"-z action should lower the eef across ticks: {zs}"
            print("\ne2e ledger smoke OK: reset obs raw at tick 0, env stepped on ledger")


if __name__ == "__main__":
    asyncio.run(main())
