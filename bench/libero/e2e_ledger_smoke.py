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
    ACTION_DIM,
    EnvStepProcessor,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
)
from archetype.experiments.policy import (  # noqa: E402
    PolicyActionProcessor,
    ScriptedReachPolicy,
)

TICKS = 4


async def main() -> None:
    client = ModalEnvClient(suite="libero_spatial", task_id=0)

    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=str(Path(tmp) / "store"), namespace="libero_e2e")
        async with ArchetypeRuntime() as runtime:
            obs = client.reset(0, seed=0)
            print(f"reset obs (will be the raw tick-0 row): {obs['eef_pos']}")

            # Closed loop: scripted reach policy targets 10cm below the
            # reset pose; the env consumes the action the policy wrote
            # the same tick (a_t = pi(obs_{t-1}), obs_t = env.step(a_t)).
            target = (obs["eef_pos"][0], obs["eef_pos"][1], obs["eef_pos"][2] - 0.10)
            policy = ScriptedReachPolicy(targets={0: target}, gain=5.0, max_step=0.5)

            world = runtime.world(
                "libero-e2e",
                storage=storage,
                processors=[PolicyActionProcessor(policy), EnvStepProcessor(client)],
            )

            spawn_action = [0.0] * ACTION_DIM
            eid = await world.spawn(
                ManipProprio(
                    eef_pos=obs["eef_pos"], eef_quat=obs["eef_quat"], gripper=obs["gripper"]
                ),
                ManipAction(values=list(spawn_action)),
                ManipStatus(),
                ManipTask(suite="libero_spatial", task_id=0, instruction="", seed=0, env_key=0),
            )

            await world.run(steps=TICKS)

            history = await world.query(ManipProprio, ManipAction)
            rows = sorted(
                history.select(
                    "tick", "entity_id", "manipproprio__eef_pos", "manipaction__values"
                ).to_pylist(),
                key=lambda r: r["tick"],
            )
            print(f"\nledger for entity {eid} ({len(rows)} rows):")
            for row in rows:
                pos = row["manipproprio__eef_pos"]
                act = row["manipaction__values"]
                print(f"  tick {row['tick']}: z={pos[2]:.4f} action_z={act[2]:+.3f}")

            assert rows[0]["manipproprio__eef_pos"] == obs["eef_pos"], (
                "tick-0 row must be the raw reset observation"
            )
            assert rows[0]["manipaction__values"] == spawn_action, (
                "tick-0 row must keep the spawn action untouched"
            )

            # Action provenance against the real env: replay the policy on
            # the ledger's own observation column.
            replay = ScriptedReachPolicy(targets={0: target}, gain=5.0, max_step=0.5)
            for prev, row in zip(rows, rows[1:], strict=False):
                (want,) = replay.act(
                    [0],
                    [""],
                    [
                        {
                            "eef_pos": prev["manipproprio__eef_pos"],
                            "eef_quat": [1.0, 0.0, 0.0, 0.0],
                            "gripper": 0.0,
                        }
                    ],
                )
                assert row["manipaction__values"] == want, (
                    f"tick {row['tick']}: action on ledger != pi(prev ledger obs)"
                )

            zs = [row["manipproprio__eef_pos"][2] for row in rows]
            assert zs[-1] < zs[0], f"policy should drive the eef downward: {zs}"
            print("\ne2e closed-loop OK: raw tick-0 row, action provenance holds on real LIBERO")


if __name__ == "__main__":
    asyncio.run(main())
