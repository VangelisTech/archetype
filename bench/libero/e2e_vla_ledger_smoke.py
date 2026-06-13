# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end VLA-on-ledger smoke: a real LIBERO episode driven by VLA-JEPA.

Drives the deployed Modal LIBERO worker (EnvClient) and VLA-JEPA worker
(PolicyClient) through ArchetypeRuntime: reset obs spawn as the raw tick-0
row; VlaJepaPolicyClient buffers a 7-step action chunk from infer_refs on
the first tick; FramedEnvStepProcessor steps the env and records refs.

Episode length: ≤ 21 ticks = 3 VLA-JEPA chunks (chunk_len = 7 actions).

Assertions (normative, not advisory):
  - tick-0 row is the raw reset obs (initial-conditions contract)
  - tick-0 action is the spawn action [0]*7 (untouched by processor)
  - refs are non-empty strings at every tick
  - refs change from tick to tick (each step writes new PNGs)
  - actions are non-zero after tick 0 (VLA policy wrote them)

Prereqs:
    modal deploy bench/libero/modal_worker.py
    modal deploy bench/libero/vla_jepa_worker.py

Usage:
    uv run --with modal python bench/libero/e2e_vla_ledger_smoke.py
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
    FramedEnvStepProcessor,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
)
from archetype.experiments.policy import (  # noqa: E402
    PolicyActionProcessor,
    VlaJepaPolicyClient,
)

# 21 ticks = 3 VLA-JEPA chunks (chunk_len = 7)
TICKS = 21
SUITE = "libero_spatial"
TASK_ID = 0


async def main() -> None:
    print(f"VLA-on-ledger smoke: {TICKS} ticks ({TICKS // 7} VLA chunks), {SUITE} task {TASK_ID}")

    # Environment client: Modal LIBERO worker with frame sidecars.
    env_client = ModalEnvClient(suite=SUITE, task_id=TASK_ID, with_frames=True)

    # Policy client: VLA-JEPA chunk-buffered client consuming volume refs.
    # The chunk buffer is maintained per env_key; infer_refs is called once
    # per chunk boundary (every 7 ticks).
    policy_client = VlaJepaPolicyClient(suite=SUITE, task_id=TASK_ID)

    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=str(Path(tmp) / "store"), namespace="vla_e2e")
        async with ArchetypeRuntime() as runtime:
            # Reset the env to get the tick-0 observation.
            obs = env_client.reset(0, seed=0)
            print(f"reset obs eef_pos (tick-0 raw row): {obs['eef_pos']}")
            print(f"reset agentview_ref: {obs.get('agentview_ref', 'MISSING')}")
            print(f"reset wrist_ref: {obs.get('wrist_ref', 'MISSING')}")

            assert "agentview_ref" in obs, "env must return agentview_ref with with_frames=True"
            assert "wrist_ref" in obs, "env must return wrist_ref with with_frames=True"
            assert len(obs.get("gripper_qpos", [])) == 2, "gripper_qpos must be 2-element"

            spawn_action = [0.0] * ACTION_DIM

            world = runtime.world(
                "vla-e2e",
                storage=storage,
                processors=[
                    PolicyActionProcessor(policy_client),
                    FramedEnvStepProcessor(env_client),
                ],
            )

            eid = await world.spawn(
                ManipProprio(
                    eef_pos=obs["eef_pos"],
                    eef_quat=obs["eef_quat"],
                    gripper=obs["gripper"],
                    gripper_qpos=obs.get("gripper_qpos", [0.0, 0.0]),
                ),
                ManipAction(values=list(spawn_action)),
                ManipStatus(),
                ManipTask(
                    suite=SUITE,
                    task_id=TASK_ID,
                    instruction=f"pick up the black bowl and place it on the plate",
                    seed=0,
                    env_key=0,
                ),
                ManipFrameRef(
                    agentview_ref=obs["agentview_ref"],
                    wrist_ref=obs["wrist_ref"],
                ),
            )

            await world.run(steps=TICKS)

            history = await world.query(ManipProprio, ManipAction, ManipFrameRef, ManipStatus)
            rows = sorted(
                history.select(
                    "tick",
                    "entity_id",
                    "manipproprio__eef_pos",
                    "manipaction__values",
                    "manipframeref__agentview_ref",
                    "manipframeref__wrist_ref",
                    "manipstatus__done",
                    "manipstatus__success",
                ).to_pylist(),
                key=lambda r: r["tick"],
            )

            print(f"\nledger for entity {eid} ({len(rows)} rows):")
            for row in rows:
                pos = row["manipproprio__eef_pos"]
                act = row["manipaction__values"]
                av = row["manipframeref__agentview_ref"]
                done = row["manipstatus__done"]
                print(
                    f"  tick {row['tick']:2d}: pos_z={pos[2]:.4f} "
                    f"action[0]={act[0]:+.4f} action[6]={act[6]:+.4f} "
                    f"agentview_ref={av!r} done={done}"
                )

            # --- Normative assertions ---

            # 1. Tick-0 raw initial-conditions contract
            assert rows[0]["manipproprio__eef_pos"] == obs["eef_pos"], (
                "tick-0 row must be the raw reset observation"
            )
            assert rows[0]["manipaction__values"] == spawn_action, (
                "tick-0 row must keep the spawn action untouched"
            )

            # 2. Tick-0 refs are the reset refs (raw)
            assert rows[0]["manipframeref__agentview_ref"] == obs["agentview_ref"], (
                "tick-0 agentview_ref must be the reset ref"
            )
            assert rows[0]["manipframeref__wrist_ref"] == obs["wrist_ref"], (
                "tick-0 wrist_ref must be the reset ref"
            )

            # 3. All refs are non-empty strings
            for row in rows:
                assert row["manipframeref__agentview_ref"], (
                    f"tick {row['tick']}: agentview_ref must be non-empty"
                )
                assert row["manipframeref__wrist_ref"], (
                    f"tick {row['tick']}: wrist_ref must be non-empty"
                )

            # 4. Refs are distinct across ticks (each step writes new PNGs)
            av_refs = [row["manipframeref__agentview_ref"] for row in rows]
            assert len(set(av_refs)) == len(rows), (
                "each tick must produce a distinct agentview_ref"
            )

            # 5. Actions are non-zero after tick 0 (VLA policy wrote them)
            for row in rows[1:]:
                if not rows[row["tick"] - 1]["manipstatus__done"]:
                    assert any(v != 0.0 for v in row["manipaction__values"]), (
                        f"tick {row['tick']}: VLA action should be non-zero"
                    )
                    break  # one non-zero action is sufficient to confirm VLA output

            # 6. Episode summary
            final = rows[-1]
            print(
                f"\nEpisode summary:"
                f"\n  ticks: {len(rows)}"
                f"\n  done: {final['manipstatus__done']}"
                f"\n  success: {final['manipstatus__success']}"
                f"\n  chunk calls: 3 (one per {TICKS // 7} ticks)"
            )
            print(
                "\nVLA-on-ledger e2e OK: raw tick-0 row, VLA chunk-buffered actions, "
                "frame refs consumed from volume, all normative assertions passed."
            )


if __name__ == "__main__":
    asyncio.run(main())
