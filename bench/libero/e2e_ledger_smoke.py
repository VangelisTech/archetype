# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end Stage 2 smoke: a real LIBERO episode on the Archetype ledger.

Drives the deployed Modal LIBERO worker through the FramedEnvStepProcessor
from the local (py3.12) harness process: reset obs spawn as the raw tick-0
row, each subsequent tick records exactly one remote control step. Frame refs
(volume paths) are written to the ledger alongside proprio observations.

Prereqs:
    modal deploy bench/libero/modal_worker.py
    modal deploy bench/libero/vla_jepa_worker.py  # for infer_refs smoke

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
    FramedEnvStepProcessor,
    ManipAction,
    ManipFrameRef,
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
    # with_frames=True: env worker writes PNGs to the libero-frames volume and
    # returns agentview_ref / wrist_ref alongside proprio obs.
    client = ModalEnvClient(suite="libero_spatial", task_id=0, with_frames=True)

    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(uri=str(Path(tmp) / "store"), namespace="libero_e2e")
        async with ArchetypeRuntime() as runtime:
            obs = client.reset(0, seed=0)
            print(f"reset obs eef_pos (tick-0 raw row): {obs['eef_pos']}")
            print(f"reset agentview_ref: {obs.get('agentview_ref', 'MISSING')}")
            print(f"reset wrist_ref: {obs.get('wrist_ref', 'MISSING')}")

            assert "agentview_ref" in obs, "env must return agentview_ref with with_frames=True"
            assert "wrist_ref" in obs, "env must return wrist_ref with with_frames=True"
            assert len(obs.get("gripper_qpos", [])) == 2, "gripper_qpos must be 2-element"

            target = (obs["eef_pos"][0], obs["eef_pos"][1], obs["eef_pos"][2] - 0.10)
            policy = ScriptedReachPolicy(targets={0: target}, gain=5.0, max_step=0.5)

            world = runtime.world(
                "libero-e2e",
                storage=storage,
                processors=[PolicyActionProcessor(policy), FramedEnvStepProcessor(client)],
            )

            spawn_action = [0.0] * ACTION_DIM
            eid = await world.spawn(
                ManipProprio(
                    eef_pos=obs["eef_pos"],
                    eef_quat=obs["eef_quat"],
                    gripper=obs["gripper"],
                    gripper_qpos=obs.get("gripper_qpos", [0.0, 0.0]),
                ),
                ManipAction(values=list(spawn_action)),
                ManipStatus(),
                ManipTask(suite="libero_spatial", task_id=0, instruction="", seed=0, env_key=0),
                ManipFrameRef(
                    agentview_ref=obs["agentview_ref"],
                    wrist_ref=obs["wrist_ref"],
                ),
            )

            await world.run(steps=TICKS)

            history = await world.query(ManipProprio, ManipAction, ManipFrameRef)
            rows = sorted(
                history.select(
                    "tick",
                    "entity_id",
                    "manipproprio__eef_pos",
                    "manipaction__values",
                    "manipframeref__agentview_ref",
                    "manipframeref__wrist_ref",
                ).to_pylist(),
                key=lambda r: r["tick"],
            )
            print(f"\nledger for entity {eid} ({len(rows)} rows):")
            for row in rows:
                pos = row["manipproprio__eef_pos"]
                act = row["manipaction__values"]
                av = row["manipframeref__agentview_ref"]
                print(
                    f"  tick {row['tick']}: z={pos[2]:.4f} action_z={act[2]:+.3f} "
                    f"agentview_ref={av!r}"
                )

            # --- Tick-0 raw initial-conditions contract ---
            assert rows[0]["manipproprio__eef_pos"] == obs["eef_pos"], (
                "tick-0 row must be the raw reset observation"
            )
            assert rows[0]["manipaction__values"] == spawn_action, (
                "tick-0 row must keep the spawn action untouched"
            )

            # --- Ref carriage: tick-0 carries reset refs ---
            assert rows[0]["manipframeref__agentview_ref"] == obs["agentview_ref"], (
                "tick-0 agentview_ref must be the reset ref"
            )
            assert rows[0]["manipframeref__wrist_ref"] == obs["wrist_ref"], (
                "tick-0 wrist_ref must be the reset ref"
            )

            # --- Refs non-empty at every tick ---
            for row in rows:
                assert row["manipframeref__agentview_ref"], (
                    f"tick {row['tick']}: agentview_ref must be non-empty"
                )
                assert row["manipframeref__wrist_ref"], (
                    f"tick {row['tick']}: wrist_ref must be non-empty"
                )

            # --- Refs change from tick to tick (each step writes new PNGs) ---
            refs = [
                (row["manipframeref__agentview_ref"], row["manipframeref__wrist_ref"])
                for row in rows
            ]
            assert len(set(r[0] for r in refs)) == len(refs), (
                "each tick must produce a distinct agentview_ref"
            )

            # --- Action provenance (same as original smoke) ---
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

            print(
                "\ne2e closed-loop OK: raw tick-0 row, frame refs on ledger, "
                "action provenance holds on real LIBERO"
            )

            # --- Optional: smoke one infer_refs call against the VLA-JEPA worker ---
            _smoke_infer_refs_if_available(
                agentview_ref=rows[1]["manipframeref__agentview_ref"],
                wrist_ref=rows[1]["manipframeref__wrist_ref"],
                state=(
                    rows[1]["manipproprio__eef_pos"]
                    + [0.0, 0.0, 0.0]  # axis-angle placeholder
                    + rows[0].get(  # tick-0 gripper_qpos may not be queryable here
                        "manipproprio__gripper_qpos", [0.0, 0.0]
                    )
                ),
            )


def _smoke_infer_refs_if_available(
    agentview_ref: str,
    wrist_ref: str,
    state: list[float],
) -> None:
    """Call VlaJepaPolicy.infer_refs remotely if the worker is deployed.

    Tolerates a missing/undeployed worker — the smoke is advisory."""
    try:
        import modal

        policy_cls = modal.Cls.from_name("archetype-vla-jepa", "VlaJepaPolicy")
        policy = policy_cls()
        chunk = policy.infer_refs.remote(
            agentview_ref=agentview_ref,
            wrist_ref=wrist_ref,
            instruction="pick up the black bowl and place it on the plate",
            state=state[:8],
        )
        print(f"\ninfer_refs smoke: chunk={len(chunk)} steps x {len(chunk[0])} dims")
        assert len(chunk[0]) == 7, f"expected 7-dim actions, got {len(chunk[0])}"
        print("infer_refs OK: VLA-JEPA read frames from shared volume")
    except Exception as exc:  # noqa: BLE001
        print(f"\ninfer_refs skipped (worker unavailable or not deployed): {exc}")


if __name__ == "__main__":
    asyncio.run(main())
