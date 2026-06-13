# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Scaling probe — the HARD GATE for WF1.

Runs ONE ``libero_spatial`` task as a *batched world* at N = 1, 4, 16, 32
entities (seeds) for a short horizon and measures the wall-clock seconds per
control step at each N. This is the same world loop the GEPA runner drives
(``gepa_runner.run_cell``): N seeds → N entities in one world → each
``world.step()`` is ONE batched ``env.step([live])`` plus ONE batched GPU
forward (``infer_refs_batch`` over the live, buffer-empty envs).

The gate question is whether the Entity axis actually *batches*:

    scales = True   iff   sec/step at N=32 is NOT ~32x the sec/step at N=1.

If sec/step grows roughly linearly with N, the "batch" is a Python loop in
disguise and the batching claim is false — that is the STOP signal for the
day plan (libero-para-day-plan.md §10, the HARD GATE).

The driver runs co-located on Modal (same region as the env + policy workers),
so the per-step cost is real batched compute + in-region RPC, not Mac↔cloud
latency. Wall time is measured around the control-step loop only — the one-time
``reset_batch`` + ``spawn_many`` setup is excluded so the number is the steady
per-step batching cost.

Prereqs (the workers must be deployed):
    modal deploy bench/libero/modal_worker.py        # archetype-libero-env (reset_batch)
    modal deploy bench/libero/vla_jepa_worker.py     # archetype-vla-jepa (infer_refs_batch)

Run the gate:
    modal run bench/libero/scaling_probe.py
    # custom: modal run bench/libero/scaling_probe.py --suite libero_spatial \
    #         --task-id 0 --ns "1,4,16,32" --max-steps 40
"""

from __future__ import annotations

import os
import time
from typing import Any

import modal

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

# The co-located driver image (py3.12 archetype, no [sim] extra), defined
# inline rather than imported from colocated_runner: Modal auto-mounts only the
# entrypoint module on the remote container, so a top-level cross-file import of
# `image` would fail there with ModuleNotFoundError. The image copies the whole
# repo to /repo and installs archetype editable, so the function bodies' archetype
# imports resolve. This is the runner's world loop with per-step timing wrapped
# around it.
_ROOT = "/repo"
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("uv")
    .add_local_dir(
        ".",
        _ROOT,
        copy=True,
        ignore=[
            ".git",
            "**/__pycache__",
            ".claude",
            "target",
            "*.mp4",
            ".venv",
            "**/*.parquet",
        ],
    )
    .run_commands(
        f"cd {_ROOT} && uv pip install --system -e . && uv pip install --system pillow numpy"
    )
)

app = modal.App("archetype-scaling-probe", image=image)
results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)
RESULTS_DIR = "/results"
CANONICAL_NS = "libero_para_probe"  # isolated namespace: the probe never pollutes the study store


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=3600)
def probe_n(
    suite: str,
    task_id: int,
    instruction: str,
    n: int,
    max_steps: int,
) -> dict[str, Any]:
    """Drive ONE task as a batched world of N entities; time the step loop.

    Returns the per-N measurement row: wall seconds over the control-step loop,
    the number of ``world.step()`` calls actually driven, and the derived
    sec/step. Mirrors ``gepa_runner.run_cell`` (same processors, same done-row
    freeze) so the number reflects the real GEPA throughput, not a toy path.
    """
    import asyncio

    async def _run() -> dict[str, Any]:
        from daft import col

        from archetype import ArchetypeRuntime
        from archetype.app.auth.guard import reset_tick_counters
        from archetype.core.config import StorageConfig
        from archetype.experiments.manipulation import (
            FramedEnvStepProcessor,
            ManipAction,
            ManipFrameRef,
            ManipProprio,
            ManipStatus,
            ManipTask,
        )
        from archetype.experiments.policy import (
            EnvClientSpec,
            PolicyActionProcessor,
            PolicyClientSpec,
        )

        store = StorageConfig(uri=f"{RESULTS_DIR}/probe", namespace=CANONICAL_NS)
        env_spec = EnvClientSpec(suite=suite, task_id=task_id, with_frames=True)
        pol_spec = PolicyClientSpec(suite=suite, task_id=task_id)

        async with ArchetypeRuntime() as rt:
            env_client = env_spec.build()
            pol_client = pol_spec.build()

            world = rt.world(
                name=f"probe-{suite}-t{task_id}-n{n}",
                storage=store,
                processors=[
                    PolicyActionProcessor(pol_client),
                    FramedEnvStepProcessor(env_client),
                ],
            )

            # --- one-time setup (NOT timed): batched reset + batched spawn ---
            seeds = list(range(n))
            env_keys = list(range(n))
            reset_obs = env_client.reset_batch(env_keys, seeds)
            await world.spawn_many(
                [
                    [
                        ManipProprio(
                            eef_pos=o["eef_pos"],
                            eef_quat=o["eef_quat"],
                            gripper=o["gripper"],
                            gripper_qpos=o["gripper_qpos"],
                        ),
                        ManipAction(),
                        ManipStatus(),
                        ManipTask(
                            suite=suite,
                            task_id=task_id,
                            instruction=instruction,
                            seed=seeds[i],
                            env_key=env_keys[i],
                            run_name="probe",
                        ),
                        ManipFrameRef(agentview_ref=o["agentview_ref"], wrist_ref=o["wrist_ref"]),
                    ]
                    for i, o in enumerate(reset_obs)
                ]
            )

            # --- timed control-step loop: one world.step() == one batched tick ---
            steps_driven = 0
            started = time.perf_counter()
            for _tick in range(max_steps):
                reset_tick_counters()
                await world.step()
                steps_driven += 1
                # Early-out only when every entity is done (a pure read). Keeps
                # the measured window to genuine sim work without a sync-in-async
                # termination callable.
                info = await world.info()
                rows = (
                    (await world.query(ManipStatus)).where(col("tick") == info.tick - 1).to_pylist()
                )
                if rows and all(r["manipstatus__done"] for r in rows):
                    break
            wall_s = time.perf_counter() - started

            info = await world.info()
            final = (
                (await world.query(ManipStatus)).where(col("tick") == info.tick - 1).to_pylist()
            )
            successes = sum(1 for r in final if r["manipstatus__success"])

            return {
                "n": n,
                "steps_driven": steps_driven,
                "wall_s": round(wall_s, 2),
                "sec_per_step": round(wall_s / steps_driven, 3) if steps_driven else None,
                "successes": successes,
                "run_id": str(info.run_id or ""),
                "world_id": str(info.world_id),
            }

    results_volume.reload()
    out = asyncio.run(_run())
    results_volume.commit()
    return out


def _verdict(rows: list[dict[str, Any]]) -> tuple[bool, str]:
    """Judge the scaling gate from the per-N measurement rows.

    scales = True iff the largest N's sec/step is NOT ~linear in N relative to
    the smallest N's sec/step. Concretely: with the batch growing by a factor
    ``g = N_max / N_min``, linear (no batching) would multiply sec/step by ~g.
    We pass if the observed multiplier is comfortably sublinear — at most a
    quarter of the linear blow-up (a 4x safety margin against pure looping).
    """
    usable = [r for r in rows if r.get("sec_per_step")]
    if len(usable) < 2:
        return False, "not enough measurements to judge scaling"

    usable.sort(key=lambda r: r["n"])
    lo, hi = usable[0], usable[-1]
    n_ratio = hi["n"] / lo["n"]
    sps_ratio = hi["sec_per_step"] / lo["sec_per_step"] if lo["sec_per_step"] else float("inf")
    # Linear cost would be ~n_ratio; allow up to a quarter of that (4x margin).
    threshold = max(1.5, n_ratio / 4.0)
    scales = sps_ratio <= threshold
    verdict = (
        f"N {lo['n']}→{hi['n']} ({n_ratio:.0f}x entities): sec/step "
        f"{lo['sec_per_step']}→{hi['sec_per_step']} = {sps_ratio:.2f}x "
        f"(linear would be ~{n_ratio:.0f}x; pass threshold {threshold:.2f}x). "
        f"{'SUBLINEAR — real batching.' if scales else 'LINEAR — batching is fake. STOP.'}"
    )
    return scales, verdict


@app.local_entrypoint()
def main(
    suite: str = "libero_spatial",
    task_id: int = 0,
    ns: str = "1,4,16,32",
    max_steps: int = 40,
):
    """Sweep N over the batched world and print the N→sec/step gate table.

    The baseline instruction is the raw LIBERO task language (the honest
    default), fetched once from the env worker so every N runs the identical
    task — only the entity count changes.
    """
    import json

    n_list = [int(x) for x in str(ns).split(",") if x.strip()]

    # Raw task language = the honest default instruction (fairness baseline).
    env_cls = modal.Cls.from_name("archetype-libero-env", "LiberoEnvBatch")
    instruction = env_cls(suite=suite, task_id=task_id).task_language.remote()
    print(f"task language (instruction held constant across N): {instruction!r}\n")

    # Fan one container per N so the measurements do not share a warm cache or
    # contend for the GPU — each N gets a clean, isolated batched run.
    cells = [(suite, task_id, instruction, n, max_steps) for n in n_list]
    rows = list(probe_n.starmap(cells))
    rows.sort(key=lambda r: r["n"])

    print("=== scaling gate: N → sec/step ===")
    print(f"{'N':>4} | {'steps':>5} | {'wall_s':>8} | {'sec/step':>9} | {'succ':>4}")
    print("-" * 44)
    for r in rows:
        print(
            f"{r['n']:>4} | {r['steps_driven']:>5} | {r['wall_s']:>8} | "
            f"{str(r['sec_per_step']):>9} | {r['successes']:>4}"
        )

    scales, verdict = _verdict(rows)
    print("\n" + verdict)
    print(f"\nscales = {scales}")
    print("\nraw rows:")
    print(json.dumps(rows, indent=2, sort_keys=True))
