# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""GEPA rollout orchestrator — run one paraphrase *arm* across LIBERO-Para.

The reusable rollout primitive. Both the baseline sweep and every GEPA round
call ``run_strategy``. It is built strictly on the Archetype service layer;
the only non-runtime code is the Modal ``.starmap`` shell (horizontal scale)
and three small supporting changes flagged below.

Dimension → primitive (locked):
    arm / GEPA variant      → ManipTask.run_name  (queryable component field; "baseline"|"gepa-v{k}"|"test")
    canonical lineage       → run_id              (immutable uuidv7 — NEVER overloaded with semantics)
    (suite, task)           → World               (one batched world per cell)
    seed / trial            → Entity              (N entities in one world → batched tick)
    control step            → tick
    everything, queryable   → the canonical Store, keyed (world_id, run_id, entity_id, tick)

Aggregate an arm across all its task-worlds: read the canonical store and filter
``maniptask__run_name == arm`` — run_id stays uuidv7. Leakage guard: the mutator
queries run_name in {val arms} only; the test arm is written once at freeze.

Scaling:
    Horizontal (World axis)  → Modal .starmap over (suite, task) cells
    Batch     (Entity axis)  → world.spawn_many(N seeds) → one env.step([N]) + one GPU forward

Supporting changes this script assumes (the ONLY new code; flagged for the workflow):
    [S1] ManipTask.run_name field — DONE (added to experiments/manipulation.py). No runtime change.
    [S2] env worker: reset_batch(env_keys, seeds)       (step([ids],[actions]) is already batched)
    [S3] policy worker: infer_refs_batch(refs[], instrs[], states[]) → ONE GPU forward over N
"""

from __future__ import annotations

import os
from typing import Any

import modal

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

# Reuse the co-located driver image (py3.12 archetype, no [sim] extra).
from colocated_runner import image  # noqa: E402

app = modal.App("archetype-gepa-runner", image=image)
results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)
RESULTS_DIR = "/results"
CANONICAL_NS = "libero_para"  # one namespace = the canonical store for the whole study


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=3600)
def run_cell(
    suite: str,
    task_id: int,
    run_name: str,  # the arm label: baseline | gepa-v{k} | test (queryable; run_id stays uuidv7)
    instruction: str,  # the paraphrase for THIS (arm, task); baseline = raw task language
    seeds: list[int],  # N trials → N entities, batched
    max_steps: int,
) -> dict[str, Any]:
    """One (arm, task) cell = one batched world. Pure service-layer below."""
    import asyncio

    async def _run() -> dict[str, Any]:
        from daft import col

        from archetype import ArchetypeRuntime
        from archetype.app.models import EpisodeConfig
        from archetype.core.config import StorageConfig
        from archetype.experiments.manipulation import (
            EnvClientSpec,
            ManipAction,
            ManipFrameRef,
            ManipProprio,
            ManipStatus,
            ManipTask,
        )
        from archetype.experiments.policy import PolicyClientSpec

        store = StorageConfig(uri=f"{RESULTS_DIR}/canonical", namespace=CANONICAL_NS)
        env_spec = EnvClientSpec(suite=suite, task_id=task_id, with_frames=True)
        pol_spec = PolicyClientSpec(suite=suite, task_id=task_id)

        async with ArchetypeRuntime() as rt:
            # run_id stays canonical uuidv7 (auto). The arm lives in ManipTask.run_name.
            world = rt.world(
                name=f"{suite}-t{task_id}-{run_name}",
                storage=store,
                processors=[],  # built from specs in Resources
                resources=[env_spec, pol_spec],
            )

            # Reset-before-spawn so tick-0 = raw initial conditions (x_0 given).
            # env_key = per-cell trial index; [S2] batched reset over N envs.
            env = env_spec.build()
            n = len(seeds)
            env_keys = list(range(n))
            reset_obs = env.reset_batch(env_keys, seeds)  # [S2]

            # Batch spawn: N seeds → N entities → batched ticks (Entity axis).
            world.spawn_many(
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
                            run_name=run_name,
                        ),
                        ManipFrameRef(agentview_ref=o["agentview_ref"], wrist_ref=o["wrist_ref"]),
                    ]
                    for i, o in enumerate(reset_obs)
                ]
            )

            # Runtime drives the bounded run with native termination — no hand-rolled
            # per-tick done-poll. Stop when every entity is done or max_steps hit.
            def all_done(w: Any) -> bool:
                rows = (w.query(ManipStatus)).where(col("tick") == w.tick - 1).to_pylist()
                return bool(rows) and all(r["manipstatus__done"] for r in rows)

            await world.run_episode(EpisodeConfig(max_steps=max_steps, termination=all_done))

            # Outcome read straight from the canonical store, scoped to this arm.
            final = (
                (await world.query(ManipStatus)).where(col("tick") == world.tick - 1).to_pylist()
            )
            successes = sum(1 for r in final if r["manipstatus__success"])
            return {
                "suite": suite,
                "task_id": task_id,
                "run_name": run_name,
                "run_id": str(world.run_id),  # canonical uuidv7, for provenance
                "world_id": str(world.world_id),
                "instruction": instruction,
                "n": n,
                "successes": successes,
                "success_rate": successes / n if n else 0.0,
            }

    results_volume.reload()
    out = asyncio.run(_run())

    # Cell manifest for the live monitor: one JSONL line per (world_id, run_id)
    # so bench/libero/monitor.py can query each cell via the Archetype service.
    import json
    from pathlib import Path

    manifest = Path(RESULTS_DIR) / "canonical" / "_cells.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    cell = {k: out[k] for k in ("world_id", "run_id", "run_name", "suite", "task_id")}
    with manifest.open("a") as f:
        f.write(json.dumps(cell) + "\n")

    results_volume.commit()
    return out


def run_strategy(
    *,
    run_name: str,  # the arm label, stamped into ManipTask.run_name on every rollout
    suite: str,
    task_ids: list[int],
    instruction_for: dict[int, str],  # task_id → paraphrase (baseline: raw task language)
    seeds: list[int],
    max_steps: int = 256,
) -> list[dict[str, Any]]:
    """Run one arm across all its tasks — horizontal .starmap over (suite, task)."""
    cells = [(suite, t, run_name, instruction_for[t], seeds, max_steps) for t in task_ids]
    return list(run_cell.starmap(cells))  # Modal fans one container per cell


@app.local_entrypoint()
def baseline(
    suite: str = "libero_spatial", n_tasks: int = 10, n_seeds: int = 5, max_steps: int = 256
):
    """Baseline arm: instruction = the raw LIBERO task language (the honest baseline).
    Produces per-task success → the val/test split is computed downstream."""
    import json

    task_ids = list(range(n_tasks))
    seeds = list(range(n_seeds))
    # baseline instruction = raw task language, fetched per task from the env worker.
    env_cls = modal.Cls.from_name("archetype-libero-env", "LiberoEnvBatch")
    instruction_for = {t: env_cls(suite=suite, task_id=t).task_language.remote() for t in task_ids}
    rows = run_strategy(
        run_name="baseline",
        suite=suite,
        task_ids=task_ids,
        instruction_for=instruction_for,
        seeds=seeds,
        max_steps=max_steps,
    )
    print(json.dumps(rows, indent=2, sort_keys=True))
