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

Supporting changes this script builds on (all landed; flagged for provenance):
    [S1] ManipTask.run_name field — DONE (experiments/manipulation.py). No runtime change.
    [S2] env worker: reset_batch(env_keys, seeds) → N obs in ONE remote call — DONE
         (bench/libero/modal_worker.py; step([ids],[actions]) was already batched).
    [S3] policy worker: infer_refs_batch(refs[], instrs[], states[]) → ONE GPU forward over
         N live envs — DONE (bench/libero/vla_jepa_worker.py); VlaJepaPolicyClient.act
         batches the buffer-empty envs into that single forward (experiments/policy.py).

The world carries the two service-layer processors directly (PolicyActionProcessor →
FramedEnvStepProcessor); both consume only the live (non-done) rows, so a cell's wall
time is set by its slowest still-running trial, not by N.
"""

from __future__ import annotations

import os
from typing import Any

import modal

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

# The co-located driver image (py3.12 archetype, no [sim] extra), defined inline
# rather than imported from colocated_runner: Modal auto-mounts only the entrypoint
# module on the remote container, so a top-level cross-file import of `image` would
# fail there with ModuleNotFoundError. The image copies the whole repo to /repo and
# installs archetype editable, so the function bodies' archetype imports resolve.
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
    """One (arm, task) cell = one batched world. Pure service-layer below.

    All N seeds are entities in ONE world: each ``world.step()`` is one batched
    ``env.step([live])`` (the env worker skips done envs) and one batched GPU
    forward (``infer_refs_batch`` over the live, buffer-empty envs). Finished
    trials cost ~0 — the done-row freeze in ``_FramedEnvStepper`` /
    ``_PolicyCaller`` excludes done rows from both the env call and the GPU
    batch — so we simply run to ``max_steps`` instead of an async-in-sync
    termination poll.
    """
    import asyncio
    import sys

    # archetype is installed from src/ (package-dir = {"" = "src"}), so the repo
    # root is NOT on sys.path by default. EnvClientSpec.build() resolves the env
    # client via ``from bench.libero.modal_worker import ModalEnvClient``; add
    # /repo so that dotted import (and the bench.* packages) resolve.
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)

    async def _run() -> dict[str, Any]:
        import uuid

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

        # LanceDB needs POSIX atomic-rename, which a Modal Volume does not give
        # during live writes ("rename ... Operation not permitted"). Write the
        # ledger to fast local container disk during the run, then copy this
        # cell's store into the canonical volume after the run (the colocated_runner
        # pattern). The cell's store lands under a unique subdir so concurrent
        # cells never clobber each other, while all cells still share the one
        # canonical namespace for the study.
        local_uri = f"/tmp/canonical-{uuid.uuid4().hex}"
        store = StorageConfig(uri=local_uri, namespace=CANONICAL_NS)
        env_spec = EnvClientSpec(suite=suite, task_id=task_id, with_frames=True)
        pol_spec = PolicyClientSpec(suite=suite, task_id=task_id)

        async with ArchetypeRuntime() as rt:
            # Clients built once from the picklable specs (the spec scalars are
            # what cross the Daft worker boundary; the live handle reconnects
            # lazily). The processors are the service-layer wiring: policy
            # (priority 1) writes the action before the framed env step
            # (priority 10) consumes it — see policy.py and manipulation.py.
            env_client = env_spec.build()
            pol_client = pol_spec.build()

            # run_id stays canonical uuidv7 (auto). The arm lives in ManipTask.run_name.
            world = rt.world(
                name=f"{suite}-t{task_id}-{run_name}",
                storage=store,
                processors=[
                    PolicyActionProcessor(pol_client),
                    FramedEnvStepProcessor(env_client),
                ],
            )

            # Reset-before-spawn so tick-0 = raw initial conditions (x_0 given).
            # env_key = per-cell trial index; [S2] batched reset over N envs in
            # ONE remote call → N obs (each with distinct frame refs).
            n = len(seeds)
            env_keys = list(range(n))
            reset_obs = env_client.reset_batch(env_keys, seeds)  # [S2]

            # Batch spawn: N seeds → N entities → batched ticks (Entity axis).
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
                            run_name=run_name,
                        ),
                        ManipFrameRef(agentview_ref=o["agentview_ref"], wrist_ref=o["wrist_ref"]),
                    ]
                    for i, o in enumerate(reset_obs)
                ]
            )

            # Drive the bounded run to max_steps. Step one tick at a time so the
            # per-tick RBAC quota counter (MAX_CMDS_PER_TICK) is reset each tick,
            # exactly as eval_driver does; done rows are frozen by the processors,
            # so finished trials add ~no env/GPU cost. We break early only when
            # every entity is done (a pure read, not a sim step), bounding wall
            # time without an async-in-sync termination callable.
            for _tick in range(max_steps):
                reset_tick_counters()
                await world.step()
                info = await world.info()
                latest = info.tick - 1
                rows = (
                    (await world.query(ManipStatus))
                    .where(col("tick") == latest)
                    .to_pylist()
                )
                if rows and all(r["manipstatus__done"] for r in rows):
                    break

            # Outcome read straight from the canonical store, scoped to this cell.
            info = await world.info()
            final = (
                (await world.query(ManipStatus)).where(col("tick") == info.tick - 1).to_pylist()
            )
            successes = sum(1 for r in final if r["manipstatus__success"])
            return {
                "suite": suite,
                "task_id": task_id,
                "run_name": run_name,
                "run_id": str(info.run_id or ""),  # canonical uuidv7, for provenance
                "world_id": str(info.world_id),
                "instruction": instruction,
                "n": n,
                "successes": successes,
                "success_rate": successes / n if n else 0.0,
                "_local_uri": local_uri,  # popped below; not part of the public row
            }

    import shutil
    from pathlib import Path

    out = asyncio.run(_run())

    # Persist this cell's ledger to the canonical volume: copy the locally-written
    # LanceDB store into a unique per-cell subdir (run_id keeps it append-only and
    # collision-free across concurrent cells), then commit the volume once.
    local_uri = out.pop("_local_uri")
    src = Path(local_uri)
    if src.exists():
        results_volume.reload()
        dest = Path(RESULTS_DIR) / "canonical" / out["run_name"] / (
            f"{out['suite']}-t{out['task_id']}-{out['run_id']}"
        )
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest)
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
