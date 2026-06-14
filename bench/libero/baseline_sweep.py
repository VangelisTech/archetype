# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Baseline sweep — the honest LIBERO-Para frontier → val/test split.

This is WS2's first deliverable (see ``docs/design/gepa-core-loop.md`` §4 and
``docs/design/libero-para-day-plan.md``). It runs every task in a LIBERO suite
under the **raw LIBERO instruction** (the fair baseline, never the empty
string), a few seeds each, records every rollout to the Archetype ledger, and
computes a per-task success rate. From those numbers it produces a deterministic
train/val/test split:

- **val = the hardest tasks** (lowest baseline success = most room to climb).
  The GEPA reflector sees only val.
- **test = a held-out, distribution-matched subset** — never seen by the
  reflector; measures whether the evolved paraphrase strategy generalizes.

Co-located on Modal, modelled on ``colocated_runner.py``: the Archetype world
loop (py3.12) runs *inside a Modal Function next to the env and policy workers*,
so every ``env.step`` / ``policy.infer_refs`` call is a same-region internal
call rather than a Mac->cloud round trip. We fan out **one Function per task**
with ``.starmap``; each Function builds its own ``ModalEnvClient(suite,
task_id)`` (a distinct ``LiberoEnvBatch`` parameter set → one env container per
task) and the VLA-JEPA worker's lifted ``max_containers=8`` lets those parallel
rollouts share the GPU pool.

Every rollout lands on the append-only ledger (episode worlds keyed by
``ep-{suite}-t{task_id}-trial{trial}`` + ``run_id``), so any trajectory is
queryable and replayable by ``(world_id, run_id)`` for the scorer (WS5).

Run the sweep (Modal auth required; no Anthropic key needed):
    modal run bench/libero/baseline_sweep.py

    # full default: libero_spatial, all 10 tasks, 3 seeds, 256 steps
    modal run bench/libero/baseline_sweep.py --suite libero_spatial \\
        --trials 3 --max-steps 256

The split JSON is written to ``bench/libero/out/<suite>_split.json`` (pulled
back from the results volume) and printed to stdout.
"""

import json
from pathlib import Path
from typing import Any

ROOT = "/repo"
RESULTS_DIR = "/results"


# ``modal`` is a runner-time dependency (a uv tool / global), not a project
# dependency, so it is absent in the CI test venv. Import it lazily: the pure
# split logic (``build_split``) stays importable for CI unit tests, while the
# Modal app (image, function, entrypoint) is only defined when ``modal`` is
# present (i.e. when this file is run via ``modal run``).
try:
    import modal  # type: ignore[import-not-found]

    _HAS_MODAL = True
except ImportError:  # pragma: no cover - exercised only in the CI test venv
    modal = None  # type: ignore[assignment]
    _HAS_MODAL = False

# Set under the _HAS_MODAL guard below; only ever dereferenced inside a Modal
# Function (where modal is always present), but defined here so the name exists
# at module scope for linters and standalone imports.
_RESULTS_VOLUME: Any = None


def _run_one_task(
    suite: str,
    task_id: int,
    trials: int,
    max_steps: int,
    run_id: str,
) -> dict[str, Any]:
    """Run one task's rollouts in-region and return its per-task summary.

    Drives the *real* VLA-JEPA policy against the deployed LIBERO env with the
    raw task instruction (the honest baseline). Each Function instance owns one
    ``(suite, task_id)`` env container; inference fans across the GPU pool.

    Returns ``{task_id, instruction, n, successes, success_rate, per_seed,
    ledger_path}`` — all the numbers the split needs, plus where the rollouts
    were persisted on the ledger.
    """
    import asyncio
    import shutil
    import sys
    import time
    from pathlib import Path as _Path

    # bench/libero is a directory of loose scripts; the driver imports
    # modal_worker by name, so its dir must be on the path.
    sys.path.insert(0, f"{ROOT}/bench/libero")

    from eval_driver import (  # type: ignore[import-not-found]
        DriverConfig,
        run_eval,
    )

    # Per-task ledger on fast local disk; copied to the network volume at the
    # end. Writing the ledger straight to a Modal Volume would reintroduce the
    # per-write network latency the co-location removes.
    task_run_id = f"{run_id}:task{task_id}"
    local_out = _Path("/tmp") / f"sweep-{task_run_id}"
    local_out.mkdir(parents=True, exist_ok=True)

    config = DriverConfig(
        suite=suite,
        task_ids=[task_id],
        trials=trials,
        max_steps=max_steps,
        out=str(local_out),
        run_id=task_run_id,
        env_client_type="modal",
        use_policy=True,
        # No overrides → each task conditions on its raw LIBERO task_language()
        # (the honest baseline, core-loop §2).
        instruction_overrides={},
    )

    started = time.perf_counter()
    results = asyncio.run(run_eval(config))
    wall_s = time.perf_counter() - started

    successes = sum(1 for r in results if getattr(r, "success", False))
    n = len(results)
    per_seed = [
        {
            "seed": getattr(r, "seed", None),
            "trial_idx": getattr(r, "trial_idx", None),
            "success": bool(getattr(r, "success", False)),
            "episode_length": int(getattr(r, "episode_length", 0) or 0),
        }
        for r in sorted(results, key=lambda r: getattr(r, "trial_idx", 0))
    ]

    # Persist this task's ledger to the network volume so rollouts survive the
    # Function and stay queryable by (world_id, run_id).
    dest = _Path(RESULTS_DIR) / task_run_id
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(local_out, dest)
    _RESULTS_VOLUME.commit()

    return {
        "task_id": task_id,
        "n": n,
        "successes": successes,
        "success_rate": (successes / n) if n else 0.0,
        "per_seed": per_seed,
        "wall_s": round(wall_s, 1),
        "ledger_path": str(dest),
    }


def build_split(
    per_task: list[dict[str, Any]],
    *,
    suite: str,
    val_size: int,
    test_size: int,
) -> dict[str, Any]:
    """Deterministic val/test split from per-task success rates.

    Policy (core-loop §4):
    - **val = the hardest tasks** (lowest success rate). Most room to climb;
      the reflector sees only these.
    - **test = a held-out, distribution-matched subset** drawn from the
      remaining (easier) tasks. We pick test tasks evenly spread across the
      remaining difficulty range so the test distribution is not all-easy:
      after removing the val tasks, sort the rest by success rate and sample
      ``test_size`` indices at even strides. Test is evaluated once, at the end,
      with the frozen strategy.

    Ties broken by ``task_id`` for determinism. Returns a JSON-serializable dict
    that records the split *and* the baseline numbers it was derived from, so
    the split is auditable and reproducible from the ledger.
    """
    # Ascending success, then task_id — hardest first, deterministic ties.
    ranked = sorted(per_task, key=lambda t: (t["success_rate"], t["task_id"]))

    n_tasks = len(ranked)
    val_size = max(0, min(val_size, n_tasks))
    val_tasks = ranked[:val_size]
    remaining = ranked[val_size:]

    # Distribution-matched test: even strides across the remaining difficulty
    # ordering so test is not all-easy nor adjacent-clustered.
    test_size = max(0, min(test_size, len(remaining)))
    if test_size and remaining:
        if test_size == 1:
            picks = [len(remaining) // 2]
        else:
            step = (len(remaining) - 1) / (test_size - 1)
            picks = sorted({round(i * step) for i in range(test_size)})
            # Resolve any collisions from rounding by filling forward.
            cursor = 0
            filled: list[int] = []
            for p in picks:
                p = max(p, cursor)
                filled.append(min(p, len(remaining) - 1))
                cursor = filled[-1] + 1
            picks = filled
        test_tasks = [remaining[i] for i in picks]
    else:
        test_tasks = []

    def _ids(tasks: list[dict[str, Any]]) -> list[int]:
        return sorted(t["task_id"] for t in tasks)

    return {
        "suite": suite,
        "split_policy": (
            "val = hardest (lowest baseline success, most room to climb); "
            "test = held-out subset sampled at even strides across the "
            "remaining difficulty ordering (distribution-matched, not all-easy)"
        ),
        "val_task_ids": _ids(val_tasks),
        "test_task_ids": _ids(test_tasks),
        "per_task": {
            str(t["task_id"]): {
                "success_rate": round(t["success_rate"], 4),
                "successes": t["successes"],
                "n": t["n"],
            }
            for t in ranked
        },
        "baseline": {
            "all_task_ids": _ids(per_task),
            "overall_success_rate": (
                round(sum(t["successes"] for t in per_task) / sum(t["n"] for t in per_task), 4)
                if sum(t["n"] for t in per_task)
                else 0.0
            ),
        },
    }


def aggregate_and_write(
    per_task: list[dict[str, Any]],
    *,
    suite: str,
    run_id: str,
    trials: int,
    max_steps: int,
    n_tasks: int,
    val_size: int,
    test_size: int,
) -> dict[str, Any]:
    """Print per-task numbers, build the split, write + return the split JSON.

    Pure (no Modal): takes the collected per-task summaries and produces the
    split artifact. Separated from ``main`` so it is unit-testable and so the
    entrypoint stays a thin fan-out + aggregate.
    """
    per_task_sorted = sorted(per_task, key=lambda t: t["task_id"])
    print("\n=== per-task baseline success (raw instruction) ===")
    total_succ = sum(t["successes"] for t in per_task_sorted)
    total_n = sum(t["n"] for t in per_task_sorted)
    for t in per_task_sorted:
        if t.get("error"):
            print(f"  task {t['task_id']:>2}: ERROR  {t['error']}")
            continue
        print(
            f"  task {t['task_id']:>2}: {t['successes']}/{t['n']} "
            f"= {t['success_rate'] * 100:5.1f}%  (wall={t.get('wall_s', '?')}s)"
        )
    overall = (total_succ / total_n) if total_n else 0.0
    print(f"  OVERALL: {total_succ}/{total_n} = {overall * 100:.1f}%")

    # Only tasks with completed rollouts (n > 0) inform the split; errored tasks
    # are surfaced separately so the split is built on real numbers.
    completed = [t for t in per_task_sorted if t["n"] > 0]
    errored = [t["task_id"] for t in per_task_sorted if t.get("error")]
    split = build_split(completed, suite=suite, val_size=val_size, test_size=test_size)
    split["errored_task_ids"] = sorted(errored)
    split["run_id"] = run_id
    split["config"] = {
        "trials": trials,
        "max_steps": max_steps,
        "n_tasks": n_tasks,
        "val_size": val_size,
        "test_size": test_size,
    }

    out_dir = Path(__file__).parent / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{suite}_split.json"
    out_path.write_text(json.dumps(split, indent=2) + "\n")

    print("\n=== val / test split ===")
    print(json.dumps(split, indent=2))
    print(f"\nsplit written to {out_path}")
    return split


# ---------------------------------------------------------------------------
# Modal app — defined only when ``modal`` is importable (runner-time).
# ---------------------------------------------------------------------------

if _HAS_MODAL:
    # py3.12 + the archetype package only. The env (LIBERO/MuJoCo) and policy
    # (VLA-JEPA) live in their own deployed apps; the sweep talks to them over
    # the EnvClient / PolicyClient boundary, so this image excludes the heavy
    # [sim] stack — same lean recipe as colocated_runner.py.
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("uv")
        .add_local_dir(
            ".",
            ROOT,
            copy=True,
            ignore=[
                ".git",
                "**/__pycache__",
                ".claude",
                "target",
                "*.mp4",
                ".venv",
                "**/*.parquet",
                # Run logs / outputs are written into bench/libero/out during a
                # live run; copying them into the image races the active writer
                # ("modified during build process"). They are never needed remotely.
                "**/*.log",
                "**/out/**",
            ],
        )
        .run_commands(
            f"cd {ROOT} && uv pip install --system -e . && uv pip install --system pillow numpy"
        )
    )

    app = modal.App("archetype-libero-baseline-sweep", image=image)
    _RESULTS_VOLUME = modal.Volume.from_name("libero-eval-results", create_if_missing=True)

    @app.function(volumes={RESULTS_DIR: _RESULTS_VOLUME}, timeout=21600)
    def sweep_one_task(
        suite: str,
        task_id: int,
        trials: int,
        max_steps: int,
        run_id: str,
    ) -> dict[str, Any]:
        """Modal wrapper over ``_run_one_task`` (one env container per task).

        Catches its own exceptions so one failing task cannot abort the whole
        fan-out (``.starmap`` raises on the first uncaught error). A failed task
        is reported as ``error`` with ``n=0`` and dropped from the split.
        """
        import traceback

        try:
            return _run_one_task(suite, task_id, trials, max_steps, run_id)
        except Exception as exc:  # noqa: BLE001 - isolate per-task failures
            return {
                "task_id": task_id,
                "n": 0,
                "successes": 0,
                "success_rate": 0.0,
                "per_seed": [],
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
            }

    @app.local_entrypoint()
    def main(
        suite: str = "libero_spatial",
        n_tasks: int = 10,
        task_ids: str = "",
        trials: int = 3,
        max_steps: int = 256,
        val_size: int = 4,
        test_size: int = 3,
        run_id: str = "",
    ):
        """Fan the baseline sweep across all tasks, build + write the split.

        Defaults match the locked contract: ``libero_spatial`` (all 10 tasks),
        the raw instruction, 3 seeds each, max 256 steps. The split JSON is
        written to ``bench/libero/out/<suite>_split.json`` and printed.

        ``task_ids`` (comma-separated) overrides ``n_tasks`` to target a
        specific subset — useful for retrying a flaked task without re-running
        the whole suite.
        """
        import time

        run_id = run_id or f"baseline-{suite}-{int(time.time())}"
        if task_ids:
            task_ids_list = [int(x) for x in str(task_ids).split(",") if x != ""]
        else:
            task_ids_list = list(range(n_tasks))

        print(
            f"=== baseline sweep === suite={suite} tasks={task_ids_list} "
            f"trials={trials} max_steps={max_steps} run_id={run_id}"
        )

        # Fan out one Function per task — each owns its own env container;
        # VLA-JEPA inference parallelizes across the lifted GPU pool.
        # return_exceptions=True so one preempted/killed container drops to an
        # errored cell instead of nuking the whole sweep (the RemoteError
        # "function call was cancelled" failure mode that killed earlier runs).
        args = [(suite, t, trials, max_steps, run_id) for t in task_ids_list]
        raw = list(sweep_one_task.starmap(args, return_exceptions=True))
        per_task: list[dict[str, Any]] = []
        for (_suite, t, *_rest), res in zip(args, raw, strict=True):
            if isinstance(res, BaseException):
                per_task.append(
                    {
                        "task_id": t,
                        "n": 0,
                        "successes": 0,
                        "success_rate": 0.0,
                        "per_seed": [],
                        "error": f"{type(res).__name__}: {res}",
                    }
                )
            else:
                per_task.append(res)

        aggregate_and_write(
            per_task,
            suite=suite,
            run_id=run_id,
            trials=trials,
            max_steps=max_steps,
            n_tasks=len(task_ids_list),
            val_size=val_size,
            test_size=test_size,
        )
