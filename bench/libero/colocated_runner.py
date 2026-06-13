# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Co-located eval runner: drive the LIBERO eval from inside Modal.

The Mac-driven eval pays a transcontinental round trip on every control
step (env.step + frame transfer + a per-step volume sync), which the a3
smoke measured at 6-9 s/step. Nothing there is compute — it is latency.

This runner moves the *driver itself* into a Modal Function next to the
env and policy workers. The Archetype world loop still runs in its own
py3.12 process (LIBERO's py3.10 pin is untouched — the EnvClient /
PolicyClient boundary is exactly what lets the driver relocate), but the
``env.step`` / ``policy.infer`` calls it makes are now Modal-internal,
same region, instead of Mac->cloud. The eval logic is unchanged: this
imports ``eval_driver.run_eval`` verbatim and only changes where it runs.

Storage writes go to fast local container disk; the final lab-world
parquet is copied to the ``libero-eval-results`` volume so results
survive the function and can be pulled back.

Smoke (1 task x 2 trials, builds the image on first run):
    modal run bench/libero/colocated_runner.py

Throughput-tuning knobs are CLI flags on the entrypoint below.
"""

from pathlib import Path
from typing import Any

import modal

ROOT = "/repo"
RESULTS_DIR = "/results"

# py3.12 + the archetype package. Deliberately excludes the [sim] extra
# (LIBERO/torch/mujoco) — the driver only needs the world loop and the
# Modal client; the env and policy live in their own deployed apps.
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("uv")
    .add_local_dir(
        ".",
        ROOT,
        copy=True,
        # Keep the image lean and the build cache stable: no VCS, caches,
        # local worktrees, or the heavy demo asset.
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
        f"cd {ROOT} && uv pip install --system -e . && uv pip install --system pillow numpy"
    )
)

app = modal.App("archetype-libero-eval", image=image)
results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)


def _run_one_task_eval(
    suite: str,
    task_id: int,
    trials: int,
    max_steps: int,
    use_policy: bool,
    run_id: str,
) -> dict[str, Any]:
    """Run one task's eval in-region and return a throughput summary."""
    import asyncio
    import shutil
    import sys
    import time

    # bench/libero is a directory of loose scripts, not a package; the eval
    # driver imports modal_worker by name, so it must be on the path.
    sys.path.insert(0, f"{ROOT}/bench/libero")

    from eval_driver import DriverConfig, run_eval  # type: ignore[import-not-found]

    # Storage on fast local disk; only the final parquet is copied to the
    # network volume. Writing the ledger directly to a Modal Volume would
    # reintroduce per-write network latency — the very thing we removed.
    task_run_id = f"{run_id}:task{task_id}"
    local_out = Path("/tmp") / f"eval-{run_id}-task{task_id}"
    local_out.mkdir(parents=True, exist_ok=True)

    config = DriverConfig(
        suite=suite,
        task_ids=[task_id],
        trials=trials,
        max_steps=max_steps,
        out=str(local_out),
        run_id=task_run_id,
        env_client_type="modal",
        use_policy=use_policy,
    )

    started = time.perf_counter()
    results = asyncio.run(run_eval(config))
    wall_s = time.perf_counter() - started

    total_steps = sum(getattr(r, "episode_length", 0) or 0 for r in results)
    successes = sum(1 for r in results if getattr(r, "success", False))

    dest = Path(RESULTS_DIR) / run_id / f"task{task_id}"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(local_out, dest)
    results_volume.commit()

    return {
        "suite": suite,
        "task_id": task_id,
        "trials": trials,
        "episodes": len(results),
        "successes": successes,
        "total_steps": total_steps,
        "wall_s": round(wall_s, 1),
        "sec_per_step": round(wall_s / total_steps, 3) if total_steps else None,
        "results_path": str(dest),
    }


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=3600)
def run_task_eval_remote(
    suite: str,
    task_id: int,
    trials: int,
    max_steps: int,
    use_policy: bool,
    run_id: str,
) -> dict[str, Any]:
    """Run one task's eval in its own Modal invocation."""
    return _run_one_task_eval(suite, task_id, trials, max_steps, use_policy, run_id)


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=3600)
def run_eval_remote(
    suite: str,
    task_ids: list[int],
    trials: int,
    max_steps: int,
    use_policy: bool,
    run_id: str,
) -> dict[str, Any]:
    """Compatibility wrapper: run all requested tasks serially in one container."""
    summaries = [
        _run_one_task_eval(
            suite,
            task_id,
            trials,
            max_steps,
            use_policy,
            run_id,
        )
        for task_id in task_ids
    ]
    return _combine_summaries(suite, task_ids, trials, summaries, run_id)


def _combine_summaries(
    suite: str,
    task_ids: list[int],
    trials: int,
    summaries: list[dict[str, Any]],
    run_id: str,
) -> dict[str, Any]:
    """Aggregate per-task summaries into the old top-level shape."""
    total_steps = sum(int(s.get("total_steps", 0) or 0) for s in summaries)
    wall_s = max((float(s.get("wall_s", 0.0) or 0.0) for s in summaries), default=0.0)
    return {
        "suite": suite,
        "task_ids": task_ids,
        "trials": trials,
        "episodes": sum(int(s.get("episodes", 0) or 0) for s in summaries),
        "successes": sum(int(s.get("successes", 0) or 0) for s in summaries),
        "total_steps": total_steps,
        "wall_s": round(wall_s, 1),
        "sec_per_step": round(wall_s / total_steps, 3) if total_steps else None,
        "results_path": str(Path(RESULTS_DIR) / run_id),
        "tasks": summaries,
    }


@app.local_entrypoint()
def main(
    suite: str = "libero_spatial",
    task_ids: str = "0",
    trials: int = 2,
    max_steps: int = 80,
    use_policy: bool = True,
    run_id: str = "smoke",
):
    """Throughput smoke. Defaults to 1 task x 2 trials so it is cheap.

    Compare sec_per_step here against the Mac-driven baseline (~6-9 s/step
    from the a3 eval smoke) to read the co-location speedup.
    """
    ids = [int(x) for x in str(task_ids).split(",") if x != ""]
    task_args = [
        (
            suite,
            task_id,
            trials,
            max_steps,
            use_policy,
            run_id,
        )
        for task_id in ids
    ]
    summaries = list(
        run_task_eval_remote.starmap(
            task_args,
            order_outputs=True,
        )
    )
    summary = _combine_summaries(suite, ids, trials, summaries, run_id)
    print("=== co-located eval throughput ===")
    for key, value in summary.items():
        if key == "tasks":
            print("  tasks:")
            for task_summary in value:
                print(
                    "    "
                    f"task={task_summary['task_id']} episodes={task_summary['episodes']} "
                    f"steps={task_summary['total_steps']} wall={task_summary['wall_s']}s "
                    f"sec_per_step={task_summary['sec_per_step']}"
                )
            continue
        print(f"  {key}: {value}")
    baseline = 7.3
    if summary.get("sec_per_step"):
        speedup = baseline / summary["sec_per_step"]
        print(f"  speedup_vs_mac_baseline: {speedup:.1f}x  (baseline {baseline}s/step)")
