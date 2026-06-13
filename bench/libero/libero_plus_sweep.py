# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO-Plus LANGUAGE baseline sweep — the perturbed-instruction frontier.

The LIBERO-Plus analogue of ``baseline_sweep.py``. It runs the frozen VLA-JEPA
policy against the **LIBERO-Plus Language Instructions** dimension (scene/physics/
goal byte-identical to base LIBERO; only the instruction string rewritten),
conditioning on the *perturbed* instruction — the honest LIBERO-Plus baseline.
The published reference is ≈85.4% average; this sweep produces a real measured
number on a handful of language variants.

Co-located on Modal, modelled on ``baseline_sweep.py``: the Archetype world loop
(py3.12) runs *inside a Modal Function next to the env and policy workers*, so
every ``env.step`` / ``policy.infer_refs`` call is a same-region internal call.
We fan out **one Function per language task** with ``.starmap``; each Function
builds its own ``LiberoPlusEnvClient(suite, task_id)`` (a distinct
``LiberoPlusEnvBatch`` parameter set → one env container per task), and the
already-deployed VLA-JEPA worker handles inference unchanged.

``task_id`` here is the **language ordinal** (0 = first Language Instructions
variant for the suite), NOT the absolute benchmark index — the worker resolves it.

Every rollout lands on the append-only ledger (episode worlds keyed by
``ep-{suite}-t{task_id}-batch{n}`` + ``run_id``), persisted to the
``libero-eval-results`` volume under ``{run_id}:task{task_id}``.

Run the sweep (Modal auth required; no Anthropic key needed):
    modal run bench/libero/libero_plus_sweep.py

    # small baseline: 5 language tasks x 3 seeds, max 300 steps
    modal run bench/libero/libero_plus_sweep.py --suite libero_spatial \\
        --n-tasks 5 --trials 3 --max-steps 300 \\
        --run-id liberoplus-language-baseline

Reuses the pure split/aggregate helpers from ``baseline_sweep.py`` so the split
JSON has the identical shape; it is written to
``bench/libero/out/<suite>_language_split.json`` and printed.
"""

import sys
from pathlib import Path
from typing import Any

# bench/libero is a directory of loose scripts; reuse the proven split/aggregate
# helpers from the standard sweep rather than copying them.
sys.path.insert(0, str(Path(__file__).parent))

ROOT = "/repo"
RESULTS_DIR = "/results"


try:
    import modal  # type: ignore[import-not-found]

    _HAS_MODAL = True
except ImportError:  # pragma: no cover - exercised only in the CI test venv
    modal = None  # type: ignore[assignment]
    _HAS_MODAL = False

_RESULTS_VOLUME: Any = None


def _run_one_language_task(
    suite: str,
    task_id: int,
    trials: int,
    max_steps: int,
    run_id: str,
) -> dict[str, Any]:
    """Run one LANGUAGE variant's rollouts in-region; return its per-task summary.

    Drives the real VLA-JEPA policy against the LIBERO-Plus LANGUAGE env with the
    PERTURBED instruction (the honest baseline). ``task_id`` is the language
    ordinal. Returns the same dict shape ``baseline_sweep._run_one_task`` does
    (task_id, n, successes, success_rate, per_seed, wall_s, ledger_path) plus the
    perturbed ``instruction`` string actually used.
    """
    import asyncio
    import shutil
    import time
    from pathlib import Path as _Path

    sys.path.insert(0, f"{ROOT}/bench/libero")

    from eval_driver import (  # type: ignore[import-not-found]
        DriverConfig,
        run_eval,
    )

    task_run_id = f"{run_id}:task{task_id}"
    local_out = _Path("/tmp") / f"lpsweep-{task_run_id}"
    local_out.mkdir(parents=True, exist_ok=True)

    config = DriverConfig(
        suite=suite,
        task_ids=[task_id],
        trials=trials,
        max_steps=max_steps,
        out=str(local_out),
        run_id=task_run_id,
        # The only difference from baseline_sweep: the LIBERO-Plus LANGUAGE env.
        env_client_type="modal_plus",
        use_policy=True,
        # No overrides → each task conditions on its PERTURBED task_language()
        # (the honest LIBERO-Plus baseline).
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

    # Persist this task's ledger to the network volume so rollouts stay queryable.
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


if _HAS_MODAL:
    from baseline_sweep import aggregate_and_write  # type: ignore[import-not-found]

    # py3.12 + archetype only — the env (LIBERO-Plus) and policy (VLA-JEPA) live
    # in their own deployed apps; this Function talks to them over the client
    # boundary. Same lean recipe as baseline_sweep.py.
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
                "**/*.log",
                "**/out/**",
            ],
        )
        .run_commands(
            f"cd {ROOT} && uv pip install --system -e . && uv pip install --system pillow numpy"
        )
    )

    app = modal.App("archetype-libero-plus-baseline-sweep", image=image)
    _RESULTS_VOLUME = modal.Volume.from_name("libero-eval-results", create_if_missing=True)

    @app.function(volumes={RESULTS_DIR: _RESULTS_VOLUME}, timeout=21600)
    def sweep_one_language_task(
        suite: str,
        task_id: int,
        trials: int,
        max_steps: int,
        run_id: str,
    ) -> dict[str, Any]:
        """Modal wrapper over ``_run_one_language_task`` (one env container/task).

        Catches its own exceptions so one failing task cannot abort the whole
        fan-out. A failed task is reported as ``error`` with ``n=0`` and dropped
        from the split.
        """
        import traceback

        try:
            return _run_one_language_task(suite, task_id, trials, max_steps, run_id)
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
        n_tasks: int = 5,
        task_ids: str = "",
        trials: int = 3,
        max_steps: int = 300,
        val_size: int = 2,
        test_size: int = 1,
        run_id: str = "",
    ):
        """Fan the LANGUAGE baseline sweep across language ordinals; write the split.

        Defaults to a SMALL baseline: the first 5 Language Instructions variants
        of ``libero_spatial``, 3 seeds each, max 300 steps — enough to produce a
        real measured success number near the published ≈85.4%. ``task_ids``
        (comma-separated language ordinals) overrides ``n_tasks``.

        The split JSON is written to
        ``bench/libero/out/<suite>_language_split.json`` and printed.
        """
        import time

        run_id = run_id or f"liberoplus-language-baseline-{suite}-{int(time.time())}"
        if task_ids:
            task_ids_list = [int(x) for x in str(task_ids).split(",") if x != ""]
        else:
            task_ids_list = list(range(n_tasks))

        print(
            f"=== LIBERO-Plus LANGUAGE baseline sweep === suite={suite} "
            f"language_ordinals={task_ids_list} trials={trials} "
            f"max_steps={max_steps} run_id={run_id}"
        )

        args = [(suite, t, trials, max_steps, run_id) for t in task_ids_list]
        per_task: list[dict[str, Any]] = list(sweep_one_language_task.starmap(args))

        # Reuse the standard aggregate/split writer, then re-key the output file
        # to a language-specific name so it never clobbers the standard split.
        split = aggregate_and_write(
            per_task,
            suite=suite,
            run_id=run_id,
            trials=trials,
            max_steps=max_steps,
            n_tasks=len(task_ids_list),
            val_size=val_size,
            test_size=test_size,
        )

        import json

        out_dir = Path(__file__).parent / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        lang_path = out_dir / f"{suite}_language_split.json"
        split["dimension"] = "Language Instructions"
        lang_path.write_text(json.dumps(split, indent=2) + "\n")
        print(f"\nlanguage split written to {lang_path}")
