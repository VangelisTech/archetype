# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Introspect what LIBERO exposes as ground-truth per step.

Answers the make-or-break GEPA-scorer question: are BDDL subgoal predicates
queryable natively (per step, as True/False), or do we only get a terminal
success bool? Run: modal run bench/libero/libero_introspect.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import modal
from modal_worker import image

# Bundle modal_worker into the image so the remote container can import this
# module (which imports modal_worker at top level).
image = image.add_local_python_source("modal_worker")

app = modal.App("libero-introspect", image=image)


@app.function(timeout=600)
def introspect(suite: str = "libero_spatial", task_id: int = 0) -> dict:
    import inspect
    import os

    from libero.libero import benchmark, get_libero_path
    from libero.libero.envs import OffScreenRenderEnv

    bm = benchmark.get_benchmark_dict()[suite]()
    task = bm.get_task(task_id)
    init_states = bm.get_task_init_states(task_id)
    bddl = os.path.join(get_libero_path("bddl_files"), task.problem_folder, task.bddl_file)
    env = OffScreenRenderEnv(bddl_file_name=bddl, camera_heights=128, camera_widths=128)
    env.seed(0)
    env.reset()
    obs = env.set_init_state(init_states[0])

    e = env.env  # underlying robosuite/LIBERO domain
    report: dict = {"language": str(task.language)}
    report["obs_object_keys"] = sorted(
        k for k in obs if ("object" in k.lower() or k.endswith("_pos") or k.endswith("_quat"))
    )
    report["env_goal_attrs"] = [
        m
        for m in dir(e)
        if any(s in m.lower() for s in ["pred", "goal", "success", "eval", "object_states", "state"])
    ]

    pp = getattr(e, "parsed_problem", None)
    if pp is not None:
        report["parsed_problem_keys"] = list(pp.keys())
        report["goal_state"] = pp.get("goal_state")

    # The gold question: can we eval each goal predicate individually, per step?
    for meth in ("_check_success", "check_success"):
        fn = getattr(e, meth, None) or getattr(env, meth, None)
        if fn is not None:
            try:
                report[f"src::{meth}"] = inspect.getsource(fn)
            except Exception as exc:  # noqa: BLE001
                report[f"src::{meth}"] = f"<no source: {exc}>"
            break

    # Try to find + call a per-predicate evaluator on the current state.
    osd = getattr(e, "object_states_dict", None)
    if osd is not None:
        report["object_states_dict_keys"] = list(osd.keys())
    for cand in ("_eval_predicate", "eval_predicate"):
        if hasattr(e, cand):
            report["per_predicate_evaluator"] = cand
            # Attempt to evaluate each goal predicate at the reset state.
            try:
                gs = pp.get("goal_state") if pp else None
                if gs:
                    report["predicate_values_at_reset"] = [
                        {"predicate": p, "value": bool(getattr(e, cand)(p))} for p in gs
                    ]
            except Exception as exc:  # noqa: BLE001
                report["predicate_eval_error"] = str(exc)
            break

    return report


@app.local_entrypoint()
def main(suite: str = "libero_spatial", task_id: int = 0):
    import json

    print(json.dumps(introspect.remote(suite=suite, task_id=task_id), indent=2, default=str))
