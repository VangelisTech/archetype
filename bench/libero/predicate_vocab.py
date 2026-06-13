# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Enumerate LIBERO's full predicate vocabulary on Modal.

The GEPA scorer constructs intermediate subgoals (`grasped(bowl)`,
`lifted(bowl)`, `in(bowl, region)`) and evaluates them via the native
`env.env._eval_predicate(pred)`. That helper is only concrete if we know
*which* predicates LIBERO actually understands, their arities, and how
`_eval_predicate` dispatches a `["name", arg, ...]` list onto a Python fn.

This probe answers that by introspecting the live registries inside the
LIBERO container (no guessing):

1. BDDL's `VALIDATE_PREDICATE_FN_DICT` (the `@register_predicate_fn`
   registry in the `bddl` package) — the base predicate language.
2. LIBERO's own predicate registry in `libero.libero.envs.predicates`
   (LIBERO subclasses/extends BDDL's predicate objects).
3. The source of `_eval_predicate` and `_check_success` to show how a
   parsed predicate list is dispatched to a predicate object/fn.
4. The arity (number of object arguments) of every predicate, read from
   its signature / class definition.
5. A live evaluation of a representative predicate of each arity at the
   reset state, proving the dispatch path works for constructed subgoals.

Run:  modal run bench/libero/predicate_vocab.py
      modal run bench/libero/predicate_vocab.py --suite libero_spatial --task-id 0
"""

# NOTE: no `from __future__ import annotations` — keep parity with the rest
# of bench/libero (modal.parameter validates real annotations elsewhere).
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import modal
from modal_worker import image

# Bundle modal_worker into the image so the remote container can import this
# module (it imports modal_worker at top level for the shared image recipe).
image = image.add_local_python_source("modal_worker")

app = modal.App("libero-predicate-vocab", image=image)


def _safe_source(obj) -> str:
    import inspect

    try:
        return inspect.getsource(obj)
    except Exception as exc:  # noqa: BLE001
        return f"<no source: {exc}>"


def _arity_of(fn_or_cls) -> int | None:
    """Best-effort object-argument count for a predicate callable/class.

    BDDL predicates are callables (or classes whose __call__ takes the
    referenced objects). We subtract `self` when present. Returns None if
    the signature can't be read (e.g. builtins).
    """
    import inspect

    target = fn_or_cls
    # If it's a class, the predicate logic lives on its __call__ (BDDL pattern).
    if inspect.isclass(fn_or_cls):
        call = inspect.getattr_static(fn_or_cls, "__call__", None)
        if call is not None:
            target = call
    try:
        sig = inspect.signature(target)
    except (TypeError, ValueError):
        return None
    params = [
        p
        for p in sig.parameters.values()
        if p.name != "self"
        and p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    return len(params)


def _enumerate_registry(reg) -> list[dict]:
    """Turn a {name: predicate_fn_or_cls} dict into a sorted vocab list."""
    import inspect

    out: list[dict] = []
    for name in sorted(reg.keys()):
        val = reg[name]
        kind = "class" if inspect.isclass(val) else type(val).__name__
        entry = {
            "name": name,
            "arity": _arity_of(val),
            "kind": kind,
            "qualname": getattr(val, "__qualname__", str(val)),
            "module": getattr(val, "__module__", "?"),
            "doc": (inspect.getdoc(val) or "").strip().splitlines()[:1],
        }
        out.append(entry)
    return out


@app.function(timeout=600)
def enumerate_vocab(suite: str = "libero_spatial", task_id: int = 0) -> dict:
    import inspect
    import os

    report: dict = {"suite": suite, "task_id": task_id}

    # ------------------------------------------------------------------ #
    # 1. BDDL base predicate registry.                                   #
    # ------------------------------------------------------------------ #
    # BDDL registers predicate fns via @register_predicate_fn into a module
    # dict. The exact attribute name has varied across versions, so search
    # the bddl package namespace for the registry dict rather than hardcode.
    bddl_registries: dict[str, list[dict]] = {}
    try:
        import importlib
        import pkgutil

        import bddl  # noqa: F401
        import bddl.activity  # noqa: F401

        candidate_modules = ["bddl", "bddl.activity"]
        # Walk the bddl package for any submodule that may hold the registry.
        try:
            for m in pkgutil.iter_modules(bddl.__path__, prefix="bddl."):
                candidate_modules.append(m.name)
        except Exception:  # noqa: BLE001
            pass

        seen_dicts: set[int] = set()
        for modname in candidate_modules:
            try:
                mod = importlib.import_module(modname)
            except Exception:  # noqa: BLE001
                continue
            for attr in dir(mod):
                if "PREDICATE" not in attr.upper():
                    continue
                val = getattr(mod, attr, None)
                if isinstance(val, dict) and val and id(val) not in seen_dicts:
                    # Heuristic: a predicate registry maps str -> callable/class.
                    sample = next(iter(val.values()))
                    if callable(sample) or inspect.isclass(sample):
                        seen_dicts.add(id(val))
                        bddl_registries[f"{modname}.{attr}"] = _enumerate_registry(val)
    except Exception as exc:  # noqa: BLE001
        report["bddl_error"] = str(exc)
    report["bddl_predicate_registries"] = bddl_registries

    # ------------------------------------------------------------------ #
    # 2. LIBERO predicate registry (LIBERO extends BDDL predicates).      #
    # ------------------------------------------------------------------ #
    libero_registries: dict[str, list[dict]] = {}
    libero_predicate_classes: list[dict] = []
    try:
        import importlib

        from libero.libero.envs import predicates as lp

        # (a) Any module-level registry dict.
        for attr in dir(lp):
            val = getattr(lp, attr, None)
            if isinstance(val, dict) and val:
                sample = next(iter(val.values()))
                if callable(sample) or inspect.isclass(sample):
                    libero_registries[f"predicates.{attr}"] = _enumerate_registry(val)

        # (b) The register decorator usually populates a dict somewhere; also
        #     enumerate every predicate *class* defined in the module so we
        #     don't miss any (class name often == predicate name lowercased).
        for attr in dir(lp):
            val = getattr(lp, attr, None)
            if inspect.isclass(val) and val.__module__.startswith("libero"):
                libero_predicate_classes.append(
                    {
                        "class": attr,
                        "arity": _arity_of(val),
                        "bases": [b.__name__ for b in val.__bases__],
                        "doc": (inspect.getdoc(val) or "").strip().splitlines()[:1],
                    }
                )

        # (c) Look for the registry function itself + its backing dict.
        for fn_name in ("register_predicate_fn", "get_predicate_fn"):
            fn = getattr(lp, fn_name, None)
            if fn is not None:
                report[f"libero::{fn_name}::src"] = _safe_source(fn)
    except Exception as exc:  # noqa: BLE001
        report["libero_predicates_error"] = str(exc)
    report["libero_predicate_registries"] = libero_registries
    report["libero_predicate_classes"] = libero_predicate_classes

    # Capture the whole predicates module source for the dispatch table — it
    # is small and authoritative for arity + semantics.
    try:
        from libero.libero.envs import predicates as lp

        report["libero_predicates_module_src"] = _safe_source(lp)
    except Exception as exc:  # noqa: BLE001
        report["libero_predicates_module_src"] = f"<no source: {exc}>"

    # ------------------------------------------------------------------ #
    # 3. Build a live env, capture _eval_predicate / _check_success.      #
    # ------------------------------------------------------------------ #
    from libero.libero import benchmark, get_libero_path
    from libero.libero.envs import OffScreenRenderEnv

    bm = benchmark.get_benchmark_dict()[suite]()
    task = bm.get_task(task_id)
    init_states = bm.get_task_init_states(task_id)
    bddl_path = os.path.join(get_libero_path("bddl_files"), task.problem_folder, task.bddl_file)
    env = OffScreenRenderEnv(bddl_file_name=bddl_path, camera_heights=128, camera_widths=128)
    env.seed(0)
    env.reset()
    env.set_init_state(init_states[0])

    e = env.env
    report["language"] = str(task.language)

    pp = getattr(e, "parsed_problem", None)
    if pp is not None:
        report["parsed_problem_keys"] = list(pp.keys())
        report["goal_state"] = pp.get("goal_state")
        report["obj_of_interest"] = pp.get("obj_of_interest")
        report["fixtures"] = pp.get("fixtures")
        report["objects"] = pp.get("objects")
        report["regions"] = pp.get("regions")

    osd = getattr(e, "object_states_dict", None)
    if osd is not None:
        report["object_states_dict_keys"] = sorted(osd.keys())

    for meth in ("_eval_predicate", "eval_predicate"):
        fn = getattr(e, meth, None)
        if fn is not None:
            report["dispatch::evaluator_name"] = meth
            report[f"src::{meth}"] = _safe_source(fn)
            break

    for meth in ("_check_success", "check_success"):
        fn = getattr(e, meth, None) or getattr(env, meth, None)
        if fn is not None:
            report[f"src::{meth}"] = _safe_source(fn)
            break

    # ------------------------------------------------------------------ #
    # 4. Prove the dispatch path: evaluate the goal predicates + a couple #
    #    constructed intermediate subgoals at the reset state.            #
    # ------------------------------------------------------------------ #
    evaluator = getattr(e, "_eval_predicate", None) or getattr(e, "eval_predicate", None)
    report["live_eval"] = {}
    if evaluator is not None:
        gs = pp.get("goal_state") if pp else None
        if gs:
            try:
                report["live_eval"]["goal_predicates_at_reset"] = [
                    {"predicate": p, "value": bool(evaluator(p))} for p in gs
                ]
            except Exception as exc:  # noqa: BLE001
                report["live_eval"]["goal_eval_error"] = str(exc)

        # Try constructed single-object subgoals for each obj_of_interest,
        # one per likely-unary predicate name, so the doc can state which
        # constructed subgoals actually dispatch (vs. raise).
        objs = (pp.get("obj_of_interest") if pp else None) or []
        unary_probe = ["open", "close", "turnon", "turnoff", "up", "upright"]
        binary_probe = ["on", "in", "under", "inside", "ontop"]
        probes: list[dict] = []
        for name in unary_probe:
            for obj in objs[:2]:
                pred = [name, obj]
                try:
                    probes.append(
                        {"predicate": pred, "value": bool(evaluator(pred)), "dispatched": True}
                    )
                except Exception as exc:  # noqa: BLE001
                    probes.append({"predicate": pred, "dispatched": False, "error": str(exc)[:160]})
        # Binary probes pairing objects of interest with the first region/fixture.
        regions = sorted(osd.keys()) if osd else []
        targets = (objs[1:2] or []) + (regions[:1] if regions else [])
        for name in binary_probe:
            if len(objs) >= 1 and targets:
                pred = [name, objs[0], targets[0]]
                try:
                    probes.append(
                        {"predicate": pred, "value": bool(evaluator(pred)), "dispatched": True}
                    )
                except Exception as exc:  # noqa: BLE001
                    probes.append({"predicate": pred, "dispatched": False, "error": str(exc)[:160]})
        report["live_eval"]["constructed_subgoal_probes"] = probes

    return report


@app.local_entrypoint()
def main(suite: str = "libero_spatial", task_id: int = 0):
    import json

    report = enumerate_vocab.remote(suite=suite, task_id=task_id)
    print(json.dumps(report, indent=2, default=str))
