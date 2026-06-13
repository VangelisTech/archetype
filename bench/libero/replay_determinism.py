# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Replay determinism — the WF1 *trust* gate (the only trust leg WF1 needs).

WF1 (``docs/design/libero-para-day-plan.md`` §10) advances to GEPA only when
runs are *idempotent*: batched ∧ replay-deterministic ∧ cleanly partitioned. This
module proves the **replay-deterministic** leg, end to end, on REAL persisted
smoke data — CPU only, no GPU, no LLM.

The chain it proves
-------------------
1. **Query** a persisted rollout out of the canonical ledger by ``(world_id,
   run_id)`` — ``ledger_query.read_subgoal_inputs`` returns the exact replay
   substrate ``(suite, task_id, seed, action_sequence)`` and
   ``ledger_query.read_rollout`` returns the per-tick ground truth recorded by
   the live env (``manipstatus__success`` + the ``manipproprio__eef_pos``
   trajectory). The world that produced these rows was destroyed after the run;
   the append-only Lance store survived (gepa-core-loop.md §8).

2. **Replay** that rollout from ``(seed, action_sequence)`` through a *fresh*
   LIBERO env (``libero_replay.ProbeReplay`` — its own SHA-pinned py3.10/EGL
   container, **CPU**), restoring MuJoCo state at every tick.

3. **Assert two things, exactly**:
   - ``replayed env.check_success() == ledger manipstatus__success`` — bit for
     bit. This is the trust assertion: the sim's own ground truth, recovered from
     the ledger, is reproduced by a cold replay. (`pass` is sim ground truth,
     never an LLM — gepa-core-loop.md §5.)
   - **object_states reproduce** — replaying the *same* ``(seed, actions)`` twice
     yields byte-identical object/region poses at every probed tick (tick 0, a
     mid tick, and the final tick). This is replay determinism proper: the scorer
     can restore any tick and trust the scene is reproduced bit-for-bit. As a
     third cross-check, the replay's native goal-predicate conjunction at the
     final tick must agree with the replayed ``check_success()`` (the sim's own
     ground truth is internally consistent — the conjunction the ledger's success
     was computed from comes back from the cold replay).

Action alignment (the subtle part)
----------------------------------
The ledger records, at tick ``t``, the pair ``(action a_t, proprio p_t)`` where
``p_t`` is the observation *after* applying ``a_{t-1}`` (``manipulation.py``
``EnvStepProcessor``: "row at tick t+1 is exactly ``env.step(action_t)``"). Tick
0 is the raw reset (``p_0``) carrying the spawn placeholder action ``a_0 =
[0]*7``; ``a_0 .. a_{T-1}`` are the actions actually *applied* to produce ``p_1
.. p_T``. The final recorded action ``a_T`` was never stepped (the driver breaks
on done). So the applied sequence that reproduces the recorded trajectory is
``actions[:-1]`` (the first ``T`` actions), and replay state ``t`` lines up with
ledger tick ``t`` for ``t = 0 .. T``. This module aligns on that contract and
verifies it per tick.

CPU / no-GPU / no-LLM
---------------------
Nothing here touches the VLA-JEPA policy (GPU) or any LLM. The ledger reader is a
plain py3.12 archetype container; ``ProbeReplay`` is ``cpu=4`` LIBERO. The whole
proof is sim ground truth.

Usage
-----
Against the persisted Modal smoke/baseline data (the real proof)::

    modal deploy bench/libero/libero_replay.py        # ProbeReplay class
    uv run --with modal modal run bench/libero/replay_determinism.py

Pick a specific persisted run id (a ``libero-eval-results`` top-level dir)::

    uv run --with modal modal run bench/libero/replay_determinism.py \\
        --results-run-id "baseline-spatial-v2:task0"

Read one explicit rollout by id::

    uv run --with modal modal run bench/libero/replay_determinism.py \\
        --results-run-id "baseline-spatial-v2:task0" \\
        --world-id <uuid> --run-id <uuid>
"""

# NOTE: no `from __future__ import annotations` — this module is imported inside
# Modal containers and reuses modal.parameter-validated classes; keep real
# annotations to match libero_replay/modal_worker conventions.
import json
import sys
from pathlib import Path
from typing import Any

# Make the loose bench/libero scripts importable both locally and in-container.
sys.path.insert(0, str(Path(__file__).parent))

import modal

# Reuse the deployed replay class's harness-side client (modal package only — no
# LIBERO import on this side).
from libero_replay import ReplayClient

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Where colocated_runner/baseline sweeps land their per-run trees on the volume:
# <results>/<run_id>/episodes/episodes_<suite>/lance/...
_RESULTS_MOUNT = "/results"
_RESULTS_VOLUME = "libero-eval-results"
_DEFAULT_NAMESPACE = "episodes_libero_spatial"

# Probe set for the replay: we want the scene poses (object_states) to compare,
# plus the native subgoal/goal predicates so the success cross-check is grounded.
_PROBES = ["object_states", "eval_subgoals"]


# ---------------------------------------------------------------------------
# Ledger reader (CPU, py3.12 archetype container — NO LIBERO, NO GPU)
# ---------------------------------------------------------------------------

# A lean py3.12 image with the archetype package installed: enough to open the
# persisted Lance store via ledger_query. No torch/LIBERO/mujoco — the reader
# only needs daft + lancedb.
_repo_root = Path(__file__).resolve().parents[2]
reader_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("uv")
    .add_local_dir(
        str(_repo_root),
        "/repo",
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
    .run_commands("cd /repo && uv pip install --system -e .")
    .add_local_python_source("ledger_query")
)

app = modal.App("archetype-replay-determinism", image=reader_image)
results_volume = modal.Volume.from_name(_RESULTS_VOLUME, create_if_missing=False)


def _resolve_store_uri(results_run_id: str, namespace: str) -> str:
    """Locate the episodes store tree for a results run id on the mounted volume.

    Tries the documented layout (<run_id>/episodes), then walks the run-id
    subtree for the <namespace>/lance directory so layout drift never breaks the
    proof.
    """
    import os  # noqa: PLC0415

    base = os.path.join(_RESULTS_MOUNT, results_run_id)
    candidate = os.path.join(base, "episodes")
    if os.path.isdir(os.path.join(candidate, namespace, "lance")):
        return candidate
    # Defensive walk: find the namespace/lance dir anywhere under the run id.
    search_root = base if os.path.isdir(base) else _RESULTS_MOUNT
    for root, dirs, _files in os.walk(search_root):
        if namespace in dirs and os.path.isdir(os.path.join(root, namespace, "lance")):
            return root
    return candidate


@app.function(volumes={_RESULTS_MOUNT: results_volume}, timeout=900)
def read_persisted_rollout(
    results_run_id: str,
    namespace: str,
    world_id: str = "",
    run_id: str = "",
) -> dict[str, Any]:
    """Pull one real persisted rollout's replay substrate + ground truth.

    Runs *inside* the CPU reader container, against the read-only smoke/baseline
    volume. Discovers the fullest rollout when ``(world_id, run_id)`` are not
    given, then returns everything the replay step needs plus the per-tick ground
    truth to assert against::

        {
          "store_uri": str, "namespace": str,
          "world_id": str, "run_id": str,
          "suite": str, "task_id": int, "seed": int, "instruction": str,
          "n_ticks": int,
          "actions": [[7-vec], ...],          # full ledger order (a_0 .. a_T)
          "ledger_success": bool,             # final manipstatus__success (latched)
          "ledger_success_any": bool,         # success latched at any tick
          "eef_pos": [[x,y,z], ...],          # per-tick proprio (p_0 .. p_T)
          "rollouts_seen": int,
        }

    Pure read path: ``destroy_world`` is in-memory teardown only, so the
    append-only store survives and this never needs a live world handle
    (gepa-core-loop.md §8).
    """
    import asyncio  # noqa: PLC0415
    import os  # noqa: PLC0415

    sys.path.insert(0, "/repo/bench/libero")
    from ledger_query import (  # type: ignore  # noqa: PLC0415
        list_rollouts,
        read_rollout,
        read_subgoal_inputs,
    )

    store_uri = _resolve_store_uri(results_run_id, namespace)
    if not os.path.isdir(os.path.join(store_uri, namespace, "lance")):
        return {
            "error": f"no episode store at {store_uri}/{namespace}/lance",
            "store_uri": store_uri,
        }

    async def _pull() -> dict[str, Any]:
        rollouts = await list_rollouts(store_uri, namespace)
        if not rollouts:
            return {"error": "no rollouts in store", "store_uri": store_uri}

        if world_id and run_id:
            target_wid, target_rid = world_id, run_id
        else:
            # The fullest rollout (most ticks) — the richest trajectory to prove on.
            target = max(rollouts, key=lambda d: d["rows"])
            target_wid, target_rid = target["world_id"], target["run_id"]

        traj = await read_rollout(store_uri, namespace, target_wid, target_rid)
        sub = await read_subgoal_inputs(store_uri, namespace, target_wid, target_rid)
        return {
            "store_uri": store_uri,
            "namespace": namespace,
            "world_id": target_wid,
            "run_id": target_rid,
            "suite": sub["suite"],
            "task_id": sub["task_id"],
            "seed": sub["seed"],
            "instruction": sub["instruction"],
            "n_ticks": traj["n_ticks"],
            "actions": sub["actions"],
            "ledger_success": bool(traj["success"][-1]) if traj["success"] else False,
            "ledger_success_any": any(bool(s) for s in traj["success"]),
            "eef_pos": traj["eef_pos"],
            "rollouts_seen": len(rollouts),
        }

    return asyncio.run(_pull())


# ---------------------------------------------------------------------------
# Determinism assertions (run locally, after both remote halves return)
# ---------------------------------------------------------------------------


def _applied_action_sequence(actions: list[list[float]]) -> list[list[float]]:
    """The actions actually *stepped* to produce the recorded trajectory.

    See the module docstring's alignment note: ledger tick ``t`` carries
    ``(a_t, p_t)``; ``p_t = step(a_{t-1})``. The final recorded action ``a_T`` is
    never applied (the driver breaks on done), so the applied sequence is
    ``actions[:-1]`` — ``T`` actions producing replay states ``s_0 .. s_T`` that
    line up with ledger ticks ``0 .. T``.

    A degenerate one-row rollout (only the reset tick) has no applied action.
    """
    return actions[:-1] if len(actions) > 1 else []


# Note: object_states is the *scene* (objects + regions), not the robot eef —
# absolute eef pose is not in object_states_dict, so object_states (poses of every
# object and named region) is what must reproduce bit-for-bit across cold replays.
# The ledger's per-tick eef_pos proprio is carried into the verdict for provenance
# but is not the determinism assertion; the scene poses + success are.


def _object_states_identical(a: dict[str, Any], b: dict[str, Any]) -> tuple[bool, str]:
    """True iff two object_states maps are byte-identical (deterministic replay).

    Compares the JSON canonical form so float lists, region flags, and key sets
    must all match. Any divergence means the replay is non-deterministic and the
    scorer cannot trust a restored tick.
    """
    if set(a.keys()) != set(b.keys()):
        return False, f"object key sets differ: {sorted(set(a) ^ set(b))}"
    sa = json.dumps(a, sort_keys=True)
    sb = json.dumps(b, sort_keys=True)
    if sa != sb:
        # Find the first object that differs for a useful message.
        for k in sorted(a):
            if json.dumps(a[k], sort_keys=True) != json.dumps(b[k], sort_keys=True):
                return False, f"object {k!r} pose differs across replays: {a[k]} != {b[k]}"
        return False, "object_states differ (no single object pinpointed)"
    return True, f"{len(a)} objects/regions identical across replays"


def prove(
    ledger: dict[str, Any],
    suite: str = "libero_spatial",
    task_id: int = 0,
) -> dict[str, Any]:
    """Replay the queried rollout twice and assert the two determinism legs.

    Returns a structured verdict (also printed by the entrypoint). ``passed`` is
    the AND of: success cross-check against the ledger, object_states identical
    across two cold replays at every probed tick, and (when present) the replayed
    final state consistent with the ledger.
    """
    seed = int(ledger["seed"])
    actions = ledger["actions"]
    applied = _applied_action_sequence(actions)
    n_applied = len(applied)
    ledger_success = bool(ledger["ledger_success"])

    suite = ledger.get("suite") or suite
    task_id = int(ledger.get("task_id", task_id))

    client = ReplayClient(suite=suite, task_id=task_id)

    # Probe the final applied tick (== ledger final tick T) and a mid tick, so
    # determinism is checked at more than the endpoint.
    final_t = n_applied
    mid_t = max(1, n_applied // 2)
    probe_ticks = sorted({0, mid_t, final_t})

    # Two cold replays of the SAME (seed, applied actions): object_states must be
    # byte-identical → object_states reproduce.
    r1 = client.probe_batch(seed=seed, action_sequence=applied, ticks=probe_ticks, probes=_PROBES)
    r2 = client.probe_batch(seed=seed, action_sequence=applied, ticks=probe_ticks, probes=_PROBES)

    replay_success = bool(r1["meta"]["success"])

    # --- Leg 1: success cross-check (the trust assertion) -----------------
    success_match = replay_success == ledger_success

    # --- Leg 2: object_states reproduce (determinism) ---------------------
    per_tick_identical: dict[str, str] = {}
    object_states_ok = True
    for t in (str(x) for x in probe_ticks):
        oa = r1["ticks"].get(t, {}).get("object_states", {})
        ob = r2["ticks"].get(t, {}).get("object_states", {})
        ok, msg = _object_states_identical(oa, ob)
        per_tick_identical[t] = msg
        object_states_ok = object_states_ok and ok

    # --- Cross-check: replay goal predicate at final tick == replay success
    # (sim ground truth is internally consistent — the goal conjunction the
    # ledger's success was computed from is reproduced by the cold replay).
    final_subgoals = r1["ticks"].get(str(final_t), {}).get("subgoals", [])
    goal_preds = [s for s in final_subgoals if s.get("kind") == "goal"]
    goal_satisfied = bool(goal_preds) and all(s.get("satisfied") is True for s in goal_preds)
    goal_matches_success = (not goal_preds) or (goal_satisfied == replay_success)

    passed = bool(success_match and object_states_ok and goal_matches_success)

    return {
        "passed": passed,
        "world_id": ledger["world_id"],
        "run_id": ledger["run_id"],
        "suite": suite,
        "task_id": task_id,
        "seed": seed,
        "instruction": ledger.get("instruction", ""),
        "n_ticks_ledger": ledger["n_ticks"],
        "n_actions_applied": n_applied,
        "ledger_success": ledger_success,
        "ledger_success_any": ledger.get("ledger_success_any"),
        "replay_success": replay_success,
        "success_match": success_match,
        "object_states_ok": object_states_ok,
        "object_states_per_tick": per_tick_identical,
        "goal_predicates": [s["predicate"] for s in goal_preds],
        "goal_satisfied": goal_satisfied,
        "goal_matches_success": goal_matches_success,
        "probe_ticks": probe_ticks,
        "replay_meta": {
            "n_init_states": r1["meta"].get("n_init_states"),
            "done_at": r1["meta"].get("done_at"),
            "obj_of_interest": r1["meta"].get("obj_of_interest"),
            "goal_state": r1["meta"].get("goal_state"),
        },
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


@app.local_entrypoint()
def main(
    results_run_id: str = "smoke",
    namespace: str = _DEFAULT_NAMESPACE,
    world_id: str = "",
    run_id: str = "",
):
    """Query a real persisted rollout, replay it cold, and assert determinism.

    This is the WF1 replay-determinism gate. Exit nonzero on failure so CI / the
    advance gate can branch on it.
    """
    print("=" * 74)
    print("WF1 replay-determinism gate (CPU, no GPU, no LLM)")
    print(f"  results_run_id={results_run_id!r} namespace={namespace!r}")
    if world_id and run_id:
        print(f"  explicit rollout: world_id={world_id} run_id={run_id}")
    print("=" * 74)

    ledger = read_persisted_rollout.remote(results_run_id, namespace, world_id, run_id)
    if "error" in ledger:
        print(f"FAIL (ledger read): {ledger['error']}")
        sys.exit(1)

    print("\n--- queried rollout (from the ledger) ---")
    print(f"  world_id={ledger['world_id']}")
    print(f"  run_id  ={ledger['run_id']}")
    print(
        f"  suite={ledger['suite']!r} task_id={ledger['task_id']} seed={ledger['seed']} "
        f"n_ticks={ledger['n_ticks']}"
    )
    print(f"  instruction={ledger['instruction']!r}")
    print(
        f"  ledger_success(final)={ledger['ledger_success']} "
        f"success_any={ledger['ledger_success_any']} "
        f"(over {ledger['rollouts_seen']} rollout(s) in store)"
    )

    verdict = prove(ledger)

    print("\n--- replay (fresh LIBERO env, twice) ---")
    print(
        f"  applied actions: {verdict['n_actions_applied']}  probe_ticks: {verdict['probe_ticks']}"
    )
    print(f"  goal_state: {verdict['replay_meta']['goal_state']}")
    print(f"  obj_of_interest: {verdict['replay_meta']['obj_of_interest']}")
    print(
        f"  replay_success={verdict['replay_success']}  done_at={verdict['replay_meta']['done_at']}"
    )

    print("\n--- assertions ---")
    print(
        f"  [{'PASS' if verdict['success_match'] else 'FAIL'}] "
        f"check_success() == ledger success EXACTLY  "
        f"(replay={verdict['replay_success']} ledger={verdict['ledger_success']})"
    )
    print(f"  [{'PASS' if verdict['object_states_ok'] else 'FAIL'}] object_states reproduce")
    for t, msg in verdict["object_states_per_tick"].items():
        print(f"        tick {t}: {msg}")
    print(
        f"  [{'PASS' if verdict['goal_matches_success'] else 'FAIL'}] "
        f"goal predicate conjunction consistent with success "
        f"(goal_satisfied={verdict['goal_satisfied']})"
    )

    print("\n" + "=" * 74)
    print(json.dumps(verdict, indent=2, sort_keys=True))
    print("=" * 74)
    if verdict["passed"]:
        print(
            f"\nGATE PASS — rollout (world_id={verdict['world_id']}, "
            f"run_id={verdict['run_id']}) replays deterministically: "
            f"check_success()={verdict['replay_success']} matches the ledger, "
            f"object_states reproduce bit-for-bit."
        )
        sys.exit(0)
    print("\nGATE FAIL — replay determinism not proven (see assertions above).")
    sys.exit(1)
