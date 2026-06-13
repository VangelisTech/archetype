# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ledger query for LIBERO rollouts — read a full trajectory back by id.

WS5 (see ``docs/design/libero-para-day-plan.md`` and
``docs/design/gepa-core-loop.md`` §8). The scorer and the sanity UI need to
pull a rollout's full per-tick trajectory back out of the Archetype ledger by
``(world_id, run_id)``. This module is that reader.

Why it can read destroyed worlds
---------------------------------
``destroy_world`` is in-memory teardown only — the append-only Lance store
survives (verified in code and by the smoke episode tables outliving their
worlds). So this reader never touches a live world handle; it opens the
persisted Lance store directly through the same ``AsyncLancedbStore`` reader the
runtime uses, scoped by ``(world_id, run_id)``, exactly as
``async_querier.query_archetype`` would.

Store layout
------------
An episode world spawned by ``eval_driver.run_episode`` carries the *framed*
archetype signature::

    (ManipAction, ManipFrameRef, ManipProprio, ManipStatus, ManipTask)

(sorted by class name — five components → table ``a_5c_s*``). The eval driver
writes episodes to ``StorageConfig(uri=<out>/episodes,
namespace="episodes_<suite>")``; ``AsyncLancedbStore`` then lays the table out
at ``<uri>/<namespace>/lance/<table>``. The non-framed scripted/CI path uses the
four-component signature ``(ManipAction, ManipProprio, ManipStatus, ManipTask)``
(table ``a_4c_s*``); this reader tries the framed sig first and falls back to
the non-framed sig so both paths read back.

Column naming follows ``Component.get_prefix()``: ``<classname.lower()>__<field>``
(e.g. ``manipproprio__eef_pos``, ``manipaction__values``, ``maniptask__seed``).

Lazy-eval contract
------------------
``bench/`` is exempt from ``lazy_audit.toml`` (src/ only). Even so, the lazy DAG
is kept intact until the single terminal ``to_pylist`` at the trajectory-dict
boundary — converting the queried rollout into per-tick Python arrays is the
legitimate materialization point (the scorer wants concrete arrays, not a lazy
frame).

Usage (self-test against a local Lance store)::

    uv run python bench/libero/ledger_query.py --store-uri <out>/episodes \\
        --namespace episodes_libero_spatial

Usage (verify against the Modal smoke volume ``libero-eval-results``)::

    uv run --with modal modal run bench/libero/ledger_query.py
    # or, equivalently, the CLI shim:
    uv run --with modal python bench/libero/ledger_query.py --modal-smoke
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

# Allow ``bench/libero`` helpers to be imported without package install.
sys.path.insert(0, str(Path(__file__).parent))

from archetype.core.aio.async_lancedb_store import AsyncLancedbStore  # noqa: E402
from archetype.core.archetype import Archetype  # noqa: E402
from archetype.experiments.manipulation import (  # noqa: E402
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
)

# The two archetype signatures an episode can carry. Sorted by class name —
# Archetype.get_name canonicalizes the same way, so these resolve to the
# concrete Lance table names (a_5c_s* framed, a_4c_s* non-framed).
_FRAMED_SIG = tuple(
    sorted(
        [ManipAction, ManipFrameRef, ManipProprio, ManipStatus, ManipTask],
        key=lambda t: t.__name__,
    )
)
_PLAIN_SIG = tuple(
    sorted(
        [ManipAction, ManipProprio, ManipStatus, ManipTask],
        key=lambda t: t.__name__,
    )
)

# Per-tick trajectory columns: ledger column name → output key. The frame-ref
# columns only exist on the framed signature; they are filled with "" when the
# rollout was recorded without frames.
_PROPRIO_COLS = {
    "manipproprio__eef_pos": "eef_pos",
    "manipproprio__eef_quat": "eef_quat",
    "manipproprio__gripper": "gripper",
    "manipproprio__gripper_qpos": "gripper_qpos",
}
_ACTION_COLS = {"manipaction__values": "action"}
_STATUS_COLS = {
    "manipstatus__reward": "reward",
    "manipstatus__done": "done",
    "manipstatus__success": "success",
    "manipstatus__env_step": "env_step",
}
_FRAME_COLS = {
    "manipframeref__agentview_ref": "agentview_ref",
    "manipframeref__wrist_ref": "wrist_ref",
}


async def _connect(store: AsyncLancedbStore) -> None:
    """Establish the store's lazy LanceDB connection without create-on-read.

    ``AsyncLancedbStore._ensure_table`` connects lazily, but only when given a
    signature — and it *creates* the table if absent (create-on-read), which
    would pollute the store with empty tables for signatures the rollout never
    used. We only ever want to *read* persisted data here, so connect directly
    (mirroring the store's own connect line) and let ``_list_table_names`` report
    only the tables that actually exist on disk.
    """
    if store.lancedb is not None:
        return
    import lancedb  # noqa: PLC0415

    subdir = os.environ.get("ARCT_LANCEDB_SUBDIR", "lance")
    store.lancedb = await lancedb.connect_async(os.path.join(store.uri, store.namespace, subdir))


async def _resolve_rows(
    store: AsyncLancedbStore,
    world_id: str,
    run_id: str,
) -> tuple[list[dict[str, Any]], bool]:
    """Return (rows, framed) for the given ids, trying framed sig then plain.

    Reads the *full* append-only history (``active_only=False``) so a destroyed
    world's rows are returned in full. Each tick is one row for a single-entity
    episode; multi-entity rollouts return one row per (entity, tick).

    ``rows`` is the terminal materialization of the queried DataFrame — see the
    module docstring's lazy-eval note. ``framed`` reports which signature the
    rows came from so the caller knows whether frame refs are present.
    """
    await _connect(store)
    existing = set(await store._list_table_names())
    # Framed first: that is what the live VLA eval driver writes.
    for sig, framed in ((_FRAMED_SIG, True), (_PLAIN_SIG, False)):
        table_name = Archetype.get_name(sig)
        if table_name not in existing:
            continue
        df = await store.get_archetype_df(
            sig=sig,
            world_id=world_id,
            run_id=run_id,
            active_only=False,
        )
        rows = df.sort([df["tick"], df["entity_id"]]).to_pylist()
        if rows:
            return rows, framed
    return [], False


def _column_series(rows: list[dict[str, Any]], col_map: dict[str, str], present: set[str]) -> dict:
    """Project ``rows`` into per-tick arrays for the columns in ``col_map``.

    Columns absent from the table (e.g. frame refs on a non-framed rollout) are
    emitted as empty-string arrays so the trajectory dict has a stable shape.
    """
    out: dict[str, list[Any]] = {}
    for ledger_col, out_key in col_map.items():
        if ledger_col in present:
            out[out_key] = [r.get(ledger_col) for r in rows]
        else:
            out[out_key] = ["" for _ in rows]
    return out


async def read_rollout(
    store_uri: str,
    namespace: str,
    world_id: str,
    run_id: str,
) -> dict[str, Any]:
    """Read a rollout's full per-tick trajectory back from the ledger.

    Args:
        store_uri: The storage URI passed to ``StorageConfig.uri`` when the
            episode was written (e.g. ``<out>/episodes``). The Lance store lives
            at ``<store_uri>/<namespace>/lance``.
        namespace: The ``StorageConfig.namespace`` (e.g. ``episodes_libero_spatial``).
        world_id: The episode world id (a UUID assigned at world activation).
        run_id: The episode run id (a UUID assigned at world activation).

    Returns a dict with per-tick arrays, each aligned by index to ``tick``::

        {
          "world_id": str, "run_id": str, "framed": bool, "n_ticks": int,
          "tick": [int, ...], "entity_id": [int, ...],
          "eef_pos": [[x,y,z], ...], "eef_quat": [[w,x,y,z], ...],
          "gripper": [float, ...], "gripper_qpos": [[a,b], ...],
          "action": [[7-vec], ...],        # manipaction__values
          "reward": [float, ...], "done": [bool, ...], "success": [bool, ...],
          "env_step": [int, ...],
          "agentview_ref": [str, ...], "wrist_ref": [str, ...],
        }

    Returns an empty trajectory (all arrays empty, ``n_ticks == 0``) when no
    rows match — the caller distinguishes "destroyed but persisted" (non-empty)
    from "never written" (empty).
    """
    store = AsyncLancedbStore(store_uri, namespace)
    try:
        rows, framed = await _resolve_rows(store, str(world_id), str(run_id))
    finally:
        await store.shutdown()

    present: set[str] = set(rows[0].keys()) if rows else set()

    traj: dict[str, Any] = {
        "world_id": str(world_id),
        "run_id": str(run_id),
        "framed": framed,
        "n_ticks": len(rows),
        "tick": [r["tick"] for r in rows],
        "entity_id": [r["entity_id"] for r in rows],
    }
    traj.update(_column_series(rows, _PROPRIO_COLS, present))
    traj.update(_column_series(rows, _ACTION_COLS, present))
    traj.update(_column_series(rows, _STATUS_COLS, present))
    traj.update(_column_series(rows, _FRAME_COLS, present))
    return traj


async def read_subgoal_inputs(
    store_uri: str,
    namespace: str,
    world_id: str,
    run_id: str,
) -> dict[str, Any]:
    """Read just what the scorer needs to *replay* a rollout deterministically.

    Rollouts are deterministic from ``(seed → init_state, action_sequence)``
    (core-loop §5/§8), so the replay substrate is exactly: the seed (which fixes
    the init state for the suite/task), the suite + task id, and the ordered
    action sequence.

    Returns::

        {
          "world_id": str, "run_id": str,
          "suite": str, "task_id": int, "seed": int, "env_key": int,
          "instruction": str,
          "actions": [[7-vec], ...],   # ordered by tick (manipaction__values)
          "n_steps": int,
        }

    The action at ``tick 0`` is the spawn action (``[0]*7`` — the raw
    initial-conditions row); the live actions follow. The scorer replays
    ``env.reset(seed)`` then applies ``actions`` in order. ``actions`` is the
    full ledger order including the tick-0 spawn action so replay indexing
    matches the recorded ticks one-to-one.
    """
    store = AsyncLancedbStore(store_uri, namespace)
    try:
        rows, _framed = await _resolve_rows(store, str(world_id), str(run_id))
    finally:
        await store.shutdown()

    if not rows:
        return {
            "world_id": str(world_id),
            "run_id": str(run_id),
            "suite": "",
            "task_id": 0,
            "seed": 0,
            "env_key": 0,
            "instruction": "",
            "actions": [],
            "n_steps": 0,
        }

    # Task identity is constant across ticks for one entity; read it off tick 0.
    first = rows[0]
    return {
        "world_id": str(world_id),
        "run_id": str(run_id),
        "suite": first.get("maniptask__suite", ""),
        "task_id": int(first.get("maniptask__task_id", 0)),
        "seed": int(first.get("maniptask__seed", 0)),
        "env_key": int(first.get("maniptask__env_key", 0)),
        "instruction": first.get("maniptask__instruction", ""),
        "actions": [r.get("manipaction__values") for r in rows],
        "n_steps": len(rows),
    }


async def list_rollouts(store_uri: str, namespace: str) -> list[dict[str, Any]]:
    """Enumerate the distinct ``(world_id, run_id)`` rollouts in a store.

    Discovery helper: episode ``world_id``/``run_id`` are UUIDs assigned at
    activation, so a caller querying already-persisted data needs a way to find
    them. Scans both episode signatures' tables and returns one entry per
    distinct ``(world_id, run_id)`` with its row count::

        [{"world_id": str, "run_id": str, "rows": int, "table": str}, ...]

    Sorted by descending row count (the fullest rollouts first).
    """
    store = AsyncLancedbStore(store_uri, namespace)
    counts: dict[tuple[str, str, str], int] = {}
    try:
        await _connect(store)
        table_names = set(await store._list_table_names())
        for sig in (_FRAMED_SIG, _PLAIN_SIG):
            table_name = Archetype.get_name(sig)
            if table_name not in table_names:
                continue
            async_table = await store._ensure_table(sig)
            arrow = await async_table.query().select(["world_id", "run_id"]).to_arrow()
            wids = arrow.column("world_id").to_pylist()
            rids = arrow.column("run_id").to_pylist()
            for wid, rid in zip(wids, rids, strict=True):
                counts[(str(wid), str(rid), table_name)] = (
                    counts.get((str(wid), str(rid), table_name), 0) + 1
                )
    finally:
        await store.shutdown()

    out = [
        {"world_id": wid, "run_id": rid, "rows": n, "table": table}
        for (wid, rid, table), n in counts.items()
    ]
    out.sort(key=lambda d: d["rows"], reverse=True)
    return out


# ---------------------------------------------------------------------------
# Self-test / CLI
# ---------------------------------------------------------------------------


def _trajectory_is_coherent(traj: dict[str, Any]) -> tuple[bool, str]:
    """Cheap structural checks that a trajectory is a real per-tick sequence.

    Returns (ok, message). Verifies: non-empty, ticks strictly increasing per
    entity, every per-tick array aligned to the tick count, and the action
    sequence carries 7-dim vectors.
    """
    n = traj["n_ticks"]
    if n == 0:
        return False, "empty trajectory (n_ticks == 0)"

    expected_keys = [
        "tick",
        "entity_id",
        "eef_pos",
        "eef_quat",
        "gripper",
        "gripper_qpos",
        "action",
        "reward",
        "done",
        "success",
        "env_step",
        "agentview_ref",
        "wrist_ref",
    ]
    for key in expected_keys:
        if key not in traj:
            return False, f"missing key {key!r}"
        if len(traj[key]) != n:
            return False, f"array {key!r} length {len(traj[key])} != n_ticks {n}"

    # Ticks per entity must be strictly increasing (append-only history order).
    per_entity: dict[Any, list[int]] = {}
    for eid, t in zip(traj["entity_id"], traj["tick"], strict=True):
        per_entity.setdefault(eid, []).append(t)
    for eid, ticks in per_entity.items():
        if ticks != sorted(ticks) or len(set(ticks)) != len(ticks):
            return False, f"entity {eid} ticks not strictly increasing: {ticks}"

    # Action sequence shape: each action is a 7-vector (LIBERO/OSC).
    for act in traj["action"]:
        if not isinstance(act, list) or len(act) != 7:
            return False, f"action is not a 7-vector: {act!r}"

    return True, f"coherent: {n} ticks, {len(per_entity)} entit(y/ies)"


async def verify_store(store_uri: str, namespace: str) -> dict[str, Any]:
    """Discover a rollout, read it back, and assert it is coherent.

    Returns a structured summary (also printed). Works against any persisted
    episode store — local disk or a mounted Modal volume — so the same routine
    backs both the CLI self-test and the Modal smoke verification::

        {
          "passed": bool, "message": str,
          "n_rollouts": int, "distinct_world_ids": [str, ...],
          "total_rows": int, "rollouts": [...],
          "target": {"world_id": str, "run_id": str, "n_ticks": int, ...},
        }
    """
    print(f"ledger_query verify: store_uri={store_uri!r} namespace={namespace!r}")
    rollouts = await list_rollouts(store_uri, namespace)
    if not rollouts:
        msg = (
            "no rollouts found — store_uri/namespace must point at a persisted "
            "episode store (e.g. <out>/episodes / episodes_<suite>)"
        )
        print(f"FAIL: {msg}")
        return {
            "passed": False,
            "message": msg,
            "n_rollouts": 0,
            "distinct_world_ids": [],
            "total_rows": 0,
            "rollouts": [],
            "target": None,
        }

    distinct_worlds = sorted({r["world_id"] for r in rollouts})
    total_rows = sum(r["rows"] for r in rollouts)
    print(
        f"discovered {len(rollouts)} (world_id, run_id) rollout(s); "
        f"{len(distinct_worlds)} distinct world_id(s); {total_rows} total rows"
    )
    for r in rollouts[:8]:
        print(
            f"  world_id={r['world_id']} run_id={r['run_id']} rows={r['rows']} table={r['table']}"
        )

    # Verify the fullest rollout (most ticks) — the richest trajectory.
    target = max(rollouts, key=lambda d: d["rows"])
    traj = await read_rollout(store_uri, namespace, target["world_id"], target["run_id"])
    ok, msg = _trajectory_is_coherent(traj)
    print(f"\nread_rollout({target['world_id']}, {target['run_id']}): {msg}")
    target_summary: dict[str, Any] = {
        "world_id": target["world_id"],
        "run_id": target["run_id"],
        "n_ticks": traj["n_ticks"],
        "framed": traj["framed"],
    }
    if ok:
        # Show the action sequence is a real control trajectory.
        print(f"  tick range: {traj['tick'][0]}..{traj['tick'][-1]}")
        print(f"  action[tick0]={traj['action'][0]}")
        print(f"  action[tick{traj['tick'][-1]}]={traj['action'][-1]}")
        n_distinct_actions = len({tuple(a) for a in traj["action"]})
        print(f"  distinct action vectors across ticks: {n_distinct_actions}")
        print(
            f"  final done={traj['done'][-1]} success={traj['success'][-1]} "
            f"env_step={traj['env_step'][-1]}"
        )
        if traj["framed"]:
            print(f"  agentview_ref[0]={traj['agentview_ref'][0]!r}")
        target_summary["tick_range"] = [traj["tick"][0], traj["tick"][-1]]
        target_summary["distinct_actions"] = n_distinct_actions
        target_summary["final_success"] = traj["success"][-1]

    sub = await read_subgoal_inputs(store_uri, namespace, target["world_id"], target["run_id"])
    print(
        f"\nread_subgoal_inputs: suite={sub['suite']!r} task_id={sub['task_id']} "
        f"seed={sub['seed']} env_key={sub['env_key']} n_steps={sub['n_steps']} "
        f"instruction={sub['instruction']!r}"
    )
    sub_ok = sub["n_steps"] == traj["n_ticks"] and len(sub["actions"]) == traj["n_ticks"]
    if not sub_ok:
        msg = "subgoal inputs action count does not match trajectory"
        ok = False
    target_summary["seed"] = sub["seed"]
    target_summary["suite"] = sub["suite"]
    target_summary["task_id"] = sub["task_id"]

    passed = bool(ok and sub_ok)
    if passed:
        print("\nPASS: read_rollout returned a coherent non-empty per-tick trajectory.")
    else:
        print(f"\nFAIL: {msg}")

    return {
        "passed": passed,
        "message": msg if not passed else "coherent non-empty per-tick trajectory",
        "n_rollouts": len(rollouts),
        "distinct_world_ids": distinct_worlds,
        "total_rows": total_rows,
        "rollouts": rollouts,
        "target": target_summary,
    }


async def _run_self_test(store_uri: str, namespace: str) -> int:
    """CLI wrapper around :func:`verify_store` returning a process exit code."""
    summary = await verify_store(store_uri, namespace)
    return 0 if summary["passed"] else 1


# ---------------------------------------------------------------------------
# Modal smoke verification — read the persisted smoke episodes off the volume
# ---------------------------------------------------------------------------

# The deployed env/policy apps wrote the smoke episodes here (colocated_runner
# copies the eval driver's `<out>/episodes` tree to `<results_volume>/<run_id>`).
_RESULTS_VOLUME = "libero-eval-results"
_RESULTS_MOUNT = "/results"
# colocated_runner default run_id is "smoke"; the eval driver's ep_storage is
# `<out>/episodes` with namespace `episodes_<suite>`.
_SMOKE_STORE_SUBPATH = "smoke/episodes"
_SMOKE_NAMESPACE = "episodes_libero_spatial"


def _run_modal_smoke() -> int:
    """Verify ``read_rollout`` against the persisted smoke episodes on Modal.

    Builds an ephemeral Modal app whose container has the archetype package
    installed (no LIBERO/torch — the reader only needs the Lance store + daft),
    mounts the ``libero-eval-results`` volume read-only, and runs
    :func:`verify_store` *inside* the container against
    ``/results/smoke/episodes`` (namespace ``episodes_libero_spatial``).

    ``modal`` is imported lazily: it is a separately-installed CLI tool, not a
    project dependency, so the pure-Python pytest/CLI paths never import it.
    Invoke via ``uv run --with modal python bench/libero/ledger_query.py
    --modal-smoke``.
    """
    import modal  # noqa: PLC0415

    repo_root = Path(__file__).resolve().parents[2]

    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("uv")
        .add_local_dir(
            str(repo_root),
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
        # Make this module importable inside the container by its loose path.
        .add_local_python_source("ledger_query")
    )
    app = modal.App("archetype-ledger-query-verify", image=image)
    results_volume = modal.Volume.from_name(_RESULTS_VOLUME, create_if_missing=False)

    # serialized=True: the function is defined inside _run_modal_smoke (not at
    # module global scope), so Modal must serialize it rather than import by name.
    @app.function(volumes={_RESULTS_MOUNT: results_volume}, timeout=900, serialized=True)
    def verify_smoke_remote() -> dict[str, Any]:
        import asyncio as _asyncio
        import os as _os
        import sys as _sys

        _sys.path.insert(0, "/repo/bench/libero")
        from ledger_query import verify_store as _verify_store  # type: ignore  # noqa: PLC0415

        store_uri = _os.path.join(_RESULTS_MOUNT, _SMOKE_STORE_SUBPATH)
        # Defensive: if the run_id dir layout differs, scan for the episodes tree.
        if not _os.path.isdir(_os.path.join(store_uri, _SMOKE_NAMESPACE, "lance")):
            for root, dirs, _files in _os.walk(_RESULTS_MOUNT):
                if _SMOKE_NAMESPACE in dirs and _os.path.isdir(
                    _os.path.join(root, _SMOKE_NAMESPACE, "lance")
                ):
                    store_uri = root
                    break
        return _asyncio.run(_verify_store(store_uri, _SMOKE_NAMESPACE))

    with app.run():
        summary = verify_smoke_remote.remote()

    print("\n=== Modal smoke verification summary ===")
    print(f"  passed: {summary['passed']}")
    print(f"  n_rollouts: {summary['n_rollouts']}")
    print(f"  total_rows: {summary['total_rows']}")
    print(f"  distinct_world_ids: {len(summary['distinct_world_ids'])}")
    for wid in summary["distinct_world_ids"][:8]:
        print(f"    {wid}")
    if summary.get("target"):
        print(f"  target rollout: {summary['target']}")
    return 0 if summary["passed"] else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LIBERO ledger rollout query / self-test")
    parser.add_argument(
        "--store-uri",
        default="",
        help="StorageConfig.uri the episodes were written to (e.g. <out>/episodes)",
    )
    parser.add_argument(
        "--namespace",
        default="episodes_libero_spatial",
        help="StorageConfig.namespace (default: episodes_libero_spatial)",
    )
    parser.add_argument(
        "--world-id",
        default="",
        help="Read one specific rollout (requires --run-id); otherwise discover + self-test",
    )
    parser.add_argument("--run-id", default="", help="Read one specific rollout's run_id")
    parser.add_argument(
        "--modal-smoke",
        action="store_true",
        help=(
            "Verify read_rollout against the persisted smoke episodes on the "
            "libero-eval-results Modal volume (requires `uv run --with modal`)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.modal_smoke:
        sys.exit(_run_modal_smoke())

    if not args.store_uri:
        print(
            "ERROR: --store-uri is required (the StorageConfig.uri the episodes "
            "were written to, e.g. <out>/episodes).",
            file=sys.stderr,
        )
        sys.exit(2)

    if args.world_id and args.run_id:
        traj = asyncio.run(read_rollout(args.store_uri, args.namespace, args.world_id, args.run_id))
        ok, msg = _trajectory_is_coherent(traj)
        print(f"read_rollout: {msg}")
        sys.exit(0 if ok else 1)

    sys.exit(asyncio.run(_run_self_test(args.store_uri, args.namespace)))


if __name__ == "__main__":
    main()
