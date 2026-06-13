# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Gate for bench/libero/ledger_query.py — read a rollout back from the ledger.

Generates a real persisted episode store via the eval driver's scripted path
(no Modal, no GPU), then destroys the worlds and proves the reader recovers the
full per-tick trajectory and the scorer's replay inputs from the surviving
append-only Lance store.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

# bench/libero is a directory of loose scripts (no __init__.py); import by path.
_BENCH_LIBERO = Path(__file__).resolve().parents[2] / "bench" / "libero"
if str(_BENCH_LIBERO) not in sys.path:
    sys.path.insert(0, str(_BENCH_LIBERO))

from eval_driver import DriverConfig, run_eval  # type: ignore[import-not-found]  # noqa: E402
from ledger_query import (  # type: ignore[import-not-found]  # noqa: E402
    list_rollouts,
    read_rollout,
    read_subgoal_inputs,
)

_SUITE = "libero_spatial"
_TASK_ID = 0
_TRIALS = 2
_MAX_STEPS = 10


@pytest.fixture
def episode_store(tmp_path: Path) -> tuple[str, str]:
    """Run the scripted eval driver and return (store_uri, namespace).

    The eval driver spawns one episode world per trial, runs it, and destroys
    it — exactly the "worlds destroyed, data survives" condition the reader must
    handle. Returns the StorageConfig.uri/namespace the episodes were written
    to.

    Sync fixture (drives the async driver via ``asyncio.run``) so the project's
    strict pytest-asyncio mode does not need an async-fixture plugin hook.
    """
    out = tmp_path / "eval"
    config = DriverConfig(
        suite=_SUITE,
        task_ids=[_TASK_ID],
        trials=_TRIALS,
        max_steps=_MAX_STEPS,
        out=str(out),
        run_id="pytest",
        env_client_type="scripted",
        use_policy=False,
    )
    asyncio.run(run_eval(config))
    return str(out / "episodes"), f"episodes_{_SUITE}"


@pytest.mark.asyncio
async def test_list_rollouts_finds_persisted_episodes(episode_store: tuple[str, str]) -> None:
    store_uri, namespace = episode_store
    rollouts = await list_rollouts(store_uri, namespace)
    # One (world_id, run_id) per trial; the driver destroys each world after.
    assert len(rollouts) == _TRIALS
    assert len({r["world_id"] for r in rollouts}) == _TRIALS
    for r in rollouts:
        assert r["rows"] >= 1
        assert r["world_id"] and r["run_id"]


@pytest.mark.asyncio
async def test_read_rollout_returns_per_tick_trajectory(
    episode_store: tuple[str, str],
) -> None:
    store_uri, namespace = episode_store
    rollouts = await list_rollouts(store_uri, namespace)
    target = max(rollouts, key=lambda r: r["rows"])

    traj = await read_rollout(store_uri, namespace, target["world_id"], target["run_id"])

    n = traj["n_ticks"]
    assert n == target["rows"] >= 1
    # Every advertised per-tick array is present and aligned to n_ticks.
    for key in (
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
    ):
        assert key in traj, f"missing trajectory key {key!r}"
        assert len(traj[key]) == n, f"{key!r} not aligned to n_ticks"

    # Append-only ordering: ticks strictly increasing, tick 0 first.
    assert traj["tick"] == sorted(traj["tick"])
    assert traj["tick"][0] == 0
    assert len(set(traj["tick"])) == n

    # Action sequence is a coherent control trajectory: 7-DoF per tick.
    for act in traj["action"]:
        assert isinstance(act, list) and len(act) == 7
    # Proprio eef_pos is a 3-vector per tick.
    for pos in traj["eef_pos"]:
        assert isinstance(pos, list) and len(pos) == 3

    # env_step advances with tick (it is the env's own step counter).
    assert traj["env_step"][-1] >= traj["env_step"][0]


@pytest.mark.asyncio
async def test_read_subgoal_inputs_matches_trajectory(
    episode_store: tuple[str, str],
) -> None:
    store_uri, namespace = episode_store
    rollouts = await list_rollouts(store_uri, namespace)
    target = max(rollouts, key=lambda r: r["rows"])

    traj = await read_rollout(store_uri, namespace, target["world_id"], target["run_id"])
    sub = await read_subgoal_inputs(store_uri, namespace, target["world_id"], target["run_id"])

    assert sub["suite"] == _SUITE
    assert sub["task_id"] == _TASK_ID
    # seed = task_id * 1000 + trial_idx (eval driver protocol); deterministic.
    assert sub["seed"] in {_TASK_ID * 1000 + i for i in range(_TRIALS)}
    # Replay inputs span the full recorded trajectory, one action per tick.
    assert sub["n_steps"] == traj["n_ticks"]
    assert len(sub["actions"]) == traj["n_ticks"]
    assert sub["actions"] == traj["action"]


@pytest.mark.asyncio
async def test_read_rollout_unknown_ids_returns_empty(
    episode_store: tuple[str, str],
) -> None:
    store_uri, namespace = episode_store
    traj = await read_rollout(store_uri, namespace, "no-such-world", "no-such-run")
    assert traj["n_ticks"] == 0
    assert traj["tick"] == []
    sub = await read_subgoal_inputs(store_uri, namespace, "no-such-world", "no-such-run")
    assert sub["n_steps"] == 0
    assert sub["actions"] == []
