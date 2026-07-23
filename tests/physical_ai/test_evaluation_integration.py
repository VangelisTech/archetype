# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood the eval service over raw manipulation trajectory rows (A1).

The LIBERO eval driver computes ``success`` / ``episode_length`` in Python
while it steps (eval_driver.py:249-252) and writes a pre-summarised
``EvalTrialResult``; the eval service never touches the raw rows. That makes
the numbers unauditable — exactly what corrupted the VLA-JEPA-on-LIBERO
paper.

This proves the opposite contract: run a scripted manipulation episode,
persist ``ManipStatus`` per tick, then have the **eval service** query those
raw rows and reproduce success-rate and per-trial episode length — matched
against an independent in-Python replay of the same scripted env (NOT the
ledger). If eval reproduces the accounting, ``EvalTrialResult`` is legacy
(roadmap E1).

It also exercises the two core fixes end-to-end:
- B1: ``run_episode`` drives target-tick-aware application steps without a
  process-wide quota reset callback.
- B2: termination is the value-based "all entities done" contract on
  ``ManipStatus.done`` — no hand-rolled per-tick done-check.
"""

from __future__ import annotations

import pytest
from daft import DataFrame

import archetype.app.gateway.auth.guard as guard
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.guard import reset_daily_tokens
from archetype.core.config import StorageConfig, WorldConfig
from archetype.physical_ai.manipulation import (
    ACTION_DIM,
    EnvStepProcessor,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
)
from archetype.world.models import EpisodeConfig
from evals.graders import exact_match, state_check
from evals.types import GraderResult

# Scripted env: point eef integrates the first three action components; success
# when within tolerance of the per-env target. seed 0 → start (0, 0, 0.5).
SEED = 0
TOL = 0.04
ACTION = [0.05, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3)
START = (0.001 * SEED, -0.001 * SEED, 0.5)


@pytest.fixture(autouse=True)
def _reset_quotas():
    # Quota state is process-local in PR-2; isolate test actors explicitly.
    guard._tick_counters.clear()
    reset_daily_tokens()
    yield
    guard._tick_counters.clear()
    reset_daily_tokens()


def _replay(target: tuple[float, float, float], max_env_steps: int) -> tuple[bool, int]:
    """Independent ground truth: replay ScriptedReachEnv.step in pure Python.

    Returns (success, env_steps_to_success). Mirrors the env exactly but never
    touches the ledger — this is the accounting eval must reproduce.
    """
    pos = list(START)
    for step in range(1, max_env_steps + 1):
        for axis in range(3):
            pos[axis] += ACTION[axis]
        dist = sum((pos[i] - target[i]) ** 2 for i in range(3)) ** 0.5
        if dist < TOL:
            return True, step
    return False, max_env_steps


async def _spawn_trial(world, client, env_key: int) -> int:
    """Reset-then-spawn: the env's reset obs becomes the raw tick-0 row."""
    obs = client.reset(env_key, SEED)
    return await world.create_entity(
        [
            ManipProprio(
                eef_pos=obs["eef_pos"],
                eef_quat=obs["eef_quat"],
                gripper=obs["gripper"],
                gripper_qpos=obs["gripper_qpos"],
            ),
            ManipAction(values=ACTION),
            ManipStatus(),
            ManipTask(suite="scripted", task_id=env_key, instruction="reach", env_key=env_key),
        ]
    )


def _final_per_entity(frame: DataFrame) -> dict[int, dict]:
    """Latest-tick ManipStatus row per entity (success latched, env_step frozen)."""
    by_eid: dict[int, dict] = {}
    for r in frame.collect().to_pylist():
        eid = r["entity_id"]
        if eid not in by_eid or r["tick"] > by_eid[eid]["tick"]:
            by_eid[eid] = r
    return by_eid


@pytest.mark.asyncio
async def test_eval_reproduces_success_and_length_from_raw_manipstatus(tmp_path):
    """All trials reach success at different steps; eval reproduces both the
    100% success rate and each trial's length, and B2 stops the episode early."""
    targets = {0: (0.10, 0.0, 0.5), 1: (0.20, 0.0, 0.5), 2: (0.30, 0.0, 0.5)}
    client = ScriptedReachEnv(targets=targets, tolerance=TOL)

    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="dogfood")
    try:
        world = await container.world_lifecycle.create_world(WorldConfig(name="dogfood"), storage)
        await world.add_processor(EnvStepProcessor(client))
        eids = {k: await _spawn_trial(world, client, k) for k in targets}

        episode = await container.application.run_episode(
            world.world_id,
            EpisodeConfig(
                max_steps=50,
                terminal_component=ManipStatus,
                terminal_field="done",
                terminal_all=True,
            ),
        )
        # B2: stopped on the data (all done), not the 50-step cap.
        assert episode.terminated is True
        assert episode.duration_steps < 50

        # Independent ground truth (no ledger).
        replay = {k: _replay(targets[k], 50) for k in targets}
        expected_success = {eids[k]: replay[k][0] for k in targets}
        expected_length = {eids[k]: replay[k][1] for k in targets}
        expected_rate = sum(s for s, _ in replay.values()) / len(replay)

        # The eval service reads the raw rows it persisted.
        df = await container.evaluation_service.query_episode(
            episode, components=[ManipStatus], storage_config=storage
        )
        final = _final_per_entity(df)

        # Direct cross-check: eval's view of the raw rows == the replay.
        assert {
            eid: bool(r["manipstatus__success"]) for eid, r in final.items()
        } == expected_success
        assert {eid: int(r["manipstatus__env_step"]) for eid, r in final.items()} == expected_length

        # And again through the grader path, the way a real eval is scored.
        def grade_rate(frame: DataFrame) -> GraderResult:
            rows = _final_per_entity(frame)
            rate = sum(1 for r in rows.values() if r["manipstatus__success"]) / len(rows)
            return exact_match(rate, expected_rate, name="success_rate")

        def grade_lengths(frame: DataFrame) -> GraderResult:
            rows = _final_per_entity(frame)
            checks = {
                f"len_e{eid}": int(rows[eid]["manipstatus__env_step"]) == length
                for eid, length in expected_length.items()
            }
            return state_check(checks, name="episode_lengths")

        results = await container.evaluation_service.run_graders(df, [grade_rate, grade_lengths])
        assert [r.passed for r in results] == [True, True]
        assert results[0].score == 1.0  # 3/3 success
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_eval_grades_a_failed_trial_and_fractional_success_rate(tmp_path):
    """One trial is unreachable; eval reproduces the failure and the 2/3 rate
    from raw rows — proving EvalTrialResult's summary is recomputable (E1)."""
    targets = {0: (0.10, 0.0, 0.5), 1: (0.20, 0.0, 0.5), 2: (5.00, 0.0, 0.5)}
    client = ScriptedReachEnv(targets=targets, tolerance=TOL)

    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="dogfood_fail")
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="dogfood-fail"), storage
        )
        await world.add_processor(EnvStepProcessor(client))
        eids = {k: await _spawn_trial(world, client, k) for k in targets}

        # terminal_all with one never-done trial → runs to max_steps.
        episode = await container.application.run_episode(
            world.world_id,
            EpisodeConfig(
                max_steps=10,
                terminal_component=ManipStatus,
                terminal_field="done",
                terminal_all=True,
            ),
        )

        replay = {k: _replay(targets[k], 10) for k in targets}
        expected_success = {eids[k]: replay[k][0] for k in targets}
        expected_rate = sum(s for s, _ in replay.values()) / len(replay)

        df = await container.evaluation_service.query_episode(
            episode, components=[ManipStatus], storage_config=storage
        )
        final = _final_per_entity(df)

        # Eval reproduces the per-trial outcome, including the failure.
        assert {
            eid: bool(r["manipstatus__success"]) for eid, r in final.items()
        } == expected_success
        assert expected_rate == pytest.approx(2 / 3)

        def grade_rate(frame: DataFrame) -> GraderResult:
            rows = _final_per_entity(frame)
            rate = sum(1 for r in rows.values() if r["manipstatus__success"]) / len(rows)
            return exact_match(rate, expected_rate, name="success_rate")

        # Length cross-check only for the trials that genuinely succeeded.
        def grade_success_lengths(frame: DataFrame) -> GraderResult:
            rows = _final_per_entity(frame)
            checks = {
                f"len_e{eids[k]}": int(rows[eids[k]]["manipstatus__env_step"]) == replay[k][1]
                for k in targets
                if replay[k][0]
            }
            return state_check(checks, name="success_lengths")

        results = await container.evaluation_service.run_graders(
            df, [grade_rate, grade_success_lengths]
        )
        assert [r.passed for r in results] == [True, True]
    finally:
        await container.shutdown()
