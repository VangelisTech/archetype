# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Action provenance contract for the policy-in-the-loop tick.

With PolicyActionProcessor (priority 1) ahead of EnvStepProcessor
(priority 10), every ledger row t >= 1 must satisfy, with strict float
equality:

    action_t == pi(obs_{t-1})        (the policy saw the previous row)
    obs_t    == env.step(action_t)   (the env consumed this row's action)

and the tick-0 row keeps its spawn action untouched. Together with the
Stage 2 contract this makes the ledger a complete, replayable account of
who did what to whom on every tick.
"""

import pytest

from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.manipulation import (
    ACTION_DIM,
    EnvStepProcessor,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
)
from archetype.experiments.policy import PolicyActionProcessor, ScriptedReachPolicy
from tests.conftest import make_world_service

SIG = (ManipAction, ManipProprio, ManipStatus, ManipTask)

TARGETS = {0: (0.15, 0.0, 0.5), 1: (-0.1, 0.05, 0.5)}
TICKS = 8
SPAWN_ACTION = [0.0] * ACTION_DIM


@pytest.mark.asyncio
async def test_ledger_rows_satisfy_action_provenance(tmp_path):
    env = ScriptedReachEnv(targets=TARGETS, tolerance=0.02)
    policy = ScriptedReachPolicy(targets=TARGETS)

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="policy")
        system = AsyncSystem()
        await system.add_processor(PolicyActionProcessor(policy))
        await system.add_processor(EnvStepProcessor(env))
        world = await ws.create_world(
            WorldConfig(name="policy-loop"), storage_config=storage, system=system
        )

        eids = {}
        for env_key in TARGETS:
            obs = env.reset(env_key, seed=env_key)
            eids[env_key] = await world.create_entity(
                [
                    ManipProprio(
                        eef_pos=obs["eef_pos"],
                        eef_quat=obs["eef_quat"],
                        gripper=obs["gripper"],
                    ),
                    ManipAction(values=list(SPAWN_ACTION)),
                    ManipStatus(),
                    ManipTask(
                        suite="scripted",
                        task_id=0,
                        instruction="reach",
                        seed=env_key,
                        env_key=env_key,
                    ),
                ]
            )

        await world.run(RunConfig(num_steps=TICKS))

        history = {}
        for tick in range(TICKS):
            rows = (await world.query_archetype(sig=SIG, ticks=[tick])).to_pylist()
            history[tick] = {r["entity_id"]: r for r in rows}

        replay_policy = ScriptedReachPolicy(targets=TARGETS)
        replay_env = ScriptedReachEnv(targets=TARGETS, tolerance=0.02)
        for env_key in TARGETS:
            replay_env.reset(env_key, seed=env_key)

        for env_key, eid in eids.items():
            assert history[0][eid]["manipaction__values"] == SPAWN_ACTION, (
                "tick-0 row must keep the spawn action untouched"
            )
            for tick in range(1, TICKS):
                prev = history[tick - 1][eid]
                row = history[tick][eid]
                if prev["manipstatus__done"]:
                    # Frozen rows: action and obs persist unchanged.
                    assert row["manipaction__values"] == prev["manipaction__values"]
                    assert row["manipproprio__eef_pos"] == prev["manipproprio__eef_pos"]
                    continue

                (want_action,) = replay_policy.act(
                    [env_key],
                    ["reach"],
                    [
                        {
                            "eef_pos": prev["manipproprio__eef_pos"],
                            "eef_quat": prev["manipproprio__eef_quat"],
                            "gripper": prev["manipproprio__gripper"],
                        }
                    ],
                )
                assert row["manipaction__values"] == want_action, (
                    f"env {env_key} tick {tick}: action_t != pi(obs_t-1)"
                )

                (want_obs,) = replay_env.step([env_key], [want_action])
                assert row["manipproprio__eef_pos"] == want_obs["eef_pos"], (
                    f"env {env_key} tick {tick}: obs_t != env.step(action_t)"
                )

        # The proportional controller actually reaches both targets.
        final = history[TICKS - 1]
        assert all(final[eid]["manipstatus__success"] for eid in eids.values()), (
            f"both envs should succeed within {TICKS} ticks"
        )
    finally:
        await ws.shutdown()
