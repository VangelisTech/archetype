# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Manipulation episode contract, against the scripted in-process env.

Mirrors the Stage 1 physics contract at the episode level, with strict
float equality throughout:

- reset observations land raw on the ledger at the spawn tick;
- the row at tick t+1 is exactly env.step(action_t);
- success latches and done rows freeze (terminal state persists, env is
  never stepped past done).
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
from tests.conftest import make_world_service

# Canonical signature order: sorted by class name.
SIG = (ManipAction, ManipProprio, ManipStatus, ManipTask)


async def _spawn_episode(world, client, env_key: int, seed: int, action: list[float]):
    """Reset-then-spawn: the env's reset obs become the raw tick-0 row."""
    obs = client.reset(env_key, seed)
    return await world.create_entity(
        [
            ManipProprio(eef_pos=obs["eef_pos"], eef_quat=obs["eef_quat"], gripper=obs["gripper"]),
            ManipAction(values=action),
            ManipStatus(),
            ManipTask(suite="scripted", task_id=0, instruction="reach", seed=seed, env_key=env_key),
        ]
    )


async def _rows_at(world, tick: int) -> dict[int, dict]:
    rows = (await world.query_archetype(sig=SIG, ticks=[tick])).to_pylist()
    return {r["entity_id"]: r for r in rows}


@pytest.mark.asyncio
async def test_reset_obs_are_raw_tick0_and_steps_match_env_exactly(tmp_path):
    client = ScriptedReachEnv(targets={0: (1.0, 0.0, 0.5), 1: (0.0, 1.0, 0.5)}, tolerance=0.05)
    reference = ScriptedReachEnv(targets={0: (1.0, 0.0, 0.5), 1: (0.0, 1.0, 0.5)}, tolerance=0.05)

    actions = {
        0: [0.3, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3),
        1: [0.0, 0.2, 0.0] + [0.0] * (ACTION_DIM - 3),
    }

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="manip")
        system = AsyncSystem()
        await system.add_processor(EnvStepProcessor(client))
        world = await ws.create_world(
            WorldConfig(name="reach"), storage_config=storage, system=system
        )

        eids = {}
        for env_key in (0, 1):
            eids[env_key] = await _spawn_episode(
                world, client, env_key=env_key, seed=env_key + 7, action=actions[env_key]
            )

        ticks = 3
        await world.run(RunConfig(num_steps=ticks))

        # Replay the same episodes against an identical scripted env.
        expected = {k: [reference.reset(k, k + 7)] for k in (0, 1)}
        for _ in range(ticks - 1):
            stepped = reference.step([0, 1], [actions[0], actions[1]])
            for env_key, obs in zip((0, 1), stepped, strict=True):
                expected[env_key].append(obs)

        for tick in range(ticks):
            rows = await _rows_at(world, tick)
            assert len(rows) == 2
            for env_key in (0, 1):
                row = rows[eids[env_key]]
                want = expected[env_key][tick]
                assert row["manipproprio__eef_pos"] == want["eef_pos"], (
                    f"env {env_key} tick {tick}: ledger obs != env obs"
                )
                if tick == 0:
                    assert row["manipstatus__reward"] == 0.0
                    assert row["manipstatus__env_step"] == 0
                else:
                    assert row["manipstatus__reward"] == want["reward"]
                    assert row["manipstatus__env_step"] == tick
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_success_latches_and_done_rows_freeze(tmp_path):
    # Target reachable in 2 steps of 0.05: start (0,0,0.5), target (0.1,0,0.5).
    client = ScriptedReachEnv(targets={0: (0.1, 0.0, 0.5)}, tolerance=0.06)
    action = [0.05, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3)

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="manip")
        system = AsyncSystem()
        await system.add_processor(EnvStepProcessor(client))
        world = await ws.create_world(
            WorldConfig(name="reach-done"), storage_config=storage, system=system
        )
        eid = await _spawn_episode(world, client, env_key=0, seed=0, action=action)

        await world.run(RunConfig(num_steps=5))

        history = {}
        for tick in range(5):
            history[tick] = (await _rows_at(world, tick))[eid]

        # Step 1: pos 0.05, dist 0.05 < 0.06 -> success at tick 1.
        assert history[0]["manipstatus__success"] is False
        assert history[1]["manipstatus__success"] is True
        assert history[1]["manipstatus__done"] is True

        # Done rows freeze: position, env_step, and success persist unchanged.
        for tick in (2, 3, 4):
            assert history[tick]["manipstatus__success"] is True
            assert history[tick]["manipstatus__done"] is True
            assert history[tick]["manipstatus__env_step"] == 1
            assert history[tick]["manipproprio__eef_pos"] == history[1]["manipproprio__eef_pos"]

        # The frozen env was never stepped past done.
        assert client._state[0] == [0.05, 0.0, 0.5]
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_despawned_episode_never_steps_external_env(tmp_path):
    client = ScriptedReachEnv(targets={0: (1.0, 0.0, 0.5)}, tolerance=0.05)
    action = [0.3, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3)

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="despawn_freeze")
        system = AsyncSystem()
        await system.add_processor(EnvStepProcessor(client))
        world = await ws.create_world(
            WorldConfig(name="despawn-freeze"), storage_config=storage, system=system
        )
        entity_id = await _spawn_episode(world, client, env_key=0, seed=0, action=action)
        await world.run(RunConfig(num_steps=1))
        before = list(client._state[0])

        await world.remove_entity(entity_id)
        await world.run(RunConfig(num_steps=1))

        assert client._state[0] == before
    finally:
        await ws.shutdown()
