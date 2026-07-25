# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ManipFrameRef ledger carriage contract tests (CI-safe, no Modal).

Verifies the two normative guarantees from the spec:

1. Tick-0 refs are the refs returned by ``env.reset()`` — they land raw on
   the spawn row (initial-conditions contract).
2. Tick t refs are the refs returned by ``env.step()`` at step t — written by
   the framed environment-step processor each tick (step-ref carriage contract).

Uses ``ScriptedFramedReachEnv`` which emits deterministic fake refs so the
test runs entirely in-process without a Modal volume or LIBERO installation.
"""

import pytest

from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.physical_ai.manipulation import (
    ACTION_DIM,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedFramedReachEnv,
    _FramedEnvStepProcessor,
)
from tests.conftest import make_world_harness

# Canonical signature: all five components sorted by class name.
SIG = (ManipAction, ManipFrameRef, ManipProprio, ManipStatus, ManipTask)


async def _spawn_framed_episode(
    world,
    client: ScriptedFramedReachEnv,
    env_key: int,
    seed: int,
    action: list[float],
) -> int:
    """Reset-then-spawn: the env's reset obs (including refs) become the raw tick-0 row."""
    obs = client.reset(env_key, seed)
    return await world.create_entity(
        [
            ManipProprio(
                eef_pos=obs["eef_pos"],
                eef_quat=obs["eef_quat"],
                gripper=obs["gripper"],
                gripper_qpos=obs.get("gripper_qpos", [0.0, 0.0]),
            ),
            ManipAction(values=action),
            ManipStatus(),
            ManipTask(
                suite="scripted",
                task_id=0,
                instruction="reach",
                seed=seed,
                env_key=env_key,
            ),
            ManipFrameRef(
                agentview_ref=obs["agentview_ref"],
                wrist_ref=obs["wrist_ref"],
            ),
        ]
    )


async def _rows_at(world, tick: int) -> dict[int, dict]:
    rows = (await world.query_archetype(sig=SIG, ticks=[tick])).to_pylist()
    return {r["entity_id"]: r for r in rows}


@pytest.mark.asyncio
async def test_reset_refs_raw_at_tick0_and_step_refs_carried(tmp_path):
    """Tick-0 refs are reset refs raw; subsequent ticks carry step refs."""
    client = ScriptedFramedReachEnv(
        targets={0: (1.0, 0.0, 0.5), 1: (0.0, 1.0, 0.5)}, tolerance=0.05
    )

    action_for = {
        0: [0.3, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3),
        1: [0.0, 0.2, 0.0] + [0.0] * (ACTION_DIM - 3),
    }

    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="frame_ref")
        system = AsyncSystem()
        await system.add_processor(_FramedEnvStepProcessor(client))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="frame-ref-test"), storage_config=storage, system=system
        )

        # Capture reset refs before spawning (the ScriptedFramedReachEnv already
        # emitted them during the reset call above; we re-read via a reference env).
        reference = ScriptedFramedReachEnv(
            targets={0: (1.0, 0.0, 0.5), 1: (0.0, 1.0, 0.5)}, tolerance=0.05
        )

        eids: dict[int, int] = {}
        reset_refs: dict[int, dict] = {}
        for env_key in (0, 1):
            seed = env_key + 7
            # reference env reset to capture expected refs
            ref_obs = reference.reset(env_key, seed)
            reset_refs[env_key] = {
                "agentview_ref": ref_obs["agentview_ref"],
                "wrist_ref": ref_obs["wrist_ref"],
            }
            eids[env_key] = await _spawn_framed_episode(
                world, client, env_key=env_key, seed=seed, action=action_for[env_key]
            )

        ticks = 3
        await world.run(RunConfig(num_steps=ticks))

        # --- Tick-0 contract: reset refs raw, initial-conditions invariant ---
        tick0 = await _rows_at(world, 0)
        for env_key in (0, 1):
            row = tick0[eids[env_key]]
            assert row["manipframeref__agentview_ref"] == reset_refs[env_key]["agentview_ref"], (
                f"env {env_key}: tick-0 agentview_ref must be the raw reset ref"
            )
            assert row["manipframeref__wrist_ref"] == reset_refs[env_key]["wrist_ref"], (
                f"env {env_key}: tick-0 wrist_ref must be the raw reset ref"
            )

        # --- Ticks 1..N contract: step refs from the framed processor ---
        # Build expected refs by replaying the reference env steps.
        expected_step_refs: dict[int, list[dict]] = {0: [], 1: []}
        for _ in range(ticks - 1):
            step_results = reference.step([0, 1], [action_for[0], action_for[1]])
            for env_key, obs in zip((0, 1), step_results, strict=True):
                expected_step_refs[env_key].append(
                    {
                        "agentview_ref": obs["agentview_ref"],
                        "wrist_ref": obs["wrist_ref"],
                    }
                )

        for t in range(1, ticks):
            rows_t = await _rows_at(world, t)
            for env_key in (0, 1):
                row = rows_t[eids[env_key]]
                want = expected_step_refs[env_key][t - 1]
                assert row["manipframeref__agentview_ref"] == want["agentview_ref"], (
                    f"env {env_key} tick {t}: agentview_ref mismatch"
                )
                assert row["manipframeref__wrist_ref"] == want["wrist_ref"], (
                    f"env {env_key} tick {t}: wrist_ref mismatch"
                )

        # --- Refs are non-empty strings ---
        for t in range(ticks):
            rows_t = await _rows_at(world, t)
            for env_key in (0, 1):
                row = rows_t[eids[env_key]]
                assert row["manipframeref__agentview_ref"], (
                    f"env {env_key} tick {t}: agentview_ref must be non-empty"
                )
                assert row["manipframeref__wrist_ref"], (
                    f"env {env_key} tick {t}: wrist_ref must be non-empty"
                )
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_gripper_qpos_carried_end_to_end(tmp_path):
    """ManipProprio.gripper_qpos is populated from the env obs at every tick."""
    client = ScriptedFramedReachEnv(targets={0: (1.0, 0.0, 0.5)}, tolerance=0.05)
    action = [0.3, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3)

    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="gripper_qpos")
        system = AsyncSystem()
        await system.add_processor(_FramedEnvStepProcessor(client))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="gripper-qpos-test"), storage_config=storage, system=system
        )

        eid = await _spawn_framed_episode(world, client, env_key=0, seed=0, action=action)
        await world.run(RunConfig(num_steps=3))

        for tick in range(3):
            rows = await _rows_at(world, tick)
            row = rows[eid]
            qpos = row["manipproprio__gripper_qpos"]
            assert isinstance(qpos, list) and len(qpos) == 2, (
                f"tick {tick}: gripper_qpos must be a 2-element list, got {qpos!r}"
            )
            # ScriptedFramedReachEnv always returns [0.0, 0.0].
            assert qpos == [0.0, 0.0], f"tick {tick}: expected [0.0, 0.0], got {qpos!r}"
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_done_rows_freeze_refs(tmp_path):
    """When a row goes done, refs freeze (the env is not stepped past done)."""
    client = ScriptedFramedReachEnv(targets={0: (0.1, 0.0, 0.5)}, tolerance=0.06)
    action = [0.05, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3)

    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="done_freeze_refs")
        system = AsyncSystem()
        await system.add_processor(_FramedEnvStepProcessor(client))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="done-refs-test"), storage_config=storage, system=system
        )

        eid = await _spawn_framed_episode(world, client, env_key=0, seed=0, action=action)
        await world.run(RunConfig(num_steps=5))

        history: dict[int, dict] = {}
        for tick in range(5):
            rows = await _rows_at(world, tick)
            history[tick] = rows[eid]

        # Done at tick 1 (pos reaches 0.05, dist < 0.06).
        assert history[1]["manipstatus__done"] is True

        # Refs must be the same from tick 1 onward (frozen with the rest of the row).
        terminal_av = history[1]["manipframeref__agentview_ref"]
        terminal_wr = history[1]["manipframeref__wrist_ref"]
        for tick in (2, 3, 4):
            assert history[tick]["manipframeref__agentview_ref"] == terminal_av, (
                f"tick {tick}: agentview_ref should be frozen after done"
            )
            assert history[tick]["manipframeref__wrist_ref"] == terminal_wr, (
                f"tick {tick}: wrist_ref should be frozen after done"
            )
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_despawned_framed_episode_never_steps_external_env(tmp_path):
    client = ScriptedFramedReachEnv(targets={0: (1.0, 0.0, 0.5)}, tolerance=0.05)
    action = [0.3, 0.0, 0.0] + [0.0] * (ACTION_DIM - 3)

    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="framed_despawn")
        system = AsyncSystem()
        await system.add_processor(_FramedEnvStepProcessor(client))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="framed-despawn"), storage_config=storage, system=system
        )
        entity_id = await _spawn_framed_episode(world, client, env_key=0, seed=0, action=action)
        await world.run(RunConfig(num_steps=1))
        before = list(client._state[0])

        await world.remove_entity(entity_id)
        await world.run(RunConfig(num_steps=1))

        assert client._state[0] == before
    finally:
        await ws.close()
